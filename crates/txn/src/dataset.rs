//! Transaction path for `strata-txn`. Implements spec §3's commit protocol
//! in full, including real OCC conflict detection and an atomic
//! commit critical section (Phase 6) — see
//! `docs/superpowers/specs/2026-07-21-phase-6-concurrent-write-engine-design.md`
//! and `.claude/rules/concurrency-txn-layer.md` before editing anything
//! here. Conflict detection is write-write only, keyed by row-id, and
//! scoped to in-process concurrency (multiple threads/tasks sharing one
//! `Dataset` handle) — see the design doc §1 for why cross-process
//! visibility and read-set tracking are explicit non-goals for this slice,
//! not gaps.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(not(loom))]
use std::sync::Mutex;

use arc_swap::ArcSwap;
use arrow::array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use strata_index::{
    DeltaEntry, EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers, read_delta_log,
    write_delta_log,
};
use strata_storage::{
    DataFileEntry, Manifest, commit_manifest, compute_stats, read_current, write_batch,
};

use crate::commit_log::{CommitLog, ConflictCheck};
use crate::error::{Result, TxnError};
use crate::snapshot::Snapshot;

/// The hidden internal row-id column every committed batch carries
/// alongside its logical columns. Callers can retrieve it through the
/// public `scan`/`scan_with_predicate` API, but only under one precondition:
/// the caller's schema must list every physical column in the same order
/// data was inserted in, with `ROW_ID_COLUMN` appended last (see the CLI's
/// `handle_search`, which does exactly this). A schema that omits, reorders,
/// or partially includes columns will not retrieve row-ids correctly — as
/// of the `cast_batch_to_schema` column-count check, a mismatched column
/// count now returns a typed `TxnError::SchemaMismatch` instead of silently
/// producing wrong data, but column *order* is still the caller's
/// responsibility. `row_ids_matching` (below) sidesteps this precondition
/// entirely by reading each file's raw physical batch directly rather than
/// going through the public schema-based API. See
/// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8.
pub const ROW_ID_COLUMN: &str = "_row_id";

/// Bounded capacity of the in-memory [`CommitLog`] ring buffer — generous
/// enough that ordinary workloads never evict history still needed by an
/// in-flight transaction (which would surface as a conservative
/// `InsufficientHistory` conflict), small enough to be a trivial memory
/// cost. Not a public tunable yet, per YAGNI.
const COMMIT_LOG_CAPACITY: usize = 256;

#[derive(Clone)]
pub struct Dataset {
    dir: PathBuf,
    current: Arc<ArcSwap<Snapshot>>,
    next_row_id_counter: Arc<AtomicU64>,
    /// Monotonic counter whose sole job is generating a collision-free
    /// filename prefix for each commit *attempt*'s data/delta-log files —
    /// deliberately independent of both `next_row_id_counter` and the real
    /// manifest version. See `Transaction::commit` for why filenames must
    /// not be derived from `base_manifest.version`.
    write_attempt_counter: Arc<AtomicU64>,
    /// Serializes the conflict-check → graph-apply → manifest-commit →
    /// snapshot-swap critical section of `Transaction::commit`, and guards
    /// the recent-write-set history that check reads. This is the only
    /// lock in the crate, acquired at exactly one site, so there is no
    /// lock-ordering concern.
    commit_lock: Arc<Mutex<CommitLog>>,
}

/// The single source of truth for "where does this dataset's data live,
/// relative to its root directory" — used by `Dataset::data_dir`,
/// `Transaction::commit`, and `replay_index`, which each need it from a
/// different type/context (a `&Dataset`, a `Transaction`, and a bare
/// `&Path` respectively) and previously each hardcoded `dir.join("data")`
/// independently.
pub(crate) fn data_subdir(dir: &Path) -> PathBuf {
    dir.join("data")
}

impl Dataset {
    /// Creates a brand-new, empty dataset at `dir`. Errors if one already
    /// exists there.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::{Int64Array, RecordBatch};
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use strata_txn::Dataset;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = std::env::temp_dir()
    ///     .join(format!("strata-doctest-create-{}", std::process::id()));
    /// let dataset = Dataset::create(&dir)?;
    ///
    /// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])?;
    ///
    /// let mut txn = dataset.begin();
    /// txn.insert(batch);
    /// txn.commit()?;
    ///
    /// assert_eq!(dataset.current_version(), 1);
    /// # std::fs::remove_dir_all(&dir).ok();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::AlreadyExists`] if a dataset already exists at
    /// `dir`, or an I/O/storage error if the directory or initial manifest
    /// can't be created.
    pub fn create(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        if read_current(&dir)?.is_some() {
            return Err(TxnError::AlreadyExists(dir));
        }
        std::fs::create_dir_all(dir.join("data"))?;
        let manifest = Manifest::empty();
        commit_manifest(&dir, &manifest)?;
        let graph = new_hnsw_index(0)?;
        let next_row_id_counter = Arc::new(AtomicU64::new(manifest.next_row_id));
        let write_attempt_counter = Arc::new(AtomicU64::new(manifest.next_attempt_id));
        let snapshot = Snapshot {
            dir: dir.clone(),
            version: manifest.version,
            watermark: manifest.next_row_id.saturating_sub(1),
            manifest: Arc::new(manifest),
            graph: Arc::new(graph),
            tombstones: Arc::new(im::HashSet::new()),
        };
        Ok(Self {
            dir,
            current: Arc::new(ArcSwap::new(Arc::new(snapshot))),
            next_row_id_counter,
            write_attempt_counter,
            commit_lock: Arc::new(Mutex::new(CommitLog::new(COMMIT_LOG_CAPACITY))),
        })
    }

    /// Opens an existing dataset, recovering to the last successfully
    /// committed version. This is the crash-recovery path: `read_current`
    /// can only ever see a fully-renamed manifest (see
    /// `strata_storage::manifest`), so a process killed mid-commit leaves
    /// this returning the *previous* version, never a torn one — the Phase 1
    /// MVP checklist's kill-9 test exercises exactly this.
    ///
    /// # Examples
    ///
    /// ```
    /// use strata_txn::Dataset;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = std::env::temp_dir()
    ///     .join(format!("strata-doctest-open-{}", std::process::id()));
    /// Dataset::create(&dir)?; // must exist first — `open` errors on a missing dataset
    ///
    /// let reopened = Dataset::open(&dir)?;
    /// assert_eq!(reopened.current_version(), 0);
    /// # std::fs::remove_dir_all(&dir).ok();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::NotFound`] if no dataset exists at `dir`, or a
    /// storage error if the current manifest exists but fails to read.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        let manifest = read_current(&dir)?.ok_or_else(|| TxnError::NotFound(dir.clone()))?;
        let (graph, tombstones) = replay_index(&dir, &manifest)?;
        let next_row_id_counter = Arc::new(AtomicU64::new(manifest.next_row_id));
        // The real fix for the cross-session filename-collision bug: seed
        // from the persisted `manifest.next_attempt_id`, not 0. Without
        // this, a reopened dataset would regenerate the same
        // `{attempt_id:020}-{i}.arrow`/`.deltalog` filenames a prior
        // session already committed, and `write_batch`'s `File::create`
        // would silently truncate and destroy that prior session's
        // already-durable data. See `Manifest.next_attempt_id`'s doc
        // comment and `Transaction::commit`, which persists this counter's
        // value forward on every commit the same way it does
        // `next_row_id_counter` -> `manifest.next_row_id`.
        let write_attempt_counter = Arc::new(AtomicU64::new(manifest.next_attempt_id));
        let snapshot = Snapshot {
            dir: dir.clone(),
            version: manifest.version,
            watermark: manifest.next_row_id.saturating_sub(1),
            manifest: Arc::new(manifest),
            graph: Arc::new(graph),
            tombstones: Arc::new(tombstones),
        };
        Ok(Self {
            dir,
            current: Arc::new(ArcSwap::new(Arc::new(snapshot))),
            next_row_id_counter,
            write_attempt_counter,
            commit_lock: Arc::new(Mutex::new(CommitLog::new(COMMIT_LOG_CAPACITY))),
        })
    }

    /// Returns a cheap, immutable, point-in-time view of the dataset as of
    /// whichever version was current at the moment of this call. Holding
    /// the returned `Snapshot` never blocks a concurrent writer, and never
    /// observes any commit that lands after this call returns.
    #[must_use]
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.current.load_full()
    }

    #[must_use]
    pub fn current_version(&self) -> u64 {
        self.snapshot().version
    }

    #[must_use]
    pub fn data_dir(&self) -> PathBuf {
        data_subdir(&self.dir)
    }

    /// Data file entries (name + per-column stats) belonging to the current
    /// version. Exposed for tests that need to inspect the raw on-disk
    /// representation directly.
    #[must_use]
    pub fn data_files(&self) -> Vec<DataFileEntry> {
        self.snapshot().manifest.data_files.clone()
    }

    #[must_use]
    pub fn begin(&self) -> Transaction {
        let snapshot = self.snapshot();
        Transaction {
            dir: self.dir.clone(),
            base_manifest: snapshot.manifest.as_ref().clone(),
            graph: Arc::clone(&snapshot.graph),
            pending: Vec::new(),
            pending_tombstones: Vec::new(),
            write_set: Vec::new(),
            current: Arc::clone(&self.current),
            next_row_id_counter: Arc::clone(&self.next_row_id_counter),
            write_attempt_counter: Arc::clone(&self.write_attempt_counter),
            commit_lock: Arc::clone(&self.commit_lock),
        }
    }
}

pub struct Transaction {
    dir: PathBuf,
    base_manifest: Manifest,
    graph: Arc<HnswIndex>,
    pending: Vec<RecordBatch>,
    /// Row-ids queued for tombstoning by [`Transaction::delete`]/
    /// [`Transaction::update`], applied at commit time (see
    /// [`Transaction::commit`]) — mirrors how `pending` buffers inserts.
    pending_tombstones: Vec<u64>,
    /// Every row-id this transaction has written (via `delete`, and
    /// transitively `update`) — consulted by `commit`'s conflict check
    /// against every transaction that committed after this one began.
    write_set: Vec<u64>,
    current: Arc<ArcSwap<Snapshot>>,
    next_row_id_counter: Arc<AtomicU64>,
    write_attempt_counter: Arc<AtomicU64>,
    commit_lock: Arc<Mutex<CommitLog>>,
}

impl Transaction {
    /// # Examples
    ///
    /// Buffered rows are invisible to every reader — including this same
    /// `Dataset` — until [`Transaction::commit`] succeeds:
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::{Int64Array, RecordBatch};
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use strata_txn::Dataset;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = std::env::temp_dir()
    ///     .join(format!("strata-doctest-insert-{}", std::process::id()));
    /// let dataset = Dataset::create(&dir)?;
    /// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])?;
    ///
    /// let mut txn = dataset.begin();
    /// txn.insert(batch);
    /// assert_eq!(dataset.current_version(), 0, "not visible until commit");
    /// txn.commit()?;
    /// assert_eq!(dataset.current_version(), 1);
    /// # std::fs::remove_dir_all(&dir).ok();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Buffers a batch of rows for this transaction. Nothing is visible to
    /// any other reader — including a fresh `Dataset::open` in another
    /// process — until [`Transaction::commit`] succeeds. See spec §2.
    pub fn insert(&mut self, batch: RecordBatch) {
        self.pending.push(batch);
    }

    /// Tombstones `row_id`, making it invisible to every snapshot taken
    /// after this transaction commits — see spec §2. Buffered, not
    /// applied until [`Transaction::commit`] succeeds, same as
    /// [`Transaction::insert`].
    pub fn delete(&mut self, row_id: u64) {
        self.pending_tombstones.push(row_id);
        self.write_set.push(row_id);
    }

    /// Tombstones `row_id` and inserts `batch` as its replacement, within
    /// the same transaction — commits atomically as one unit. Equivalent
    /// to calling [`Transaction::delete`] then [`Transaction::insert`],
    /// provided as one call because that's the common case and keeps the
    /// write-set bookkeeping (used by conflict detection) obviously
    /// correct at the call site rather than relying on the caller to
    /// remember both.
    pub fn update(&mut self, row_id: u64, batch: RecordBatch) {
        self.delete(row_id);
        self.insert(batch);
    }

    /// Commits per spec §3's write/durability steps (3-5), with Phase 6's
    /// real conflict check (§3.1/§3.2) in front of them. Data files are
    /// written outside any lock (they are unique to this transaction);
    /// then, inside `Dataset.commit_lock`, the *latest* committed snapshot
    /// is re-read (not this transaction's stale `begin()`-time view),
    /// `CommitLog::conflicts_with` checks every version that landed in
    /// between against this transaction's write-set, and only if clean are
    /// this commit's own new delta entries applied to the shared,
    /// ever-growing `HnswIndex` graph (no full historical replay — see
    /// `docs/superpowers/specs/2026-07-17-phase-5-mvcc-snapshot-isolation-design.md`).
    /// A conflicting transaction leaves the graph completely untouched.
    /// The new manifest and tombstone set are layered on top of the latest
    /// snapshot's state, so a clean commit composes with whatever else
    /// committed after this transaction began. Only after
    /// `commit_manifest` succeeds is the new `Snapshot` swapped in. Any
    /// `Dataset` handle sharing this same `ArcSwap` (including the one
    /// this transaction was created from) observes the new state on its
    /// next [`Dataset::snapshot`] call; nothing is mutated in place.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::{Int64Array, RecordBatch};
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use strata_txn::Dataset;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = std::env::temp_dir()
    ///     .join(format!("strata-doctest-commit-{}", std::process::id()));
    /// let dataset = Dataset::create(&dir)?;
    /// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])?;
    ///
    /// let mut txn = dataset.begin();
    /// txn.insert(batch);
    /// txn.commit()?; // durable and visible to every reader from this point on
    ///
    /// assert_eq!(dataset.data_files().len(), 1);
    /// # std::fs::remove_dir_all(&dir).ok();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::Conflict`] — naming every contested row-id — if
    /// another transaction that committed after this one began wrote any
    /// row in this transaction's write-set, or (conservatively, with this
    /// transaction's entire write-set as the contested rows) if the
    /// bounded in-memory commit log has already evicted history needed to
    /// prove cleanliness. A conflicting transaction applies none of its
    /// deltas to the shared graph and leaves the manifest unadvanced.
    ///
    /// Returns [`TxnError::NonFiniteVectorComponent`] if any pending batch's
    /// vector column contains a `NaN`/`Infinity` component — checked, and
    /// rejected, before any file for that batch is written to disk. Also
    /// returns an error if any pending batch fails to dictionary-encode, if
    /// applying this commit's new deltas to the graph fails (e.g. a
    /// dimension mismatch), or if the manifest commit's atomic rename
    /// fails. Delta-application runs before the manifest commit, so any of
    /// these failures leaves the manifest unadvanced — the new data/delta-log
    /// files are orphaned on disk but never made visible.
    ///
    /// **Formerly a known limitation, now closed:** earlier, a commit whose
    /// pending batches had inconsistent vector dimensions across batches
    /// could partially mutate the shared graph before failing — `Insert`
    /// deltas were applied to the graph in pending-batch order, so a later
    /// batch's dimension mismatch was only caught after an earlier batch's
    /// deltas had already landed in the live, shared `HnswIndex` (which has
    /// no node-removal API to undo an insert). [`validate_delta_dimensions`]
    /// now runs before any delta is applied, rejecting the entire commit —
    /// with zero graph mutation — the moment any two pending batches (or a
    /// pending batch and the graph's already-established dimension)
    /// disagree. `HnswIndex::insert`'s only fallible path is dimension
    /// validation (the underlying `hnsw_rs` call itself never fails), so
    /// this closes the practical trigger for this class of hazard
    /// entirely, not just narrows it. A residual, more exotic concern
    /// remains out of scope here: if a future change ever gives graph
    /// mutation additional failure modes beyond dimension mismatch, this
    /// same partial-mutation risk could reopen for those. (Phase 6's
    /// conflict check did not add such a mode — it runs, and returns,
    /// strictly before the first delta is applied.)
    pub fn commit(self) -> Result<()> {
        let data_dir = data_subdir(&self.dir);
        std::fs::create_dir_all(&data_dir)?;

        // Data-file writes happen before the lock — they touch only
        // files unique to this transaction and never collide with a
        // concurrent transaction's own writes. The filename prefix comes
        // from write_attempt_counter, NOT base_manifest.version + 1: two
        // truly concurrent transactions can share the same stale
        // base_manifest.version, which would make them compute the same
        // "next version" and collide on the same filename before either
        // reaches commit_lock. write_attempt_counter is unique per
        // attempt regardless of version, so no such collision is
        // possible. See design doc §3 (data-file writes need no
        // conflict information to proceed) — this counter is what makes
        // that safe to do outside the lock at all.
        let attempt_id = self
            .write_attempt_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut new_data_files = Vec::new();
        let deltas = Self::write_pending_batches(
            &self.pending,
            &data_dir,
            attempt_id,
            &self.next_row_id_counter,
            &mut new_data_files,
        )?;
        // Fsyncing each data file's *content* (already done inside
        // write_batch) is not sufficient — the new directory entries
        // themselves must also be fsynced, or a real power-loss crash can
        // leave a file's bytes durable while the file itself is absent.
        // Must happen before the graph update/manifest commit below.
        strata_storage::sync_dir(&data_dir)?;
        validate_delta_dimensions(&deltas, &self.graph)?;

        // Everything from here is the tightly-scoped critical section:
        // re-read latest state, conflict-check, apply, commit, swap. See
        // design doc §5. This is the crate's only lock, acquired at
        // exactly this one site, so no lock-ordering concern exists; a
        // poisoned lock (a prior committer panicked) is recovered rather
        // than propagated — the CommitLog is only ever mutated by `push`
        // as the final in-memory step after a durable commit, so it can't
        // be observed half-updated.
        let mut commit_log = self
            .commit_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let latest_snapshot = self.current.load_full();
        let latest_version = latest_snapshot.version;

        // Conflict detection MUST run before any mutation of the shared
        // graph: a transaction that turns out to conflict must leave the
        // graph completely untouched.
        match commit_log.conflicts_with(self.base_manifest.version, latest_version, &self.write_set)
        {
            ConflictCheck::Clean => {}
            ConflictCheck::Conflict(contested_row_ids) => {
                return Err(TxnError::Conflict { contested_row_ids });
            }
            ConflictCheck::InsufficientHistory => {
                return Err(TxnError::Conflict {
                    contested_row_ids: self.write_set.clone(),
                });
            }
        }

        let new_version = latest_version
            .checked_add(1)
            .ok_or_else(|| TxnError::ManifestOverflow(format!("version {latest_version} + 1")))?;

        // Apply only this commit's new deltas to the shared graph — the
        // fix for the O(historical)-per-commit regression. `HnswIndex`'s
        // graph only ever grows (hnsw_rs has no node-removal API), so
        // extending the same Arc'd instance every commit is safe and
        // matches what every existing/future Snapshot's Arc<HnswIndex>
        // already points at. Tombstones layer on top of the *latest*
        // snapshot's set (not this transaction's stale begin()-time view),
        // so a clean commit composes with everything that landed in
        // between.
        let mut tombstones = latest_snapshot.tombstones.as_ref().clone();
        for delta in &deltas {
            match delta {
                DeltaEntry::Insert { row_id, vector } => self.graph.insert(*row_id, vector)?,
                DeltaEntry::Tombstone { row_id } => {
                    tombstones.insert(*row_id);
                }
            }
        }
        for row_id in &self.pending_tombstones {
            tombstones.insert(*row_id);
        }

        // The new manifest is likewise built from the latest snapshot's
        // manifest: this transaction's new data files are *appended* to
        // the latest file list (never substituted for it wholesale —
        // that would silently drop data files committed by concurrent,
        // non-conflicting transactions after this one began).
        let mut manifest = latest_snapshot.manifest.as_ref().clone();
        manifest.version = new_version;
        manifest.data_files.extend(new_data_files);
        manifest.next_row_id = self
            .next_row_id_counter
            .load(std::sync::atomic::Ordering::SeqCst);
        // Mirrors next_row_id_counter -> manifest.next_row_id immediately
        // above: persist the counter's current value (already past this
        // commit's own attempt_id, via the fetch_add above) so a future
        // Dataset::open never regenerates a filename this session already
        // committed. See Manifest.next_attempt_id's doc comment.
        manifest.next_attempt_id = self
            .write_attempt_counter
            .load(std::sync::atomic::Ordering::SeqCst);
        manifest
            .tombstones
            .extend(self.pending_tombstones.iter().copied());

        commit_manifest(&self.dir, &manifest)?;

        commit_log.push(new_version, self.write_set);

        // Only after commit_manifest succeeds does the new state become
        // visible to future Dataset::snapshot() calls — the in-memory swap
        // must never run ahead of the on-disk durability point.
        let watermark = manifest.next_row_id.saturating_sub(1);
        let snapshot = Snapshot {
            dir: self.dir,
            version: new_version,
            manifest: Arc::new(manifest),
            graph: self.graph,
            watermark,
            tombstones: Arc::new(tombstones),
        };
        self.current.store(Arc::new(snapshot));

        Ok(())
    }

    /// Writes every pending batch's data file and delta-log file to
    /// `data_dir`, assigning row-ids from `row_id_counter` and appending
    /// each batch's `DataFileEntry` to `data_files` in place. Returns every
    /// `DeltaEntry` produced across all pending batches, in order —
    /// `Transaction::commit` applies these directly to the shared graph
    /// instead of re-reading them from disk.
    ///
    /// `attempt_id` is a collision-free filename-uniqueness token from
    /// `Dataset.write_attempt_counter` — **not** a manifest version. It
    /// exists only so concurrent callers never write to the same path;
    /// see `Transaction::commit` (Task 6) for why it can't be derived
    /// from `base_manifest.version` instead.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`Transaction::commit`]'s
    /// own doc comment (dictionary-encoding failure, non-finite vector
    /// component, I/O failure writing a data/delta-log file, or a
    /// [`TxnError::ManifestOverflow`] if row-id assignment would overflow).
    fn write_pending_batches(
        pending: &[RecordBatch],
        data_dir: &Path,
        attempt_id: u64,
        row_id_counter: &AtomicU64,
        data_files: &mut Vec<DataFileEntry>,
    ) -> Result<Vec<DeltaEntry>> {
        let mut all_deltas = Vec::new();
        for (i, batch) in pending.iter().enumerate() {
            // Stats computed on the original, pre-encoding, pre-row-id batch — see
            // .claude/docs/design/phase-3-query-refinement-spec.md §1 for why
            // (logical values, no dictionary-decode step needed later; _row_id is
            // an internal column, not a user column subject to file-pruning stats).
            let stats = compute_stats(batch);

            let num_rows = u64::try_from(batch.num_rows())?;
            let row_id_base =
                row_id_counter.fetch_add(num_rows, std::sync::atomic::Ordering::SeqCst);
            // fetch_add already advanced the counter before this check —
            // intentional. Row-ids are never reused (see Manifest.next_row_id's
            // doc comment), so an abandoned gap from a failed batch is
            // harmless, and there is no atomic way to "undo" a fetch_add if
            // we checked first instead.
            row_id_base.checked_add(num_rows).ok_or_else(|| {
                TxnError::ManifestOverflow(format!("next_row_id {row_id_base} + {num_rows}"))
            })?;

            let deltas = build_delta_entries(batch, row_id_base)?;
            let with_row_id = append_row_id_column(batch, row_id_base, num_rows)?;

            let encoded = strata_storage::encode_batch(&with_row_id)?;
            let file_name = format!("{attempt_id:020}-{i}.arrow");
            write_batch(&data_dir.join(&file_name), &encoded)?;

            let delta_file_name = format!("{attempt_id:020}-{i}.deltalog");
            write_delta_log(&data_dir.join(&delta_file_name), &deltas)?;

            data_files.push(DataFileEntry {
                name: file_name,
                stats,
                delta_log: delta_file_name,
            });
            all_deltas.extend(deltas);
        }
        Ok(all_deltas)
    }
}

// HNSW parameter defaults — small, correctness-only values for now.
// Task 7's benchmark is what tunes the real production defaults; see
// .claude/rules/vector-index.md ("tuned via benchmarks, not guessed").
const HNSW_MAX_NB_CONNECTION: usize = 16;
const HNSW_MAX_LAYER: usize = 16;
const HNSW_EF_CONSTRUCTION: usize = 200;

fn new_hnsw_index(capacity: usize) -> Result<HnswIndex> {
    Ok(HnswIndex::new(
        MaxConnections(HNSW_MAX_NB_CONNECTION),
        MaxElements(capacity.max(1)),
        MaxLayers(HNSW_MAX_LAYER),
        EfConstruction(HNSW_EF_CONSTRUCTION),
    )?)
}

/// Sane ceiling for a manifest's `next_row_id` before it's used to size an
/// eager HNSW allocation. `hnsw_rs::Hnsw::new`'s `max_elements` parameter
/// drives a `Vec::with_capacity` sized proportionally to it (verified
/// against the installed `hnsw_rs-0.3.4` source) — an unvalidated,
/// manifest-controlled value near `u64::MAX` would attempt an
/// unreasonably large allocation on open of a corrupted/hostile dataset
/// instead of returning a typed error. One billion rows is far beyond any
/// realistic embedded dataset today; revisit if a real workload needs more.
const MAX_REASONABLE_ROW_ID_CAPACITY: u64 = 1_000_000_000;

/// Rebuilds a fresh `HnswIndex` plus its tombstone set by replaying every
/// delta-log entry across every committed data file in `manifest`, in
/// order. Used only by [`Dataset::open`] (crash recovery / process start) —
/// `Transaction::commit` no longer calls this; it applies only its own new
/// delta entries directly to the already-shared graph instead (see
/// `docs/superpowers/specs/2026-07-17-phase-5-mvcc-snapshot-isolation-design.md`).
///
/// # Errors
///
/// Returns an error if any delta-log file listed in `manifest` fails to
/// read or parse, if `manifest.next_row_id` exceeds
/// [`MAX_REASONABLE_ROW_ID_CAPACITY`], or (via [`TxnError::Index`]) if a
/// replayed `DeltaEntry::Insert`'s vector length doesn't match the
/// dimensionality established by the first vector ever inserted into the
/// index.
fn replay_index(dir: &Path, manifest: &Manifest) -> Result<(HnswIndex, im::HashSet<u64>)> {
    if manifest.next_row_id > MAX_REASONABLE_ROW_ID_CAPACITY {
        return Err(TxnError::UnreasonableCapacity(
            manifest.next_row_id,
            MAX_REASONABLE_ROW_ID_CAPACITY,
        ));
    }
    let capacity = usize::try_from(manifest.next_row_id).unwrap_or(usize::MAX);
    let index = new_hnsw_index(capacity)?;
    let mut tombstones: im::HashSet<u64> = im::HashSet::new();
    let data_dir = data_subdir(dir);
    for entry in &manifest.data_files {
        for delta in read_delta_log(&safe_join(&data_dir, &entry.delta_log)?)? {
            match delta {
                DeltaEntry::Insert { row_id, vector } => index.insert(row_id, &vector)?,
                DeltaEntry::Tombstone { row_id } => {
                    tombstones.insert(row_id);
                }
            }
        }
    }
    for row_id in &manifest.tombstones {
        tombstones.insert(*row_id);
    }
    Ok((index, tombstones))
}

/// Builds one `Insert` delta-log entry per row in `batch` with a non-null
/// vector, keyed by the row-ids assigned starting at `row_id_base` — see
/// `.claude/docs/design/phase-4-vector-index-spec.md` §2. A `batch` with no
/// `"vector"` column at all (a table with no vector column defined) simply
/// produces no entries — that's not an error, unlike a `"vector"` column
/// present with the wrong type, which is.
///
/// Also rejects any row whose vector contains a non-finite (`NaN`/`Infinity`)
/// component: the delta log is serialized as JSON (`serde_json`), which
/// silently encodes non-finite `f32`s as `null` and then fails to parse them
/// back — letting one through here would durably commit a row that
/// permanently breaks every future `replay_index` (including the very one
/// `Transaction::commit` runs on its own return path). Must run before any
/// file for this batch is written to disk — see the call site in
/// `Transaction::commit`.
///
/// # Errors
///
/// Returns an error if `batch` has a `"vector"` column that isn't a
/// `FixedSizeList<Float32>`, or if any row's vector contains a non-finite
/// component.
fn build_delta_entries(batch: &RecordBatch, row_id_base: u64) -> Result<Vec<DeltaEntry>> {
    let Ok(vec_idx) = batch.schema_ref().index_of("vector") else {
        return Ok(Vec::new());
    };
    let vectors = batch
        .column(vec_idx)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeListArray>()
        .ok_or_else(|| {
            TxnError::Arrow(arrow::error::ArrowError::CastError(
                "vector column must be FixedSizeList".to_string(),
            ))
        })?;

    let mut entries = Vec::with_capacity(vectors.len());
    for i in 0..vectors.len() {
        if vectors.is_null(i) {
            continue;
        }
        let row = vectors.value(i);
        let row: &arrow::array::Float32Array = row.as_any().downcast_ref().ok_or_else(|| {
            TxnError::Arrow(arrow::error::ArrowError::CastError(
                "vector column's inner type must be Float32".to_string(),
            ))
        })?;
        let row_id = row_id_base.checked_add(u64::try_from(i)?).ok_or_else(|| {
            TxnError::ManifestOverflow(format!("row_id_base {row_id_base} + {i}"))
        })?;
        if row.values().iter().any(|component| !component.is_finite()) {
            return Err(TxnError::NonFiniteVectorComponent { row_id });
        }
        entries.push(DeltaEntry::Insert {
            row_id,
            vector: row.values().to_vec(),
        });
    }
    Ok(entries)
}

/// Validates that every [`DeltaEntry::Insert`] in `deltas` shares one
/// consistent vector dimension — both against each other, and against
/// `graph`'s already-established dimension (if any) — before any of them
/// are applied. `HnswIndex::insert`'s only fallible path is dimension
/// validation (the underlying `hnsw_rs` call itself never fails), so this
/// closes the real trigger for a partial-graph-mutation-then-fail
/// scenario: without it, `Insert` deltas are applied to the shared graph
/// in pending-batch order, and a later batch's dimension mismatch is only
/// caught after an earlier batch's deltas have already mutated the shared
/// graph.
///
/// # Errors
///
/// Returns [`TxnError::Index`] wrapping an [`strata_index::IndexError::DimensionMismatch`]
/// if any `Insert` delta's vector length disagrees with the graph's
/// established dimension, or with an earlier delta's length in this same
/// batch of deltas if the graph has no dimension established yet.
fn validate_delta_dimensions(deltas: &[DeltaEntry], graph: &HnswIndex) -> Result<()> {
    let mut expected = graph.established_dimension();
    for delta in deltas {
        if let DeltaEntry::Insert { vector, .. } = delta {
            if expected == 0 {
                expected = vector.len();
            } else if vector.len() != expected {
                return Err(TxnError::Index(
                    strata_index::IndexError::DimensionMismatch {
                        query_len: vector.len(),
                        expected,
                    },
                ));
            }
        }
    }
    Ok(())
}

/// Joins `name` onto `data_dir`, rejecting any `name` whose path
/// components aren't all bare filename segments (`Component::Normal`) — a
/// `name` containing `..` or an absolute path (which `Path::join` would
/// otherwise resolve/replace unchecked) must never let a corrupted/hostile
/// manifest read a file outside the dataset's own `data/` directory.
/// `DataFileEntry.name`/`.delta_log` are documented as "relative to the
/// dataset's data/ directory" (`crates/storage/src/manifest.rs`) — this is
/// what actually enforces that contract instead of merely documenting it.
pub(crate) fn safe_join(data_dir: &Path, name: &str) -> Result<PathBuf> {
    let candidate = Path::new(name);
    let all_normal = candidate
        .components()
        .all(|c| matches!(c, std::path::Component::Normal(_)));
    if !all_normal {
        return Err(TxnError::UnsafeManifestPath(name.to_string()));
    }
    Ok(data_dir.join(candidate))
}

/// Casts every column of `batch` to the corresponding field type in
/// `schema`, leaving already-matching columns untouched (a cheap `Arc`
/// clone, not a copy). See [`crate::snapshot::Snapshot::scan`]'s doc comment
/// for why this is necessary rather than a defensive nicety.
///
/// Every committed file physically carries a trailing hidden
/// [`ROW_ID_COLUMN`] (see `append_row_id_column`). It only counts toward
/// the batch's *logical* width when the caller's `schema` explicitly
/// requests it (as the CLI's `search` subcommand does) — otherwise the
/// positional zip below deliberately drops it. Any other width
/// disagreement is an error: without the up-front check, `Iterator::zip`
/// would silently truncate to the shorter side — dropping real columns, or
/// worse, pairing the hidden row-id column with a caller field and casting
/// row-ids into the caller's data.
///
/// # Errors
///
/// Returns [`TxnError::SchemaMismatch`] if `schema`'s field count doesn't
/// match `batch`'s logical column count, or an Arrow error if a column
/// can't be cast to its corresponding field's type.
pub(crate) fn cast_batch_to_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let physical = batch.num_columns();
    let hidden_row_id = batch.schema_ref().index_of(ROW_ID_COLUMN).is_ok()
        && schema.index_of(ROW_ID_COLUMN).is_err();
    let logical = if hidden_row_id {
        physical.saturating_sub(1)
    } else {
        physical
    };
    if logical != schema.fields().len() {
        return Err(TxnError::SchemaMismatch {
            expected: schema.fields().len(),
            actual: logical,
        });
    }
    let columns: std::result::Result<Vec<ArrayRef>, arrow::error::ArrowError> = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column.as_ref(), field.data_type())
            }
        })
        .collect();
    Ok(RecordBatch::try_new(Arc::clone(schema), columns?)?)
}

/// Appends a `_row_id: UInt64` column to `batch`, assigning
/// `row_id_base..row_id_base + num_rows` in row order. This is what makes
/// every committed row addressable by a stable, global identity — see
/// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8.
fn append_row_id_column(
    batch: &RecordBatch,
    row_id_base: u64,
    num_rows: u64,
) -> Result<RecordBatch> {
    let row_ids: Vec<u64> = (0..num_rows).map(|i| row_id_base + i).collect();
    let row_id_array: ArrayRef = Arc::new(UInt64Array::from(row_ids));

    let mut fields: Vec<Field> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(ROW_ID_COLUMN, DataType::UInt64, false));

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(row_id_array);

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use strata_storage::read_batch;

    use super::*;

    /// Locks in `AtomicU64::fetch_add`'s contract in isolation — before
    /// `next_row_id_counter` is wired into `Dataset`/`write_pending_batches`
    /// (below), this is what proves 8 concurrent `fetch_add(10)`s hand out
    /// non-overlapping, contiguous ranges. Uses `std::thread::scope` rather
    /// than `unsafe { transmute }` to borrow the stack-local `counter`
    /// safely — see Task 5's brief for why the `transmute` draft was
    /// rejected (this workspace's "safe Rust by default" convention).
    #[test]
    fn row_id_counter_hands_out_non_overlapping_ranges_under_concurrent_fetch_add() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let counter = AtomicU64::new(0);
        let mut bases: Vec<u64> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| counter.fetch_add(10, Ordering::SeqCst)))
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        bases.sort_unstable();
        for (i, base) in bases.iter().enumerate() {
            assert_eq!(
                *base,
                (i as u64) * 10,
                "ranges must be contiguous, non-overlapping"
            );
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("strata-txn-test-{label}-{}", std::process::id()))
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    #[test]
    fn scan_succeeds_on_a_dictionary_encoded_low_cardinality_column() {
        // Regression test: found by the Phase 2 whole-branch review.
        // encode_batch dictionary-encodes low-cardinality columns
        // (crates/storage::encoding) before write_batch, but scan() used to
        // pass the caller's original logical schema straight into
        // concat_batches — which rejects any batch whose physical column
        // type doesn't exactly match. A 100-row, 2-distinct-value batch
        // (well under the 0.4 encoding threshold) reproduced this
        // deterministically: scan() returned
        // Err(InvalidArgumentError("expected Utf8 but found
        // Dictionary(Int32, Utf8)")) for every realistic low-cardinality
        // dataset. Fixed by cast_batch_to_schema casting each file's
        // columns back to the logical schema before concatenation.
        use arrow::array::StringArray;
        let dir = temp_dir("scan-dict-encoded");
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let names: Vec<&str> = (0..100)
            .map(|i| if i % 2 == 0 { "alice" } else { "bob" })
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(names.clone()))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        // Confirm the file really was dictionary-encoded, so this test
        // can't silently stop testing the regression it exists to catch.
        let on_disk = read_batch(&ds.data_dir().join(&ds.data_files()[0].name)).unwrap();
        assert!(
            matches!(
                on_disk.schema_ref().field(0).data_type(),
                DataType::Dictionary(_, _)
            ),
            "test data must actually trigger dictionary encoding to be a valid regression test"
        );

        let scanned = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(scanned.schema_ref().field(0).data_type(), &DataType::Utf8);
        let scanned_names = scanned
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..scanned.num_rows())
            .map(|i| scanned_names.value(i))
            .collect();
        assert_eq!(got, names);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn create_then_open_recovers_same_version() {
        let dir = temp_dir("create-open");
        let ds = Dataset::create(&dir).unwrap();
        assert_eq!(ds.current_version(), 0);

        let reopened = Dataset::open(&dir).unwrap();
        assert_eq!(reopened.current_version(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_after_reopen_does_not_destroy_prior_sessions_data_files() {
        // Regression test for the cross-session filename-collision bug
        // found during Task 6 self-review (see "Concern 1" in
        // .superpowers/sdd/task-6-report.md): before the fix,
        // `write_attempt_counter` was reseeded to 0 on every `Dataset::open`,
        // so a session that reopened an existing dataset and committed
        // again would regenerate the exact same `{attempt_id:020}-{i}`
        // data/delta-log filenames a prior session already committed.
        // `write_batch` uses `File::create`, which truncates — silently
        // destroying the prior session's already-durable data file, while
        // the manifest ended up referencing the same filename twice. The
        // empirically-confirmed symptom was a scan returning fewer rows
        // than were ever committed (3 destroyed, 1 new double-counted via
        // the duplicate manifest entry, netting 2 instead of 4). The fix
        // persists the counter in `Manifest.next_attempt_id`, seeded on
        // `open` the same way `next_row_id_counter` is seeded from
        // `manifest.next_row_id`.
        let dir = temp_dir("reopen-no-filename-collision");
        let schema = test_schema();

        {
            let ds = Dataset::create(&dir).unwrap();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let mut txn = ds.begin();
            txn.insert(batch);
            txn.commit().unwrap();
            // `ds` (and with it, its in-memory write_attempt_counter) is
            // dropped at the end of this block — the next session has no
            // memory of attempt_id 0 having already been used, except
            // through whatever `Dataset::open` reads back from disk.
        }

        let reopened = Dataset::open(&dir).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![4]))])
            .unwrap();
        let mut txn = reopened.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let scanned = reopened.snapshot().scan(&schema).unwrap();
        let ids = scanned
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut got: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
        got.sort_unstable();
        assert_eq!(
            got,
            vec![1, 2, 3, 4],
            "all rows from both sessions must be present — the first \
             session's committed data file must not be silently truncated \
             by the second session reusing its filename"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn insert_then_commit_then_scan_round_trips() {
        let dir = temp_dir("insert-scan");
        let schema = test_schema();
        let ds = Dataset::create(&dir).unwrap();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch.clone());
        txn.commit().unwrap();

        assert_eq!(ds.current_version(), 1);
        let scanned = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(scanned, batch);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn create_twice_errors() {
        let dir = temp_dir("create-twice");
        let _ds = Dataset::create(&dir).unwrap();
        let result = Dataset::create(&dir);
        assert!(matches!(result, Err(TxnError::AlreadyExists(_))));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_computes_and_stores_column_stats() {
        let dir = temp_dir("commit-stats");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![30, 10, 20]))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let entry = &ds.data_files()[0];
        let id_stats = entry.stats.get("id").unwrap();
        assert_eq!(id_stats.min, strata_storage::Value::Int64(10));
        assert_eq!(id_stats.max, strata_storage::Value::Int64(30));

        std::fs::remove_dir_all(&dir).ok();
    }

    // NOTE (Batch 1, Task 2): the plan also specified a sibling test,
    // `commit_errors_instead_of_overflowing_when_next_row_id_would_wrap`,
    // crafting a hostile manifest with `next_row_id: u64::MAX - 1`. Task 2
    // deferred it because `Dataset::open` -> `replay_index` panicked
    // ("capacity overflow") on such a manifest before `commit` ever ran.
    // Resolved by Batch 1, Task 4: `replay_index` now rejects any manifest
    // whose `next_row_id` exceeds `MAX_REASONABLE_ROW_ID_CAPACITY` with a
    // typed `TxnError::UnreasonableCapacity` at open — covered by
    // `open_errors_instead_of_attempting_a_huge_allocation_on_an_unreasonable_next_row_id`
    // below. The capacity ceiling makes a near-`u64::MAX` `next_row_id`
    // unreachable through `open`, so the originally-specified commit-time
    // wrap test is intentionally subsumed by the open-time guard test.

    #[test]
    fn open_errors_instead_of_attempting_a_huge_allocation_on_an_unreasonable_next_row_id() {
        let dir = temp_dir("unreasonable-capacity");
        let hostile = Manifest {
            version: 0,
            data_files: Vec::new(),
            next_row_id: u64::MAX,
            tombstones: Vec::new(),
            next_attempt_id: 0,
        };
        strata_storage::commit_manifest(&dir, &hostile).unwrap();

        let result = Dataset::open(&dir);
        // `Dataset` doesn't implement `Debug` (its HNSW index can't), so
        // only the `Err` side is printable on failure.
        assert!(
            matches!(result, Err(TxnError::UnreasonableCapacity(_, _))),
            "expected UnreasonableCapacity, got {:?}",
            result.err()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_errors_instead_of_overflowing_when_version_would_wrap() {
        let dir = temp_dir("version-overflow");
        // Craft a manifest whose version sits at u64::MAX, bypassing the
        // normal create/commit path (which could never reach this value in
        // practice) to simulate a hostile/corrupted manifest.
        let hostile = Manifest {
            version: u64::MAX,
            data_files: Vec::new(),
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
        };
        strata_storage::commit_manifest(&dir, &hostile).unwrap();
        let ds = Dataset::open(&dir).unwrap();

        let schema = test_schema();
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        let result = txn.commit();

        // `Dataset` doesn't implement `Debug` (its HNSW index can't), so
        // only the `Err` side is printable on failure.
        assert!(
            matches!(&result, Err(TxnError::ManifestOverflow(_))),
            "expected ManifestOverflow, got {:?}",
            result.err()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn low_cardinality_column_is_dictionary_encoded_on_commit() {
        use arrow::array::StringArray;
        use arrow::datatypes::DataType;

        let dir = temp_dir("encode-on-commit");
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let names: Vec<&str> = vec!["x"; 20]; // single distinct value, well under threshold
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        // Read the raw written file back directly (bypassing Dataset::scan's
        // concat_batches, which would already show us the encoded type, but
        // reading the file directly proves the *durable* representation is
        // encoded, not just an in-memory artifact).
        let data_dir = ds.data_dir();
        let file_name = &ds.data_files()[0].name;
        let on_disk = strata_storage::read_batch(&data_dir.join(file_name)).unwrap();
        assert!(matches!(
            on_disk.schema_ref().field(0).data_type(),
            DataType::Dictionary(_, _)
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn explain_reports_skipped_files_by_range() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("explain-skip");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir).unwrap();

        // Two commits, disjoint id ranges -> two files with non-overlapping stats.
        let low = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(low);
        txn.commit().unwrap();

        let high = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![100, 101, 102]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(high);
        txn.commit().unwrap();

        let predicate = Predicate::Eq("id".to_string(), Value::Int64(2));
        let result = ds.snapshot().explain(&predicate);

        assert_eq!(result.total_files, 2);
        assert_eq!(
            result.scanned.len(),
            1,
            "only the [1,3] file could match id=2"
        );
        assert_eq!(
            result.skipped.len(),
            1,
            "the [100,102] file must be skipped"
        );
        let low_file_name = ds.data_files()[0].name.clone();
        let high_file_name = ds.data_files()[1].name.clone();
        assert_eq!(
            result.scanned,
            vec![low_file_name],
            "the [1,3] file must be the one actually named in scanned, not just counted"
        );
        assert_eq!(
            result.skipped,
            vec![high_file_name],
            "the [100,102] file must be the one actually named in skipped, not just counted"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn row_ids_are_assigned_sequentially_and_monotonically_across_commits() {
        let dir = temp_dir("row-id-sequential");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let first = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(first);
        txn.commit().unwrap();

        let second =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![40, 50]))]).unwrap();
        let mut txn = ds.begin();
        txn.insert(second);
        txn.commit().unwrap();

        let data_dir = ds.data_dir();
        let first_on_disk = read_batch(&data_dir.join(&ds.data_files()[0].name)).unwrap();
        let second_on_disk = read_batch(&data_dir.join(&ds.data_files()[1].name)).unwrap();

        let row_id_col = |batch: &RecordBatch| -> Vec<u64> {
            let idx = batch.schema_ref().index_of(ROW_ID_COLUMN).unwrap();
            let arr = batch
                .column(idx)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            (0..arr.len()).map(|i| arr.value(i)).collect()
        };

        assert_eq!(row_id_col(&first_on_disk), vec![0, 1, 2]);
        assert_eq!(row_id_col(&second_on_disk), vec![3, 4]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn row_id_column_never_leaks_into_scan_output() {
        let dir = temp_dir("row-id-hidden");
        let schema = test_schema(); // just "id", no _row_id
        let ds = Dataset::create(&dir).unwrap();

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let scanned = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(
            scanned.schema_ref().fields().len(),
            1,
            "_row_id must not appear in scan() output when the caller's schema doesn't ask for it"
        );
        assert!(scanned.schema_ref().index_of(ROW_ID_COLUMN).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_with_predicate_returns_only_matching_rows_from_unskipped_files() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("scan-with-predicate");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let low = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(low);
        txn.commit().unwrap();

        let high = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![100, 101, 102]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(high);
        txn.commit().unwrap();

        let predicate = Predicate::Eq("id".to_string(), Value::Int64(2));
        let result = ds
            .snapshot()
            .scan_with_predicate(&schema, &predicate)
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    fn vector_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]))
    }

    #[allow(clippy::needless_pass_by_value)]
    fn vector_batch(ids: Vec<i64>, vectors: Vec<[f32; 3]>) -> RecordBatch {
        let id_arr = Arc::new(Int64Array::from(ids));
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let flat: Vec<f32> = vectors.iter().flatten().copied().collect();
        let values = Arc::new(arrow::array::Float32Array::from(flat));
        let vec_arr = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field, 3, values, None,
        ));
        RecordBatch::try_new(vector_test_schema(), vec![id_arr, vec_arr]).unwrap()
    }

    #[test]
    fn vector_search_without_predicate_finds_the_true_nearest_neighbor() {
        let dir = temp_dir("vector-search-unfiltered");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(
            vec![1, 2, 3],
            vec![[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [10.0, 10.0, 10.0]],
        );
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let results = ds
            .snapshot()
            .vector_search(&[0.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_id, 0); // row-id 0 is the first committed row (id=1)

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Generates `count` points scattered within a small cube of side
    /// `spacing` around `center`. Mirrors `crates/index/src/hnsw.rs`'s own
    /// `insert_cluster` test helper (see commit `733579f`): `hnsw_rs`'s
    /// `StdRng::from_os_rng()` layer-assignment RNG has no exposed seed, so
    /// tiny (2-3 point) fixtures occasionally produce a graph shape where
    /// greedy search misses the true nearest neighbor. Many points spread
    /// across well-separated clusters makes "which cluster is nearest"
    /// unambiguous regardless of layer-assignment luck. Offsets come from
    /// an irrational-multiplier equidistribution sequence rather than a
    /// line/grid, since collinear near-duplicate points let `hnsw_rs`'s
    /// neighbor-diversification heuristic prune almost all direct links
    /// between them.
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    fn cluster_vectors(count: usize, center: [f32; 3], spacing: f32) -> Vec<[f32; 3]> {
        const PHI: f64 = 0.618_033_988_749_895; // fractional part of the golden ratio
        const SQRT2: f64 = 0.414_213_562_373_095; // fractional part of sqrt(2)
        const SQRT3: f64 = 0.732_050_807_568_877; // fractional part of sqrt(3)
        (0..count)
            .map(|i| {
                let n = i as f64;
                let frac = |mult: f64| (n * mult).fract();
                let dx = (frac(PHI) as f32) * spacing;
                let dy = (frac(SQRT2) as f32) * spacing;
                let dz = (frac(SQRT3) as f32) * spacing;
                [center[0] + dx, center[1] + dy, center[2] + dz]
            })
            .collect()
    }

    #[test]
    fn vector_search_with_predicate_only_returns_matching_rows() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("vector-search-filtered");
        let ds = Dataset::create(&dir).unwrap();

        // Two well-separated 15-point clusters, mirroring
        // crates/index/src/hnsw.rs's own flaky-test fix (commit 733579f):
        // a 2-point fixture is fragile against hnsw_rs's unseeded internal
        // RNG on tiny graphs. id=1's cluster sits at the origin (where the
        // query point also sits, so the *unfiltered* nearest neighbors are
        // unambiguously from this cluster); id=2's cluster sits 1000 units
        // away. `Predicate::Eq("id", 2)` must narrow results to only the
        // far cluster, even though every one of its points is vastly
        // farther from the query than every point in the near cluster.
        let near_cluster = cluster_vectors(15, [0.0, 0.0, 0.0], 0.01);
        let far_cluster = cluster_vectors(15, [1000.0, 0.0, 0.0], 0.01);
        let mut ids = vec![1i64; 15];
        ids.extend(vec![2i64; 15]);
        let mut vectors = near_cluster;
        vectors.extend(far_cluster);
        let batch = vector_batch(ids, vectors);
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        // Sanity check: without the predicate, the true nearest neighbors
        // really do come from the near (non-matching) cluster — otherwise
        // this test wouldn't prove the predicate is doing any narrowing.
        // Both reads below share a single snapshot, so they observe exactly
        // the same committed state.
        let snapshot = ds.snapshot();
        let unfiltered = snapshot.vector_search(&[0.0, 0.0, 0.0], 3, None).unwrap();
        assert_eq!(unfiltered.len(), 3);
        assert!(
            unfiltered.iter().all(|r| r.row_id < 15),
            "unfiltered nearest neighbors must come from the near cluster: {unfiltered:?}"
        );

        let predicate = Predicate::Eq("id".to_string(), Value::Int64(2));
        let results = snapshot
            .vector_search(&[0.0, 0.0, 0.0], 3, Some(&predicate))
            .unwrap();

        assert_eq!(results.len(), 3, "unexpected results: {results:?}");
        assert!(
            results.iter().all(|r| r.row_id >= 15),
            "predicate must narrow results to only the far (id=2) cluster: {results:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reopening_a_dataset_rebuilds_the_vector_index_from_the_delta_log() {
        let dir = temp_dir("delta-log-replay");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));
        let ds = Dataset::create(&dir).unwrap();

        let ids = Arc::new(Int64Array::from(vec![1, 2]));
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let values = Arc::new(arrow::array::Float32Array::from(vec![
            0.0, 0.0, 0.0, // row 0's vector
            9.0, 9.0, 9.0, // row 1's vector
        ]));
        let vectors = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field, 3, values, None,
        ));
        let batch = RecordBatch::try_new(schema, vec![ids, vectors]).unwrap();

        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();
        drop(ds);

        // Force a real replay from disk, not an in-memory shortcut — this is
        // the crash-recovery-equivalent test for the index (a fresh Dataset
        // struct, same process, but the index cache is definitely rebuilt from
        // the delta-log file, not carried over).
        let reopened = Dataset::open(&dir).unwrap();
        let results = reopened
            .snapshot()
            .vector_search(&[0.0, 0.0, 0.0], 1, None)
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].row_id, 0,
            "row 0's vector [0,0,0] is the true nearest match"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn committing_a_batch_with_a_non_finite_vector_component_is_rejected_cleanly() {
        // Regression test for the Phase 4 final-review finding: a
        // non-finite (NaN/Infinity) vector component used to durably
        // commit — serde_json silently encodes it as JSON `null` — and
        // then permanently brick the dataset, since every future
        // replay_index (including Dataset::open) would fail to parse that
        // `null` back into an f32. Must now be rejected upfront, before any
        // file for the offending batch is written to disk, leaving no
        // trace: no manifest advance, no orphaned-but-referenced files.
        let dir = temp_dir("non-finite-vector-rejected");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(vec![1, 2], vec![[0.0, 0.0, 0.0], [f32::NAN, 1.0, 1.0]]);
        let mut txn = ds.begin();
        txn.insert(batch);
        let result = txn.commit();

        match result {
            Err(TxnError::NonFiniteVectorComponent { row_id }) => {
                assert_eq!(row_id, 1, "row-id 1 (the second row) carries the NaN");
            }
            Err(other) => {
                panic!("expected NonFiniteVectorComponent, got a different error: {other}")
            }
            Ok(()) => panic!("commit of a NaN vector component must not succeed"),
        }

        // The rejected commit must have left no trace: the manifest never
        // advanced, and the dataset still opens and scans cleanly
        // afterward — not a permanently bricked dataset.
        let reopened = Dataset::open(&dir).unwrap();
        assert_eq!(reopened.current_version(), 0);
        assert!(reopened.data_files().is_empty());

        let scanned = reopened.snapshot().scan(&vector_test_schema()).unwrap();
        assert_eq!(scanned.num_rows(), 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn row_ids_stay_disjoint_across_multiple_pending_batches_in_one_transaction() {
        let dir = temp_dir("row-id-multi-batch-txn");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let first = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .unwrap();
        let second =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![30, 40, 50]))])
                .unwrap();

        let mut txn = ds.begin();
        txn.insert(first);
        txn.insert(second);
        txn.commit().unwrap();

        let data_dir = ds.data_dir();
        let first_on_disk = read_batch(&data_dir.join(&ds.data_files()[0].name)).unwrap();
        let second_on_disk = read_batch(&data_dir.join(&ds.data_files()[1].name)).unwrap();

        let row_id_col = |batch: &RecordBatch| -> Vec<u64> {
            let idx = batch.schema_ref().index_of(ROW_ID_COLUMN).unwrap();
            let arr = batch
                .column(idx)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            (0..arr.len()).map(|i| arr.value(i)).collect()
        };

        assert_eq!(row_id_col(&first_on_disk), vec![0, 1]);
        assert_eq!(row_id_col(&second_on_disk), vec![2, 3, 4]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_errors_instead_of_traversing_outside_data_dir_on_an_unsafe_manifest_entry() {
        let dir = temp_dir("path-traversal");
        Dataset::create(&dir).unwrap();

        // Simulate a hostile manifest: hand-craft a DataFileEntry whose name
        // tries to escape data/ via a parent-directory component. No real
        // commit can ever produce this - file names are always generated
        // internally - so this is only reachable via a corrupted/hand-edited
        // manifest, which is exactly the threat model this guards against.
        let hostile = Manifest {
            version: 1,
            data_files: vec![DataFileEntry {
                name: "../../etc/passwd".to_string(),
                stats: std::collections::HashMap::new(),
                delta_log: "d.deltalog".to_string(),
            }],
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
        };
        strata_storage::commit_manifest(&dir, &hostile).unwrap();
        // The delta log must exist (empty is fine — it replays to zero
        // entries) or Dataset::open's replay_index fails on a plain
        // missing-file I/O error before scan ever sees the hostile name.
        std::fs::write(dir.join("data").join("d.deltalog"), "").unwrap();
        let ds = Dataset::open(&dir).unwrap();

        let result = ds.snapshot().scan(&test_schema());
        assert!(
            matches!(result, Err(TxnError::UnsafeManifestPath(_))),
            "expected UnsafeManifestPath, got {result:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_errors_on_column_count_mismatch_between_physical_file_and_caller_schema() {
        let dir = temp_dir("schema-mismatch");
        let write_schema = test_schema(); // single "id" column
        let ds = Dataset::create(&dir).unwrap();

        let batch = RecordBatch::try_new(
            write_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        // Caller asks to scan with a schema declaring 2 columns, but the
        // committed file only has 1 logical column ("id" — the trailing
        // hidden _row_id doesn't count unless the caller requests it) -
        // must error, not silently zip/truncate or, worse, cast the hidden
        // row-id column into the caller's "extra" field.
        let mismatched_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("extra", DataType::Utf8, false),
        ]));
        let result = ds.snapshot().scan(&mismatched_schema);
        assert!(
            matches!(
                result,
                Err(TxnError::SchemaMismatch {
                    expected: 2,
                    actual: 1
                })
            ),
            "expected SchemaMismatch, got {result:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn vector_search_with_predicate_skips_pruned_files() {
        use strata_query::Predicate;
        use strata_storage::Value;

        // Mirrors explain_reports_skipped_files_by_range's fixture shape
        // (two commits with disjoint id ranges, so should_scan_file prunes
        // one file entirely for an id=2 predicate), but with a vector
        // column so this also exercises row_ids_matching's file-pruning
        // branch on the vector_search path, not just explain().
        let dir = temp_dir("vector-search-file-pruning");
        let ds = Dataset::create(&dir).unwrap();

        let low = vector_batch(vec![1, 1], vec![[0.0, 0.0, 0.0], [0.01, 0.01, 0.01]]);
        let mut txn = ds.begin();
        txn.insert(low);
        txn.commit().unwrap();

        let high = vector_batch(
            vec![2, 2],
            vec![[1000.0, 1000.0, 1000.0], [1000.01, 1000.01, 1000.01]],
        );
        let mut txn = ds.begin();
        txn.insert(high);
        txn.commit().unwrap();

        // Sanity: the id=1 file's stats don't overlap id=2's, so explain()
        // must confirm one file is prunable for this predicate — otherwise
        // this test wouldn't actually exercise the pruning branch. Both reads
        // below share a single snapshot, so they observe exactly the same
        // committed state.
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(2));
        let snapshot = ds.snapshot();
        let explain = snapshot.explain(&predicate);
        assert_eq!(explain.scanned.len(), 1);
        assert_eq!(explain.skipped.len(), 1);

        let results = snapshot
            .vector_search(&[1000.0, 1000.0, 1000.0], 2, Some(&predicate))
            .unwrap();

        assert_eq!(results.len(), 2, "unexpected results: {results:?}");
        assert!(
            results.iter().all(|r| r.row_id >= 2),
            "only the surviving (id=2) file's rows may be considered: {results:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn open_errors_with_not_found_for_a_nonexistent_dataset() {
        let dir = temp_dir("open-missing");
        let result = Dataset::open(&dir);
        // `Dataset` doesn't implement `Debug` (its HNSW index can't), so
        // only the `Err` side is printable on failure.
        assert!(
            matches!(result, Err(TxnError::NotFound(_))),
            "expected NotFound, got {:?}",
            result.err()
        );
    }

    #[test]
    fn committing_a_transaction_with_zero_pending_batches_still_advances_the_version() {
        let dir = temp_dir("empty-commit");
        let ds = Dataset::create(&dir).unwrap();
        let txn = ds.begin();
        txn.commit().unwrap();

        assert_eq!(
            ds.current_version(),
            1,
            "an empty commit still advances the manifest version"
        );
        assert!(
            ds.data_files().is_empty(),
            "an empty commit adds no data files"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_errors_cleanly_when_a_manifest_listed_file_is_missing_from_disk() {
        let dir = temp_dir("scan-missing-file");
        let schema = test_schema();
        let ds = Dataset::create(&dir).unwrap();

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let data_dir = ds.data_dir();
        std::fs::remove_file(data_dir.join(&ds.data_files()[0].name)).unwrap();

        let result = ds.snapshot().scan(&schema);
        assert!(
            result.is_err(),
            "scan must error cleanly, not panic, when a manifest-listed file is missing"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_concatenates_two_files_with_genuinely_different_physical_encodings() {
        use arrow::array::StringArray;
        let dir = temp_dir("mixed-encoding-scan");
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let ds = Dataset::create(&dir).unwrap();

        // First commit: high-cardinality (all-distinct) -> stays plain Utf8.
        let owned: Vec<String> = (0..20).map(|i| format!("name-{i}")).collect();
        let high_card: Vec<&str> = owned.iter().map(String::as_str).collect();
        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(high_card))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch1);
        txn.commit().unwrap();

        // Second commit: low-cardinality (2 distinct values over 20 rows) ->
        // gets dictionary-encoded.
        let low_card: Vec<&str> = (0..20)
            .map(|i| if i % 2 == 0 { "alice" } else { "bob" })
            .collect();
        let batch2 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(low_card))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch2);
        txn.commit().unwrap();

        // Confirm the two files really do have different physical
        // encodings, so this test can't silently stop testing the scenario
        // it exists for.
        let data_dir = ds.data_dir();
        let file0 = read_batch(&data_dir.join(&ds.data_files()[0].name)).unwrap();
        let file1 = read_batch(&data_dir.join(&ds.data_files()[1].name)).unwrap();
        assert_eq!(file0.schema_ref().field(0).data_type(), &DataType::Utf8);
        assert!(matches!(
            file1.schema_ref().field(0).data_type(),
            DataType::Dictionary(_, _)
        ));

        let scanned = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(scanned.num_rows(), 40);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn build_delta_entries_skips_null_vector_rows_without_erroring() {
        let ids = Arc::new(Int64Array::from(vec![1, 2]));
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let values = Arc::new(arrow::array::Float32Array::from(vec![
            1.0, 2.0, 3.0, 0.0, 0.0, 0.0,
        ]));
        let null_buffer = arrow::buffer::NullBuffer::from(vec![true, false]);
        let vectors = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field,
            3,
            values,
            Some(null_buffer),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(schema, vec![ids, vectors]).unwrap();

        let deltas = build_delta_entries(&batch, 0).unwrap();
        assert_eq!(
            deltas.len(),
            1,
            "the null-vector row must be skipped, not errored on"
        );
        match &deltas[0] {
            DeltaEntry::Insert { row_id, .. } => assert_eq!(*row_id, 0),
            DeltaEntry::Tombstone { .. } => panic!("expected an Insert entry"),
        }
    }

    #[test]
    fn build_delta_entries_errors_when_vector_column_is_not_a_fixed_size_list() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("vector", DataType::Int64, false), // wrong type
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap();

        let result = build_delta_entries(&batch, 0);
        assert!(result.is_err(), "expected an error, got {result:?}");
    }

    #[test]
    fn build_delta_entries_errors_when_vector_inner_type_is_not_float32() {
        let item_field = Arc::new(Field::new("item", DataType::Int32, false));
        let values = Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]));
        let vectors = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field, 3, values, None,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 3),
                false,
            ),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1])), vectors])
                .unwrap();

        let result = build_delta_entries(&batch, 0);
        assert!(result.is_err(), "expected an error, got {result:?}");
    }

    #[test]
    fn replay_index_applies_tombstone_entries_from_the_delta_log() {
        // Well-separated clusters, not a 2-point fixture - hnsw_rs's
        // unseeded layer-assignment RNG has repeatedly made tiny (2-3
        // point) fixtures flaky elsewhere in this file and in
        // crates/index/src/hnsw.rs's own tests (see cluster_vectors'/
        // insert_cluster's doc comments); the same precaution applies here.
        let dir = temp_dir("tombstone-replay");
        let ds = Dataset::create(&dir).unwrap();

        let near_cluster = cluster_vectors(15, [0.0, 0.0, 0.0], 0.01);
        let far_cluster = cluster_vectors(15, [1000.0, 0.0, 0.0], 0.01);
        let mut ids = vec![1i64; 15];
        ids.extend(vec![2i64; 15]);
        let mut vectors = near_cluster;
        vectors.extend(far_cluster);
        let batch = vector_batch(ids, vectors);
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        // Hand-append a Tombstone entry for row 0 (the exact-match nearest
        // neighbor in the near cluster) to the just-written delta-log file,
        // simulating what a future real DELETE path (Phase 5/6) will
        // produce - build_delta_entries itself never emits Tombstone
        // entries today.
        let data_dir = ds.data_dir();
        let delta_log_path = data_dir.join(&ds.data_files()[0].delta_log);
        let mut entries = strata_index::read_delta_log(&delta_log_path).unwrap();
        entries.push(DeltaEntry::Tombstone { row_id: 0 });
        strata_index::write_delta_log(&delta_log_path, &entries).unwrap();

        drop(ds);
        let reopened = Dataset::open(&dir).unwrap();
        // k=3, matching this file's other vector_search tests against the
        // same cluster shape (e.g. vector_search_with_predicate_only_returns_matching_rows) -
        // production HNSW defaults (EF_SEARCH_DEFAULT=32, not the much
        // wider tuned constants crates/index/src/hnsw.rs's own unit tests
        // use) don't reliably surface a larger k against this fixture.
        let results = reopened
            .snapshot()
            .vector_search(&[0.0, 0.0, 0.0], 3, None)
            .unwrap();

        assert_eq!(
            results.len(),
            3,
            "the near cluster has 14 live rows left after the tombstone, all vastly \
             closer than the far cluster, so the top 3 must still be fully populated: {results:?}"
        );
        assert!(
            results.iter().all(|r| r.row_id != 0),
            "the hand-tombstoned row must be excluded after replay: {results:?}"
        );
        assert!(
            results.iter().all(|r| r.row_id < 15),
            "every returned row must still be a genuine near-cluster neighbor, \
             not a fallback to the far cluster: {results:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn delete_tombstones_a_row_and_it_becomes_invisible() {
        let dir = temp_dir("delete-basic");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let mut txn = ds.begin();
        txn.delete(0);
        txn.commit().unwrap();

        assert!(!ds.snapshot().is_visible(0));
    }

    #[test]
    fn update_tombstones_old_row_and_makes_new_row_visible() {
        let dir = temp_dir("update-basic");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let replacement = vector_batch(vec![1i64], cluster_vectors(1, [5.0, 5.0, 5.0], 0.0));
        let mut txn = ds.begin();
        txn.update(0, replacement);
        txn.commit().unwrap();

        let snapshot = ds.snapshot();
        assert!(!snapshot.is_visible(0), "old row must be tombstoned");
        assert!(snapshot.is_visible(1), "replacement row must be visible");
    }

    #[test]
    fn tombstones_persist_across_reopen() {
        let dir = temp_dir("delete-persists");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let mut txn = ds.begin();
        txn.delete(0);
        txn.commit().unwrap();
        drop(ds);

        let reopened = Dataset::open(&dir).unwrap();
        assert!(!reopened.snapshot().is_visible(0));
    }

    #[test]
    fn concurrent_delete_of_the_same_row_conflicts() {
        let dir = temp_dir("commit-lock-conflict");
        let ds = Dataset::create(&dir).unwrap();
        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(batch);
        setup.commit().unwrap();

        let mut txn_a = ds.begin();
        txn_a.delete(0);
        let mut txn_b = ds.begin();
        txn_b.delete(0);

        txn_a.commit().unwrap();
        let result = txn_b.commit();
        match result {
            Err(TxnError::Conflict { contested_row_ids }) => {
                assert_eq!(contested_row_ids, vec![0]);
            }
            other => panic!("expected Conflict, got {other:?}"),
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn concurrent_delete_of_disjoint_rows_both_commit() {
        let dir = temp_dir("commit-lock-no-conflict");
        let ds = Dataset::create(&dir).unwrap();
        let batch = vector_batch(vec![1i64, 2i64], cluster_vectors(2, [0.0, 0.0, 0.0], 0.01));
        let mut setup = ds.begin();
        setup.insert(batch);
        setup.commit().unwrap();

        let mut txn_a = ds.begin();
        txn_a.delete(0);
        let mut txn_b = ds.begin();
        txn_b.delete(1);

        txn_a.commit().unwrap();
        txn_b.commit().unwrap();

        let snapshot = ds.snapshot();
        assert!(!snapshot.is_visible(0));
        assert!(!snapshot.is_visible(1));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_version_is_sourced_from_latest_state_not_stale_base_manifest() {
        // Regression test for the original unconditional
        // `base_manifest.version + 1` bug: txn_a and txn_b both begin
        // against version 0; txn_a commits (version 1); txn_b's disjoint
        // write must land at version 2, not also attempt version 1.
        let dir = temp_dir("commit-version-source");
        let ds = Dataset::create(&dir).unwrap();
        let batch = vector_batch(vec![1i64, 2i64], cluster_vectors(2, [0.0, 0.0, 0.0], 0.01));
        let mut setup = ds.begin();
        setup.insert(batch);
        setup.commit().unwrap();
        assert_eq!(ds.current_version(), 1);

        let mut txn_a = ds.begin();
        txn_a.delete(0);
        let mut txn_b = ds.begin();
        txn_b.delete(1);

        txn_a.commit().unwrap();
        assert_eq!(ds.current_version(), 2);
        txn_b.commit().unwrap();
        assert_eq!(ds.current_version(), 3);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn concurrent_inserts_preserve_both_transactions_data_files() {
        // Insert-only transactions have empty write-sets, so two of them
        // never conflict — both must commit, and the second's manifest
        // must *append* its files to the latest committed file list, not
        // substitute a stale base_manifest-derived list for it (which
        // would silently drop the first transaction's committed data — a
        // lost update the conflict check can't catch, because there is no
        // write-write overlap to detect).
        let dir = temp_dir("concurrent-insert-data-files");
        let ds = Dataset::create(&dir).unwrap();
        let schema = test_schema();

        let mut txn_a = ds.begin();
        txn_a.insert(
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
                .unwrap(),
        );
        let mut txn_b = ds.begin();
        txn_b.insert(
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![2]))])
                .unwrap(),
        );

        txn_a.commit().unwrap();
        txn_b.commit().unwrap();

        assert_eq!(
            ds.data_files().len(),
            2,
            "both transactions' data files must survive in the final manifest"
        );
        let scanned = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(scanned.num_rows(), 2, "no committed row may be lost");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_computes_stats_for_multiple_columns_including_utf8() {
        let dir = temp_dir("multi-column-stats");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ds = Dataset::create(&dir).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![30, 10, 20])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "banana", "apple", "cherry",
                ])),
            ],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let entry = &ds.data_files()[0];
        let id_stats = entry.stats.get("id").unwrap();
        assert_eq!(id_stats.min, strata_storage::Value::Int64(10));
        assert_eq!(id_stats.max, strata_storage::Value::Int64(30));

        let name_stats = entry.stats.get("name").unwrap();
        assert_eq!(
            name_stats.min,
            strata_storage::Value::Utf8("apple".to_string())
        );
        assert_eq!(
            name_stats.max,
            strata_storage::Value::Utf8("cherry".to_string())
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn explain_on_a_dataset_with_no_data_files_reports_zero_scanned_and_skipped() {
        let dir = temp_dir("explain-empty-dataset");
        let ds = Dataset::create(&dir).unwrap();

        let predicate =
            strata_query::Predicate::Eq("id".to_string(), strata_storage::Value::Int64(1));
        let result = ds.snapshot().explain(&predicate);

        assert_eq!(result.total_files, 0);
        assert!(result.scanned.is_empty());
        assert!(result.skipped.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_with_predicate_on_a_dataset_with_no_data_files_returns_an_empty_batch() {
        let dir = temp_dir("scan-with-predicate-empty-dataset");
        let schema = test_schema();
        let ds = Dataset::create(&dir).unwrap();

        let predicate =
            strata_query::Predicate::Eq("id".to_string(), strata_storage::Value::Int64(1));
        let result = ds
            .snapshot()
            .scan_with_predicate(&schema, &predicate)
            .unwrap();

        assert_eq!(result.num_rows(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn explain_reports_every_file_skipped_when_the_predicate_matches_none() {
        let dir = temp_dir("explain-all-pruned");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let predicate =
            strata_query::Predicate::Eq("id".to_string(), strata_storage::Value::Int64(999));
        let result = ds.snapshot().explain(&predicate);

        assert_eq!(result.total_files, 1);
        assert!(result.scanned.is_empty());
        assert_eq!(result.skipped.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn commit_applies_only_its_own_new_deltas_not_the_full_history() {
        let dir = std::env::temp_dir().join(format!(
            "strata-replay-cost-regression-{}",
            std::process::id()
        ));
        Dataset::create(&dir).unwrap();
        let dataset = Dataset::open(&dir).unwrap();

        // Commit 3 separate single-row batches first, establishing history.
        // `mvp_row(id, name, vector)` builds one row in mvp_schema()'s
        // shape — `id` is the schema's business column, unrelated to the
        // internal system row-id the commit path assigns automatically.
        for i in 0..3i64 {
            let mut txn = dataset.begin();
            txn.insert(crate::mvp_fixtures::mvp_row(i, "row", [i as f32, 0.0, 0.0]).unwrap());
            txn.commit().unwrap();
        }

        // The 4th commit's own pending batch has exactly 1 row (1 new
        // delta entry). Applying it must not require touching the 3
        // earlier commits' delta-log files at all — confirmed indirectly
        // here by checking the resulting snapshot's watermark/row count
        // match "3 history rows + 1 new row", which would only be wrong if
        // either too few (this commit's row lost) or suspiciously
        // history-dependent logic silently reprocessed old entries into a
        // wrong count.
        let mut txn = dataset.begin();
        txn.insert(crate::mvp_fixtures::mvp_row(3, "row", [3.0, 0.0, 0.0]).unwrap());
        txn.commit().unwrap();

        let snapshot = dataset.snapshot();
        assert_eq!(
            snapshot.watermark, 3,
            "expected exactly 4 rows total (system row-ids 0..=3)"
        );
        assert_eq!(
            snapshot
                .scan(&crate::mvp_fixtures::mvp_schema())
                .unwrap()
                .num_rows(),
            4
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_rejects_inconsistent_batch_dimensions_before_touching_the_shared_graph() {
        // Regression test for the hazard the Phase 5 final whole-branch
        // review flagged: Transaction::commit applies Insert deltas to the
        // shared, ever-growing Arc<HnswIndex> in pending-batch order, so a
        // later pending batch's dimension mismatch was only ever caught
        // after an earlier batch's deltas had already mutated the shared
        // graph -- even though commit() returns Err and the manifest never
        // advances. See validate_delta_dimensions's doc comment.
        let dir = temp_dir("inconsistent-batch-dimensions");
        let ds = Dataset::create(&dir).unwrap();

        // Establish a real baseline: one successful 3-d commit, via the
        // existing mvp_fixtures shape (FixedSizeList<Float32, 3>).
        let mut seed_txn = ds.begin();
        seed_txn.insert(crate::mvp_fixtures::mvp_row(0, "seed", [0.0, 0.0, 0.0]).unwrap());
        seed_txn.commit().unwrap();

        let snapshot_before = ds.snapshot();
        let version_before = snapshot_before.version;
        let established_before = snapshot_before.graph.established_dimension();
        assert_eq!(
            established_before, 3,
            "the seed commit must have established dimension 3"
        );

        // Build a second, valid 3-d batch (via mvp_fixtures) and an
        // inconsistent 5-d batch (hand-built, since mvp_fixtures is fixed
        // at 3 dimensions) -- the exact scenario the review flagged: Insert
        // deltas apply to the graph in pending-batch order, so without
        // pre-validation the 3-d batch's insert (row-id 1) would succeed
        // before the 5-d batch's insert (row-id 2) fails.
        let batch_3d = crate::mvp_fixtures::mvp_row(1, "still-3d", [1.0, 0.0, 0.0]).unwrap();

        let schema_5d = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 5),
                false,
            ),
        ]));
        let batch_5d = RecordBatch::try_new(
            schema_5d,
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(arrow::array::FixedSizeListArray::new(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    5,
                    Arc::new(arrow::array::Float32Array::from(vec![
                        2.0, 0.0, 0.0, 0.0, 0.0,
                    ])),
                    None,
                )),
            ],
        )
        .unwrap();

        let mut txn = ds.begin();
        txn.insert(batch_3d);
        txn.insert(batch_5d);
        let result = txn.commit();

        assert!(
            result.is_err(),
            "a transaction whose pending batches have inconsistent vector dimensions \
             must fail at commit()"
        );

        // Sanity checks on the durable/externally-visible side of the
        // invariant: version never advances, and established_dimension is
        // unchanged. NEITHER of these two assertions alone actually
        // distinguishes fixed-from-buggy in this specific scenario --
        // established_dimension() is already 3 both before and after,
        // with or without the fix, because the seed commit already set it
        // to 3 and the first (still-3-d) pending batch's vector matches
        // that already-established value either way, so it never changes
        // what established_dimension() reads even when wrongly applied.
        // Kept here only as baseline sanity checks, not as the regression
        // assertion -- see below for the one that actually discriminates.
        let snapshot_after = ds.snapshot();
        assert_eq!(
            snapshot_after.version, version_before,
            "a rejected commit must not advance the visible version at all"
        );
        assert_eq!(
            snapshot_after.graph.established_dimension(),
            established_before,
            "sanity check only -- see the row-id-1-leak assertion below for the actual \
             regression this test exists to catch"
        );

        // The assertion that actually discriminates fixed-from-buggy:
        // row-id 1 (the mismatched transaction's first, individually-valid
        // 3-d batch) must never have been physically inserted into the
        // shared HnswIndex graph. Pre-fix, its `HnswIndex::insert` call
        // succeeds (its dimension matches the graph's already-established
        // one) before the second batch's 5-d insert fails -- silently
        // mutating the graph even though the whole commit is rejected.
        // `Snapshot::vector_search` can't observe this: it filters by
        // `is_visible` (row_id <= watermark), and row-id 1's watermark is
        // never advanced by this rejected commit either way, so it would
        // hide the leaked row regardless of whether the fix exists. This
        // instead calls `HnswIndex::search` directly on
        // `snapshot_after.graph` (the same shared `Arc<HnswIndex>` the
        // failed commit mutated in place -- `pub(crate) graph` is
        // reachable from this same-crate test) with an always-true
        // visibility predicate, bypassing the watermark filter entirely to
        // see exactly what's physically in the graph.
        let leaked = snapshot_after
            .graph
            .search(&[1.0, 0.0, 0.0], 2, 200, |_| true)
            .unwrap();
        assert!(
            leaked.iter().all(|m| m.row_id != 1),
            "row-id 1 must never have been inserted into the shared graph -- a rejected \
             commit must apply zero of its deltas, not just the ones that come after the \
             first failure: {leaked:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn losing_transactions_graph_insert_never_lands_when_it_conflicts() {
        // Deterministic, not loom: both transactions begin from the same
        // snapshot, then commit sequentially (not concurrently) so which
        // one wins is fixed by test order, not explored interleavings —
        // there is no concurrency to model here, only a specific sequence
        // to regression-test. This is what actually exercises the
        // graph-mutation-ordering bug (design doc §2): both transactions
        // use `update`, not `delete`, since a delete-only transaction has
        // nothing to insert and can't trigger this bug at all.
        let dir = temp_dir("abort-leaves-no-graph-trace");
        let ds = Dataset::create(&dir).unwrap();
        let setup_batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(setup_batch);
        setup.commit().unwrap();

        // Distinctive, far-apart, never-elsewhere-used coordinates so a
        // vector_search near either one unambiguously reveals whether
        // that specific insert ever reached the graph.
        let winner_batch = vector_batch(vec![2i64], cluster_vectors(1, [500.0, 500.0, 500.0], 0.0));
        let loser_batch = vector_batch(vec![3i64], cluster_vectors(1, [900.0, 900.0, 900.0], 0.0));

        let mut txn_winner = ds.begin();
        txn_winner.update(0, winner_batch);
        let mut txn_loser = ds.begin();
        txn_loser.update(0, loser_batch);

        txn_winner.commit().unwrap();
        let result = txn_loser.commit();
        assert!(
            matches!(result, Err(TxnError::Conflict { .. })),
            "expected the second update to conflict on row 0, got {result:?}"
        );

        // The loser's insert must never have reached the graph — search
        // near its distinctive coordinates and confirm nothing close
        // exists (a large squared_distance means the nearest match found
        // is the unrelated winner/setup data, not the loser's own point).
        let results = ds
            .snapshot()
            .vector_search(&[900.0, 900.0, 900.0], 1, None)
            .unwrap();
        assert!(
            results.is_empty() || results[0].squared_distance > 1000.0,
            "loser's vector must not be findable near its own coordinates, got {results:?}"
        );

        // Without this, `temp_dir`'s PID-only naming can collide with a
        // leftover directory from an earlier process that happened to
        // reuse the same PID (observed in practice on Windows) — a stale
        // manifest from that leftover directory makes the next
        // `Dataset::create` at this path fail with `AlreadyExists`.
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    // COMMIT_LOG_CAPACITY (256) comfortably fits in i64/i16 for this loop's
    // small range (capacity + 2), matching the existing cast-allow precedent
    // on `cluster_vectors` above.
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    fn a_transaction_whose_history_has_aged_out_of_the_commit_log_conflicts_conservatively() {
        // COMMIT_LOG_CAPACITY is 256 in production; committing that many
        // transactions here to force wraparound is wasteful but the most
        // direct way to exercise the real end-to-end path without adding
        // a test-only constructor parameter for capacity. Kept small
        // enough (log capacity + a few) to run quickly.
        let dir = temp_dir("commit-log-wraparound-e2e");
        let ds = Dataset::create(&dir).unwrap();
        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(batch);
        setup.commit().unwrap();

        // txn begins here, before every filler commit below — its
        // base_manifest.version stays fixed at whatever ds.current_version()
        // is right now.
        let mut txn = ds.begin();
        txn.delete(0);

        // Commit enough disjoint no-op-ish filler transactions to push the
        // CommitLog's oldest retained entry past txn's read-version.
        for i in 0..(super::COMMIT_LOG_CAPACITY as i64 + 2) {
            let filler = vector_batch(
                vec![100 + i],
                cluster_vectors(1, [f32::from(i as i16), 0.0, 0.0], 0.0),
            );
            let mut filler_txn = ds.begin();
            filler_txn.insert(filler);
            filler_txn.commit().unwrap();
        }

        let result = txn.commit();
        assert!(
            matches!(result, Err(TxnError::Conflict { .. })),
            "expected a conservative conflict once history aged out, got {result:?}"
        );

        // Same PID-reuse collision risk as
        // `losing_transactions_graph_insert_never_lands_when_it_conflicts` —
        // see that test's cleanup comment for why this matters.
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    // Same cast-allow precedent as the sibling test above.
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    fn an_insert_only_transaction_whose_history_has_aged_out_of_the_commit_log_still_commits() {
        // Mirrors `a_transaction_whose_history_has_aged_out_of_the_commit_log_conflicts_conservatively`
        // but for an insert-only transaction (never calls update/delete, so
        // its write_set is empty). Per the design doc's "appends never
        // conflict" rule, an empty write-set can never conflict with
        // anything, regardless of how much commit history has aged out of
        // the bounded CommitLog — this must succeed even when its
        // base_manifest.version has aged out of the ring buffer.
        let dir = temp_dir("commit-log-wraparound-insert-only-e2e");
        let ds = Dataset::create(&dir).unwrap();

        // txn begins here, before every filler commit below — its
        // base_manifest.version stays fixed at whatever ds.current_version()
        // is right now.
        let mut txn = ds.begin();
        let insert_only_batch =
            vector_batch(vec![99_999], cluster_vectors(1, [500.0, 500.0, 500.0], 0.0));
        txn.insert(insert_only_batch);

        // Commit enough disjoint filler transactions to push the
        // CommitLog's oldest retained entry past txn's read-version.
        for i in 0..(super::COMMIT_LOG_CAPACITY as i64 + 2) {
            let filler = vector_batch(
                vec![100 + i],
                cluster_vectors(1, [f32::from(i as i16), 0.0, 0.0], 0.0),
            );
            let mut filler_txn = ds.begin();
            filler_txn.insert(filler);
            filler_txn.commit().unwrap();
        }

        let result = txn.commit();
        assert!(
            result.is_ok(),
            "insert-only transactions have an empty write-set and can never \
             conflict, even with aged-out history, but got {result:?}"
        );

        // Same PID-reuse collision risk as
        // `losing_transactions_graph_insert_never_lands_when_it_conflicts` —
        // see that test's cleanup comment for why this matters.
        std::fs::remove_dir_all(&dir).ok();
    }
}

/// Run with:
/// `cargo rustc -p strata-txn --lib --profile test -- --cfg loom` to build,
/// then execute the resulting `target/debug/deps/strata_txn-*` binary
/// (filter to `dataset::loom_tests` to run just this module).
///
/// **Why not the simpler `RUSTFLAGS="--cfg loom" cargo test -p strata-txn
/// --lib`:** that form sets `--cfg loom` for *every* crate rustc compiles
/// for this invocation, not just `strata-txn`. `strata-txn` depends on
/// `strata-index` as a regular (non-dev) dependency, and `strata-index`'s
/// own `hnsw.rs` has a pre-existing `#[cfg(loom)]`/`#[cfg(not(loom))]` shim
/// that imports the real `loom` crate under `cfg(loom)` — but `loom` is only
/// a *dev*-dependency of `strata-index`, unavailable to the plain (non-test)
/// library build that `strata-txn` links against. The global `RUSTFLAGS`
/// form was verified to fail with `cannot find module or crate 'loom'` at
/// `crates/index/src/hnsw.rs:5` (confirmed independent of this task's
/// changes: `RUSTFLAGS="--cfg loom" cargo build -p strata-index --lib`
/// fails identically on its own). `cargo rustc -p strata-txn -- --cfg loom`
/// scopes the flag to only `strata-txn`'s own compilation unit, leaving
/// `strata-index` (and every other dependency) compiled normally, which
/// sidesteps the conflict without touching `crates/index`.
///
/// **Research note (Task 7):** `arc-swap` (resolved to 1.9.2 in Cargo.lock)
/// has no documented `loom` integration or feature flag — confirmed against
/// docs.rs/arc-swap/1.9.2, crates.io's listed features (only an optional
/// `serde` feature), and the crate's own upstream `Cargo.toml` (features:
/// `weak`, `internal-test-strategies`, `experimental-strategies`,
/// `experimental-thread-local` — no mention of loom anywhere). `loom` can
/// only explore interleavings of its own instrumented primitives, so it
/// cannot see inside `arc-swap`'s real internal atomics without `arc-swap`
/// itself being loom-aware — the same reason `crates/index`'s earlier loom
/// test (`hnsw.rs`'s `establish_or_check_dimension`) needed a
/// `#[cfg(loom)]`/`#[cfg(not(loom))]` shim swapping in loom's atomic types.
/// This test therefore does **not** instrument the real `Dataset`/`ArcSwap`
/// type directly; it models the *shape* of the `Dataset::snapshot()` /
/// `Transaction::commit()` race — one writer storing a new value, one or
/// more readers loading concurrently — directly on loom's own
/// `sync::atomic::AtomicUsize`, proving the swap-then-load pattern itself is
/// race-free (no torn reads, no panics, no deadlocks) under loom's
/// exhaustive interleaving exploration. This is the same relationship a
/// hand-rolled `Mutex`-guarded swap would have to a loom test: the pattern
/// is verified, not the third-party crate's own internals.
#[cfg(loom)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod loom_tests {
    use std::sync::Arc as StdArc;

    #[test]
    fn one_writer_store_races_safely_with_many_readers_load() {
        loom::model(|| {
            // Models the Dataset::snapshot() / Transaction::commit() race
            // directly on loom's own primitives (see this module's doc
            // comment for why — arc-swap's internal atomics aren't
            // loom-instrumented).
            let current = StdArc::new(loom::sync::atomic::AtomicUsize::new(0));

            let writer_current = StdArc::clone(&current);
            let writer = loom::thread::spawn(move || {
                writer_current.store(1, loom::sync::atomic::Ordering::SeqCst);
            });

            let reader_current = StdArc::clone(&current);
            let reader = loom::thread::spawn(move || {
                // A reader must only ever observe 0 (before the store) or 1
                // (after it) — never a torn/intermediate value, and it must
                // never panic or deadlock racing the writer's store.
                let observed = reader_current.load(loom::sync::atomic::Ordering::SeqCst);
                assert!(
                    observed == 0 || observed == 1,
                    "observed torn value: {observed}"
                );
            });

            writer.join().unwrap();
            reader.join().unwrap();
        });
    }

    #[test]
    fn two_threads_deleting_the_same_row_exactly_one_conflicts() {
        loom::model(|| {
            let dir = std::env::temp_dir().join(format!(
                "strata-loom-conflict-{}-{:?}",
                std::process::id(),
                loom::thread::current().id()
            ));
            let ds = crate::Dataset::create(&dir).unwrap();
            let schema = StdArc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            ]));
            let batch = arrow::array::RecordBatch::try_new(
                schema,
                vec![StdArc::new(arrow::array::Int64Array::from(vec![1]))],
            )
            .unwrap();
            let mut setup = ds.begin();
            setup.insert(batch);
            setup.commit().unwrap();

            let ds_a = ds.clone();
            let ds_b = ds.clone();

            // Both transactions begin (and capture their shared, fixed base
            // snapshot version) before either thread starts, mirroring the
            // deterministic `losing_transactions_graph_insert_never_lands_when_it_conflicts`
            // test above. This guarantees the two transactions are actually
            // concurrent (design doc §7's intent) instead of allowing loom
            // to explore a schedule where thread A's begin()-through-commit()
            // runs to completion before thread B's begin() even executes —
            // under that schedule B would legitimately observe A's commit
            // as "nothing changed since I began" and its delete(0) on an
            // already-tombstoned row would be an idempotent no-op success,
            // not a real conflict. See task-7-report.md for the full
            // root-cause diagnosis.
            let mut txn_a = ds_a.begin();
            txn_a.delete(0);
            let mut txn_b = ds_b.begin();
            txn_b.delete(0);

            let thread_a = loom::thread::spawn(move || txn_a.commit());
            let thread_b = loom::thread::spawn(move || txn_b.commit());

            let result_a = thread_a.join().unwrap();
            let result_b = thread_b.join().unwrap();
            let successes = [&result_a, &result_b].iter().filter(|r| r.is_ok()).count();
            let conflicts = [&result_a, &result_b]
                .iter()
                .filter(|r| matches!(r, Err(crate::TxnError::Conflict { .. })))
                .count();
            assert_eq!(successes, 1, "exactly one commit must succeed");
            assert_eq!(conflicts, 1, "exactly one commit must report a conflict");

            std::fs::remove_dir_all(&dir).ok();
        });
    }

    #[test]
    fn two_threads_deleting_disjoint_rows_both_succeed() {
        loom::model(|| {
            let dir = std::env::temp_dir().join(format!(
                "strata-loom-disjoint-{}-{:?}",
                std::process::id(),
                loom::thread::current().id()
            ));
            let ds = crate::Dataset::create(&dir).unwrap();
            let schema = StdArc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            ]));
            let batch = arrow::array::RecordBatch::try_new(
                schema,
                vec![StdArc::new(arrow::array::Int64Array::from(vec![1, 2]))],
            )
            .unwrap();
            let mut setup = ds.begin();
            setup.insert(batch);
            setup.commit().unwrap();

            let ds_a = ds.clone();
            let ds_b = ds.clone();
            let thread_a = loom::thread::spawn(move || {
                let mut txn = ds_a.begin();
                txn.delete(0);
                txn.commit()
            });
            let thread_b = loom::thread::spawn(move || {
                let mut txn = ds_b.begin();
                txn.delete(1);
                txn.commit()
            });

            assert!(thread_a.join().unwrap().is_ok());
            assert!(thread_b.join().unwrap().is_ok());

            std::fs::remove_dir_all(&dir).ok();
        });
    }
}
