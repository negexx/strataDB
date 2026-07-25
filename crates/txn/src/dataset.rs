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
use std::sync::atomic::{AtomicI64, AtomicU64};

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
    ColumnStats, DataFileEntry, Manifest, Value, commit_manifest, compute_stats, read_current,
    write_batch,
};

use crate::commit_log::{CommitLog, ConflictCheck};
use crate::error::{Result, TxnError};
use crate::row_id::{RowIdAllocator, RowIdClaim};
use crate::snapshot::Snapshot;

/// The hidden internal row-id column every committed batch carries
/// alongside its logical columns. Callers can retrieve it through the
/// public `scan`/`scan_with_predicate` API by listing `ROW_ID_COLUMN`
/// (and/or `TIMESTAMP_COLUMN`) anywhere in their requested schema (see the
/// CLI's `handle_search`, which does exactly this) — `cast_batch_to_schema`
/// matches hidden columns by *name*, not position, so any combination of
/// `_row_id`/`_timestamp` can be requested, in any position, independently
/// of the other. Only the *visible* (user) columns must still be listed in
/// the same order the data was inserted in; a mismatched visible-column
/// count returns a typed `TxnError::SchemaMismatch` instead of silently
/// producing wrong data. `row_ids_matching` (below) sidesteps this
/// precondition entirely by reading each file's raw physical batch directly
/// rather than going through the public schema-based API. See
/// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8.
pub const ROW_ID_COLUMN: &str = "_row_id";

/// The hidden internal commit-time column every committed batch carries
/// alongside its logical columns and `_row_id` — see
/// `docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md`.
/// Every row in one transaction shares one value: microseconds since the
/// Unix epoch, captured once per commit. Unlike `_row_id`, this column
/// *does* get a `should_scan_file`-visible stats entry — see
/// `write_pending_batches`.
pub const TIMESTAMP_COLUMN: &str = "_timestamp";

/// Bounded capacity of the in-memory [`CommitLog`] ring buffer — generous
/// enough that ordinary workloads never evict history still needed by an
/// in-flight transaction (which would surface as a conservative
/// `InsufficientHistory` conflict), small enough to be a trivial memory
/// cost. Not a public tunable yet, per YAGNI.
///
/// Bumped from 256 to 2048 per the design council's recommendation on
/// whether to build epoch-based dynamic history pruning: rather than adding
/// a new active-transaction-lifetime registry on unmeasured need, widen the
/// cheap, already-bounded window and instrument how often
/// `InsufficientHistory` actually fires (see
/// [`Dataset::insufficient_history_conflict_count`]). If telemetry later
/// shows this still firing under real concurrent-agent load, revisit with
/// data — either a further bump or, per the council's stronger alternative,
/// a row/version-keyed overlap check that doesn't need a bounded window at
/// all.
const COMMIT_LOG_CAPACITY: usize = 2048;

#[derive(Clone)]
pub struct Dataset {
    dir: PathBuf,
    current: Arc<ArcSwap<Snapshot>>,
    /// Hands out row-id ranges *and* tracks which of them belong to
    /// transactions still in flight, so a published watermark can exclude
    /// them. Replaces the bare `AtomicU64` counter this used to be: the
    /// counter advance and the in-flight registration have to be one atomic
    /// step, or a publisher can observe a bound that covers a claim the
    /// registry does not yet list. See [`crate::row_id`].
    row_ids: Arc<RowIdAllocator>,
    /// Monotonic counter whose sole job is generating a collision-free
    /// filename prefix for each commit *attempt*'s data/delta-log files —
    /// deliberately independent of both the row-id allocator and the real
    /// manifest version. See `Transaction::commit` for why filenames must
    /// not be derived from `base_version`.
    write_attempt_counter: Arc<AtomicU64>,
    /// Serializes the conflict-check → graph-apply → manifest-commit →
    /// snapshot-swap critical section of `Transaction::commit`, and guards
    /// the recent-write-set history that check reads. Acquired at exactly
    /// one site (`Transaction::commit`).
    ///
    /// **Lock order: this, then `row_ids`' internal lock — never the
    /// reverse.** It is the outer of the crate's two locks: `commit`
    /// acquires `row_ids`' lock (via `claim`/`visibility_bound_excluding`/
    /// `RowIdClaim::release`) both before taking this one and while holding
    /// it, but nothing ever reaches for this one from inside `row_ids`. See
    /// [`crate::row_id`]'s module doc.
    commit_lock: Arc<Mutex<CommitLog>>,
    /// Counts every commit that hit `ConflictCheck::InsufficientHistory` —
    /// its read-version aged out of `COMMIT_LOG_CAPACITY` before it could
    /// commit. Pure observability, not used for any decision; exists so a
    /// real firing rate can be measured before ever building the more
    /// complex active-transaction-lifetime tracking this was weighed
    /// against. See [`Dataset::insufficient_history_conflict_count`].
    insufficient_history_conflicts: Arc<AtomicU64>,
    /// Lock-free issuance floor for `_timestamp` values — see
    /// `issue_timestamp`. Seeded from `Manifest.commit_time_high_water` on
    /// both `create` and `open`, so this floor survives a restart.
    last_issued_timestamp: Arc<AtomicI64>,
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

/// Captures the current wall-clock time (microseconds since the Unix
/// epoch) and issues it through `last_issued`, guaranteeing the returned
/// value is never less than any value this `Dataset` has issued before —
/// including across a wall-clock regression (an NTP step, a manual clock
/// change), not just under concurrency. See
/// `docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md` §3.
///
/// No `Mutex` is needed here, unlike `RowIdAllocator`: there is no paired
/// state that must advance atomically together, just one independent
/// integer high-water mark — a single `fetch_max` is sufficient.
///
/// # Errors
///
/// Returns [`TxnError::Clock`] if the system clock reports a time
/// before the Unix epoch (`SystemTime::now() < UNIX_EPOCH`), or
/// [`TxnError::TryFromInt`] if the current time in microseconds since the
/// epoch overflows `i64` (not reachable before the year 292471, but
/// checked rather than assumed).
///
/// Called once at the top of [`Transaction::commit`], before `write_phase`,
/// to stamp every row this commit writes with the single shared
/// `_timestamp` value.
fn issue_timestamp(last_issued: &AtomicI64) -> Result<i64> {
    let now_us = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| TxnError::Clock(e.to_string()))?
            .as_micros(),
    )?;
    let prev = last_issued.fetch_max(now_us, std::sync::atomic::Ordering::SeqCst);
    Ok(prev.max(now_us))
}

/// Determines the correct starting value for `write_attempt_counter` on
/// `Dataset::open`.
///
/// Normally this is simply `manifest.next_attempt_id`, persisted forward on
/// every commit (see `Manifest.next_attempt_id`'s doc comment). A manifest
/// that genuinely went through the current commit path always has
/// `next_attempt_id >= 1` after its very first commit — the counter is
/// `fetch_add`'d before every data/delta-log file write and persisted
/// forward every time.
///
/// A manifest written BEFORE `next_attempt_id` existed as a field
/// deserializes it as 0 via `#[serde(default)]`, even though `data_files`
/// may already hold legacy, VERSION-prefixed filenames
/// (`{version:020}-{i}.arrow`, from before the attempt-id naming scheme
/// replaced version-based naming). So `next_attempt_id == 0` together with
/// a non-empty `data_files` is an unambiguous signal this is a legacy
/// manifest needing migration — seeding at 0 would let the next commit's
/// *second* write reuse an attempt id that collides byte-for-byte with an
/// already-durable legacy filename, and `write_batch`'s `File::create`
/// would silently truncate it. Migrate by seeding one past the highest
/// attempt-id-shaped numeric prefix already used in `data_files`.
fn seed_write_attempt_counter(manifest: &Manifest) -> Result<u64> {
    if manifest.next_attempt_id != 0 || manifest.data_files.is_empty() {
        return Ok(manifest.next_attempt_id);
    }
    let highest = manifest
        .data_files
        .iter()
        .filter_map(|entry| parse_attempt_id_prefix(&entry.name));
    match highest.max() {
        // No entry parsed as an attempt-id-shaped prefix, so no existing
        // filename occupies that numeric namespace at all — seeding at 0
        // cannot collide with any of them, even though 0 looks like the
        // vulnerable value at a glance. Only reachable via a corrupt/
        // hostile manifest; every filename this codebase itself generates
        // parses.
        None => Ok(0),
        Some(highest) => highest.checked_add(1).ok_or_else(|| {
            TxnError::ManifestOverflow(format!("legacy attempt-id prefix {highest} + 1"))
        }),
    }
}

/// Parses the leading `{prefix}-{i}.ext` numeric prefix from a data-file
/// name, if present. Used only by [`seed_write_attempt_counter`]'s legacy-
/// manifest migration path — every filename this codebase itself ever
/// generates (version-prefixed or attempt-id-prefixed) matches this shape.
fn parse_attempt_id_prefix(file_name: &str) -> Option<u64> {
    file_name.split('-').next()?.parse().ok()
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
        Self::create_with_commit_log_capacity(dir, COMMIT_LOG_CAPACITY)
    }

    /// Same as [`Dataset::create`], but with an explicit `CommitLog`
    /// capacity instead of the production [`COMMIT_LOG_CAPACITY`] default.
    /// Private to this module (no `pub`/`pub(crate)` — even more
    /// restrictive than crate-wide reach) — exists so the wraparound/`InsufficientHistory`
    /// regression tests can prove the eviction logic is correct at a small
    /// capacity (milliseconds) instead of paying the real production
    /// capacity's fill cost (which grows with the capacity itself, since
    /// proving eviction requires actually filling it — see the git history
    /// around `COMMIT_LOG_CAPACITY`'s 256→2048 bump for the ~50x test
    /// runtime cost that motivated this). The capacity value itself has no
    /// bearing on whether the conflict-detection *logic* is correct, only
    /// on how much history it retains — so testing the logic at a small
    /// capacity is exactly as rigorous as testing it at the real one.
    fn create_with_commit_log_capacity(
        dir: impl Into<PathBuf>,
        commit_log_capacity: usize,
    ) -> Result<Self> {
        let dir = dir.into();
        if read_current(&dir)?.is_some() {
            return Err(TxnError::AlreadyExists(dir));
        }
        std::fs::create_dir_all(dir.join("data"))?;
        let manifest = Manifest::empty();
        commit_manifest(&dir, &manifest)?;
        let last_issued_timestamp = Arc::new(AtomicI64::new(manifest.commit_time_high_water));
        let graph = new_hnsw_index(0)?;
        let row_ids = Arc::new(RowIdAllocator::new(manifest.next_row_id));
        let write_attempt_counter = Arc::new(AtomicU64::new(manifest.next_attempt_id));
        let snapshot = Snapshot {
            dir: dir.clone(),
            version: manifest.version,
            watermark: manifest.next_row_id.saturating_sub(1),
            // A freshly created dataset has no transaction in flight.
            in_flight: Vec::new().into(),
            manifest: Arc::new(manifest),
            index: strata_index::SegmentSet::from_live(Arc::new(graph)),
            tombstones: Arc::new(imbl::HashSet::new()),
        };
        Ok(Self {
            dir,
            current: Arc::new(ArcSwap::new(Arc::new(snapshot))),
            row_ids,
            write_attempt_counter,
            commit_lock: Arc::new(Mutex::new(CommitLog::new(commit_log_capacity))),
            insufficient_history_conflicts: Arc::new(AtomicU64::new(0)),
            last_issued_timestamp,
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
        let row_ids = Arc::new(RowIdAllocator::new(manifest.next_row_id));
        // The real fix for the cross-session filename-collision bug: seed
        // from the persisted `manifest.next_attempt_id`, not 0. Without
        // this, a reopened dataset would regenerate the same
        // `{attempt_id:020}-{i}.arrow`/`.deltalog` filenames a prior
        // session already committed, and `write_batch`'s `File::create`
        // would silently truncate and destroy that prior session's
        // already-durable data. See `Manifest.next_attempt_id`'s doc
        // comment and `Transaction::commit`, which persists this counter's
        // value forward on every commit the same way it does
        // the row-id allocator -> `manifest.next_row_id`.
        //
        // A second, narrower case `next_attempt_id` alone doesn't cover: a
        // manifest written BEFORE this field existed deserializes it as 0
        // via `#[serde(default)]`, even though `data_files` may already
        // hold legacy, VERSION-prefixed filenames (`{version:020}-{i}...`,
        // from before the attempt-id naming scheme replaced version-based
        // naming). Seeding at 0 in that case reproduces the exact same
        // collision-and-silent-truncation bug against those legacy files.
        // `seed_write_attempt_counter` detects and migrates this case; see
        // its own doc comment.
        let write_attempt_counter =
            Arc::new(AtomicU64::new(seed_write_attempt_counter(&manifest)?));
        let last_issued_timestamp = Arc::new(AtomicI64::new(manifest.commit_time_high_water));
        let snapshot = Snapshot {
            dir: dir.clone(),
            version: manifest.version,
            watermark: manifest.next_row_id.saturating_sub(1),
            // Nothing is in flight in a process that has just opened this
            // dataset. A *prior* session's abandoned claims need no entry
            // either: `manifest.next_row_id` may cover them, but their data
            // files never entered a manifest and their delta logs are not
            // replayed, so nothing exists at those ids to be found. That is
            // why this hazard was only ever transient, never survivable
            // across a restart.
            in_flight: Vec::new().into(),
            manifest: Arc::new(manifest),
            index: strata_index::SegmentSet::from_live(Arc::new(graph)),
            tombstones: Arc::new(tombstones),
        };
        Ok(Self {
            dir,
            current: Arc::new(ArcSwap::new(Arc::new(snapshot))),
            row_ids,
            write_attempt_counter,
            commit_lock: Arc::new(Mutex::new(CommitLog::new(COMMIT_LOG_CAPACITY))),
            insufficient_history_conflicts: Arc::new(AtomicU64::new(0)),
            last_issued_timestamp,
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
            base_version: snapshot.version,
            graph: snapshot.index.live_arc(),
            pending: Vec::new(),
            pending_tombstones: Vec::new(),
            write_set: Vec::new(),
            current: Arc::clone(&self.current),
            row_ids: Arc::clone(&self.row_ids),
            write_attempt_counter: Arc::clone(&self.write_attempt_counter),
            commit_lock: Arc::clone(&self.commit_lock),
            insufficient_history_conflicts: Arc::clone(&self.insufficient_history_conflicts),
            last_issued_timestamp: Arc::clone(&self.last_issued_timestamp),
            #[cfg(any(test, loom))]
            inject_manifest_commit_failure: false,
            #[cfg(test)]
            pause_after_row_id_claim: None,
            #[cfg(test)]
            pause_after_graph_apply: None,
        }
    }

    /// How many commits have hit `ConflictCheck::InsufficientHistory` over
    /// this `Dataset` handle's lifetime — a transaction whose read-version
    /// aged out of the bounded commit-log before it could commit, and was
    /// therefore conservatively rejected even though it may never have
    /// touched a contested row. Pure observability: this number existing
    /// and staying at (or near) zero under real workloads is the evidence
    /// needed before ever building active-transaction-lifetime tracking to
    /// eliminate the false-positive case entirely.
    #[must_use]
    pub fn insufficient_history_conflict_count(&self) -> u64 {
        self.insufficient_history_conflicts
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub struct Transaction {
    dir: PathBuf,
    /// This transaction's read-version, captured at `begin()`. The only
    /// piece of `base`-time state this type retains — `commit()` rebuilds
    /// `data_files`/`tombstones`/the rest of the manifest from the *latest*
    /// snapshot inside the lock, not from anything captured here, so
    /// cloning the whole `Manifest` at `begin()` time (as earlier Phase 6
    /// commits did) was pure waste: every field but `version` went unused.
    base_version: u64,
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
    row_ids: Arc<RowIdAllocator>,
    write_attempt_counter: Arc<AtomicU64>,
    commit_lock: Arc<Mutex<CommitLog>>,
    insufficient_history_conflicts: Arc<AtomicU64>,
    /// Consumed by [`Transaction::commit`] via `issue_timestamp`, as the
    /// very first step of `commit`, before `write_phase` runs.
    last_issued_timestamp: Arc<AtomicI64>,
    /// Test-only fault injection: makes [`Transaction::commit`]'s durability
    /// step fail, modelling a recoverable I/O error (e.g. ENOSPC) *after*
    /// this commit's deltas have already reached the shared graph. See
    /// [`Transaction::inject_manifest_commit_failure`].
    ///
    /// Scoped to one `Transaction` rather than a thread-local because
    /// `loom` multiplexes its model threads, which would let a thread-local
    /// flag armed for one transaction be consumed by another.
    #[cfg(any(test, loom))]
    inject_manifest_commit_failure: bool,
    /// Test-only: stops this commit at the instant its row-ids have been
    /// claimed but nothing shared has been touched yet. See [`Checkpoint`].
    #[cfg(test)]
    pause_after_row_id_claim: Option<Checkpoint>,
    /// Test-only: stops this commit at the instant its vectors are in the
    /// shared graph but the commit is not yet durable. See [`Checkpoint`].
    #[cfg(test)]
    pause_after_graph_apply: Option<Checkpoint>,
}

/// Test-only rendezvous that stops a [`Transaction::commit`] at an exact
/// instant so another thread can observe the shared state *as of that
/// instant*, then releases it.
///
/// The windows this crate's snapshot-isolation regression tests care about
/// are a single `fsync` wide, so racing them with sleeps would be flaky in
/// both directions — a missed window silently passes. A checkpoint turns
/// the race into a deterministic schedule: the committing thread blocks
/// until the observing thread has looked.
///
/// Gated on `cfg(test)` alone, unlike
/// [`Transaction::inject_manifest_commit_failure`]'s `cfg(any(test,
/// loom))` — not an oversight. `--cfg loom` is layered *on top of* the test
/// profile (see `.claude/rules/concurrency-txn-layer.md`'s `cargo rustc
/// -p strata-txn --lib --profile test -- --cfg loom` recipe), so `test` is
/// set in a loom build too and these still compile there. The wider gate on
/// the injector exists because a loom model uses it; nothing here needs to
/// widen until a loom model blocks on a rendezvous — which it must not,
/// since loom schedules the threads itself.
#[cfg(test)]
pub(crate) struct Checkpoint {
    reached: std::sync::mpsc::SyncSender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}

/// The test side of a [`Checkpoint`] — see [`checkpoint_pair`].
#[cfg(test)]
pub(crate) struct CheckpointControl {
    reached: std::sync::mpsc::Receiver<()>,
    resume: std::sync::mpsc::SyncSender<()>,
}

#[cfg(test)]
impl Checkpoint {
    /// Announces that the committing thread has reached this point and
    /// blocks until the test releases it. Both halves are deliberately
    /// infallible-by-ignoring: if the test side has been dropped, a commit
    /// that would otherwise hang forever simply runs on.
    fn arrive(&self) {
        let _ = self.reached.send(());
        let _ = self.resume.recv();
    }
}

// A test rig deadlocking or losing its peer is a test bug, and panicking
// at the exact call is the most useful place to learn about it — the
// alternative is a hang with no output. Same rationale as `mod tests`'
// blanket allow; this type only exists under `cfg(test)`.
#[cfg(test)]
#[allow(clippy::expect_used)]
impl CheckpointControl {
    /// Blocks until the committing thread reaches its checkpoint.
    fn wait(&self) {
        self.reached
            .recv()
            .expect("committing thread dropped before reaching the checkpoint");
    }

    /// Lets the committing thread continue past its checkpoint.
    fn release(&self) {
        self.resume
            .send(())
            .expect("committing thread dropped before it could be released");
    }
}

/// Builds a linked [`Checkpoint`]/[`CheckpointControl`] pair. Rendezvous
/// channels rather than a `Barrier` so the observing side controls *both*
/// edges: it learns when the commit arrived, looks at whatever it needs to,
/// and only then lets the commit proceed.
#[cfg(test)]
pub(crate) fn checkpoint_pair() -> (Checkpoint, CheckpointControl) {
    let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(1);
    (
        Checkpoint {
            reached: reached_tx,
            resume: resume_rx,
        },
        CheckpointControl {
            reached: reached_rx,
            resume: resume_tx,
        },
    )
}

/// Undoes this commit's in-memory graph inserts if the commit never reaches
/// its durability point.
///
/// [`Transaction::commit`] applies each `Insert` delta's vector to the
/// shared `Arc<HnswIndex>` *before* `commit_manifest` makes the commit
/// durable — a Phase 5 optimization (apply only this commit's own new
/// deltas instead of replaying all history) layered on top of the spec's
/// disk-based model, which has no such in-memory step. Without this guard,
/// any failure between the first `graph.insert` and a successful
/// `commit_manifest` leaves that transaction's vectors in the shared graph
/// with no manifest entry backing them. Their row-ids were already claimed
/// by `write_phase`, so the *next* successful commit persists a
/// `manifest.next_row_id` past them and publishes a `watermark` covering
/// them — at which point [`crate::Snapshot::is_visible`] starts passing and
/// `vector_search` returns them as dangling hits: rows `scan` can never
/// see, because their data files never entered the manifest. Row-id gaps
/// from a failed attempt are explicitly safe (spec §8); a *searchable* gap
/// is not, and is exactly what the "no silently stale vector search
/// results" claim rules out.
///
/// On drop this soft-deletes every row-id it recorded, unless
/// [`Self::disarm`] was called — which happens only once `commit_manifest`
/// has succeeded and those rows are genuinely committed. Being a `Drop`
/// impl rather than an `if let Err(..)` arm, it fires on **both** an early
/// `?` return and a panic unwinding out of the apply loop.
///
/// It must be declared *after* `commit`'s `commit_lock` guard, so
/// reverse-declaration drop order runs this compensation before the lock is
/// released, rather than leaving residue live for the next committer to
/// build on.
///
/// **What this closes, and what covers the rest.** This guard is what
/// guarantees no *permanent* residue: once the failing committer returns,
/// nothing it inserted is reachable by any later search. It is deliberately
/// not what keeps a residue row-id invisible *while* the failing commit is
/// still running — between its `graph.insert` and this guard firing, the
/// row is physically in the shared graph, and readers take no
/// `commit_lock`. That narrower window is [`crate::row_id`]'s job: the
/// row-ids were claimed before the lock and stay registered as in-flight
/// until this transaction reaches its durability point, so every snapshot
/// published in the meantime excludes them. The two compose — the
/// exclusion set hides the residue while the commit is in flight, and this
/// guard removes it before the claim is released, so there is no instant at
/// which the row is both un-excluded and still in the graph.
///
/// The same in-flight exclusion also covers the *success* path, which this
/// guard never touched: before it existed, a reader could see a row between
/// another transaction's `graph.insert` and its `commit_manifest` even when
/// that commit went on to succeed — a plain violation of spec §2's "not
/// visible to any other transaction until commit succeeds."
struct GraphResidueGuard {
    /// Its own `Arc` clone rather than a borrow of `Transaction::graph`, so
    /// `commit` can still move that field into the new `Snapshot`.
    graph: Arc<HnswIndex>,
    applied: Vec<u64>,
    armed: bool,
}

impl GraphResidueGuard {
    fn new(graph: Arc<HnswIndex>) -> Self {
        Self {
            graph,
            applied: Vec::new(),
            armed: true,
        }
    }

    /// Records a row-id whose vector has just entered the shared graph.
    fn record(&mut self, row_id: u64) {
        self.applied.push(row_id);
    }

    /// Marks this commit as past its durability point: the recorded row-ids
    /// are genuinely committed now and must survive this guard's drop.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for GraphResidueGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        for &row_id in &self.applied {
            self.graph.remove(row_id);
        }
    }
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

    /// Test-only: makes this transaction's [`Self::commit`] fail at its
    /// durability step, *after* its deltas have already been applied to the
    /// shared graph. Models a recoverable I/O failure (e.g. ENOSPC writing
    /// the manifest) — the one failure shape that leaves the process alive
    /// and therefore exposes the dangling-search-hit hazard
    /// [`GraphResidueGuard`] closes. The `chaos-injection` harness cannot
    /// stand in for this: its `chaos_checkpoint` calls
    /// `std::process::abort()`, and the restart that forces *heals* the
    /// hazard, since `replay_index` rebuilds only from manifest-listed
    /// delta logs.
    #[cfg(any(test, loom))]
    pub(crate) fn inject_manifest_commit_failure(&mut self) {
        self.inject_manifest_commit_failure = true;
    }

    /// Test-only: stops [`Self::commit`] once this transaction's row-ids
    /// have been claimed and its data files written, but *before* it
    /// acquires `commit_lock` — so a concurrent committer can run to
    /// completion while this transaction's claim is outstanding.
    #[cfg(test)]
    pub(crate) fn pause_after_row_id_claim(&mut self, checkpoint: Checkpoint) {
        self.pause_after_row_id_claim = Some(checkpoint);
    }

    /// Test-only: stops [`Self::commit`] once its vectors are in the shared
    /// graph but `commit_manifest` has not yet made them durable — the
    /// instant at which an uncommitted row is physically reachable by a
    /// reader that takes no lock.
    #[cfg(test)]
    pub(crate) fn pause_after_graph_apply(&mut self, checkpoint: Checkpoint) {
        self.pause_after_graph_apply = Some(checkpoint);
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
    /// dimension mismatch), or if the manifest commit's atomic rename fails.
    ///
    /// **Every one of these leaves the dataset with nothing this transaction
    /// wrote reachable by any later reader.** The manifest stays unadvanced,
    /// so the new data/delta-log files are orphaned on disk and invisible to
    /// [`crate::Snapshot::scan`], which reads only manifest-listed files.
    /// That much has always held. What it does *not* cover on its own is the
    /// shared in-memory graph: delta-application runs before the manifest
    /// commit, so a failure after it would otherwise leave this
    /// transaction's vectors physically in the graph, and a later commit's
    /// watermark would eventually make them visible to
    /// [`crate::Snapshot::vector_search`] — a hit `scan` could never
    /// corroborate. [`GraphResidueGuard`] soft-deletes them on the way out
    /// (on an early return *or* a panic), which is what makes "invisible"
    /// true for the search path too, not just for `scan`. See that type for
    /// the one narrower, pre-existing window this does not close.
    ///
    /// Three in-memory traces do outlive a failed commit, none of them
    /// reachable as data: the row-ids it claimed (never recycled — a row-id
    /// gap is explicitly safe, a *searchable* gap is not, spec §8); the
    /// soft-deleted nodes themselves, which stay physically present as
    /// traversal waypoints until Phase 8 compaction, so repeated failures
    /// accumulate memory until restart; and, if this was the first-ever
    /// vector commit, the graph's established dimension, which
    /// `check_or_establish_dimension` sets permanently and no removal
    /// resets. That last one means a retry at a *different* dimension stays
    /// rejected for the rest of the session and then succeeds after a
    /// restart (`replay_index` rebuilds from manifest-listed logs only) — a
    /// typed error, never a wrong answer.
    ///
    /// **Formerly a known limitation, now closed:** earlier, a commit whose
    /// pending batches had inconsistent vector dimensions across batches
    /// could partially mutate the shared graph before failing — `Insert`
    /// deltas are applied to the graph in pending-batch order, so a later
    /// batch's dimension mismatch was only caught after an earlier batch's
    /// deltas had already landed in the live, shared `HnswIndex`.
    /// [`validate_delta_dimensions`] now runs before any delta is applied,
    /// rejecting the entire commit — with zero graph mutation — the moment
    /// any two pending batches (or a pending batch and the graph's
    /// already-established dimension) disagree. The residual cases that
    /// pre-validation alone cannot cover — a failure *after* the first delta
    /// has landed, whether from a concurrent first-insert establishing a
    /// different dimension between the pre-lock check and the in-lock apply,
    /// an I/O failure in the manifest commit, or a panic mid-loop — are
    /// covered by [`GraphResidueGuard`] instead.
    pub fn commit(self) -> Result<()> {
        let ts = issue_timestamp(&self.last_issued_timestamp)?;
        let data_dir = data_subdir(&self.dir);

        let (new_data_files, deltas, mut claim) = self.write_phase(&data_dir, ts)?;
        validate_delta_dimensions(&deltas, &self.graph)?;

        // Test-only rendezvous: row-ids claimed, data files written, but
        // `commit_lock` not yet acquired and the shared graph not yet
        // touched. Absent entirely from production builds.
        #[cfg(test)]
        if let Some(checkpoint) = &self.pause_after_row_id_claim {
            checkpoint.arrive();
        }

        // Everything from here is the tightly-scoped critical section:
        // re-read latest state, conflict-check, apply, commit, swap. See
        // design doc §5. This is the crate's *outer* lock and its only
        // acquisition site; the row-id allocator's lock is the inner one
        // and is taken below while this is held (never the reverse — see
        // `Dataset::commit_lock`'s doc and `crate::row_id`). A poisoned
        // lock (a prior committer panicked) is recovered rather than
        // propagated — the CommitLog is only ever mutated by `push` as the
        // final in-memory step after a durable commit, so it can't be
        // observed half-updated.
        let mut commit_log = self
            .commit_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let latest_snapshot = self.current.load_full();
        let latest_version = latest_snapshot.version;

        // Conflict detection MUST run before any mutation of the shared
        // graph: a transaction that turns out to conflict must leave the
        // graph completely untouched.
        match commit_log.conflicts_with(self.base_version, latest_version, &self.write_set) {
            ConflictCheck::Clean => {}
            ConflictCheck::Conflict(contested_row_ids) => {
                return Err(TxnError::Conflict { contested_row_ids });
            }
            ConflictCheck::InsufficientHistory => {
                self.insufficient_history_conflicts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(TxnError::Conflict {
                    contested_row_ids: self.write_set.clone(),
                });
            }
        }

        let new_version = latest_version
            .checked_add(1)
            .ok_or_else(|| TxnError::ManifestOverflow(format!("version {latest_version} + 1")))?;

        // Apply only this commit's new deltas to the shared graph — the
        // fix for the O(historical)-per-commit regression. Extending the
        // same Arc'd instance every commit matches what every
        // existing/future Snapshot's Arc<HnswIndex> already points at; what
        // makes that safe is that a node is only ever *added* here, and
        // `is_visible`'s watermark decides whether readers may see it. The
        // graph does have a removal API (`HnswIndex::remove`, a soft-delete
        // — the backend is `crates/index`'s own lock-free graph, not
        // `hnsw_rs`), but its only use on this path is
        // `GraphResidueGuard`'s undo of a commit that never became durable.
        // Tombstones layer on top of the *latest*
        // snapshot's set (not this transaction's stale begin()-time view),
        // so a clean commit composes with everything that landed in
        // between.
        let mut tombstones = latest_snapshot.tombstones.as_ref().clone();
        // Declared after `commit_log` above, so reverse-declaration drop
        // order compensates the shared graph *before* the commit lock is
        // released on any early return or panic below. Tombstone deltas
        // need no compensation: they only touch the `tombstones` local,
        // which is discarded unless the new `Snapshot` is published.
        //
        // It must equally be declared *after* `claim`, for the same reason
        // in the other direction: reverse-declaration drop order then
        // scrubs the graph before the claim is released, so a residue
        // row-id is never simultaneously un-excluded and still in the
        // graph. Rebinding `claim` below this line would silently reopen
        // the very window this whole mechanism closes, with every test
        // still green — see [`GraphResidueGuard`]'s doc.
        let mut residue_guard = GraphResidueGuard::new(Arc::clone(&self.graph));
        for delta in deltas {
            match delta {
                DeltaEntry::Insert { row_id, vector } => {
                    self.graph.insert_owned(row_id, vector)?;
                    residue_guard.record(row_id);
                }
                DeltaEntry::Tombstone { row_id } => {
                    tombstones.insert(row_id);
                }
            }
        }
        // Test-only rendezvous: this commit's vectors are physically in the
        // shared graph, but `commit_manifest` below has not yet made them
        // durable. Absent entirely from production builds.
        #[cfg(test)]
        if let Some(checkpoint) = &self.pause_after_graph_apply {
            checkpoint.arrive();
        }

        // The new manifest is likewise built from the latest snapshot's
        // manifest: this transaction's new data files are *appended* to
        // the latest file list (never substituted for it wholesale —
        // that would silently drop data files committed by concurrent,
        // non-conflicting transactions after this one began).
        let mut manifest = latest_snapshot.manifest.as_ref().clone();
        manifest.version = new_version;
        manifest.data_files.extend(new_data_files);
        // The bound and the exclusion set this commit's snapshot will carry,
        // read as one unit under the allocator lock so they cannot disagree
        // — the disagreement being precisely the bug this closes. Every
        // *other* transaction's outstanding claim is excluded; this one's
        // own is not, because it is about to become durable and an
        // acknowledged write must be immediately visible.
        //
        // Read here, while `commit_lock` is held, so no other transaction
        // can publish a snapshot between this read and the store below.
        // `next_row_id` is the allocation high-water mark, which is what
        // the manifest must persist for restart safety (a reopened dataset
        // must never reuse an id, committed or abandoned — spec §8).
        let visibility = self.row_ids.visibility_bound_excluding(claim.as_ref());
        manifest.next_row_id = visibility.next_row_id;
        // SeqCst ordering justification for the `.load()` below (distinct
        // from the pre-lock fetch_add's justification in `write_phase`): it must
        // observe a value at least as large as every commit that landed
        // before this one, including this transaction's own prior
        // fetch_add — not just its own thread's value. That's guaranteed
        // by commit_lock's Acquire/Release chain across successive lock
        // holders, transitively carrying each earlier committer's
        // program-order-prior fetch_add forward to every later committer's
        // load — a property of the Mutex, not of the fetch_add/load calls'
        // own Ordering (Relaxed on both ends would be equally sound here).
        // SeqCst is kept as the simple, always-correct default; see the
        // fetch_add comment above for why the negligible cost isn't worth
        // trading for reduced auditability.
        //
        // Persist the counter's current value (already past this commit's
        // own attempt_id, via `write_phase`'s fetch_add) so a future
        // Dataset::open never regenerates a filename this session already
        // committed. See Manifest.next_attempt_id's doc comment.
        manifest.next_attempt_id = self
            .write_attempt_counter
            .load(std::sync::atomic::Ordering::SeqCst);
        // Non-decreasing across versions by construction: this is a running
        // max computed under commit_lock, decoupled from any individual
        // row's own captured value — see
        // docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md
        // §4 for why that decoupling is deliberate, not a gap.
        manifest.commit_time_high_water = manifest.commit_time_high_water.max(ts);
        // Dedup against both the current in-memory tombstone set and
        // duplicates within this same transaction's own pending_tombstones
        // (e.g. two delete() calls on the same row): without this check,
        // an idempotent re-delete of an already-tombstoned row would grow
        // the *persisted* manifest.tombstones Vec unboundedly (cloned,
        // JSON-serialized, and fsynced on every future commit; replayed on
        // every open), even though the in-memory imbl::HashSet below already
        // dedupes for free.
        for row_id in &self.pending_tombstones {
            if !tombstones.contains(row_id) {
                manifest.tombstones.push(*row_id);
            }
            tombstones.insert(*row_id);
        }

        // Test-only fault injection modelling a recoverable I/O failure
        // (e.g. ENOSPC) of the durability step below, occurring *after* this
        // commit's deltas have already been applied to the shared graph.
        // Absent entirely from production builds.
        #[cfg(any(test, loom))]
        if self.inject_manifest_commit_failure {
            return Err(TxnError::Io(std::io::Error::other(
                "injected manifest-commit failure (test fault injection)",
            )));
        }

        commit_manifest(&self.dir, &manifest)?;

        // Past the durability point: this commit's graph inserts are now
        // genuinely committed and must survive the guard's drop. Disarmed
        // here rather than after the snapshot swap because *this* is the
        // instant the rows become committed — nothing after it may undo
        // them, even if a later step were to fail.
        residue_guard.disarm();

        // Same instant, same reason: these row-ids are committed, so they
        // must stop being excluded from *later* commits' snapshots. Doing
        // it explicitly here rather than leaving it to the claim's `Drop`
        // keeps the release inside `commit_lock`, so no other transaction
        // can publish a snapshot between the durability point and the
        // release and briefly hide rows that are already durable. On any
        // earlier return the `Drop` still fires and the ids simply become
        // permanent gaps, which spec §8 explicitly allows.
        if let Some(claim) = &mut claim {
            claim.release();
        }

        commit_log.push(new_version, self.write_set);

        // Only after commit_manifest succeeds does the new state become
        // visible to future Dataset::snapshot() calls — the in-memory swap
        // must never run ahead of the on-disk durability point.
        let watermark = manifest.next_row_id.saturating_sub(1);
        let snapshot = Snapshot {
            dir: self.dir,
            version: new_version,
            manifest: Arc::new(manifest),
            index: strata_index::SegmentSet::from_live(self.graph),
            watermark,
            in_flight: visibility.in_flight,
            tombstones: Arc::new(tombstones),
        };
        self.current.store(Arc::new(snapshot));

        Ok(())
    }

    /// Spec §3 step 3's durable write, run *before* `commit_lock` is
    /// acquired. Claims this transaction's row-ids, writes its data and
    /// delta-log files, and fsyncs them — none of which needs conflict
    /// information to proceed, and none of which can collide with a
    /// concurrent transaction's own writes, because every path it touches
    /// is unique to this attempt.
    ///
    /// The filename prefix comes from `write_attempt_counter`, **not**
    /// `base_version + 1`: two truly concurrent transactions can share the
    /// same stale `base_version`, which would make them compute the same
    /// "next version" and collide on the same filename before either
    /// reaches `commit_lock`. `write_attempt_counter` is unique per attempt
    /// regardless of version, which is what makes doing any of this outside
    /// the lock safe at all.
    ///
    /// Returns the new `DataFileEntry`s, this commit's delta entries, and
    /// the row-id claim to hold until the commit reaches its durability
    /// point (`None` for a delete-only transaction, which inserts no rows,
    /// claims no row-ids, and has nothing to hide from concurrent readers).
    ///
    /// # Errors
    ///
    /// Same conditions as [`Transaction::commit`]'s own doc comment:
    /// dictionary-encoding failure, a non-finite vector component, an I/O
    /// failure writing or fsyncing a file, or [`TxnError::ManifestOverflow`]
    /// if the row-id range would run past `u64::MAX`.
    fn write_phase(
        &self,
        data_dir: &Path,
        ts: i64,
    ) -> Result<(Vec<DataFileEntry>, Vec<DeltaEntry>, Option<RowIdClaim>)> {
        // Skipped entirely when there's nothing to insert: a delete-only
        // transaction writes no new files, so there's no new directory
        // entry to create or fsync and no attempt_id needs reserving.
        // `Dataset::create`/`open` already ensure `data_dir` exists once
        // per `Dataset` lifetime; recreating it on every single commit
        // regardless of whether it had anything to write was redundant.
        if self.pending.is_empty() {
            return Ok((Vec::new(), Vec::new(), None));
        }
        for batch in &self.pending {
            for name in HIDDEN_COLUMNS {
                if batch.schema_ref().index_of(name).is_ok() {
                    return Err(TxnError::ReservedColumnName((*name).to_string()));
                }
            }
        }
        std::fs::create_dir_all(data_dir)?;
        // SeqCst ordering justification (this pre-lock fetch_add): the
        // only property it needs is per-atomic RMW uniqueness — no two
        // fetch_adds on the same AtomicU64 ever return the same value
        // — which every atomic's own total modification order already
        // guarantees regardless of the chosen Ordering, even Relaxed.
        // commit_lock plays no role here; this runs *before* it's
        // acquired. (The *load* site in `commit`, which persists this
        // counter's value into the manifest, has a different
        // justification — see the comment there.) SeqCst is kept
        // anyway as the simple, always-correct default: the cost
        // difference against this function's dominant work (fsync,
        // JSON serialization) is immaterial, and isn't worth the
        // reduced auditability of proving a weaker ordering correct
        // per site. See `.claude/rules/concurrency-txn-layer.md`.
        //
        // Row-ids are *not* handed out this way. They need the counter
        // advance and the in-flight registration to happen as one
        // atomic step, which no lone atomic can give — see
        // `crate::row_id` and `RowIdAllocator::claim`.
        let attempt_id = self
            .write_attempt_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // One claim for the whole transaction, spec §8's "a commit writing
        // N rows atomically claims the contiguous range `[next_row_id,
        // next_row_id + N)`" — rather than the per-pending-batch claim this
        // replaces, which could interleave one transaction's batches with
        // another's under concurrency and would put one exclusion entry per
        // batch into every concurrent snapshot.
        let total_rows = self.pending.iter().try_fold(0u64, |total, batch| {
            let rows = u64::try_from(batch.num_rows())?;
            total
                .checked_add(rows)
                .ok_or_else(|| TxnError::ManifestOverflow(format!("pending rows {total} + {rows}")))
        })?;
        let claim = self.row_ids.claim(total_rows)?;
        let mut new_data_files = Vec::new();
        let deltas = Self::write_pending_batches(
            &self.pending,
            data_dir,
            attempt_id,
            &claim,
            ts,
            &mut new_data_files,
        )?;
        // Fsyncing each data file's *content* (already done inside
        // write_batch) is not sufficient — the new directory entries
        // themselves must also be fsynced, or a real power-loss crash
        // can leave a file's bytes durable while the file itself is
        // absent. Must happen before the graph update/manifest commit.
        strata_storage::sync_dir(data_dir)?;
        Ok((new_data_files, deltas, Some(claim)))
    }

    /// Writes every pending batch's data file and delta-log file to
    /// `data_dir`, assigning row-ids out of `claim` and appending
    /// each batch's `DataFileEntry` to `data_files` in place. Returns every
    /// `DeltaEntry` produced across all pending batches, in order —
    /// `Transaction::commit` applies these directly to the shared graph
    /// instead of re-reading them from disk.
    ///
    /// `attempt_id` is a collision-free filename-uniqueness token from
    /// `Dataset.write_attempt_counter` — **not** a manifest version. It
    /// exists only so concurrent callers never write to the same path;
    /// see `Transaction::commit` (Task 6) for why it can't be derived
    /// from `base_version` instead.
    ///
    /// `claim` is the transaction's single, already-registered row-id
    /// range, sized to hold every pending batch's rows. Batches are laid
    /// out consecutively inside it, so the ids of one transaction's batches
    /// are contiguous even under concurrency — the per-batch `fetch_add`
    /// this replaces could interleave two transactions' batches. The claim
    /// stays outstanding for as long as it is borrowed here and beyond, up
    /// to this commit's durability point, which is what keeps these
    /// not-yet-committed ids invisible to concurrent readers; see
    /// [`crate::row_id`].
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`Transaction::commit`]'s
    /// own doc comment (dictionary-encoding failure, non-finite vector
    /// component, or an I/O failure writing a data/delta-log file). Row-id
    /// overflow is no longer possible here — the whole range was bounds-checked
    /// when it was claimed.
    fn write_pending_batches(
        pending: &[RecordBatch],
        data_dir: &Path,
        attempt_id: u64,
        claim: &RowIdClaim,
        ts: i64,
        data_files: &mut Vec<DataFileEntry>,
    ) -> Result<Vec<DeltaEntry>> {
        let mut all_deltas = Vec::new();
        let mut row_id_base = claim.base();
        for (i, batch) in pending.iter().enumerate() {
            // Stats computed on the original, pre-encoding, pre-hidden-column
            // batch — see .claude/docs/design/phase-3-query-refinement-spec.md
            // §1 for why (logical values, no dictionary-decode step needed
            // later). _row_id gets no stats entry (nothing predicates on it);
            // _timestamp DOES, inserted explicitly below, since every row in
            // this batch shares one value and it exists specifically to be
            // predicated on and pruned by — see design doc §7.
            let mut stats = compute_stats(batch);
            stats.insert(
                TIMESTAMP_COLUMN.to_string(),
                ColumnStats {
                    min: Value::Int64(ts),
                    max: Value::Int64(ts),
                },
            );

            let num_rows = u64::try_from(batch.num_rows())?;

            let deltas = build_delta_entries(batch, row_id_base)?;
            let with_row_id = append_row_id_column(batch, row_id_base, num_rows)?;
            let with_timestamp = append_timestamp_column(&with_row_id, ts, num_rows)?;

            let encoded = strata_storage::encode_batch(&with_timestamp)?;
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
            // Cannot overflow: `write_phase` sized the claim as the checked
            // sum of every pending batch's row count, and the claim itself
            // was bounds-checked against `u64::MAX` before it was handed
            // out.
            row_id_base += num_rows;
        }
        // `pending` and `claim` arrive as separate parameters, so nothing in
        // the type system ties the claim's size to the rows about to be
        // laid out inside it. If they ever diverge, this hands out row-ids
        // *past* the claimed range — ids no snapshot's exclusion set covers,
        // which is exactly the un-hidden-uncommitted-row hazard `claim`
        // exists to prevent, and it would fail silently. Cheap to assert at
        // the one place being wrong is invisible.
        debug_assert_eq!(
            row_id_base,
            claim.base() + claim.len(),
            "every claimed row-id must be consumed, and none beyond them"
        );
        Ok(all_deltas)
    }
}

// HNSW parameter defaults — tuned via benchmarks, not guessed, per
// .claude/rules/vector-index.md.
const HNSW_MAX_NB_CONNECTION: usize = 16;
const HNSW_MAX_LAYER: usize = 16;
// ef_construction is the build-cost dial: the insert-time saturation
// early-exit is disabled, so every build traversal runs the full beam and
// cost is near-linear in this value. Lowered 200 -> 100 based on
// bench/benches/ef_construction_sweep_bench.rs (run it to reproduce; the
// numbers below are its default 100k-row / 200-query configuration, the
// same scale as vector_search_bench):
//   - recall@10: 0.9855 (ef=200) -> 0.9820 (ef=100), a 0.35pp drop, still
//     well clear of this project's recall@10 >= 0.9 ship floor.
//   - build time: 225.15s (ef=200) -> 101.26s (ef=100), a 2.2x speedup.
//   - cross-checked two other ways: production-path recall via
//     vector_search_bench (100k rows, real Dataset) gives the same
//     direction and magnitude (0.9850 -> 0.9800); end-to-end ingest+commit
//     and recovery wall time via lifecycle_bench (25k rows, real commit
//     path) both drop ~1.8x (37.30s -> 21.17s, 36.12s -> 19.41s).
const HNSW_EF_CONSTRUCTION: usize = 100;

fn new_hnsw_index(capacity: usize) -> Result<HnswIndex> {
    Ok(HnswIndex::new(
        MaxConnections(HNSW_MAX_NB_CONNECTION),
        MaxElements(capacity.max(1)),
        MaxLayers(HNSW_MAX_LAYER),
        EfConstruction(HNSW_EF_CONSTRUCTION),
    )?)
}

/// Sane ceiling for a manifest's `next_row_id`, enforced at open before any
/// row-id from that manifest can reach the index.
///
/// This is a **panic-safety bound, not an allocation-size guard**.
/// `crates/index`'s `NodeTable` sizes its chunk-pointer directory for
/// exactly this ceiling (its own `MAX_ROW_ID_CAPACITY` is documented as
/// matching this constant) and then indexes into that directory directly,
/// so a row-id past the ceiling would be an out-of-bounds index rather than
/// a typed error. [`HnswIndex::new`]'s `max_elements` argument does *not*
/// drive it: `NodeTable::new` takes that hint for API symmetry and ignores
/// it, because `MaxElements` is documented as a sizing hint and explicitly
/// "not a hard cap". A corrupted or hostile manifest claiming a
/// `next_row_id` near `u64::MAX` therefore has to be rejected here.
///
/// One billion rows is far beyond any realistic embedded dataset today;
/// revisit if a real workload needs more — and change both constants
/// together, since the two crates' values must stay equal.
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
fn replay_index(dir: &Path, manifest: &Manifest) -> Result<(HnswIndex, imbl::HashSet<u64>)> {
    if manifest.next_row_id > MAX_REASONABLE_ROW_ID_CAPACITY {
        return Err(TxnError::UnreasonableCapacity(
            manifest.next_row_id,
            MAX_REASONABLE_ROW_ID_CAPACITY,
        ));
    }
    let capacity = usize::try_from(manifest.next_row_id).unwrap_or(usize::MAX);
    let index = new_hnsw_index(capacity)?;
    let mut tombstones: imbl::HashSet<u64> = imbl::HashSet::new();
    let data_dir = data_subdir(dir);
    for entry in &manifest.data_files {
        for delta in read_delta_log(&safe_join(&data_dir, &entry.delta_log)?)? {
            match delta {
                DeltaEntry::Insert { row_id, vector } => index.insert_owned(row_id, vector)?,
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

    // Downcast the flattened child array once, before the per-row loop,
    // instead of calling `vectors.value(i)` (a fresh sliced ArrayRef + Arc
    // allocation) and re-downcasting the result on every row -- the
    // concrete child type is invariant per column, only the row index
    // changes (mirrors the fix already applied in group_by.rs). Every row's
    // slice is then a plain `i * value_length` index into the flat buffer:
    // `FixedSizeListArray::offset()` and `Float32Array::offset()` are both
    // hardcoded to 0 in arrow-array 58.3.0 (`slice()` bakes any logical
    // offset directly into a new, already-adjusted `values` buffer rather
    // than tracking a separate offset field -- confirmed against the
    // installed source), so no extra offset arithmetic is needed here.
    let value_length = usize::try_from(vectors.value_length()).unwrap_or(0);
    let flat: &arrow::array::Float32Array =
        vectors.values().as_any().downcast_ref().ok_or_else(|| {
            TxnError::Arrow(arrow::error::ArrowError::CastError(
                "vector column's inner type must be Float32".to_string(),
            ))
        })?;
    let flat_values = flat.values();

    let mut entries = Vec::with_capacity(vectors.len());
    for i in 0..vectors.len() {
        if vectors.is_null(i) {
            continue;
        }
        let start = i * value_length;
        let row = &flat_values[start..start + value_length];
        let row_id = row_id_base.checked_add(u64::try_from(i)?).ok_or_else(|| {
            TxnError::ManifestOverflow(format!("row_id_base {row_id_base} + {i}"))
        })?;
        if row.iter().any(|component| !component.is_finite()) {
            return Err(TxnError::NonFiniteVectorComponent { row_id });
        }
        entries.push(DeltaEntry::Insert {
            row_id,
            vector: row.to_vec(),
        });
    }
    Ok(entries)
}

/// Validates that every [`DeltaEntry::Insert`] in `deltas` shares one
/// consistent vector dimension — both against each other, and against
/// `graph`'s already-established dimension (if any) — before any of them
/// are applied. `HnswIndex::insert`'s only fallible path is dimension
/// validation (the graph insert underneath it is infallible), so this
/// closes the common trigger for a partial-graph-mutation-then-fail
/// scenario: without it, `Insert` deltas are applied to the shared graph
/// in pending-batch order, and a later batch's dimension mismatch is only
/// caught after an earlier batch's deltas have already mutated the shared
/// graph. It is a pre-lock fast path, not the last line of defence —
/// [`GraphResidueGuard`] is what undoes an already-applied delta when a
/// commit fails after this check has passed.
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

/// Hidden columns every committed batch carries alongside its logical
/// (user) columns: `_row_id` and `_timestamp`, both unconditionally
/// (since W2).
/// `cast_batch_to_schema` matches these by *name*, not position; every
/// other (visible) column is still matched positionally against `schema`'s
/// fields, so the caller's `schema` must still list its visible fields in
/// the same order the data was inserted in.
const HIDDEN_COLUMNS: [&str; 2] = [ROW_ID_COLUMN, TIMESTAMP_COLUMN];

/// Casts `batch`'s physical columns to `schema`'s logical field types,
/// reattaching any hidden column (`_row_id`, `_timestamp`) `schema`
/// explicitly requests. See
/// `docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md` §5
/// for why matching a *second* hidden column by position was unsound (it
/// either misfired a spurious `SchemaMismatch`, or — in the mixed-request
/// case — silently paired the wrong physical column against the wrong
/// logical field).
///
/// # Errors
///
/// Returns [`TxnError::SchemaMismatch`] if the number of *visible* columns
/// `schema` requests doesn't match the number of visible physical columns
/// in `batch`, an [`TxnError::Arrow`] if `schema` requests a hidden column
/// not present in `batch`, or an [`TxnError::Arrow`]/[`TxnError`] wrapping
/// a cast failure if a column's physical type can't convert to its
/// requested logical type.
pub(crate) fn cast_batch_to_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let physical_schema = batch.schema_ref();

    let mut hidden_physical: std::collections::HashMap<&str, ArrayRef> =
        std::collections::HashMap::new();
    let mut visible_physical: Vec<ArrayRef> = Vec::new();
    for (field, column) in physical_schema.fields().iter().zip(batch.columns()) {
        if HIDDEN_COLUMNS.contains(&field.name().as_str()) {
            hidden_physical.insert(field.name().as_str(), Arc::clone(column));
        } else {
            visible_physical.push(Arc::clone(column));
        }
    }

    let visible_requested = schema
        .fields()
        .iter()
        .filter(|f| !HIDDEN_COLUMNS.contains(&f.name().as_str()))
        .count();
    if visible_requested != visible_physical.len() {
        return Err(TxnError::SchemaMismatch {
            expected: visible_requested,
            actual: visible_physical.len(),
        });
    }

    let mut visible_iter = visible_physical.into_iter();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column = if HIDDEN_COLUMNS.contains(&field.name().as_str()) {
            hidden_physical
                .get(field.name().as_str())
                .cloned()
                .ok_or_else(|| {
                    TxnError::Arrow(arrow::error::ArrowError::SchemaError(format!(
                        "requested hidden column '{}' not present in this batch",
                        field.name()
                    )))
                })?
        } else {
            // `visible_requested == visible_physical.len()` was already
            // checked above, so this iterator has exactly as many elements
            // as there are non-hidden fields in `schema` — it cannot run
            // dry before this loop does.
            match visible_iter.next() {
                Some(column) => column,
                None => unreachable!(
                    "visible column count was checked equal to visible field count above"
                ),
            }
        };
        let casted = if column.data_type() == field.data_type() {
            column
        } else {
            cast(column.as_ref(), field.data_type())?
        };
        columns.push(casted);
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
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

/// Appends a `_timestamp: Int64` column to `batch`, every row sharing the
/// single value `ts` — microseconds since the Unix epoch, captured once per
/// transaction by `issue_timestamp`. See
/// `docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md` §2-3.
fn append_timestamp_column(batch: &RecordBatch, ts: i64, num_rows: u64) -> Result<RecordBatch> {
    let num_rows = usize::try_from(num_rows)?;
    let timestamps: Vec<i64> = vec![ts; num_rows];
    let timestamp_array: ArrayRef = Arc::new(arrow::array::Int64Array::from(timestamps));

    let mut fields: Vec<Field> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(TIMESTAMP_COLUMN, DataType::Int64, false));

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(timestamp_array);

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

    /// Small `CommitLog` capacity used only by the wraparound/
    /// `InsufficientHistory` regression tests below, via
    /// `Dataset::create_with_commit_log_capacity`. The eviction *logic*
    /// under test doesn't care what the capacity's magnitude is, only that
    /// eviction happens once it's exceeded — so proving it at 8 is exactly
    /// as rigorous as proving it at the real `COMMIT_LOG_CAPACITY` (2048),
    /// without paying that capacity's fill cost (see
    /// `create_with_commit_log_capacity`'s doc comment).
    const TEST_COMMIT_LOG_CAPACITY: usize = 8;

    /// Proves 8 concurrent claims hand out non-overlapping, contiguous
    /// ranges, and that every one of them is registered as in-flight while
    /// it is held. Previously asserted this of a bare
    /// `AtomicU64::fetch_add`, which is no longer how row-ids are
    /// allocated — a `fetch_add` cannot advance the counter and register
    /// the claim as one step, which is the whole point of
    /// [`crate::row_id::RowIdAllocator`]. Uses `std::thread::scope` rather
    /// than `unsafe { transmute }` to borrow the stack-local safely — see
    /// Task 5's brief for why the `transmute` draft was rejected (this
    /// workspace's "safe Rust by default" convention).
    #[test]
    fn concurrent_claims_hand_out_non_overlapping_ranges_and_all_register() {
        use crate::row_id::RowIdAllocator;

        let allocator = Arc::new(RowIdAllocator::new(0));
        let claims: Vec<_> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| allocator.claim(10).unwrap()))
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        let mut bases: Vec<u64> = claims.iter().map(super::RowIdClaim::base).collect();
        bases.sort_unstable();
        for (i, base) in bases.iter().enumerate() {
            assert_eq!(
                *base,
                (i as u64) * 10,
                "ranges must be contiguous, non-overlapping"
            );
        }

        // Every claim is still held, so every one must be excluded from a
        // snapshot published now — the property a bare counter can't give.
        let bound = allocator.visibility_bound_excluding(None);
        assert_eq!(bound.next_row_id, 80);
        for base in &bases {
            assert!(
                bound.in_flight.iter().any(|range| range.contains(*base)),
                "row-id {base} is claimed but not committed, so it must be excluded"
            );
        }

        drop(claims);
        assert!(
            allocator
                .visibility_bound_excluding(None)
                .in_flight
                .is_empty(),
            "and released once every claim is dropped"
        );
    }

    fn temp_dir(label: &str) -> PathBuf {
        tempfile::Builder::new()
            .prefix(&format!("strata-txn-test-{label}-"))
            .tempdir()
            .unwrap()
            .keep()
    }

    #[test]
    fn issue_timestamp_never_decreases_even_if_the_clock_would() {
        let last_issued = std::sync::atomic::AtomicI64::new(0);

        let first = issue_timestamp(&last_issued).unwrap();
        assert!(
            first > 0,
            "a real clock read must be positive microseconds-since-epoch"
        );

        // Simulate a wall-clock regression (an NTP step backward) by seeding
        // the atomic far ahead of any real clock reading it could compete
        // against - `fetch_max` must keep the issued value at or above this,
        // never let a subsequent "now()" that's smaller win.
        last_issued.store(first + 1_000_000_000, std::sync::atomic::Ordering::SeqCst);
        let second = issue_timestamp(&last_issued).unwrap();
        assert_eq!(
            second,
            first + 1_000_000_000,
            "issuance must never go backward, even against a clock read that would be smaller"
        );

        let third = issue_timestamp(&last_issued).unwrap();
        assert!(
            third >= second,
            "a normal (non-regressing) subsequent issuance must still be non-decreasing"
        );
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    #[test]
    fn every_row_in_one_transaction_shares_the_identical_timestamp() {
        let dir = temp_dir("timestamp-shared-per-txn");
        let ds = Dataset::create(&dir).unwrap();

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap(),
        );
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![3, 4]))])
                .unwrap(),
        );
        txn.commit().unwrap();

        let snapshot = ds.snapshot();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
        ]));
        let batch = snapshot.scan(&schema).unwrap();
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let first = timestamps.value(0);
        assert_eq!(batch.num_rows(), 4);
        for i in 0..batch.num_rows() {
            assert_eq!(
                timestamps.value(i),
                first,
                "every row across both pending batches in one transaction must share one timestamp"
            );
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_delete_only_commit_still_advances_commit_time_high_water() {
        let dir = temp_dir("timestamp-delete-only-advances");
        let ds = Dataset::create(&dir).unwrap();

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        );
        txn.commit().unwrap();
        let after_insert = ds.snapshot().manifest.commit_time_high_water;
        assert!(after_insert > 0);

        // Sleep-free: two distinct clock reads are only guaranteed distinct
        // if enough wall-clock time actually elapses, which isn't
        // guaranteed in a fast test - so this asserts non-decreasing
        // (`>=`), matching the spec's own "non-decreasing" wording, not
        // strictly increasing.
        let mut txn = ds.begin();
        txn.delete(0);
        txn.commit().unwrap();
        let after_delete = ds.snapshot().manifest.commit_time_high_water;
        assert!(
            after_delete >= after_insert,
            "a delete-only commit must still advance (or at least not regress) the high-water mark"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_time_high_water_is_non_decreasing_across_several_commits() {
        let dir = temp_dir("timestamp-high-water-monotonic");
        let ds = Dataset::create(&dir).unwrap();

        let mut last = 0i64;
        for i in 0..5 {
            let mut txn = ds.begin();
            txn.insert(
                RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![i]))])
                    .unwrap(),
            );
            txn.commit().unwrap();
            let current = ds.snapshot().manifest.commit_time_high_water;
            assert!(
                current >= last,
                "commit {i}: high-water mark regressed from {last} to {current}"
            );
            last = current;
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_time_high_water_does_not_regress_when_a_smaller_timestamp_commits_later() {
        // Deterministic concurrency, not a race: constructs the exact
        // adversarial interleaving design doc §1/§3 accepts as possible - a
        // transaction that captured an EARLIER (smaller) timestamp reaches
        // commit_lock and actually commits AFTER a transaction that
        // captured a LATER (larger) one. Proves Layer 2 (the manifest's
        // commit_time_high_water) stays non-decreasing across versions even
        // though the current committer's own captured value is smaller
        // than what a concurrent commit already published. Uses this
        // file's existing `checkpoint_pair`/`pause_after_row_id_claim`
        // mechanism (see `in-flight-commit-not-visible-to-reader`-style
        // tests elsewhere in this module for the same pattern), not a
        // sleep-raced schedule - only the wall-clock *value* gap uses a
        // short sleep below, not the interleaving itself.
        let dir = temp_dir("timestamp-high-water-concurrent-non-regression");
        let ds = Dataset::create(&dir).unwrap();

        let (claim_point, claimed) = checkpoint_pair();

        // "slow": captures its timestamp first (small - issue_timestamp
        // runs at the very top of commit(), before write_phase, which is
        // what pause_after_row_id_claim pauses after), then parks before
        // acquiring commit_lock.
        let mut slow = ds.begin();
        slow.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        );
        slow.pause_after_row_id_claim(claim_point);
        let slow_thread = std::thread::spawn(move || slow.commit());

        claimed.wait();

        // Not racing an observation window (this file's other
        // Checkpoint-based tests correctly avoid that) - only guaranteeing
        // "fast"'s own clock read is strictly later in wall-clock time,
        // which real elapsed time already all but guarantees at
        // microsecond resolution; this removes any doubt.
        std::thread::sleep(std::time::Duration::from_millis(2));

        // "fast": captures a strictly later (larger) timestamp and commits
        // to completion, uncontested, while "slow" is parked.
        let mut fast = ds.begin();
        fast.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![2]))]).unwrap(),
        );
        fast.commit().unwrap();
        let high_water_after_fast = ds.snapshot().manifest.commit_time_high_water;
        assert!(high_water_after_fast > 0);

        // Release "slow" - it builds its manifest from "fast"'s
        // already-published snapshot (commit_time_high_water ==
        // high_water_after_fast), then applies `.max(its own smaller
        // timestamp)` on top of that.
        claimed.release();
        slow_thread.join().unwrap().unwrap();

        let high_water_after_slow = ds.snapshot().manifest.commit_time_high_water;
        assert_eq!(
            high_water_after_slow, high_water_after_fast,
            "a later-committing transaction with an EARLIER captured timestamp must not \
             regress commit_time_high_water below what an already-published concurrent \
             commit established"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn timestamp_gets_a_stats_entry_with_min_equal_to_max() {
        let dir = temp_dir("timestamp-file-pruning");
        let ds = Dataset::create(&dir).unwrap();

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        );
        txn.commit().unwrap();

        let files = ds.data_files();
        assert_eq!(files.len(), 1);
        let stats = files[0].stats.get(TIMESTAMP_COLUMN).expect(
            "_timestamp must have a stats entry (unlike _row_id, which deliberately has none)",
        );
        assert_eq!(
            stats.min, stats.max,
            "every row in one file shares one timestamp"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn should_scan_file_prunes_files_using_timestamp_stats() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("timestamp-real-file-pruning");
        let ds = Dataset::create(&dir).unwrap();

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        );
        txn.commit().unwrap();
        let ts_after_commit_1 = ds.snapshot().manifest.commit_time_high_water;

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![2]))]).unwrap(),
        );
        txn.commit().unwrap();

        let snapshot = ds.snapshot();
        let predicate = Predicate::Gt(
            TIMESTAMP_COLUMN.to_string(),
            Value::Int64(ts_after_commit_1),
        );
        let explain = snapshot.explain(&predicate);
        assert_eq!(explain.total_files, 2);
        assert_eq!(
            explain.scanned.len(),
            1,
            "only the second file's timestamp can be strictly greater than the first commit's own timestamp"
        );
        assert_eq!(explain.skipped.len(), 1);
        assert_eq!(
            explain.scanned[0],
            ds.data_files()[1].name,
            "the surviving file must be the second commit's, not the first"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn insert_rejects_a_batch_whose_schema_reuses_a_reserved_column_name() {
        let dir = temp_dir("timestamp-reserved-column-name-rejected");
        let ds = Dataset::create(&dir).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(ROW_ID_COLUMN, DataType::Int64, false), // user column colliding with the hidden one
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![999])),
            ],
        )
        .unwrap();

        let mut txn = ds.begin();
        txn.insert(batch);
        let result = txn.commit();
        assert!(
            matches!(result, Err(TxnError::ReservedColumnName(ref name)) if name == ROW_ID_COLUMN),
            "expected ReservedColumnName(_row_id), got {result:?}"
        );

        // Nothing must have been written - the dataset stays at version 0.
        assert_eq!(ds.current_version(), 0);

        std::fs::remove_dir_all(&dir).ok();
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
        // `open` the same way the row-id allocator is seeded from
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
    fn opening_a_legacy_pre_attempt_id_manifest_does_not_destroy_its_data_files() {
        // Regression test for a bug found via an external optimization
        // report's audit (Section 2 of the pipeline docs): before this
        // fix, `write_attempt_counter` seeded straight from
        // `manifest.next_attempt_id` (see `Dataset::open` above) -- correct
        // for any manifest produced by the current commit path, since that
        // path always persists `next_attempt_id >= 1` after its very first
        // commit. But a manifest written BEFORE `next_attempt_id` existed
        // as a field deserializes it as 0 via `#[serde(default)]`, even
        // though `data_files` may already hold legacy, VERSION-prefixed
        // filenames (`{version:020}-{i}.arrow`, from before the
        // attempt-id naming scheme replaced version-based naming). Seeding
        // the counter at 0 in that case means the next commit's first
        // *fetch_add* returns 0 (harmless -- legacy version 0 has no data
        // file), but its *second* commit uses attempt id 1, colliding
        // byte-for-byte with the legacy version-1 data file's name.
        // `write_batch` uses `File::create`, which truncates -- silently
        // destroying that already-durable file.
        //
        // This test simulates that legacy manifest directly (bypassing the
        // normal create/commit path, which can no longer produce this
        // shape) via `strata_storage::commit_manifest`, matching the
        // existing hostile-manifest test pattern in this file.
        let dir = temp_dir("legacy-manifest-migration");
        let versions_dir_data = dir.join("data");
        std::fs::create_dir_all(&versions_dir_data).unwrap();

        // Simulate two legacy commits' worth of already-durable data files,
        // named the OLD way: prefixed by their own commit's version number.
        let legacy_batch_v1 = arrow::array::Int64Array::from(vec![1, 2, 3]);
        let file_v1 = versions_dir_data.join(format!("{:020}-0.arrow", 1u64));
        strata_storage::write_batch(
            &file_v1,
            &RecordBatch::try_new(test_schema(), vec![Arc::new(legacy_batch_v1)]).unwrap(),
        )
        .unwrap();

        // A legacy manifest: data_files references the version-1 file, but
        // (matching every manifest written before this field existed)
        // carries no next_attempt_id -- it deserializes to 0.
        let legacy_manifest = Manifest {
            version: 1,
            data_files: vec![DataFileEntry {
                name: format!("{:020}-0.arrow", 1u64),
                stats: std::collections::HashMap::new(),
                delta_log: format!("{:020}-0.deltalog", 1u64),
            }],
            next_row_id: 3,
            tombstones: Vec::new(),
            next_attempt_id: 0, // <-- the exact legacy-deserialize shape
            commit_time_high_water: 0,
            segments: Vec::new(),
        };
        // The delta log referenced above must exist too (replay_index reads
        // it on open), but can be empty -- this test's batch has no vector
        // column, so no Insert deltas were ever produced for it.
        strata_index::write_delta_log(
            &versions_dir_data.join(format!("{:020}-0.deltalog", 1u64)),
            &[],
        )
        .unwrap();
        strata_storage::commit_manifest(&dir, &legacy_manifest).unwrap();

        // Open (must migrate the attempt-id counter away from 0) and commit
        // twice -- the second commit is the one that would use attempt id 1
        // under the old, buggy seeding.
        let reopened = Dataset::open(&dir).unwrap();
        let schema = test_schema();
        for value in [4i64, 5i64] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![value]))],
            )
            .unwrap();
            let mut txn = reopened.begin();
            txn.insert(batch);
            txn.commit().unwrap();
        }

        // The legacy file must survive untouched, AND all rows (legacy +
        // both new commits) must be visible -- neither silently destroyed
        // nor silently double-counted via a reused manifest entry name.
        assert!(
            file_v1.exists(),
            "the legacy version-1 data file must not have been overwritten"
        );
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
            vec![1, 2, 3, 4, 5],
            "legacy rows plus both post-migration commits must all be present"
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
            commit_time_high_water: 0,
            segments: Vec::new(),
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
            commit_time_high_water: 0,
            segments: Vec::new(),
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
    fn cast_batch_to_schema_reattaches_neither_hidden_column_by_default() {
        let dir = temp_dir("cast-hidden-neither");
        let ds = Dataset::create(&dir).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap(),
        );
        txn.commit().unwrap();

        let batch = ds.snapshot().scan(&test_schema()).unwrap();
        assert_eq!(
            batch.num_columns(),
            1,
            "requesting no hidden columns must return just 'id'"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cast_batch_to_schema_reattaches_row_id_only() {
        let dir = temp_dir("cast-hidden-row-id-only");
        let ds = Dataset::create(&dir).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap(),
        );
        txn.commit().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(ROW_ID_COLUMN, DataType::UInt64, false),
        ]));
        let batch = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(batch.num_columns(), 2);
        let row_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(row_ids.values(), &[0, 1]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cast_batch_to_schema_reattaches_timestamp_only() {
        let dir = temp_dir("cast-hidden-timestamp-only");
        let ds = Dataset::create(&dir).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap(),
        );
        txn.commit().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
        ]));
        let batch = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(batch.num_columns(), 2);
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(timestamps.value(0) > 0);
        assert_eq!(
            timestamps.value(0),
            timestamps.value(1),
            "this is the exact case that risked a silent miscast under the old positional logic - \
             both rows must show the SAME real timestamp, not a row-id value reinterpreted as Int64"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cast_batch_to_schema_reattaches_both_hidden_columns() {
        let dir = temp_dir("cast-hidden-both");
        let ds = Dataset::create(&dir).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap(),
        );
        txn.commit().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(ROW_ID_COLUMN, DataType::UInt64, false),
            Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
        ]));
        let batch = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(batch.num_columns(), 3);
        let row_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(row_ids.values(), &[0, 1]);
        let timestamps = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(timestamps.value(0), timestamps.value(1));

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
    fn vector_search_with_compound_predicate_narrows_across_two_columns() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("vector-search-filtered-compound");
        let ds = Dataset::create(&dir).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));

        // Three 10-point clusters at increasing distance from the query
        // point (the origin): near (id=1, category="a", irrelevant noise),
        // mid (id=2, category="b", distance 500), far (id=2, category="a",
        // distance 1000). A single-column `id=2` filter alone cannot
        // distinguish mid from far - both match - and since mid is closer
        // to the query, an id-only filtered search returns mid. Only
        // `id=2 AND category="a"` correctly excludes mid and returns far
        // instead, proving the compound predicate changes the result set
        // in a way neither leaf alone could, and that resolving it
        // required both columns to be readable (row_ids_matching's
        // projection must include both, or `mask` errors on the missing
        // one).
        let near_cluster = cluster_vectors(10, [0.0, 0.0, 0.0], 0.01);
        let mid_cluster = cluster_vectors(10, [500.0, 0.0, 0.0], 0.01);
        let far_cluster = cluster_vectors(10, [1000.0, 0.0, 0.0], 0.01);

        let mut ids = vec![1i64; 10];
        ids.extend(vec![2i64; 10]);
        ids.extend(vec![2i64; 10]);
        let mut categories = vec!["a"; 10];
        categories.extend(vec!["b"; 10]);
        categories.extend(vec!["a"; 10]);
        let mut vectors = near_cluster;
        vectors.extend(mid_cluster);
        vectors.extend(far_cluster);
        let flat: Vec<f32> = vectors.iter().flatten().copied().collect();

        let id_arr = Arc::new(Int64Array::from(ids));
        let cat_arr = Arc::new(arrow::array::StringArray::from(categories));
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let values = Arc::new(arrow::array::Float32Array::from(flat));
        let vec_arr = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field, 3, values, None,
        ));
        let batch = RecordBatch::try_new(schema, vec![id_arr, cat_arr, vec_arr]).unwrap();

        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let snapshot = ds.snapshot();

        // id=2 alone: the 20 matching points are the mid (rows 10..20) and
        // far (rows 20..30) clusters. Nearest to the origin among those 20
        // is the mid cluster (distance 500 < 1000), so an id-only filtered
        // search returns mid-cluster row-ids.
        let id_only = Predicate::Eq("id".to_string(), Value::Int64(2));
        let id_only_results = snapshot
            .vector_search(&[0.0, 0.0, 0.0], 3, Some(&id_only))
            .unwrap();
        assert_eq!(
            id_only_results.len(),
            3,
            "unexpected results: {id_only_results:?}"
        );
        assert!(
            id_only_results.iter().all(|r| (10..20).contains(&r.row_id)),
            "id=2 alone must return the closer mid cluster (row-ids 10..20): {id_only_results:?}"
        );

        // id=2 AND category="a": only the far cluster (rows 20..30)
        // qualifies - the mid cluster is category "b" and must be
        // excluded, even though it's closer to the query point.
        let compound = Predicate::And(
            Box::new(id_only.clone()),
            Box::new(Predicate::Eq(
                "category".to_string(),
                Value::Utf8("a".to_string()),
            )),
        );
        let compound_results = snapshot
            .vector_search(&[0.0, 0.0, 0.0], 3, Some(&compound))
            .unwrap();
        assert_eq!(
            compound_results.len(),
            3,
            "unexpected results: {compound_results:?}"
        );
        assert!(
            compound_results
                .iter()
                .all(|r| (20..30).contains(&r.row_id)),
            "id=2 AND category=a must exclude the closer but wrong-category mid cluster \
             and return the far cluster instead (row-ids 20..30): {compound_results:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn timestamps_and_the_high_water_mark_survive_reopen() {
        let dir = temp_dir("timestamp-survives-reopen");
        let ds = Dataset::create(&dir).unwrap();

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(
                test_schema(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap(),
        );
        txn.commit().unwrap();

        let high_water_before_close = ds.snapshot().manifest.commit_time_high_water;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
        ]));
        let timestamps_before_close: Vec<i64> = {
            let batch = ds.snapshot().scan(&schema).unwrap();
            let col = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            (0..batch.num_rows()).map(|i| col.value(i)).collect()
        };
        drop(ds);

        // Reopen — this is the actual file-read path (dictionary-encoded,
        // per design doc §9), not an in-memory batch, so this is the test
        // that would catch a dictionary-encoding round-trip bug the
        // in-memory-only tests above cannot.
        let reopened = Dataset::open(&dir).unwrap();
        let timestamps_after_reopen: Vec<i64> = {
            let batch = reopened.snapshot().scan(&schema).unwrap();
            let col = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            (0..batch.num_rows()).map(|i| col.value(i)).collect()
        };
        assert_eq!(
            timestamps_before_close, timestamps_after_reopen,
            "per-row timestamps must round-trip through the actual (dictionary-encoded) file format"
        );

        // A predicate against the reopened, dictionary-encoded column must
        // still work - the comparison kernel unwraps dictionaries
        // transparently, but this proves it end to end rather than trusting
        // that in isolation.
        let predicate = strata_query::Predicate::GtEq(
            TIMESTAMP_COLUMN.to_string(),
            strata_storage::Value::Int64(timestamps_after_reopen[0]),
        );
        let filtered = reopened
            .snapshot()
            .scan_with_predicate(&schema, &predicate)
            .unwrap();
        assert_eq!(
            filtered.num_rows(),
            3,
            "all 3 rows share the same timestamp, so all must match"
        );

        let mut txn = reopened.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![4]))]).unwrap(),
        );
        txn.commit().unwrap();
        // Smoke check only - this assertion holds regardless of whether the
        // restart floor was seeded correctly, since a fresh post-reopen
        // timestamp is always >= an earlier one by ordinary clock advancement.
        // The real proof that Dataset::open seeds last_issued_timestamp from
        // the persisted value is
        // last_issued_timestamp_is_seeded_from_the_persisted_high_water_mark_on_reopen.
        assert!(
            reopened.snapshot().manifest.commit_time_high_water >= high_water_before_close,
            "commit_time_high_water must not regress across a restart"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    // The three-cluster fixture (near/far/mid, each with its own commit and
    // category) is what makes this test discriminate "AND narrows correctly"
    // from "either leaf alone happens to work" - splitting it into a helper
    // would obscure exactly the structure the test is proving.
    #[allow(clippy::too_many_lines)]
    fn vector_search_with_timestamp_and_category_compound_predicate() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("timestamp-compound-vector-search");
        let ds = Dataset::create(&dir).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));

        // Three commits, three clusters at increasing distance from the
        // query point (the origin): near (id 0..10, category "a", commit 1
        // - EARLIEST timestamp), far (id 10..20, category "a", commit 2),
        // mid (id 20..30, category "b", commit 3 - LATEST timestamp).
        //
        // Predicate: timestamp >= ts_after_commit_2 AND category = "a".
        // - category="a" ALONE matches near+far (0..20); nearest to the
        //   origin is the near cluster (distance 0) - so category alone
        //   returns the WRONG answer (near is commit 1, excluded by the
        //   timestamp leaf).
        // - timestamp>=ts_after_commit_2 ALONE matches far+mid (10..30);
        //   nearest to the origin is the mid cluster (distance 500) - so
        //   timestamp alone ALSO returns the WRONG answer (mid is category
        //   "b", excluded by the category leaf).
        // - Only the AND correctly identifies the far cluster (10..20):
        //   it's the only cluster satisfying both conjuncts, and neither
        //   leaf alone could identify it.
        let near_cluster = cluster_vectors(10, [0.0, 0.0, 0.0], 0.01);
        let far_cluster = cluster_vectors(10, [1000.0, 0.0, 0.0], 0.01);
        let mid_cluster = cluster_vectors(10, [500.0, 0.0, 0.0], 0.01);
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(arrow::array::StringArray::from(vec!["a"; 10])),
                    Arc::new(arrow::array::FixedSizeListArray::new(
                        item_field.clone(),
                        3,
                        Arc::new(arrow::array::Float32Array::from(
                            near_cluster.iter().flatten().copied().collect::<Vec<f32>>(),
                        )),
                        None,
                    )),
                ],
            )
            .unwrap(),
        );
        txn.commit().unwrap();

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(arrow::array::StringArray::from(vec!["a"; 10])),
                    Arc::new(arrow::array::FixedSizeListArray::new(
                        item_field.clone(),
                        3,
                        Arc::new(arrow::array::Float32Array::from(
                            far_cluster.iter().flatten().copied().collect::<Vec<f32>>(),
                        )),
                        None,
                    )),
                ],
            )
            .unwrap(),
        );
        txn.commit().unwrap();
        let ts_after_commit_2 = ds.snapshot().manifest.commit_time_high_water;

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(arrow::array::StringArray::from(vec!["b"; 10])),
                    Arc::new(arrow::array::FixedSizeListArray::new(
                        item_field,
                        3,
                        Arc::new(arrow::array::Float32Array::from(
                            mid_cluster.iter().flatten().copied().collect::<Vec<f32>>(),
                        )),
                        None,
                    )),
                ],
            )
            .unwrap(),
        );
        txn.commit().unwrap();

        let snapshot = ds.snapshot();

        // Control 1: category="a" alone must return the near cluster (the
        // WRONG answer per the predicate's intent - proves category alone
        // is insufficient).
        let category_only = Predicate::Eq("category".to_string(), Value::Utf8("a".to_string()));
        let category_only_results = snapshot
            .vector_search(&[0.0, 0.0, 0.0], 5, Some(&category_only))
            .unwrap();
        assert_eq!(
            category_only_results.len(),
            5,
            "unexpected: {category_only_results:?}"
        );
        assert!(
            category_only_results.iter().all(|r| r.row_id < 10),
            "category=a alone must return the near cluster (row-ids 0..10) - proving it alone \
             is the wrong answer: {category_only_results:?}"
        );

        // Control 2: timestamp>=ts_after_commit_2 alone must return the mid
        // cluster (also the WRONG answer - proves timestamp alone is
        // insufficient too).
        let timestamp_only = Predicate::GtEq(
            TIMESTAMP_COLUMN.to_string(),
            Value::Int64(ts_after_commit_2),
        );
        let timestamp_only_results = snapshot
            .vector_search(&[0.0, 0.0, 0.0], 5, Some(&timestamp_only))
            .unwrap();
        assert_eq!(
            timestamp_only_results.len(),
            5,
            "unexpected: {timestamp_only_results:?}"
        );
        assert!(
            timestamp_only_results
                .iter()
                .all(|r| (20..30).contains(&r.row_id)),
            "timestamp>=ts_after_commit_2 alone must return the mid cluster (row-ids 20..30) - \
             proving it alone is ALSO the wrong answer: {timestamp_only_results:?}"
        );

        // The AND: only the far cluster satisfies both conjuncts, and
        // neither control above could identify it alone.
        let predicate = Predicate::And(
            Box::new(timestamp_only.clone()),
            Box::new(category_only.clone()),
        );
        let results = snapshot
            .vector_search(&[0.0, 0.0, 0.0], 5, Some(&predicate))
            .unwrap();
        assert_eq!(results.len(), 5, "unexpected results: {results:?}");
        assert!(
            results.iter().all(|r| (10..20).contains(&r.row_id)),
            "timestamp>=ts_after_commit_2 AND category=a must return the far cluster \
             (row-ids 10..20) - the only cluster satisfying both, which neither leaf alone \
             (near, for category; mid, for timestamp) could identify: {results:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn last_issued_timestamp_is_seeded_from_the_persisted_high_water_mark_on_reopen() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("timestamp-restart-floor-seeding");
        let ds = Dataset::create(&dir).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        );
        txn.commit().unwrap();
        drop(ds);

        // Directly overwrite the on-disk manifest with a commit_time_high_water
        // far in the future - simulating what a real prior session's clock
        // would eventually produce, without waiting for it. If Dataset::open
        // does NOT seed last_issued_timestamp from this persisted value (e.g.
        // seeds from 0, or from a fresh unclamped wall-clock read), the next
        // commit's real timestamp will land far BELOW this floor, and the
        // assertions below catch it.
        let mut manifest = strata_storage::read_current(&dir).unwrap().unwrap();
        let far_future = manifest.commit_time_high_water + 1_000_000_000_000; // ~11.6 days ahead, in microseconds
        manifest.commit_time_high_water = far_future;
        strata_storage::commit_manifest(&dir, &manifest).unwrap();

        let reopened = Dataset::open(&dir).unwrap();
        assert_eq!(
            reopened.snapshot().manifest.commit_time_high_water,
            far_future,
            "sanity check: the persisted value itself must survive the reopen unchanged"
        );

        let mut txn = reopened.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![2]))]).unwrap(),
        );
        txn.commit().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
        ]));
        let batch = reopened
            .snapshot()
            .scan_with_predicate(&schema, &Predicate::Eq("id".to_string(), Value::Int64(2)))
            .unwrap();
        let new_row_ts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert!(
            new_row_ts >= far_future,
            "the new commit's own timestamp must be >= the persisted restart floor - \
             last_issued_timestamp must be seeded from commit_time_high_water on open, not \
             from 0 or an unclamped fresh wall-clock read: new_row_ts={new_row_ts}, far_future={far_future}"
        );
        assert!(
            reopened.snapshot().manifest.commit_time_high_water >= far_future,
            "commit_time_high_water itself must never regress below the persisted floor"
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
            commit_time_high_water: 0,
            segments: Vec::new(),
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
        // committed file only has 1 logical column ("id" — the hidden
        // _row_id and _timestamp columns don't count unless the caller
        // requests them) - must error, not silently zip/truncate or,
        // worse, cast a hidden column into the caller's "extra" field.
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
    fn build_delta_entries_produces_the_correct_vector_per_row() {
        // Distinct, easily-distinguishable vectors per row -- a flat-buffer
        // indexing bug (e.g. an off-by-one in row * value_length, or
        // accidentally reading a neighboring row's slice) would surface as
        // a row getting the wrong vector, which a row_id-only assertion
        // (as the other build_delta_entries tests use) would never catch.
        let ids = Arc::new(Int64Array::from(vec![10, 11, 12]));
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let values = Arc::new(arrow::array::Float32Array::from(vec![
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
        ]));
        let vectors = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field, 3, values, None,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(schema, vec![ids, vectors]).unwrap();

        let deltas = build_delta_entries(&batch, 100).unwrap();
        assert_eq!(deltas.len(), 3);
        let as_insert = |d: &DeltaEntry| match d {
            DeltaEntry::Insert { row_id, vector } => (*row_id, vector.clone()),
            DeltaEntry::Tombstone { .. } => panic!("expected an Insert entry"),
        };
        assert_eq!(as_insert(&deltas[0]), (100, vec![1.0, 2.0, 3.0]));
        assert_eq!(as_insert(&deltas[1]), (101, vec![4.0, 5.0, 6.0]));
        assert_eq!(as_insert(&deltas[2]), (102, vec![7.0, 8.0, 9.0]));
    }

    #[test]
    fn build_delta_entries_reads_the_correct_vector_from_a_sliced_batch() {
        // Pins the assumption build_delta_entries's downcast-hoist rewrite
        // rests on: FixedSizeListArray::offset()/Float32Array::offset() are
        // both always 0 in the installed arrow-array version, because
        // slicing bakes the offset into a new `values` buffer rather than
        // tracking a separate offset field. If a future arrow upgrade ever
        // changed that representation, a flat `i * value_length` index
        // would silently read the wrong vector for every row of a sliced
        // batch -- with every *other* test here still green, since none of
        // them exercise a sliced batch. This one does.
        let ids = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let values = Arc::new(arrow::array::Float32Array::from(vec![
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0,
        ]));
        let vectors = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field, 3, values, None,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(schema, vec![ids, vectors]).unwrap();
        // Rows 2..4 of the original batch: expected vectors [7,8,9] and
        // [10,11,12], not the first two rows' [1,2,3]/[4,5,6].
        let sliced = batch.slice(2, 2);

        let deltas = build_delta_entries(&sliced, 0).unwrap();
        assert_eq!(deltas.len(), 2);
        let as_insert = |d: &DeltaEntry| match d {
            DeltaEntry::Insert { row_id, vector } => (*row_id, vector.clone()),
            DeltaEntry::Tombstone { .. } => panic!("expected an Insert entry"),
        };
        assert_eq!(as_insert(&deltas[0]), (0, vec![7.0, 8.0, 9.0]));
        assert_eq!(as_insert(&deltas[1]), (1, vec![10.0, 11.0, 12.0]));
    }

    #[test]
    fn build_delta_entries_errors_on_wrong_inner_type_even_with_zero_rows() {
        // Behavior change from the downcast-hoist rewrite: the Float32
        // downcast used to happen lazily inside the per-row loop, so a
        // batch with zero surviving rows (empty, or every vector null)
        // never triggered it, silently returning Ok(vec![]) even for a
        // wrong-typed vector column. Hoisting the downcast to run once,
        // unconditionally, before the loop now catches this upfront --
        // matching this function's own doc comment ("a `vector` column
        // present with the wrong type... is [an error]"), which the old
        // lazy check didn't actually honor for this case.
        let item_field = Arc::new(Field::new("item", DataType::Int32, false));
        let values = Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]));
        let null_buffer = arrow::buffer::NullBuffer::from(vec![false]);
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
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 3),
                true,
            ),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1])), vectors])
                .unwrap();

        let result = build_delta_entries(&batch, 0);
        assert!(
            result.is_err(),
            "a wrong inner type must error even when every row is null: {result:?}"
        );
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
    fn redeleting_an_already_tombstoned_row_does_not_duplicate_it_in_the_persisted_manifest() {
        let dir = temp_dir("tombstone-dedup-cross-txn");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(batch);
        setup.commit().unwrap();

        let mut txn_a = ds.begin();
        txn_a.delete(0);
        txn_a.commit().unwrap();
        assert_eq!(ds.snapshot().manifest.tombstones.len(), 1);

        // A second, later transaction re-deleting the same already-tombstoned
        // row is Clean (no write-set overlap with anything that committed in
        // between) and must not grow the persisted tombstones list, even
        // though it's a genuinely separate commit.
        let mut txn_b = ds.begin();
        txn_b.delete(0);
        txn_b.commit().unwrap();
        assert_eq!(
            ds.snapshot().manifest.tombstones.len(),
            1,
            "re-deleting an already-tombstoned row in a later transaction must not duplicate it"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn deleting_the_same_row_twice_in_one_transaction_does_not_duplicate_persisted_tombstone() {
        let dir = temp_dir("tombstone-dedup-intra-txn");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(batch);
        setup.commit().unwrap();

        let mut txn = ds.begin();
        txn.delete(0);
        txn.delete(0); // duplicate delete() call within the same transaction
        txn.commit().unwrap();

        assert_eq!(
            ds.snapshot().manifest.tombstones.len(),
            1,
            "calling delete() twice on the same row within one transaction must not duplicate it"
        );

        std::fs::remove_dir_all(&dir).ok();
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
        let dir = tempfile::Builder::new()
            .prefix("strata-replay-cost-regression-")
            .tempdir()
            .unwrap()
            .keep();
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
        let established_before = snapshot_before.index.established_dimension();
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
            snapshot_after.index.established_dimension(),
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
        // instead calls `SegmentSet::search` directly on
        // `snapshot_after.index` (wrapping the same shared `Arc<HnswIndex>`
        // the failed commit mutated in place -- `pub(crate) index` is
        // reachable from this same-crate test) with an always-true
        // visibility predicate, bypassing the watermark filter entirely to
        // see exactly what's physically in the graph.
        let leaked = snapshot_after
            .index
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
    fn a_failed_commits_vector_is_never_searchable_after_a_later_commit_advances_the_watermark() {
        // Regression test for the dangling-search-hit hazard. `commit`
        // applies this transaction's HNSW `Insert` deltas to the shared
        // `Arc<HnswIndex>` *before* `commit_manifest` makes the commit
        // durable. If `commit_manifest` fails (e.g. ENOSPC) — modelled here
        // by `inject_manifest_commit_failure`, injected at exactly that
        // step — the failed transaction's vector is left in the shared graph
        // with no manifest entry backing it, and its row-id was already
        // allocated by `write_phase`. A *later* successful commit
        // then persists `manifest.next_row_id` past that residue row-id and
        // publishes `watermark = next_row_id - 1`, so `Snapshot::is_visible`
        // starts passing for the residue id. With no manifest-membership
        // cross-check on the search path, `vector_search` would then return
        // the residue as a dangling hit — a row `scan` can never see —
        // violating the flagship "no silently stale vector search results"
        // claim. The fix soft-deletes a failed commit's graph inserts on the
        // error path (see `GraphResidueGuard`), so this must hold.
        let dir = temp_dir("failed-commit-no-dangling-search-hit");
        let ds = Dataset::create(&dir).unwrap();

        // Seed: one durable row (system row-id 0), far from the residue's
        // distinctive coordinates. Establishes the graph's dimension and
        // gives the post-seed watermark a meaningful value of 0.
        let mut seed = ds.begin();
        seed.insert(vector_batch(
            vec![1i64],
            cluster_vectors(1, [0.0, 0.0, 0.0], 0.0),
        ));
        seed.commit().unwrap();

        // T1: insert a vector at distinctive, never-reused coordinates, then
        // fail at the manifest-commit step (after the delta has already
        // reached the shared graph). Its row-id (1) is allocated but never
        // committed.
        let mut failing = ds.begin();
        failing.insert(vector_batch(
            vec![2i64],
            cluster_vectors(1, [900.0, 900.0, 900.0], 0.0),
        ));
        failing.inject_manifest_commit_failure();
        let failing_result = failing.commit();
        assert!(
            failing_result.is_err(),
            "the injected manifest-commit failure must make T1 fail, else this \
             test proves nothing: {failing_result:?}"
        );
        assert_eq!(
            ds.snapshot().version,
            1,
            "a failed commit must not advance the visible version"
        );

        // T2: an unrelated successful commit at its own distinctive
        // location. This advances `next_row_id` past the residue row-id 1
        // and publishes a watermark (2) that now covers it — the trigger
        // that makes the residue visible to `is_visible`.
        let mut later = ds.begin();
        later.insert(vector_batch(
            vec![3i64],
            cluster_vectors(1, [500.0, 500.0, 500.0], 0.0),
        ));
        later.commit().unwrap();

        let snapshot = ds.snapshot();
        assert!(
            snapshot.is_visible(1),
            "precondition: the later commit must have advanced the watermark \
             past the residue row-id, or this test isn't exercising the hazard"
        );

        // The discriminating assertion: searching at T1's distinctive
        // coordinates must not return its residue vector. Pre-fix, the
        // residue (row-id 1 at [900,900,900]) is both physically in the graph
        // and now visible, so it comes back with ~0 squared distance.
        // Post-fix it was soft-deleted on T1's error path, so the nearest
        // live match is the far-away seed/T2 data.
        let results = snapshot
            .vector_search(&[900.0, 900.0, 900.0], 1, None)
            .unwrap();
        assert!(
            results.is_empty() || results[0].squared_distance > 1000.0,
            "a failed commit's vector must never be searchable, even after a \
             later commit advances the watermark past its row-id: {results:?}"
        );

        // Positive controls, so the assertion above can't pass vacuously.
        // The failed transaction really did reach the graph (it established
        // the dimension, which no removal resets), and search itself really
        // is working on this snapshot — so "not found" above means
        // *excluded*, not "nothing was ever inserted" or "search is broken".
        assert_eq!(
            snapshot.index.established_dimension(),
            3,
            "the failed commit's vector must genuinely have reached the graph"
        );
        let seed_hit = snapshot.vector_search(&[0.0, 0.0, 0.0], 1, None).unwrap();
        assert_eq!(
            seed_hit.first().map(|m| m.row_id),
            Some(0),
            "the durably committed seed row must still be searchable: {seed_hit:?}"
        );

        // Cross-check the search/scan consistency directly: only the seed and
        // the later commit are durably in the manifest, so a scan sees
        // exactly 2 rows — the residue is not among them.
        assert_eq!(
            snapshot.scan(&vector_test_schema()).unwrap().num_rows(),
            2,
            "only the seed and the later commit are durably committed; the \
             failed commit's row must never appear in a scan"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_concurrent_reader_never_sees_an_in_flight_commits_vector() {
        // Regression test for the snapshot-isolation window spec §2 rules
        // out: "a transaction's writes are never visible to any other
        // transaction until commit succeeds."
        //
        // Row-ids are claimed *before* `commit_lock`, in
        // `write_phase`. The visibility watermark, though, is
        // published from the *global* row-id counter inside some *other*
        // transaction's critical section — so that other transaction's
        // watermark covers row-ids this transaction has claimed but not
        // committed. Between this transaction's `graph.insert` and its
        // `commit_manifest`, its vector is therefore both physically in the
        // shared `Arc<HnswIndex>` and (pre-fix) passing `is_visible` on the
        // currently published snapshot. Readers take no `commit_lock`
        // (`Snapshot::vector_search`), so nothing stops one observing it —
        // a search hit for a row no `scan` can see, roughly one
        // `commit_manifest` fsync wide.
        //
        // Unlike `a_failed_commits_vector_is_never_searchable_...` above,
        // this is the *success* path: the slow transaction goes on to
        // commit cleanly. `GraphResidueGuard` deliberately does not close
        // this (see its doc comment) — it closes the permanent-residue case.
        //
        // The window is one fsync wide, so the schedule is made
        // deterministic with `Checkpoint`s rather than raced with sleeps: a
        // sleep-based version would pass vacuously whenever it missed.
        let dir = temp_dir("in-flight-commit-not-visible-to-reader");
        let ds = Dataset::create(&dir).unwrap();

        // Seed row-id 0: establishes the graph's dimension and gives the
        // pre-existing watermark a meaningful value.
        let mut seed = ds.begin();
        seed.insert(vector_batch(
            vec![1i64],
            cluster_vectors(1, [0.0, 0.0, 0.0], 0.0),
        ));
        seed.commit().unwrap();

        let (claim_point, claimed) = checkpoint_pair();
        let (apply_point, applied) = checkpoint_pair();

        // The slow transaction: inserts at distinctive, never-reused
        // coordinates so a hit for it is unambiguous.
        let mut slow = ds.begin();
        slow.insert(vector_batch(
            vec![2i64],
            cluster_vectors(1, [900.0, 900.0, 900.0], 0.0),
        ));
        slow.pause_after_row_id_claim(claim_point);
        slow.pause_after_graph_apply(apply_point);
        let slow_thread = std::thread::spawn(move || slow.commit());

        // Step 1: the slow transaction has claimed row-id 1 and written its
        // data files, but holds no lock and has touched nothing shared.
        claimed.wait();

        // Step 2: an unrelated transaction commits. It claims row-id 2 and
        // publishes `manifest.next_row_id = 3` — read from the global
        // counter, which already includes the slow transaction's claim — so
        // its watermark (2) covers the slow transaction's uncommitted
        // row-id 1. An insert-only transaction has an empty write-set, so
        // this cannot conflict with the slow one.
        let mut other = ds.begin();
        other.insert(vector_batch(
            vec![3i64],
            cluster_vectors(1, [500.0, 500.0, 500.0], 0.0),
        ));
        other.commit().unwrap();

        // Step 3: release the slow transaction as far as the shared graph,
        // and stop it before `commit_manifest`. The window is now open.
        claimed.release();
        applied.wait();

        // Step 4: a reader thread races the apply loop. It takes no
        // `commit_lock`, so it runs freely while the slow commit is parked
        // mid-critical-section.
        let reader_ds = ds.clone();
        let (version, results) = std::thread::spawn(move || {
            let snapshot = reader_ds.snapshot();
            let results = snapshot
                .vector_search(&[900.0, 900.0, 900.0], 1, None)
                .unwrap();
            (snapshot.version, results)
        })
        .join()
        .unwrap();

        assert_eq!(
            version, 2,
            "precondition: the reader must see the unrelated commit's version, \
             not the slow transaction's (which has not committed yet)"
        );

        // The discriminating assertion. Pre-fix the slow transaction's
        // vector is in the graph and `is_visible(1)` passes, so row-id 1
        // comes back at ~0 squared distance. Asserted on the row-id rather
        // than a distance threshold, so it can't pass on an empty result
        // set — visibility is filtered *during* traversal, so a miss here
        // would otherwise be indistinguishable from a broken search.
        assert_ne!(
            results.first().map(|m| m.row_id),
            Some(1),
            "an in-flight transaction's vector must not be visible to any other \
             transaction before its commit succeeds (spec §2/§3 step 5): {results:?}"
        );

        // Positive controls, so "not found" above means *excluded* rather
        // than "search is broken" or "the exclusion set over-hides". The
        // first is the other direction of the invariant at `Dataset` level:
        // the concurrent committer's own row must be visible in the very
        // snapshot it published, even with another claim outstanding.
        assert_eq!(
            results.first().map(|m| m.row_id),
            Some(2),
            "the nearest committed row must still come back — only the in-flight \
             claim may be hidden: {results:?}"
        );
        let committed_hit = ds
            .snapshot()
            .vector_search(&[0.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert_eq!(
            committed_hit.first().map(|m| m.row_id),
            Some(0),
            "the durably committed seed row must still be searchable: {committed_hit:?}"
        );

        // Step 5: let the slow transaction finish. Its row is committed now,
        // so it must become visible — the fix must hide in-flight rows, not
        // committed ones.
        applied.release();
        slow_thread.join().unwrap().unwrap();

        let after = ds
            .snapshot()
            .vector_search(&[900.0, 900.0, 900.0], 1, None)
            .unwrap();
        assert_eq!(
            after.first().map(|m| m.row_id),
            Some(1),
            "once its commit succeeds, the same row must be searchable: {after:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    // TEST_COMMIT_LOG_CAPACITY comfortably fits in i64/i16 for this loop's
    // small range (capacity + 2), matching the existing cast-allow precedent
    // on `cluster_vectors` above.
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    fn a_transaction_whose_history_has_aged_out_of_the_commit_log_conflicts_conservatively() {
        // Uses TEST_COMMIT_LOG_CAPACITY (not the real, much larger
        // production COMMIT_LOG_CAPACITY) via
        // create_with_commit_log_capacity — see that constant's doc
        // comment for why this is exactly as rigorous a test.
        let dir = temp_dir("commit-log-wraparound-e2e");
        let ds = Dataset::create_with_commit_log_capacity(&dir, TEST_COMMIT_LOG_CAPACITY).unwrap();
        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(batch);
        setup.commit().unwrap();

        // txn begins here, before every filler commit below — its
        // base_version stays fixed at whatever ds.current_version()
        // is right now.
        let mut txn = ds.begin();
        txn.delete(0);

        // Commit enough disjoint no-op-ish filler transactions to push the
        // CommitLog's oldest retained entry past txn's read-version.
        for i in 0..(TEST_COMMIT_LOG_CAPACITY as i64 + 2) {
            let filler = vector_batch(
                vec![100 + i],
                cluster_vectors(1, [f32::from(i as i16), 0.0, 0.0], 0.0),
            );
            let mut filler_txn = ds.begin();
            filler_txn.insert(filler);
            filler_txn.commit().unwrap();
        }

        assert_eq!(
            ds.insufficient_history_conflict_count(),
            0,
            "no InsufficientHistory conflict should have fired yet"
        );

        let result = txn.commit();
        assert!(
            matches!(result, Err(TxnError::Conflict { .. })),
            "expected a conservative conflict once history aged out, got {result:?}"
        );
        assert_eq!(
            ds.insufficient_history_conflict_count(),
            1,
            "the aged-out commit should have incremented the observability counter exactly once"
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
        // base_version has aged out of the ring buffer.
        let dir = temp_dir("commit-log-wraparound-insert-only-e2e");
        let ds = Dataset::create_with_commit_log_capacity(&dir, TEST_COMMIT_LOG_CAPACITY).unwrap();

        // txn begins here, before every filler commit below — its
        // base_version stays fixed at whatever ds.current_version()
        // is right now.
        let mut txn = ds.begin();
        let insert_only_batch =
            vector_batch(vec![99_999], cluster_vectors(1, [500.0, 500.0, 500.0], 0.0));
        txn.insert(insert_only_batch);

        // Commit enough disjoint filler transactions to push the
        // CommitLog's oldest retained entry past txn's read-version.
        for i in 0..(TEST_COMMIT_LOG_CAPACITY as i64 + 2) {
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

    /// Stack for any loom thread that runs a full `Transaction::commit`.
    ///
    /// **Not a tuning knob — omitting it segfaults.** loom runs each model
    /// thread on a `generator` coroutine whose default stack is
    /// `generator::DEFAULT_STACK_SIZE` (`0x1000`), and `Stack::new`
    /// multiplies that by `size_of::<usize>()` — so the default is 32 KiB on
    /// a 64-bit target, not the megabytes a real thread gets. `commit` runs
    /// Arrow encoding, manifest JSON serialization and an HNSW insert, which
    /// overruns that, and a coroutine stack overflow is an access violation
    /// with no backtrace, not a clean `stack overflow` abort. The
    /// `two_threads_deleting_*` models below had fit until this change (they
    /// are sized now regardless); the vector path never had much headroom,
    /// and the row-id allocator's frames removed what was left.
    ///
    /// Only *spawned* threads can be sized (`loom::thread::Builder`); the
    /// model's own root thread always gets the 32 KiB default and loom
    /// exposes no way to change it. So the rule for these models is: the
    /// root thread does setup and assertions only, and every `commit` runs
    /// on a thread spawned through [`spawn_committer`].
    ///
    /// "Setup and assertions" is an empirical boundary, not a safe one. The
    /// root still runs `Dataset::create` (serde_json serialize + write +
    /// fsync, plus `new_hnsw_index`) and, in the residue model,
    /// `Snapshot::vector_search` (HNSW candidate heaps at
    /// `EF_SEARCH_DEFAULT`) — the same *class* of work that just overran 32
    /// KiB, only smaller. Those two are the next suspects if a model here
    /// ever exits 139 again.
    ///
    /// **Spawning is not free: loom caps threads at 5 *created* per
    /// execution** (`loom::MAX_THREADS`), and terminated threads never free
    /// their slot — `rt::thread::new_thread` asserts against the total ever
    /// created. The cap is not raisable either; it sizes fixed-length arrays
    /// inside loom (`FirstSeen([u16; MAX_THREADS])`), so a larger
    /// `model::Builder::max_threads` indexes out of bounds. All three
    /// commit-running models sit at 4 of 5 (root + 3). One more
    /// `spawn_committer` in any of them trips an assert inside loom, so a
    /// commit that only needs the stack — not the concurrency — still costs
    /// a hard-capped slot.
    ///
    /// loom documents this value as bytes while `generator` consumes it as
    /// words, so the real stack is 8 MiB today. Left uncompensated on
    /// purpose: `1 << 20` is ample under *both* readings (1 MiB if that
    /// discrepancy is ever fixed, which still far exceeds what `commit`
    /// needs), whereas dividing by 8 to hit a byte target would hard-code a
    /// transitive dependency's undocumented unit and break on exactly that
    /// fix. The measured cost of the over-provision is nil.
    const COMMIT_STACK_SIZE: usize = 1 << 20;

    /// Spawns a loom thread with a stack that can actually hold a
    /// `Transaction::commit`. See [`COMMIT_STACK_SIZE`].
    fn spawn_committer<F, T>(f: F) -> loom::thread::JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        loom::thread::Builder::new()
            .stack_size(COMMIT_STACK_SIZE)
            .spawn(f)
            .expect("loom thread spawn")
    }

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
            let dir = tempfile::Builder::new()
                .prefix(&format!(
                    "strata-loom-conflict-{}-{:?}-",
                    std::process::id(),
                    loom::thread::current().id()
                ))
                .tempdir()
                .unwrap()
                .keep();
            let ds = crate::Dataset::create(&dir).unwrap();
            let schema = StdArc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            ]));
            let batch = arrow::array::RecordBatch::try_new(
                schema,
                vec![StdArc::new(arrow::array::Int64Array::from(vec![1]))],
            )
            .unwrap();
            // Spawned for the stack, not the concurrency — the model root
            // thread cannot hold a `commit` (see `COMMIT_STACK_SIZE`).
            let ds_setup = ds.clone();
            spawn_committer(move || {
                let mut setup = ds_setup.begin();
                setup.insert(batch);
                setup.commit()
            })
            .join()
            .unwrap()
            .unwrap();

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

            let thread_a = spawn_committer(move || txn_a.commit());
            let thread_b = spawn_committer(move || txn_b.commit());

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
            let dir = tempfile::Builder::new()
                .prefix(&format!(
                    "strata-loom-disjoint-{}-{:?}-",
                    std::process::id(),
                    loom::thread::current().id()
                ))
                .tempdir()
                .unwrap()
                .keep();
            let ds = crate::Dataset::create(&dir).unwrap();
            let schema = StdArc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            ]));
            let batch = arrow::array::RecordBatch::try_new(
                schema,
                vec![StdArc::new(arrow::array::Int64Array::from(vec![1, 2]))],
            )
            .unwrap();
            // Spawned for the stack, not the concurrency — the model root
            // thread cannot hold a `commit` (see `COMMIT_STACK_SIZE`).
            let ds_setup = ds.clone();
            spawn_committer(move || {
                let mut setup = ds_setup.begin();
                setup.insert(batch);
                setup.commit()
            })
            .join()
            .unwrap()
            .unwrap();

            let ds_a = ds.clone();
            let ds_b = ds.clone();
            let thread_a = spawn_committer(move || {
                let mut txn = ds_a.begin();
                txn.delete(0);
                txn.commit()
            });
            let thread_b = spawn_committer(move || {
                let mut txn = ds_b.begin();
                txn.delete(1);
                txn.commit()
            });

            assert!(thread_a.join().unwrap().is_ok());
            assert!(thread_b.join().unwrap().is_ok());

            std::fs::remove_dir_all(&dir).ok();
        });
    }

    #[test]
    fn a_published_bound_never_covers_another_transactions_outstanding_claim() {
        // The interleaving proof for the snapshot-isolation fix, and the
        // `loom` test `.claude/rules/concurrency-txn-layer.md` requires for
        // the counter-advance step (spec §8 names it explicitly: "the
        // counter-bump-plus-CAS step needs a `loom` interleaving test
        // proving it is genuinely atomic under concurrent commit
        // attempts").
        //
        // The hazard is a *torn pair*: if a claim could ever be observed
        // after its row-ids were reflected in `next_row_id` but before it
        // appeared in the in-flight registry, a publisher reading that
        // instant would stamp a watermark covering an uncommitted row-id
        // with nothing excluding it — which is exactly the bug being
        // closed. `RowIdAllocator` makes the advance and the registration
        // one locked step; this asserts no interleaving can pull them
        // apart.
        //
        // Modelled on the allocator directly rather than through
        // `Dataset::commit`: the pair's atomicity is the whole property,
        // and a model with no filesystem I/O in it explores exhaustively in
        // a fraction of the time. The end-to-end consequence is covered
        // deterministically by
        // `dataset::tests::a_concurrent_reader_never_sees_an_in_flight_commits_vector`.
        loom::model(|| {
            let allocator = StdArc::new(crate::row_id::RowIdAllocator::new(0));

            // Thread A models a transaction between its pre-lock row-id
            // claim and its commit: it claims and *keeps* the claim, so it
            // is still in flight when the assertions below run.
            let a_allocator = StdArc::clone(&allocator);
            // Unsized on purpose — see `COMMIT_STACK_SIZE`. This model and
            // the next touch only a `Mutex`, an integer add and a tiny
            // `Vec`; no `commit`, so no Arrow/serde/HNSW frames, and 32 KiB
            // is ample. Sizing them would burn 8 MiB per thread per
            // execution for nothing and blur the rule into "size
            // everything", losing why it exists.
            let in_flight = loom::thread::spawn(move || a_allocator.claim(1).unwrap());

            // Thread B models a committer publishing a snapshot: claim,
            // then read the bound it will stamp into that snapshot,
            // excluding only its own about-to-be-durable claim.
            let b_allocator = StdArc::clone(&allocator);
            let publisher = loom::thread::spawn(move || {
                let claim = b_allocator.claim(1).unwrap();
                let bound = b_allocator.visibility_bound_excluding(Some(&claim));
                (claim, bound)
            });

            let in_flight_claim = in_flight.join().unwrap();
            let (published_claim, bound) = publisher.join().unwrap();

            // `Snapshot::is_visible` is `row_id <= watermark`, and
            // `watermark` is `next_row_id - 1` — so "covered by the bound"
            // is exactly `id < next_row_id`.
            let covered = |id: u64| id < bound.next_row_id;
            let excluded = |id: u64| bound.in_flight.iter().any(|range| range.contains(id));

            assert!(
                covered(published_claim.base()) && !excluded(published_claim.base()),
                "a publisher must never hide its own rows: an acknowledged write is \
                 immediately visible"
            );
            assert!(
                !covered(in_flight_claim.base()) || excluded(in_flight_claim.base()),
                "no interleaving may publish a bound that covers a still-outstanding \
                 claim without excluding it — that is a row visible before its commit \
                 succeeded (spec §2)"
            );
        });
    }

    #[test]
    fn no_interleaving_strands_a_claim_in_the_registry() {
        // The other half of the property: exclusion must be *temporary*.
        // A claim that outlived its transaction would blind every later
        // reader to that stretch of row-ids permanently — turning a
        // too-visible bug into a too-invisible one. `RowIdClaim`'s `Drop`
        // is what guarantees it, so this exercises both exit paths (an
        // explicit `release` on the durable path, a bare drop on the
        // abandoned path) against each other.
        loom::model(|| {
            let allocator = StdArc::new(crate::row_id::RowIdAllocator::new(0));

            let committed_allocator = StdArc::clone(&allocator);
            let committed = loom::thread::spawn(move || {
                let mut claim = committed_allocator.claim(2).unwrap();
                claim.release();
            });

            let abandoned_allocator = StdArc::clone(&allocator);
            let abandoned = loom::thread::spawn(move || {
                // Dropped without releasing — an early `?` out of `commit`.
                let _claim = abandoned_allocator.claim(3).unwrap();
            });

            committed.join().unwrap();
            abandoned.join().unwrap();

            let bound = allocator.visibility_bound_excluding(None);
            assert!(
                bound.in_flight.is_empty(),
                "every claim must be released by the time its transaction returns, \
                 under every interleaving: {:?}",
                bound.in_flight
            );
            assert_eq!(
                bound.next_row_id, 5,
                "both claims consumed their ids regardless of outcome — gaps are \
                 safe, reuse is forbidden (spec §8)"
            );
        });
    }

    /// One row, one 3-d vector, in the shape `build_delta_entries` expects.
    /// Defined locally rather than reusing `dataset::tests`' `vector_batch`
    /// so this module stays compilable under `--cfg loom` regardless of
    /// whether `cfg(test)` is also set for that build.
    fn loom_vector_batch(id: i64, vector: [f32; 3]) -> arrow::array::RecordBatch {
        let item = || {
            StdArc::new(arrow::datatypes::Field::new(
                "item",
                arrow::datatypes::DataType::Float32,
                false,
            ))
        };
        let schema = StdArc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new(
                "vector",
                arrow::datatypes::DataType::FixedSizeList(item(), 3),
                false,
            ),
        ]));
        let ids = StdArc::new(arrow::array::Int64Array::from(vec![id]));
        let values = StdArc::new(arrow::array::Float32Array::from(vector.to_vec()));
        let vectors = StdArc::new(arrow::array::FixedSizeListArray::new(
            item(),
            3,
            values,
            None,
        ));
        arrow::array::RecordBatch::try_new(schema, vec![ids, vectors]).unwrap()
    }

    /// One row, no `"vector"` column — so `build_delta_entries` yields no
    /// entries and the commit does zero HNSW work while still claiming a
    /// row-id and advancing the watermark. Used for the committers whose
    /// only job here is to move the watermark, keeping the loom model
    /// small enough to explore.
    fn loom_plain_batch(id: i64) -> arrow::array::RecordBatch {
        let schema = StdArc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        arrow::array::RecordBatch::try_new(
            schema,
            vec![StdArc::new(arrow::array::Int64Array::from(vec![id]))],
        )
        .unwrap()
    }

    #[test]
    fn a_failed_commits_graph_residue_is_never_searchable_under_concurrent_commits() {
        // The interleaving counterpart to
        // `dataset::tests::a_failed_commits_vector_is_never_searchable_after_a_later_commit_advances_the_watermark`.
        // That test fixes the order (fail, then commit); this one lets loom
        // explore every order in which a *compensating* committer and a
        // *succeeding* committer reach and release the commit lock.
        //
        // The property under test is a quiescent one: once both committers
        // have returned, no schedule leaves the failed commit's vector
        // reachable by a search. What it pins down is that
        // `GraphResidueGuard` fires on the error path under every
        // interleaving of the two committers, rather than only in the
        // single order the deterministic sibling test fixes.
        //
        // The complementary *transient* property — that a row is never
        // visible before its commit succeeds, whether or not that commit
        // eventually does — belongs to the in-flight claim registry, and is
        // covered by
        // `a_published_bound_never_covers_another_transactions_outstanding_claim`
        // below (interleavings) and
        // `dataset::tests::a_concurrent_reader_never_sees_an_in_flight_commits_vector`
        // (a real reader thread racing the apply loop end-to-end).
        //
        // Deliberately minimal: only the failing transaction inserts a
        // vector (one HNSW node), and the concurrent committer uses a
        // vector-free batch. `loom::model` re-runs this closure once per
        // interleaving, and every run does real filesystem I/O, so keeping
        // per-run work down is what makes the model tractable.
        loom::model(|| {
            let dir = tempfile::Builder::new()
                .prefix(&format!(
                    "strata-loom-residue-{}-{:?}-",
                    std::process::id(),
                    loom::thread::current().id()
                ))
                .tempdir()
                .unwrap()
                .keep();
            let ds = crate::Dataset::create(&dir).unwrap();

            let ds_failing = ds.clone();
            let ds_ok = ds.clone();

            let failing = spawn_committer(move || {
                let mut txn = ds_failing.begin();
                txn.insert(loom_vector_batch(1, [900.0, 900.0, 900.0]));
                txn.inject_manifest_commit_failure();
                txn.commit()
            });
            let succeeding = spawn_committer(move || {
                let mut txn = ds_ok.begin();
                txn.insert(loom_plain_batch(2));
                txn.commit()
            });

            assert!(
                failing.join().unwrap().is_err(),
                "the injected manifest-commit failure must make this commit fail"
            );
            assert!(
                succeeding.join().unwrap().is_ok(),
                "an insert-only transaction has an empty write-set and cannot conflict"
            );

            // Guarantees the watermark covers whatever row-id the failed
            // commit claimed, in *every* interleaving — without this, the
            // schedules where it claimed a row-id above the watermark would
            // satisfy the assertion below vacuously.
            //
            // Spawned rather than run inline for the stack, not the
            // concurrency: the root thread's 32 KiB cannot hold a `commit`
            // (see `COMMIT_STACK_SIZE`). It is joined immediately, so the
            // schedule is unaffected.
            let ds_final = ds.clone();
            spawn_committer(move || {
                let mut final_txn = ds_final.begin();
                final_txn.insert(loom_plain_batch(3));
                final_txn.commit()
            })
            .join()
            .unwrap()
            .unwrap();

            let results = ds
                .snapshot()
                .vector_search(&[900.0, 900.0, 900.0], 1, None)
                .unwrap();
            assert!(
                results.is_empty(),
                "the failed commit's vector was the only one ever inserted, and it \
                 must never be searchable under any interleaving: {results:?}"
            );

            std::fs::remove_dir_all(&dir).ok();
        });
    }
}
