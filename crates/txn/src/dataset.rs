//! Transaction path for `strata-txn`. Implements spec §3's commit protocol
//! in full, including real OCC conflict detection and an atomic
//! commit critical section (Phase 6) — see
//! `docs/design.md`
//! and `docs/phase-1-audit.md` before editing anything
//! here. Conflict detection is write-write only, keyed by row-id, and
//! scoped to in-process concurrency (multiple threads/tasks sharing one
//! `Dataset` handle) — see the design doc §1 for why cross-process
//! visibility and read-set tracking are explicit non-goals for this slice,
//! not gaps.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64};

#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(not(loom))]
use std::sync::Mutex;

// `arc_swap::ArcSwap` backs `SnapshotCell` in production (see that type's
// doc comment below) but is unused under `--cfg loom`, where `SnapshotCell`
// is a `Mutex`-backed shim instead — gated the same way `Mutex` above is,
// to avoid an unused-import warning in the loom build.
#[cfg(not(loom))]
use arc_swap::ArcSwap;
use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
use strata_storage::{
    ColumnStats, DataFileEntry, Manifest, SegmentEntry, Value, commit_manifest, compute_stats,
    initialize_row_id_high_water, read_batch, read_current, read_row_id_high_water, sync_dir,
    write_batch, write_bytes,
};

use crate::commit_log::{CommitLog, ConflictCheck};
use crate::error::{Result, TxnError};
use crate::live_set_cache::LiveSetCache;
use crate::row_id::{RowIdAllocator, RowIdRange};
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
/// `docs/design.md`.
pub const ROW_ID_COLUMN: &str = "_row_id";

/// The hidden internal commit-time column every committed batch carries
/// alongside its logical columns and `_row_id` — see
/// `docs/design.md`.
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

/// Storage backing `Dataset.current` / `Transaction.current` — the shared
/// cell holding whichever `Snapshot` is currently visible to new readers.
///
/// In production this is `arc_swap::ArcSwap`, chosen for its lock-free
/// load/store. `arc_swap` (1.9.2, per `Cargo.lock`) has no documented `loom`
/// integration or feature flag — confirmed against docs.rs/arc-swap/1.9.2,
/// crates.io's listed features, and the crate's own upstream `Cargo.toml`
/// (features: `weak`, `internal-test-strategies`, `experimental-strategies`,
/// `experimental-thread-local` — no mention of loom). `loom`'s DPOR
/// scheduler only branches at accesses to primitives it instruments; a
/// reader calling `Dataset::snapshot()` (-> `ArcSwap::load_full`) and a
/// committer calling `Transaction::commit`'s final `ArcSwap::store` are, as
/// far as loom can see, two threads doing nothing at all to shared state —
/// so DPOR collapses them to a single equivalence class instead of
/// exploring the relative orderings that actually matter. That is the
/// mechanism behind this crate's loom models needing this shim at all: see
/// `loom_tests::a_failed_commits_segment_is_never_visible_to_a_concurrent_reader`
/// and `loom_tests::a_commits_row_and_its_segment_become_visible_as_one_atomic_step`.
///
/// Under `--cfg loom`, `SnapshotCell` becomes a `Mutex`-backed equivalent
/// exposing the same `load`/`load_full`/`store` surface `Dataset` and
/// `Transaction` actually call (checked against every call site before
/// choosing this shape — there are exactly three: `snapshot()`'s
/// `load_full`, `write_phase`'s `load`, and `commit`'s `load_full` +
/// `store`). This follows `row_id.rs`'s existing
/// `#[cfg(loom)]`/`#[cfg(not(loom))]` dual-primitive pattern precedent, not
/// a new abstraction: a small `#[cfg]`-gated type standing in for a
/// production primitive loom cannot see into.
///
/// The non-loom definition is a bare type alias, so `Arc<SnapshotCell>` is
/// byte-identical to `Arc<ArcSwap<Snapshot>>` and every call
/// (`SnapshotCell::new`, `.load()`, `.load_full()`, `.store()`) resolves
/// straight to `ArcSwap`'s own method of the same name — this shim changes
/// nothing about the production build's type, behavior, or performance.
#[cfg(not(loom))]
type SnapshotCell = ArcSwap<Snapshot>;

/// `loom`-instrumented stand-in for [`SnapshotCell`] under `--cfg loom` —
/// see that type's doc comment for why this exists at all. A `Mutex` rather
/// than an atomic-pointer swap because loom has no lock-free "swap an `Arc`"
/// primitive analogous to `ArcSwap`; the only property this shim needs to
/// provide is that a reader's load and a writer's store are both
/// loom-instrumented, dependent accesses on the same object, and a `Mutex`
/// around the shared `Arc<Snapshot>` gives exactly that. It is not
/// attempting to model `ArcSwap`'s lock-freedom, only its externally
/// observable load/store behavior.
#[cfg(loom)]
struct SnapshotCell(Mutex<Arc<Snapshot>>);

#[cfg(loom)]
impl SnapshotCell {
    /// Mirrors `ArcSwap::new`.
    fn new(snapshot: Arc<Snapshot>) -> Self {
        Self(Mutex::new(snapshot))
    }

    /// Mirrors `ArcSwap::load_full` — clones the currently-stored `Arc`.
    fn load_full(&self) -> Arc<Snapshot> {
        Arc::clone(&self.lock())
    }

    /// Mirrors `ArcSwap::load`. Real `ArcSwap::load` returns a lightweight
    /// `Guard` rather than cloning the `Arc`; under a `Mutex` there is no
    /// equivalent, so this clones like `load_full` does. Not on any
    /// interleaving-sensitive path this shim exists to test — `write_phase`
    /// uses it for a pre-lock, best-effort dimension check that gets
    /// re-validated inside `commit_lock` regardless (see `commit`'s doc
    /// comment on the authoritative in-lock re-check).
    fn load(&self) -> Arc<Snapshot> {
        self.load_full()
    }

    /// Mirrors `ArcSwap::store`.
    fn store(&self, snapshot: Arc<Snapshot>) {
        *self.lock() = snapshot;
    }

    /// A poisoned lock is recovered rather than propagated, for the same
    /// reason `RowIdAllocator::lock` and `Dataset.commit_lock` are: the
    /// guarded state is only ever replaced by a whole-value assignment, so a
    /// panicking holder cannot leave it half-updated.
    fn lock(&self) -> loom::sync::MutexGuard<'_, Arc<Snapshot>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone)]
pub struct Dataset {
    dir: PathBuf,
    current: Arc<SnapshotCell>,
    /// Hands out contiguous row-id ranges from a single global counter.
    /// See [`crate::row_id`] for why the counter needs a lock rather than a
    /// bare `AtomicU64`, and for why this no longer also tracks which
    /// claims are still in flight.
    row_ids: Arc<RowIdAllocator>,
    /// Monotonic counter whose sole job is generating a collision-free
    /// filename prefix for each commit *attempt*'s data/segment files —
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
    /// acquires `row_ids`' lock (via `claim`/`next_row_id`) both before
    /// taking this one and while holding it, but nothing ever reaches for
    /// this one from inside `row_ids`. See [`crate::row_id`]'s module doc.
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
/// `Transaction::commit`, and `load_segments`, which each need it from a
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
/// `docs/design.md`.
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
/// `fetch_add`'d before every data/segment file write and persisted
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
    /// `dir`'s immediate parent must already exist: it is the caller's
    /// durable anchor. Creation synchronizes the dataset directory and that
    /// immediate parent, including when a retry follows a pre-publication
    /// directory-sync failure. It does not create or synchronize an
    /// arbitrary caller-owned ancestor chain.
    ///
    /// A directory-sync failure after initial-manifest publication is
    /// uncertain: the manifest can be visible although this call returns an
    /// error. In that case, call [`Dataset::open`] before retrying creation.
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
    /// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let dataset = Dataset::create(&dir, Arc::clone(&schema))?;
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])?;
    ///
    /// let mut txn = dataset.begin();
    /// txn.insert(batch)?;
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
    /// `dir`, [`TxnError::NotFound`] if its immediate parent is absent, or an
    /// I/O/storage error if the bounded directory-sync or initial-manifest
    /// publication path fails.
    pub fn create(dir: impl Into<PathBuf>, schema: SchemaRef) -> Result<Self> {
        Self::create_with_commit_log_capacity(dir, schema, COMMIT_LOG_CAPACITY)
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
        schema: SchemaRef,
        commit_log_capacity: usize,
    ) -> Result<Self> {
        validate_dataset_schema(&schema)?;
        let dir = dir.into();
        if read_current(&dir)?.is_some() {
            return Err(TxnError::AlreadyExists(dir));
        }
        sync_dataset_directory_anchor(&dir)?;
        let manifest = Manifest::empty_with_schema(schema.as_ref());
        initialize_row_id_high_water(&dir)?;
        commit_manifest(&dir, &manifest)?;
        let last_issued_timestamp = Arc::new(AtomicI64::new(manifest.commit_time_high_water));
        let durable_high_water = read_row_id_high_water(&dir)?.unwrap_or(0);
        let row_ids = Arc::new(RowIdAllocator::new(
            dir.clone(),
            manifest.next_row_id.max(durable_high_water),
        ));
        let write_attempt_counter = Arc::new(AtomicU64::new(manifest.next_attempt_id));
        let snapshot = Snapshot {
            dir: dir.clone(),
            version: manifest.version,
            schema,
            manifest: Arc::new(manifest),
            // A brand-new dataset has committed no vectors, so it has no
            // segments — not an empty graph. `vector_search` on it returns
            // an empty result, which is what it always did.
            index: strata_index::SegmentSet::empty(),
            tombstones: Arc::new(imbl::HashSet::new()),
            live_set_cache: LiveSetCache::new(crate::snapshot::LIVE_SET_CACHE_BYTE_BUDGET),
        };
        Ok(Self {
            dir,
            current: Arc::new(SnapshotCell::new(Arc::new(snapshot))),
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
    /// Index recovery is loading `manifest.segments` — `O(bytes)` of
    /// validation per segment, with zero distance evaluations and zero
    /// graph construction — not replaying an insert log.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::datatypes::Schema;
    /// use strata_txn::Dataset;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = std::env::temp_dir()
    ///     .join(format!("strata-doctest-open-{}", std::process::id()));
    /// Dataset::create(&dir, Arc::new(Schema::empty()))?; // must exist first — `open` errors on a missing dataset
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
        let manifest_path = dir
            .join("_versions")
            .join(format!("{:020}.manifest", manifest.version));
        let schema = manifest.schema(&manifest_path)?;
        validate_dataset_schema(&schema)?;
        // The capacity guard used to live inside the old delta-replay open
        // path, which sized an `HnswIndex` from `next_row_id`. Nothing sizes
        // an allocation from it any more, but the ceiling is still a
        // panic-safety bound on what row-ids may reach `NodeTable` — see
        // `MAX_REASONABLE_ROW_ID_CAPACITY`.
        let durable_high_water = read_row_id_high_water(&dir)?.unwrap_or(0);
        let allocation_floor = manifest.next_row_id.max(durable_high_water);
        if allocation_floor > MAX_REASONABLE_ROW_ID_CAPACITY {
            return Err(TxnError::UnreasonableCapacity(
                allocation_floor,
                MAX_REASONABLE_ROW_ID_CAPACITY,
            ));
        }
        let owned_rows = validate_data_files(&dir, &manifest, &schema)?;
        validate_tombstones(&dir, &manifest, &owned_rows)?;
        let index = load_segments(&dir, &manifest, &owned_rows)?;
        // The manifest's tombstone list is now the only source: index-level
        // tombstone entries went away with the delta log, and never had a
        // producer on the commit path anyway.
        let tombstones: imbl::HashSet<u64> = manifest.tombstones.iter().copied().collect();
        let row_ids = Arc::new(RowIdAllocator::new(dir.clone(), allocation_floor));
        // The real fix for the cross-session filename-collision bug: seed
        // from the persisted `manifest.next_attempt_id`, not 0. Without
        // this, a reopened dataset would regenerate the same
        // `{attempt_id:020}-{i}.arrow` filenames a prior
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
            schema,
            manifest: Arc::new(manifest),
            index,
            tombstones: Arc::new(tombstones),
            live_set_cache: LiveSetCache::new(crate::snapshot::LIVE_SET_CACHE_BYTE_BUDGET),
        };
        Ok(Self {
            dir,
            current: Arc::new(SnapshotCell::new(Arc::new(snapshot))),
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

    /// The immutable logical schema supplied when this dataset was created.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        self.snapshot().schema()
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
            base_snapshot: Arc::clone(&snapshot),
            schema: snapshot.schema(),
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
            #[cfg(any(test, loom))]
            inject_panic_before_manifest_commit: false,
            #[cfg(test)]
            pause_after_row_id_claim: None,
            #[cfg(test)]
            pause_before_manifest_commit: None,
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

fn validate_dataset_schema(schema: &SchemaRef) -> Result<()> {
    let mut names = HashSet::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name();
        if HIDDEN_COLUMNS.contains(&name.as_str()) {
            return Err(TxnError::ReservedColumnName(name.to_string()));
        }
        if !names.insert(name.clone()) {
            return Err(TxnError::BatchSchemaMismatch {
                expected: "unique dataset field names".to_string(),
                actual: format!("duplicate field name {name:?}"),
            });
        }
    }
    Ok(())
}

/// Creates `dir/data` after checking that `dir`'s immediate parent already
/// exists, then durably publishes only the dataset-owned directory boundary.
///
/// The immediate parent is the caller's durable anchor. Requiring it to
/// pre-exist prevents `Dataset::create` from silently extending its
/// durability responsibility into an arbitrary ancestor chain (which may be
/// inaccessible on Windows). Syncing `dir` makes the new `data` entry
/// durable; syncing its immediate parent makes the new dataset entry durable.
///
/// Both syncs run on every call, including a retry after a prior sync failure:
/// a failed attempt can leave `dir` visible but not known durable.
fn sync_dataset_directory_anchor(dir: &Path) -> Result<()> {
    let parent = dir
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.try_exists()? {
        return Err(TxnError::NotFound(parent.to_path_buf()));
    }
    std::fs::create_dir_all(dir.join("data"))?;
    sync_dir(dir)?;
    sync_dir(parent)?;
    Ok(())
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
    /// The ownership and liveness view captured at `begin`. Delete and
    /// update validate against this immutable base before they buffer any
    /// tombstone or replacement.
    base_snapshot: Arc<Snapshot>,
    /// Dataset-owned logical schema captured from `base_snapshot`.
    schema: SchemaRef,
    pending: Vec<RecordBatch>,
    /// Row-ids queued for tombstoning by [`Transaction::delete`]/
    /// [`Transaction::update`], applied at commit time (see
    /// [`Transaction::commit`]) — mirrors how `pending` buffers inserts.
    pending_tombstones: Vec<u64>,
    /// Every row-id this transaction has written (via `delete`, and
    /// transitively `update`) — consulted by `commit`'s conflict check
    /// against every transaction that committed after this one began.
    write_set: Vec<u64>,
    current: Arc<SnapshotCell>,
    row_ids: Arc<RowIdAllocator>,
    write_attempt_counter: Arc<AtomicU64>,
    commit_lock: Arc<Mutex<CommitLog>>,
    insufficient_history_conflicts: Arc<AtomicU64>,
    /// Consumed by [`Transaction::commit`] via `issue_timestamp`, as the
    /// very first step of `commit`, before `write_phase` runs.
    last_issued_timestamp: Arc<AtomicI64>,
    /// Test-only fault injection: makes [`Transaction::commit`]'s durability
    /// step fail, modelling a recoverable I/O error (e.g. ENOSPC) *after*
    /// this commit's segment (if any) has already been built and fsynced in
    /// `write_phase`. See [`Transaction::inject_manifest_commit_failure`].
    ///
    /// Scoped to one `Transaction` rather than a thread-local because
    /// `loom` multiplexes its model threads, which would let a thread-local
    /// flag armed for one transaction be consumed by another.
    #[cfg(any(test, loom))]
    inject_manifest_commit_failure: bool,
    /// Test-only fault injection: makes [`Transaction::commit`] panic at
    /// the instant between this commit's segment being fsynced and its
    /// manifest being swapped in — the one window where a crash could, in
    /// principle, leave a durable segment referenced by nothing. Distinct
    /// from [`Self::inject_manifest_commit_failure`], which returns a typed
    /// error at the same point: a panic unwinds through every guard and
    /// `Drop` on the way out, which is the failure shape that would expose
    /// a compensating action that only ran on the `?` path.
    ///
    /// Scoped to one `Transaction` rather than a thread-local for the same
    /// reason as the sibling injector: `loom` multiplexes its model
    /// threads.
    #[cfg(any(test, loom))]
    inject_panic_before_manifest_commit: bool,
    /// Test-only: stops this commit at the instant its row-ids have been
    /// claimed but nothing shared has been touched yet. See [`Checkpoint`].
    #[cfg(test)]
    pause_after_row_id_claim: Option<Checkpoint>,
    /// Test-only: stops this commit inside `commit_lock`, after its
    /// conflict check has passed and its segment is already fsynced, but
    /// before `commit_manifest` makes any of it durable. See [`Checkpoint`].
    #[cfg(test)]
    pause_before_manifest_commit: Option<Checkpoint>,
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
/// profile (see `docs/phase-1-audit.md`'s `cargo rustc
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

/// A commit's index segment, in the two forms `commit` needs: the manifest
/// entry that makes it durable, and an already-validated reader over the
/// same bytes that were just fsynced, so the new snapshot's `SegmentSet`
/// needs no read-back.
struct PublishedSegment {
    entry: SegmentEntry,
    reader: Arc<strata_index::SegmentReader>,
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
    /// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let dataset = Dataset::create(&dir, Arc::clone(&schema))?;
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])?;
    ///
    /// let mut txn = dataset.begin();
    /// txn.insert(batch)?;
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
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::BatchSchemaMismatch`] when `batch` does not
    /// exactly match the schema owned by this dataset.
    pub fn insert(&mut self, batch: RecordBatch) -> Result<()> {
        self.validate_batch(&batch)?;
        self.pending.push(batch);
        Ok(())
    }

    /// Tombstones `row_id`, making it invisible to every snapshot taken
    /// after this transaction commits — see spec §2. Buffered, not
    /// applied until [`Transaction::commit`] succeeds, same as
    /// [`Transaction::insert`].
    ///
    /// # Errors
    ///
    /// Returns a typed target error if `row_id` is not live and owned by the
    /// transaction's base snapshot, or was already targeted by this transaction.
    pub fn delete(&mut self, row_id: u64) -> Result<()> {
        self.validate_target(row_id)?;
        self.pending_tombstones.push(row_id);
        self.write_set.push(row_id);
        Ok(())
    }

    /// Test-only: makes this transaction's [`Self::commit`] fail at its
    /// durability step, *after* its segment (if any) has already been built
    /// and fsynced in `write_phase` and its conflict check has passed
    /// in-lock. Models a recoverable I/O failure (e.g. ENOSPC writing the
    /// manifest) — the one failure shape that leaves the process alive and
    /// therefore exposes the dangling-search-hit hazard the old graph-residue
    /// guard used to compensate for. The `chaos-injection` harness cannot stand in
    /// for this: its `chaos_checkpoint` calls `std::process::abort()`, and
    /// the restart that forces *heals* the hazard, since a restart's
    /// `Dataset::open` loads only manifest-listed segments.
    #[cfg(any(test, loom))]
    pub(crate) fn inject_manifest_commit_failure(&mut self) {
        self.inject_manifest_commit_failure = true;
    }

    /// Test-only: see [`Self::inject_panic_before_manifest_commit`].
    #[cfg(any(test, loom))]
    pub(crate) fn inject_panic_before_manifest_commit(&mut self) {
        self.inject_panic_before_manifest_commit = true;
    }

    /// Test-only: stops [`Self::commit`] once this transaction's row-ids
    /// have been claimed and its data files written, but *before* it
    /// acquires `commit_lock` — so a concurrent committer can run to
    /// completion while this transaction's claim is outstanding.
    #[cfg(test)]
    pub(crate) fn pause_after_row_id_claim(&mut self, checkpoint: Checkpoint) {
        self.pause_after_row_id_claim = Some(checkpoint);
    }

    /// Test-only: stops [`Self::commit`] inside `commit_lock`, after the
    /// conflict check and after this commit's `.seg` file is durable, but
    /// before `commit_manifest`. The instant at which a concurrent reader
    /// could observe a partially-applied commit, if one were possible —
    /// after W3.2a it is not, because nothing shared has been touched yet.
    #[cfg(test)]
    pub(crate) fn pause_before_manifest_commit(&mut self, checkpoint: Checkpoint) {
        self.pause_before_manifest_commit = Some(checkpoint);
    }

    /// Tombstones `row_id` and inserts `batch` as its replacement, within
    /// the same transaction — commits atomically as one unit. Equivalent
    /// to calling [`Transaction::delete`] then [`Transaction::insert`],
    /// provided as one call because that's the common case and keeps the
    /// write-set bookkeeping (used by conflict detection) obviously
    /// correct at the call site rather than relying on the caller to
    /// remember both.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::InvalidUpdateShape`] unless `batch` contains
    /// exactly one row, or a typed schema/target error when either input is
    /// invalid for this dataset and transaction.
    pub fn update(&mut self, row_id: u64, batch: RecordBatch) -> Result<()> {
        self.validate_batch(&batch)?;
        if batch.num_rows() != 1 {
            return Err(TxnError::InvalidUpdateShape {
                actual_rows: batch.num_rows(),
            });
        }
        self.validate_target(row_id)?;
        self.pending_tombstones.push(row_id);
        self.write_set.push(row_id);
        self.pending.push(batch);
        Ok(())
    }

    fn validate_batch(&self, batch: &RecordBatch) -> Result<()> {
        if batch.schema_ref().as_ref() != self.schema.as_ref() {
            return Err(TxnError::BatchSchemaMismatch {
                expected: format!("{:?}", self.schema),
                actual: format!("{:?}", batch.schema_ref()),
            });
        }
        Ok(())
    }

    fn validate_target(&self, row_id: u64) -> Result<()> {
        if self.pending_tombstones.contains(&row_id) {
            return Err(TxnError::DuplicateTarget { row_id });
        }
        if !self.base_snapshot.owns_row(row_id) {
            return Err(TxnError::RowNotFound { row_id });
        }
        if !self.base_snapshot.is_visible(row_id) {
            return Err(TxnError::RowNotLive { row_id });
        }
        Ok(())
    }

    /// Commits per spec §3's write/durability steps (3-5), with Phase 6's
    /// real conflict check (§3.1/§3.2) in front of them. Data files and this
    /// commit's index segment are both built and fsynced outside any lock in
    /// `write_phase` (they are unique to this transaction); then, inside
    /// `Dataset.commit_lock`, the *latest* committed snapshot is re-read (not
    /// this transaction's stale `begin()`-time view), and
    /// `CommitLog::conflicts_with` checks every version that landed in
    /// between against this transaction's write-set.
    ///
    /// A conflicting transaction leaves the manifest — and therefore every
    /// reader's index view — completely untouched. The new manifest,
    /// segment list and tombstone set are layered on top of the latest
    /// snapshot's state, so a clean commit composes with whatever else
    /// committed after this transaction began. Only after `commit_manifest`
    /// succeeds is the new `Snapshot` swapped in.
    ///
    /// **This commit's index segment is built, serialized and fsynced in
    /// `write_phase`, outside `commit_lock`** — the real HNSW construction
    /// cost is not in the critical section, and the in-lock step performs
    /// no index mutation of any kind. An interrupted or unfsynced segment
    /// write leaves an orphaned `.seg` file that no manifest references,
    /// exactly like an orphaned row data file.
    ///
    /// Any `Dataset` handle sharing this same `SnapshotCell` (including the
    /// one this transaction was created from) observes the new state on its
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
    /// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let dataset = Dataset::create(&dir, Arc::clone(&schema))?;
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])?;
    ///
    /// let mut txn = dataset.begin();
    /// txn.insert(batch)?;
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
    /// another transaction that committed after this one began wrote any row
    /// in this transaction's write-set. Returns
    /// [`TxnError::InsufficientHistory`] when the bounded in-memory commit
    /// log has evicted history needed to prove cleanliness; that conservative
    /// rejection reports retained-version context and never invents contested
    /// row IDs.
    ///
    /// Returns [`TxnError::NonFiniteVectorComponent`] if any pending batch's
    /// vector column contains a `NaN`/`Infinity` component — checked, and
    /// rejected, before any file for that batch is written to disk. Returns
    /// [`TxnError::Index`] wrapping a `DimensionMismatch` if this commit's
    /// vectors disagree with each other or with the dimension already
    /// established by committed segments — checked twice: once in
    /// `write_phase`, before the segment is built (so a half-built segment
    /// can never be fsynced), against a snapshot that may be stale by the
    /// time `commit_lock` is acquired; and again inside `commit_lock`,
    /// against the *freshly reloaded* `latest_snapshot`, which is what
    /// actually closes the race between two concurrent first-vector commits
    /// at different dimensions (the pre-lock check alone would let both
    /// pass when neither has established a dimension yet). Also returns an
    /// error if any pending batch fails to dictionary-encode, if the segment
    /// can't be serialized or written, or if the manifest commit's atomic
    /// rename fails.
    ///
    /// **Every one of these leaves the dataset with nothing this transaction
    /// wrote reachable by any later reader**, and needs no compensating
    /// action to make that true. The manifest stays unadvanced, so this
    /// commit's data files and its `.seg` file are orphaned on disk and
    /// invisible to both [`crate::Snapshot::scan`] (which reads only
    /// manifest-listed data files) and [`crate::Snapshot::vector_search`]
    /// (which searches only manifest-listed segments). There is no shared
    /// mutable graph for a failed commit to leave residue in — the old
    /// graph-residue guard that used to compensate for that hazard was
    /// removed in S1 W3.2b once this structural guarantee took over.
    ///
    /// Two in-memory traces do outlive a failed commit, neither reachable
    /// as data: the row-ids it claimed (never recycled — a row-id gap is
    /// explicitly safe, a *searchable* gap is not, spec §8), and the
    /// orphaned `.seg` file itself, which stays on disk until a future
    /// garbage-collection pass. Unlike before W3.2a, a failed first-ever
    /// vector commit no longer poisons the session's established dimension:
    /// that is read from the manifest's segments, which the failed commit
    /// never joined.
    ///
    /// # Panics
    ///
    /// Only in test/loom builds, and only if this transaction's caller
    /// explicitly armed `Self::inject_panic_before_manifest_commit`: this
    /// then panics at the instant this commit's segment is fsynced but its
    /// manifest is not, modelling a crash there. Absent entirely from
    /// production builds and never triggered otherwise.
    #[allow(clippy::too_many_lines)]
    pub fn commit(self) -> Result<()> {
        let ts = issue_timestamp(&self.last_issued_timestamp)?;
        let data_dir = data_subdir(&self.dir);

        let (new_data_files, new_segment) = self.write_phase(&data_dir, ts)?;

        // Test-only rendezvous: row-ids claimed, data files written, this
        // commit's segment (if any) already fsynced, but `commit_lock` not
        // yet acquired and nothing shared touched. Absent entirely from
        // production builds.
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

        // Conflict detection MUST run before the manifest is touched at all:
        // a transaction that turns out to conflict must leave the manifest,
        // and therefore every reader's index view, completely untouched.
        match commit_log.conflicts_with(self.base_version, latest_version, &self.write_set) {
            ConflictCheck::Clean => {}
            ConflictCheck::Conflict(contested_row_ids) => {
                return Err(TxnError::Conflict { contested_row_ids });
            }
            ConflictCheck::InsufficientHistory {
                oldest_retained_version,
            } => {
                self.insufficient_history_conflicts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(TxnError::InsufficientHistory {
                    base_version: self.base_version,
                    oldest_retained_version,
                    latest_version,
                });
            }
        }

        // The base snapshot check in delete/update gives callers immediate,
        // typed feedback. This second check runs under the publication lock
        // against the latest snapshot, closing the gap to a concurrent
        // tombstone even if a bounded history implementation changes later.
        for &row_id in &self.write_set {
            if !latest_snapshot.owns_live_row(row_id) {
                return Err(TxnError::Conflict {
                    contested_row_ids: vec![row_id],
                });
            }
        }

        let new_version = latest_version
            .checked_add(1)
            .ok_or_else(|| TxnError::ManifestOverflow(format!("version {latest_version} + 1")))?;

        // Tombstones layer on top of the *latest* snapshot's set (not this
        // transaction's stale begin()-time view), so a clean commit
        // composes with everything that landed in between.
        let mut tombstones = latest_snapshot.tombstones.as_ref().clone();

        // **No index mutation happens here, or anywhere else inside this
        // lock.** This commit's segment was built and fsynced in
        // `write_phase`, outside the lock; publishing it is the
        // `manifest.segments.push` below, which is part of the same atomic
        // manifest swap that publishes the row data. That is the entire
        // point of the S1 W3.2 migration.

        // The new manifest is likewise built from the latest snapshot's
        // manifest: this transaction's new data files are *appended* to
        // the latest file list (never substituted for it wholesale —
        // that would silently drop data files committed by concurrent,
        // non-conflicting transactions after this one began).
        let mut manifest = latest_snapshot.manifest.as_ref().clone();
        manifest.version = new_version;
        manifest.data_files.extend(new_data_files);
        // The index side of the same atomic publish. Appended, never
        // substituted: a concurrent, non-conflicting transaction's segment
        // that landed after this one began is already in
        // `latest_snapshot.manifest.segments` and must survive.
        //
        // **The authoritative dimension check.** `write_phase`'s
        // `validate_vector_dimensions` ran *before* `commit_lock` was
        // acquired, against whatever `established_dimension()` was at that
        // (now possibly stale) moment — it is a cheap pre-lock rejection
        // for the common case, not the source of truth. Two concurrent
        // first-vector commits at different dimensions can both read
        // `established_dimension() == 0` there and both pass. This is the
        // re-check that actually closes the race: it reads
        // `latest_snapshot` — the snapshot freshly loaded *inside* this
        // critical section, above — not `write_phase`'s stale view. Because
        // every committer must hold this same `commit_lock` to reach this
        // line, and `self.current` (and therefore `established_dimension()`)
        // is only ever advanced by a prior lock holder before releasing the
        // lock, whichever of two racing dimension-mismatched commits gets
        // here first publishes and establishes the dimension; the second
        // one's `latest_snapshot` (reloaded fresh under this same lock
        // acquisition) already reflects that, so a single comparison —
        // no retry loop — is sufficient. Must run, and fail, before
        // `manifest.segments.push` below and before `commit_manifest`: a
        // rejected commit must leave no trace, same as a conflict
        // rejection above.
        //
        // Precondition this mutual exclusion actually relies on: it holds
        // for every committer sharing *one* `Dataset` instance (one
        // process's one open handle on this directory), not across two
        // separate `Dataset::open` handles on the same directory — that
        // cross-handle case is a pre-existing whole-layer assumption
        // shared with conflict detection and manifest versioning
        // generally (spec §1), not something specific to this check.
        if let Some(published) = &new_segment {
            let established = latest_snapshot.index.established_dimension();
            let segment_dimension = usize::try_from(published.entry.dimension)?;
            if established != 0 && segment_dimension != established {
                return Err(TxnError::Index(
                    strata_index::IndexError::DimensionMismatch {
                        query_len: segment_dimension,
                        expected: established,
                    },
                ));
            }
            manifest.segments.push(published.entry.clone());
        }
        // Read here, while `commit_lock` is held, so no other transaction
        // can publish a snapshot between this read and the store below.
        // `next_row_id` is the allocation high-water mark, which is what
        // the manifest must persist for restart safety (a reopened dataset
        // must never reuse an id, committed or abandoned — spec §8).
        manifest.next_row_id = self.row_ids.next_row_id();
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
        // docs/design.md for why that decoupling is deliberate, not a gap.
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

        // Test-only rendezvous: this commit's `.seg` file and data files are
        // durable and its conflict check has passed, but `commit_manifest`
        // below has not yet made any of it visible. Absent entirely from
        // production builds.
        #[cfg(test)]
        if let Some(checkpoint) = &self.pause_before_manifest_commit {
            checkpoint.arrive();
        }

        // Test-only fault injection modelling a recoverable I/O failure
        // (e.g. ENOSPC) of the durability step below, occurring *after* this
        // commit's segment has already been fsynced and its conflict check
        // has passed. Absent entirely from production builds.
        #[cfg(any(test, loom))]
        if self.inject_manifest_commit_failure {
            return Err(TxnError::Io(std::io::Error::other(
                "injected manifest-commit failure (test fault injection)",
            )));
        }

        // Test-only fault injection modelling a panic at the instant this
        // commit's segment is durable but its manifest is not. Absent
        // entirely from production builds.
        #[cfg(any(test, loom))]
        assert!(
            !self.inject_panic_before_manifest_commit,
            "injected panic between segment fsync and manifest swap (test fault injection)"
        );

        commit_manifest(&self.dir, &manifest)?;

        // This is the durability point: `commit_manifest` has returned
        // successfully, so this commit's rows and segment are now on disk
        // and reachable by a future `Dataset::open`. Nothing from here on
        // may run in a way that could undo the commit — nor does anything
        // need to, since there is nothing left to compensate for.

        commit_log.push(new_version, self.write_set);

        // Only after commit_manifest succeeds does the new state become
        // visible to future Dataset::snapshot() calls — the in-memory swap
        // must never run ahead of the on-disk durability point.
        // The new snapshot's segment set is the previous snapshot's parts
        // plus a reader over the very bytes just fsynced — no read-back.
        let index = match new_segment {
            Some(published) => {
                let zone_map: Arc<dyn std::any::Any + Send + Sync> =
                    Arc::new(published.entry.zone_map);
                latest_snapshot
                    .index
                    .with_appended(published.reader, zone_map)
            }
            None => latest_snapshot.index.clone(),
        };
        debug_assert_eq!(
            index.len(),
            manifest.segments.len(),
            "a snapshot's segment set must be exactly its manifest's segment list"
        );
        let snapshot = Snapshot {
            dir: self.dir,
            version: new_version,
            schema: Arc::clone(&self.schema),
            manifest: Arc::new(manifest),
            index,
            tombstones: Arc::new(tombstones),
            // A fresh, empty cache per commit — see `crate::live_set_cache`'s
            // module doc: the previous snapshot's cache is dropped with it,
            // not carried forward, which is what keeps this bounded.
            live_set_cache: LiveSetCache::new(crate::snapshot::LIVE_SET_CACHE_BYTE_BUDGET),
        };
        self.current.store(Arc::new(snapshot));

        Ok(())
    }

    /// Spec §3 step 3's durable write, run *before* `commit_lock` is
    /// acquired. Claims this transaction's row-ids, writes its data files,
    /// builds and fsyncs this commit's index segment, and fsyncs the data
    /// directory — none of which needs conflict information to proceed, and
    /// none of which can collide with a concurrent transaction's own
    /// writes, because every path it touches is unique to this attempt.
    ///
    /// The filename prefix comes from `write_attempt_counter`, **not**
    /// `base_version + 1`: two truly concurrent transactions can share the
    /// same stale `base_version`, which would make them compute the same
    /// "next version" and collide on the same filename before either
    /// reaches `commit_lock`. `write_attempt_counter` is unique per attempt
    /// regardless of version, which is what makes doing any of this outside
    /// the lock safe at all.
    ///
    /// Building the segment out here is the whole point of the S1 W3.2
    /// migration: the real HNSW construction cost leaves the critical
    /// section entirely, and an interrupted or unfsynced segment write is
    /// just an orphaned file nothing points to — exactly like today's
    /// orphaned row data files.
    ///
    /// Returns the new `DataFileEntry`s and this commit's published segment
    /// (`None` for a commit that carries no vectors — see
    /// [`Self::build_and_write_segment`]).
    ///
    /// # Errors
    ///
    /// Same conditions as [`Transaction::commit`]'s own doc comment:
    /// dictionary-encoding failure, a non-finite vector component, a
    /// vector-dimension disagreement, an I/O failure writing or fsyncing a
    /// file, or [`TxnError::ManifestOverflow`] if the row-id range would run
    /// past `u64::MAX`.
    fn write_phase(
        &self,
        data_dir: &Path,
        ts: i64,
    ) -> Result<(Vec<DataFileEntry>, Option<PublishedSegment>)> {
        // Skipped entirely when there's nothing to insert: a delete-only
        // transaction writes no new files, so there's no new directory
        // entry to create or fsync and no attempt_id needs reserving.
        // `Dataset::create`/`open` already ensure `data_dir` exists once
        // per `Dataset` lifetime; recreating it on every single commit
        // regardless of whether it had anything to write was redundant.
        if self.pending.is_empty() {
            return Ok((Vec::new(), None));
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
        // per site. See `docs/design.md`.
        //
        // Row-ids are *not* handed out this way. `RowIdAllocator::claim`
        // needs a *checked* add — an allocation that would run past
        // `u64::MAX` has to be rejected *before* it consumes any ids —
        // which a bare `fetch_add` cannot express, so that counter stays
        // behind a `Mutex`. See `crate::row_id`'s module doc (§Locking) and
        // `RowIdAllocator::claim`.
        let attempt_id = self
            .write_attempt_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // One claim for the whole transaction, spec §8's "a commit writing
        // N rows atomically claims the contiguous range `[next_row_id,
        // next_row_id + N)`" — rather than the per-pending-batch claim this
        // replaces, which could interleave one transaction's batches with
        // another's under concurrency, leaving neither transaction's
        // row-ids contiguous.
        let total_rows = self.pending.iter().try_fold(0u64, |total, batch| {
            let rows = u64::try_from(batch.num_rows())?;
            total
                .checked_add(rows)
                .ok_or_else(|| TxnError::ManifestOverflow(format!("pending rows {total} + {rows}")))
        })?;
        let claim = self.row_ids.claim(total_rows)?;
        let mut new_data_files = Vec::new();
        let (inserts, zone_map) = Self::write_pending_batches(
            &self.pending,
            data_dir,
            attempt_id,
            &claim,
            ts,
            &mut new_data_files,
        )?;

        // Pre-validate before building anything: `insert_owned`'s only
        // fallible path is dimension validation, so a ragged commit must be
        // rejected before a half-built segment can be produced. Sourced
        // from the current snapshot's segment set, not a live graph handle.
        let established_dimension = self.current.load().index.established_dimension();
        validate_vector_dimensions(&inserts, established_dimension)?;

        let segment = Self::build_and_write_segment(data_dir, attempt_id, inserts, zone_map)?;

        // Fsyncing each file's *content* (already done inside `write_batch`
        // and `write_bytes`) is not sufficient — the new directory entries
        // themselves must also be fsynced, or a real power-loss crash can
        // leave a file's bytes durable while the file itself is absent.
        // Must happen before the manifest commit.
        strata_storage::sync_dir(data_dir)?;
        Ok((new_data_files, segment))
    }

    /// Builds this commit's index segment from `inserts`, writes and fsyncs
    /// it as `{attempt_id:020}.seg`, and returns everything `commit` needs
    /// to publish it — or `None` if this commit carries no vectors.
    ///
    /// **A vector-less commit writes no segment and pushes no
    /// `SegmentEntry`** (post-W3.1 amendment §3c). That is simpler than
    /// writing an empty segment, which would need its own `node_count == 0`
    /// support in `SegmentReader`; `manifest.segments.len() == N` therefore
    /// holds after N *vector-carrying* commits, not after N commits.
    ///
    /// The working index is keyed by **segment-local ordinals `0..N`**, not
    /// by global row-ids (amendment §3b). Two reasons, both concrete:
    /// `NodeTable` demand-allocates a fixed-size chunk per 65536-row-id
    /// span regardless of how few ids land in it, so a 10-row commit at
    /// row-id 5,000,000 would allocate a whole chunk for ten slots; and
    /// keying `0..N` makes the segment's `row_ids` section a direct
    /// positional dump that is ascending by construction, with no remap
    /// pass.
    ///
    /// `zone_map` is this commit's already-merged per-column stats (see
    /// [`merge_zone_map_stats`]), attached to the produced `SegmentEntry`
    /// unchanged. Consumed for pruning since S1 W4b by
    /// `strata_txn::snapshot::zone_map_permits_scan` — see the design
    /// amendment. If this commit carries no vectors, the map is simply
    /// discarded along with everything else here — there is no segment to
    /// attach it to.
    ///
    /// # Errors
    ///
    /// [`TxnError::Index`] if the working index rejects an insert or the
    /// serializer rejects the built graph, [`TxnError::Io`] if the `.seg`
    /// file can't be written or fsynced, or [`TxnError::TryFromInt`] if a
    /// count doesn't fit its manifest field.
    fn build_and_write_segment(
        data_dir: &Path,
        attempt_id: u64,
        inserts: Vec<VectorInsert>,
        zone_map: std::collections::HashMap<String, ColumnStats>,
    ) -> Result<Option<PublishedSegment>> {
        if inserts.is_empty() {
            return Ok(None);
        }
        let node_count = inserts.len();
        let index = new_hnsw_index(node_count)?;
        let mut row_ids = Vec::with_capacity(node_count);
        let mut rows = Vec::with_capacity(node_count);
        for (local, insert) in inserts.into_iter().enumerate() {
            row_ids.push(insert.row_id);
            rows.push((u64::try_from(local)?, insert.vector));
        }
        // Gated behind the `parallel-insert` feature (off by default --
        // see that feature's own doc comment in Cargo.toml and
        // `docs/architecture.md`). When enabled, parallelizes
        // across up to `PARALLEL_INSERT_THREADS` worker threads once
        // `rows` is large enough to be worth it (see
        // `insert_batch_parallel`'s own doc comment for the chunking
        // threshold and the mandatory-first-sequential-insert invariant
        // that makes this sound on a brand-new, empty per-commit index).
        // `row_ids` was already fully built above from the original order,
        // so the row-id/ordinal association survives regardless of which
        // thread inserts which ordinal.
        #[cfg(feature = "parallel-insert")]
        index.insert_batch_parallel(rows, PARALLEL_INSERT_THREADS)?;
        // Default path: a plain sequential per-row loop, identical in
        // effect to what `insert_batch_parallel` itself falls back to
        // below its own small-batch threshold -- no worker threads, no
        // chunking, nothing for hazard #4's mechanism (see `Graph::
        // insert`'s doc comment in `crates/index/src/graph.rs`) to exploit
        // even in principle, regardless of how well-verified the fix is.
        #[cfg(not(feature = "parallel-insert"))]
        for (row_id, vector) in rows {
            index.insert_owned(row_id, vector)?;
        }
        let bytes = index.to_segment_bytes(&row_ids)?;

        let name = format!("{attempt_id:020}.seg");
        let path = data_dir.join(&name);
        write_bytes(&path, &bytes)?;

        // Built from the same buffer that was just fsynced — no read-back
        // on the commit path (base design doc §4).
        let reader = strata_index::SegmentReader::from_bytes(&bytes)?;

        // Debug-only structural cross-check that what landed on disk parses
        // back to the same segment. Excluded under `loom`: loom re-runs the
        // model closure once per interleaving, and an extra whole-file read
        // per commit would multiply an already-expensive model's I/O.
        #[cfg(all(debug_assertions, not(loom)))]
        {
            match std::fs::read(&path)
                .map_err(TxnError::from)
                .and_then(|on_disk| {
                    strata_index::SegmentReader::from_bytes(&on_disk).map_err(TxnError::from)
                }) {
                Ok(reread) => {
                    debug_assert_eq!(
                        reread.node_count(),
                        reader.node_count(),
                        "the fsynced segment must parse back to the same node count"
                    );
                    debug_assert_eq!(
                        reread.row_id_range(),
                        reader.row_id_range(),
                        "the fsynced segment must parse back to the same row-id range"
                    );
                    debug_assert_eq!(
                        reread.byte_len(),
                        reader.byte_len(),
                        "the fsynced segment must be exactly as long as the buffer written"
                    );
                }
                Err(e) => debug_assert!(false, "the just-fsynced segment failed to re-read: {e}"),
            }
        }

        // Non-empty (checked above) and strictly ascending (enforced by the
        // serializer, which would have errored otherwise).
        let (Some(&row_id_min), Some(&row_id_max)) = (row_ids.first(), row_ids.last()) else {
            unreachable!("row_ids is non-empty: `inserts` was checked non-empty above")
        };

        let entry = SegmentEntry {
            name,
            format_version: strata_index::SEGMENT_FORMAT_VERSION,
            vector_count: u64::try_from(node_count)?,
            dimension: u32::try_from(index.established_dimension())?,
            row_id_min,
            row_id_max,
            byte_len: u64::try_from(bytes.len())?,
            // Computed (merged across this commit's batches) since S1 W4a —
            // see `merge_zone_map_stats` and the design amendment §5.
            // Consumed for pruning since S1 W4b by `Snapshot::vector_search`
            // via `zone_map_permits_scan`; an absent or empty zone map must
            // still always mean "must scan", never "may prune".
            zone_map,
        };
        Ok(Some(PublishedSegment {
            entry,
            reader: Arc::new(reader),
        }))
    }

    /// Writes every pending batch's data file to
    /// `data_dir`, assigning row-ids out of `claim` and appending
    /// each batch's `DataFileEntry` to `data_files` in place. Returns every
    /// [`VectorInsert`] produced across all pending batches, in order — the
    /// segment build consumes these directly — alongside this commit's
    /// zone map: each batch's own `ColumnStats` (the same map already
    /// attached to that batch's `DataFileEntry`, `_timestamp` entry
    /// included) merged via [`merge_zone_map_stats`] into one map covering
    /// every batch in this commit. `build_and_write_segment` attaches it,
    /// unchanged, to the produced `SegmentEntry` (S1 W4a).
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
    /// component, or an I/O failure writing a data file). Row-id
    /// overflow is no longer possible here — the whole range was bounds-checked
    /// when it was claimed.
    fn write_pending_batches(
        pending: &[RecordBatch],
        data_dir: &Path,
        attempt_id: u64,
        claim: &RowIdRange,
        ts: i64,
        data_files: &mut Vec<DataFileEntry>,
    ) -> Result<(
        Vec<VectorInsert>,
        std::collections::HashMap<String, ColumnStats>,
    )> {
        let mut all_inserts = Vec::new();
        let mut zone_map = std::collections::HashMap::new();
        let mut row_id_base = claim.base();
        for (i, batch) in pending.iter().enumerate() {
            // Stats computed on the original, pre-encoding, pre-hidden-column
            // batch — see docs/design.md
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

            // Same per-batch stats map (already including `_timestamp`)
            // that's about to be attached to this batch's `DataFileEntry`
            // below — merged into this commit's segment-level zone map
            // before `stats` is moved into the `DataFileEntry` push.
            merge_zone_map_stats(&mut zone_map, &stats, i == 0);

            let num_rows = u64::try_from(batch.num_rows())?;

            let inserts = build_vector_inserts(batch, row_id_base)?;
            let with_row_id = append_row_id_column(batch, row_id_base, num_rows)?;
            let with_timestamp = append_timestamp_column(&with_row_id, ts, num_rows)?;

            let encoded = strata_storage::encode_batch(&with_timestamp)?;
            let file_name = format!("{attempt_id:020}-{i}.arrow");
            let metadata = write_batch(&data_dir.join(&file_name), &encoded)?;
            let row_id_range = match num_rows {
                0 => None,
                _ => Some((row_id_base, row_id_base + num_rows - 1)),
            };

            data_files.push(DataFileEntry {
                name: file_name,
                byte_len: metadata.byte_len,
                crc32c: metadata.crc32c,
                row_count: num_rows,
                row_id_range,
                stats,
            });
            all_inserts.extend(inserts);
            // Cannot overflow: `write_phase` sized the claim as the checked
            // sum of every pending batch's row count, and the claim itself
            // was bounds-checked against `u64::MAX` before it was handed
            // out.
            row_id_base += num_rows;
        }
        // `pending` and `claim` arrive as separate parameters, so nothing in
        // the type system ties the claim's size to the rows about to be
        // laid out inside it. If they ever diverge, this hands out row-ids
        // *past* the claimed range — ids some other transaction's claim
        // already covers, which is exactly the reuse spec §8 forbids
        // outright ("gaps are safe, reuse is forbidden"). This is a real
        // runtime check (not a `debug_assert` that release builds silently
        // drop): a divergence is a spec §8 violation that must surface as a
        // typed error, not pass invisibly. `claim.base() + claim.len()`
        // cannot overflow — `RowIdAllocator::claim` already bounds-checked
        // the range against `u64::MAX` before handing it out.
        let claimed_end = claim.base() + claim.len();
        if row_id_base != claimed_end {
            return Err(TxnError::RowIdRangeMismatch {
                claimed_end,
                actual_end: row_id_base,
            });
        }
        Ok((all_inserts, zone_map))
    }
}

/// Merges one batch's `ColumnStats` (`batch_stats`) into a commit's
/// running segment-level zone map (`accumulated`) — S1 W4a, see the design
/// amendment §5 for the binding rule this implements: **a column survives
/// in the merged map only if every batch in the commit contributed a stats
/// entry for it, and every batch's entry is the same `Value` variant.** A
/// column any batch is missing stats for, or that disagrees in type across
/// batches, is dropped from the merged map entirely — never partially
/// represented — matching the "absent/empty zone map always means must
/// scan" fail-safe invariant `SegmentEntry::zone_map` already documents.
///
/// `Value` derives `PartialOrd`, but Rust's *derived* `PartialOrd` on an
/// enum returns a real (not `None`) ordering even when comparing different
/// variants (it orders by declaration position first) — so naively calling
/// `.partial_cmp()`/`<`/`>` across two `Value`s of different variants would
/// silently produce a meaningless-but-valid comparison instead of signaling
/// "can't compare." This matches explicitly on the 4-tuple `(min, other_min,
/// max, other_max)` instead, so a cross-variant tuple can only ever hit the
/// `_` arm (drop the column), never a same-variant comparison arm.
fn merge_zone_map_stats(
    accumulated: &mut std::collections::HashMap<String, ColumnStats>,
    batch_stats: &std::collections::HashMap<String, ColumnStats>,
    is_first_batch: bool,
) {
    if is_first_batch {
        accumulated.clone_from(batch_stats);
        return;
    }
    accumulated.retain(|column, stats| {
        let Some(other) = batch_stats.get(column) else {
            return false; // this batch has no stats for this column at all -> drop
        };
        match (&stats.min, &other.min, &stats.max, &other.max) {
            (
                Value::Int64(a_min),
                Value::Int64(b_min),
                Value::Int64(a_max),
                Value::Int64(b_max),
            ) => {
                stats.min = Value::Int64((*a_min).min(*b_min));
                stats.max = Value::Int64((*a_max).max(*b_max));
                true
            }
            (
                Value::Float64(a_min),
                Value::Float64(b_min),
                Value::Float64(a_max),
                Value::Float64(b_max),
            ) => {
                stats.min = Value::Float64(a_min.min(*b_min));
                stats.max = Value::Float64(a_max.max(*b_max));
                true
            }
            (Value::Utf8(a_min), Value::Utf8(b_min), Value::Utf8(a_max), Value::Utf8(b_max)) => {
                if b_min < a_min {
                    stats.min = Value::Utf8(b_min.clone());
                }
                if b_max > a_max {
                    stats.max = Value::Utf8(b_max.clone());
                }
                true
            }
            _ => false, // type mismatch across batches for this column -> drop, never partially represented
        }
    });
}

// HNSW parameter defaults — tuned via benchmarks, not guessed, per
// docs/architecture.md.
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

// Worker-thread count for `HnswIndex::insert_batch_parallel` at the commit
// site above -- only meaningful, and only compiled, when the
// `parallel-insert` feature is enabled (see that call site's own comment
// and the feature's doc comment in Cargo.toml). Ported from this crate's
// pre-S1 parallel-insert design
// (deferred, not carried over, when Phase S1 merged -- see git history on
// `build_and_write_segment`), which measured (lifecycle_bench, 25k rows,
// 512-dim, 12-core dev machine): threads=1 25.09s (baseline), threads=4
// 9.77s (2.57x), threads=8 10.00s (2.51x -- statistically indistinguishable
// from threads=4).
//
// That measurement's own default was 8, not 4, reasoned as "no real
// downside to leaving headroom" -- but that reasoning assumed `commit_lock`
// serialized every commit, so at most one `insert_batch_parallel` call was
// ever in flight at a time. Post-S1, segment building happens OUTSIDE
// `commit_lock` (the entire point of the S1 W3.2 migration), so N
// concurrently-committing transactions can now each spawn
// `PARALLEL_INSERT_THREADS` workers simultaneously -- the old "no downside"
// argument no longer holds, since 8 threads x N concurrent commits can
// oversubscribe a machine's cores in a way a single in-flight call never
// could. Set to 4 here instead of re-adopting 8: the historical measurement
// above already showed 4 and 8 performing identically for one build, so
// this loses ~nothing in the single-committer case while meaningfully
// bounding worst-case thread count under genuine multi-committer
// contention. Not independently re-validated under real concurrent-commit
// load with this specific value -- a follow-up re-tuning pass under actual
// multi-writer contention (rather than reasoning from a single-committer
// number) would be worth doing before treating 4 as final.
//
// Post-S1 re-measurement after this port landed (`lifecycle_bench`, 25k
// rows, 512-dim, threads=4, single committer): ingest+commit wall time
// dropped from 11.53s (sequential insert, S1-merged baseline) to 3.51s
// (3.28x), confirming the parallel path is doing real work end-to-end
// through the new segment-per-commit build path, not just in isolation.
#[cfg(feature = "parallel-insert")]
const PARALLEL_INSERT_THREADS: usize = 4;

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
///
/// Enforced in [`Dataset::open`] directly (it used to live in the old
/// delta-replay open path, which no longer exists).
const MAX_REASONABLE_ROW_ID_CAPACITY: u64 = 1_000_000_000;

fn manifest_catalog_path(dir: &Path, version: u64) -> PathBuf {
    dir.join("_versions")
        .join(format!("{version:020}.manifest"))
}

fn corrupt_catalog(dir: &Path, manifest: &Manifest, reason: impl Into<String>) -> TxnError {
    TxnError::Storage(strata_storage::StorageError::CorruptManifest(
        manifest_catalog_path(dir, manifest.version),
        reason.into(),
    ))
}

fn stored_type_matches_owned_schema(stored: &DataType, owned: &DataType) -> bool {
    stored == owned || matches!(stored, DataType::Dictionary(_, value) if value.as_ref() == owned)
}

fn validate_physical_batch_schema(
    dir: &Path,
    manifest: &Manifest,
    logical_schema: &SchemaRef,
    batch: &RecordBatch,
    file_name: &str,
) -> Result<()> {
    let physical = batch.schema_ref();
    if physical.metadata() != logical_schema.metadata() {
        return Err(corrupt_catalog(
            dir,
            manifest,
            format!("data file {file_name} does not preserve owned schema metadata"),
        ));
    }
    let expected_len = logical_schema.fields().len() + HIDDEN_COLUMNS.len();
    if physical.fields().len() != expected_len {
        return Err(corrupt_catalog(
            dir,
            manifest,
            format!(
                "data file {file_name} has {} schema fields; expected {expected_len}",
                physical.fields().len()
            ),
        ));
    }

    for (stored, owned) in physical.fields().iter().zip(logical_schema.fields()) {
        if stored.name() != owned.name()
            || stored.is_nullable() != owned.is_nullable()
            || stored.metadata() != owned.metadata()
            || !stored_type_matches_owned_schema(stored.data_type(), owned.data_type())
        {
            return Err(corrupt_catalog(
                dir,
                manifest,
                format!(
                    "data file {file_name} does not preserve owned field {:?}",
                    owned.name()
                ),
            ));
        }
    }

    let hidden = &physical.fields()[logical_schema.fields().len()..];
    let expected_hidden = [
        (ROW_ID_COLUMN, DataType::UInt64),
        (TIMESTAMP_COLUMN, DataType::Int64),
    ];
    for (stored, (name, data_type)) in hidden.iter().zip(expected_hidden) {
        let type_matches = if name == TIMESTAMP_COLUMN {
            stored_type_matches_owned_schema(stored.data_type(), &data_type)
        } else {
            stored.data_type() == &data_type
        };
        if stored.name() != name || !type_matches || stored.is_nullable() {
            return Err(corrupt_catalog(
                dir,
                manifest,
                format!("data file {file_name} has an invalid physical {name} column"),
            ));
        }
    }
    Ok(())
}

fn validate_data_file_row_ids(
    dir: &Path,
    manifest: &Manifest,
    entry: &DataFileEntry,
    batch: &RecordBatch,
    expected_range: Option<(u64, u64)>,
    owned_rows: &mut HashSet<u64>,
) -> Result<()> {
    let row_id_idx = batch.schema_ref().index_of(ROW_ID_COLUMN)?;
    let row_ids = batch
        .column(row_id_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            corrupt_catalog(
                dir,
                manifest,
                format!("data file {} has non-UInt64 _row_id", entry.name),
            )
        })?;
    if row_ids.null_count() != 0 {
        return Err(corrupt_catalog(
            dir,
            manifest,
            format!("data file {} has null _row_id values", entry.name),
        ));
    }
    for (offset, &row_id) in row_ids.values().iter().enumerate() {
        let Some((first, _)) = expected_range else {
            return Err(corrupt_catalog(
                dir,
                manifest,
                format!("empty file {} has row IDs", entry.name),
            ));
        };
        let offset = u64::try_from(offset).map_err(|_| {
            corrupt_catalog(
                dir,
                manifest,
                format!(
                    "data file {} row_id_range offset {offset} cannot be represented as u64",
                    entry.name
                ),
            )
        })?;
        let expected = first.checked_add(offset).ok_or_else(|| {
            corrupt_catalog(
                dir,
                manifest,
                format!(
                    "data file {} row_id_range [{first}, ..] overflows at offset {offset}",
                    entry.name
                ),
            )
        })?;
        if row_id != expected {
            return Err(corrupt_catalog(
                dir,
                manifest,
                format!(
                    "data file {} has row_id {row_id} at offset {offset}; expected {expected}",
                    entry.name
                ),
            ));
        }
        if !owned_rows.insert(row_id) {
            return Err(corrupt_catalog(
                dir,
                manifest,
                format!("row_id {row_id} is owned by more than one data file"),
            ));
        }
    }
    Ok(())
}

fn catalog_row_id_range(
    dir: &Path,
    manifest: &Manifest,
    entry: &DataFileEntry,
) -> Result<Option<(u64, u64)>> {
    match entry.row_count {
        0 => {
            if entry.row_id_range.is_some() {
                return Err(corrupt_catalog(
                    dir,
                    manifest,
                    format!("empty data file {} has a row_id_range", entry.name),
                ));
            }
            Ok(None)
        }
        count => {
            let Some((first, last)) = entry.row_id_range else {
                return Err(corrupt_catalog(
                    dir,
                    manifest,
                    format!("non-empty data file {} is missing row_id_range", entry.name),
                ));
            };
            let expected_last = first.checked_add(count - 1).ok_or_else(|| {
                corrupt_catalog(
                    dir,
                    manifest,
                    format!(
                        "data file {} row_id_range [{first}, {last}] overflows for row_count {count}",
                        entry.name
                    ),
                )
            })?;
            if last != expected_last {
                return Err(corrupt_catalog(
                    dir,
                    manifest,
                    format!(
                        "data file {} has row_id_range [{first}, {last}] incompatible with row_count {count}",
                        entry.name
                    ),
                ));
            }
            Ok(Some((first, last)))
        }
    }
}

fn validate_data_files(
    dir: &Path,
    manifest: &Manifest,
    logical_schema: &SchemaRef,
) -> Result<HashSet<u64>> {
    let data_dir = data_subdir(dir);
    let mut owned_rows = HashSet::new();
    let mut data_file_names = HashSet::with_capacity(manifest.data_files.len());
    for entry in &manifest.data_files {
        if !data_file_names.insert(&entry.name) {
            return Err(corrupt_catalog(
                dir,
                manifest,
                format!(
                    "data file {} appears more than once in the manifest",
                    entry.name
                ),
            ));
        }
        let path = safe_join(&data_dir, &entry.name)?;
        let bytes = std::fs::read(&path)?;
        let actual_len = u64::try_from(bytes.len())?;
        if actual_len != entry.byte_len {
            return Err(TxnError::Storage(
                strata_storage::StorageError::CorruptDataFile(
                    path,
                    format!(
                        "byte_len is {actual_len} on disk but catalog records {}",
                        entry.byte_len
                    ),
                ),
            ));
        }
        let actual_crc = strata_storage::crc32c_checksum(&bytes);
        if actual_crc != entry.crc32c {
            return Err(TxnError::Storage(
                strata_storage::StorageError::CorruptDataFile(
                    path,
                    format!(
                        "crc32c is {actual_crc} on disk but catalog records {}",
                        entry.crc32c
                    ),
                ),
            ));
        }
        let batch = read_batch(&data_dir.join(&entry.name))?;
        validate_physical_batch_schema(dir, manifest, logical_schema, &batch, &entry.name)?;
        let actual_rows = u64::try_from(batch.num_rows())?;
        if actual_rows != entry.row_count {
            return Err(TxnError::Storage(
                strata_storage::StorageError::CorruptDataFile(
                    data_dir.join(&entry.name),
                    format!(
                        "row_count is {actual_rows} on disk but catalog records {}",
                        entry.row_count
                    ),
                ),
            ));
        }
        let expected_range = catalog_row_id_range(dir, manifest, entry)?;
        validate_data_file_row_ids(
            dir,
            manifest,
            entry,
            &batch,
            expected_range,
            &mut owned_rows,
        )?;
    }
    Ok(owned_rows)
}

fn validate_tombstones(dir: &Path, manifest: &Manifest, owned_rows: &HashSet<u64>) -> Result<()> {
    let mut seen = HashSet::with_capacity(manifest.tombstones.len());
    for &row_id in &manifest.tombstones {
        if !owned_rows.contains(&row_id) {
            return Err(corrupt_catalog(
                dir,
                manifest,
                format!("tombstone row_id {row_id} has no row-file owner"),
            ));
        }
        if !seen.insert(row_id) {
            return Err(corrupt_catalog(
                dir,
                manifest,
                format!("tombstone row_id {row_id} appears more than once"),
            ));
        }
    }
    Ok(())
}

/// Loads every segment `manifest` lists into a [`strata_index::SegmentSet`],
/// in manifest order. This is what replaced delta-log replay: a segment is
/// the durable built graph, so recovery is `O(bytes)` validation with zero
/// distance evaluations and zero graph construction, rather than replaying
/// every historical insert through `HnswIndex::insert_owned`.
///
/// Used only by [`Dataset::open`]. A freshly created dataset has no
/// segments and starts from [`strata_index::SegmentSet::empty`].
///
/// # Errors
///
/// Returns [`TxnError::UnsafeManifestPath`] if a segment name tries to
/// escape `data/`, [`TxnError::Io`] if a listed segment can't be read,
/// [`TxnError::CorruptSegment`] if a segment's on-disk length, format
/// version, vector count, dimension, or row-id range disagrees with what
/// the manifest records for it (a truncated/overwritten file, or a
/// manifest whose `SegmentEntry` was corrupted), if a segment's dimension
/// disagrees with a dimension an earlier segment in the *same* manifest
/// already established (the actual second line of defense against Finding
/// 1's dimension race: the per-entry checks above only ever compare a
/// segment against its own on-disk bytes, so a manifest listing two
/// mutually self-consistent segments at different dimensions — exactly
/// the corruption the original race could produce — would pass every one
/// of them; only this cross-segment check catches it), or
/// [`TxnError::Index`] if a segment fails its own header/body validation.
fn load_segments(
    dir: &Path,
    manifest: &Manifest,
    owned_rows: &HashSet<u64>,
) -> Result<strata_index::SegmentSet> {
    let data_dir = data_subdir(dir);
    let mut parts = Vec::with_capacity(manifest.segments.len());
    let mut established_dimension: Option<u32> = None;
    let mut segment_names = HashSet::with_capacity(manifest.segments.len());
    let mut vector_rows = HashSet::new();
    for entry in &manifest.segments {
        if !segment_names.insert(&entry.name) {
            return Err(TxnError::CorruptSegment(format!(
                "segment {} appears more than once in the manifest",
                entry.name
            )));
        }
        let path = safe_join(&data_dir, &entry.name)?;
        let bytes = std::fs::read(&path)?;
        // Checked before parsing so a truncated file is reported as the
        // truncation it is, rather than as whichever internal check its
        // remaining bytes happen to trip first. `SegmentEntry.byte_len`
        // exists for exactly this (base design doc §3).
        if u64::try_from(bytes.len())? != entry.byte_len {
            return Err(TxnError::CorruptSegment(format!(
                "segment {} is {} bytes on disk but the manifest records {}",
                entry.name,
                bytes.len(),
                entry.byte_len
            )));
        }
        let reader = strata_index::SegmentReader::from_bytes(&bytes)?;
        // Cross-check every other field the manifest records about this
        // segment, not just its byte length — same rationale as the
        // `byte_len` check above. Each field here is compared only against
        // this segment's *own* on-disk bytes, catching a `SegmentEntry`
        // that disagrees with what its own segment file actually encodes.
        // This does NOT catch two mutually self-consistent segments at
        // different dimensions — see the cross-segment check after this
        // segment's own checks, below, for the actual second line of
        // defense against Finding 1's dimension race.
        if reader.format_version() != entry.format_version {
            return Err(TxnError::CorruptSegment(format!(
                "segment {} has format_version {} on disk but the manifest records {}",
                entry.name,
                reader.format_version(),
                entry.format_version
            )));
        }
        if u64::try_from(reader.node_count())? != entry.vector_count {
            return Err(TxnError::CorruptSegment(format!(
                "segment {} has {} vectors on disk but the manifest records {}",
                entry.name,
                reader.node_count(),
                entry.vector_count
            )));
        }
        if u32::try_from(reader.dimension())? != entry.dimension {
            return Err(TxnError::CorruptSegment(format!(
                "segment {} has dimension {} on disk but the manifest records {}",
                entry.name,
                reader.dimension(),
                entry.dimension
            )));
        }
        let (row_id_min, row_id_max) = reader.row_id_range();
        if row_id_min != entry.row_id_min || row_id_max != entry.row_id_max {
            return Err(TxnError::CorruptSegment(format!(
                "segment {} has row-id range [{row_id_min}, {row_id_max}] on disk but the \
                 manifest records [{}, {}]",
                entry.name, entry.row_id_min, entry.row_id_max
            )));
        }
        // The actual second line of defense against Finding 1's dimension
        // race: every check above only ever compares this segment against
        // its own bytes, so a manifest listing two mutually
        // self-consistent segments at different dimensions — exactly the
        // corruption the original race could produce — would pass all of
        // them. Tracking the first segment's dimension here and rejecting
        // any later disagreement converts that from a permanently
        // unsearchable dataset (every future `vector_search` hitting
        // `DimensionMismatch` on whichever segment doesn't match the
        // query) into a typed error at `Dataset::open`, before the
        // dataset is ever handed back to a caller.
        match established_dimension {
            Some(expected) if entry.dimension != expected => {
                return Err(TxnError::CorruptSegment(format!(
                    "segment {} has dimension {} but an earlier segment in this \
                     manifest already established dimension {expected}",
                    entry.name, entry.dimension
                )));
            }
            Some(_) => {}
            None => established_dimension = Some(entry.dimension),
        }
        for row_id in reader.row_ids() {
            if !owned_rows.contains(&row_id) {
                return Err(TxnError::CorruptSegment(format!(
                    "segment {} row_id {row_id} has no row-file owner",
                    entry.name
                )));
            }
            if !vector_rows.insert(row_id) {
                return Err(TxnError::CorruptSegment(format!(
                    "duplicate vector row_id {row_id} across manifest segments"
                )));
            }
        }
        let zone_map: Arc<dyn std::any::Any + Send + Sync> = Arc::new(entry.zone_map.clone());
        parts.push((Arc::new(reader), zone_map));
    }
    Ok(strata_index::SegmentSet::from_segments(parts))
}

/// One row's vector, ready to be inserted into a segment's working index.
/// The in-memory carrier between `write_pending_batches` and the segment
/// build — the role the index crate's old append-only mutation-log entry
/// type used to play before that log was removed. There is no `Tombstone`
/// counterpart: deletion is the manifest's versioned `tombstones` list,
/// never an index-level entry (base design doc §5).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VectorInsert {
    pub(crate) row_id: u64,
    pub(crate) vector: Vec<f32>,
}

/// Extracts one [`VectorInsert`] per row in `batch` with a non-null vector,
/// keyed by the row-ids assigned starting at `row_id_base`. A `batch` with
/// no `"vector"` column at all (a table with no vector column defined)
/// simply produces no entries — that's not an error, unlike a `"vector"`
/// column present with the wrong type, which is. A commit that produces
/// zero entries writes **no segment at all** (see `build_and_write_segment`
/// and `docs/design.md`).
///
/// Also rejects any row whose vector contains a non-finite (`NaN`/`Infinity`)
/// component. This guard predates the segment format — it was originally
/// justified by the delta log's JSON encoding, which silently wrote
/// non-finite `f32`s as `null` — but the reason to keep it is independent
/// of any on-disk encoding: a `NaN` component poisons every distance
/// comparison in `search_layer_generic` (`Candidate::cmp`'s `partial_cmp`
/// fallback silently treats an incomparable pair as equal), so one bad
/// vector would corrupt search results for the whole segment. Must run
/// before any file for this batch is written to disk.
///
/// # Errors
///
/// Returns an error if `batch` has a `"vector"` column that isn't a
/// `FixedSizeList<Float32>`, or if any row's vector contains a non-finite
/// component.
fn build_vector_inserts(batch: &RecordBatch, row_id_base: u64) -> Result<Vec<VectorInsert>> {
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
        entries.push(VectorInsert {
            row_id,
            vector: row.to_vec(),
        });
    }
    Ok(entries)
}

/// Validates that every vector in `inserts` shares one consistent
/// dimension — both against each other, and against `established` (the
/// dimension already fixed by whatever has been committed so far, or `0` if
/// nothing has) — before a segment is built from any of them.
///
/// This is a **pre-build, pre-lock** check, and it is what keeps a
/// half-built segment from ever being fsynced or published: `insert_owned`'s
/// only fallible path is dimension validation, so without this a ragged
/// batch would fail partway through the working index's construction after
/// earlier vectors had already been inserted. The half-built index is
/// discarded either way, but failing before any I/O keeps the error cheap
/// and keeps the failure mode identical whichever pending batch is ragged.
///
/// `established` is a plain `usize`, not a graph handle: after S1 W3.2a
/// there is no shared live graph to ask. The caller sources it from the
/// current snapshot's `SegmentSet::established_dimension()` — available
/// without opening any segment file. See
/// `docs/design.md`.
///
/// # Errors
///
/// Returns [`TxnError::Index`] wrapping an
/// [`strata_index::IndexError::DimensionMismatch`] if any vector's length
/// disagrees with `established`, or with an earlier vector's length in this
/// same commit when `established` is `0`.
fn validate_vector_dimensions(inserts: &[VectorInsert], established: usize) -> Result<()> {
    let mut expected = established;
    for insert in inserts {
        if expected == 0 {
            expected = insert.vector.len();
        } else if insert.vector.len() != expected {
            return Err(TxnError::Index(
                strata_index::IndexError::DimensionMismatch {
                    query_len: insert.vector.len(),
                    expected,
                },
            ));
        }
    }
    Ok(())
}

/// Joins `name` onto `data_dir`, rejecting any `name` whose path
/// components aren't all bare filename segments (`Component::Normal`) — a
/// `name` containing `..` or an absolute path (which `Path::join` would
/// otherwise resolve/replace unchecked) must never let a corrupted/hostile
/// manifest read a file outside the dataset's own `data/` directory.
/// `DataFileEntry.name` and `SegmentEntry.name`
/// (`crates/storage/src/manifest.rs`) are both documented as "relative to
/// the dataset's data/ directory" — this is what actually enforces that
/// contract instead of merely documenting it, for both.
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
/// `cast_batch_to_schema` matches every requested field by its owned name,
/// never by physical position. This preserves projections and reordering
/// without allowing a caller to relabel one stored field as another.
const HIDDEN_COLUMNS: [&str; 2] = [ROW_ID_COLUMN, TIMESTAMP_COLUMN];

/// Projects `batch`'s physical columns by their owned names, casting only
/// storage-introduced encodings (such as a dictionary encoded UTF-8 column)
/// back to the already-validated requested logical type. See
/// `docs/design.md`
/// for why matching a *second* hidden column by position was unsound (it
/// either misfired a spurious `SchemaMismatch`, or — in the mixed-request
/// case — silently paired the wrong physical column against the wrong
/// logical field).
///
/// # Errors
///
/// Returns a typed schema error if a requested owned field is missing from
/// the physical batch, or an Arrow error if a storage encoding cannot be
/// decoded back to the owned type.
pub(crate) fn cast_batch_to_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let physical_schema = batch.schema_ref();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let index =
            physical_schema
                .index_of(field.name())
                .map_err(|_| TxnError::BatchSchemaMismatch {
                    expected: format!("physical field named {:?}", field.name()),
                    actual: format!("{physical_schema:?}"),
                })?;
        let column = Arc::clone(batch.column(index));
        let casted = if column.data_type() == field.data_type() {
            column
        } else {
            cast(column.as_ref(), field.data_type())?
        };
        columns.push(casted);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &options,
    )?)
}

/// Appends a `_row_id: UInt64` column to `batch`, assigning
/// `row_id_base..row_id_base + num_rows` in row order. This is what makes
/// every committed row addressable by a stable, global identity — see
/// `docs/design.md`.
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

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema_ref().metadata().clone(),
    ));
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Appends a `_timestamp: Int64` column to `batch`, every row sharing the
/// single value `ts` — microseconds since the Unix epoch, captured once per
/// transaction by `issue_timestamp`. See
/// `docs/design.md`.
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

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema_ref().metadata().clone(),
    ));
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatchOptions};
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

    #[test]
    fn create_requires_an_existing_immediate_parent() {
        // Break caught: recursively creating a caller-owned parent tree
        // expands the durability boundary beyond the dataset's owned root.
        let root = temp_dir("create-missing-immediate-parent-root");
        let parent = root.join("missing-parent");
        let dir = parent.join("dataset");

        let result = Dataset::create(&dir, test_schema());

        assert!(
            matches!(result, Err(TxnError::NotFound(ref missing)) if missing == &parent),
            "creation must reject a missing immediate parent before creating directories"
        );
        assert!(
            !parent.exists(),
            "a rejected create must not manufacture the missing immediate parent"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn create_persists_the_initial_immutable_row_id_high_water_record() {
        // COR-02 / CONC-03: the initial allocation floor must exist before
        // create acknowledges the dataset, rather than being reconstructed
        // solely from a later manifest.
        let dir = temp_dir("initial-row-id-high-water");

        Dataset::create(&dir, test_schema()).unwrap();

        let record = dir
            .join("_meta")
            .join("row-id-high-water")
            .join("00000000000000000000.reservation");
        assert!(
            record.is_file(),
            "create must publish the immutable zero high-water record before acknowledgement"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(feature = "test-fault-injection")]
    #[test]
    fn a_pre_publish_reservation_failure_writes_no_row_file_and_leaves_the_range_reusable() {
        // The failure occurs before the immutable record is linked into
        // place, so the transaction never receives a range and the next
        // successful transaction may claim the same initial id.
        let dir = temp_dir("row-id-reservation-before-publish");
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let mut failing = ds.begin();
        failing
            .insert(
                RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1_i64]))])
                    .unwrap(),
            )
            .unwrap();
        let fault =
            strata_storage::row_id_high_water::test_support::fail_reservation_before_publish(
                std::io::ErrorKind::Other,
            );

        let error = failing.commit().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("injected row-id reservation publication failure"),
            "the failure must be returned from the reservation boundary, got {error:?}"
        );
        assert!(
            ds.data_files().is_empty(),
            "the failed claim must not publish a manifest entry"
        );
        assert!(
            std::fs::read_dir(ds.data_dir()).unwrap().next().is_none(),
            "a pre-publication reservation failure must happen before any row file is written"
        );
        drop(fault);

        let mut retry = ds.begin();
        retry
            .insert(
                RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![2_i64]))])
                    .unwrap(),
            )
            .unwrap();
        retry.commit().unwrap();
        assert_eq!(ds.data_files()[0].row_id_range, Some((0, 0)));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(feature = "test-fault-injection")]
    #[test]
    fn a_post_publish_reservation_sync_failure_returns_a_typed_error_and_consumes_the_range() {
        // `put_if_absent` has linked the immutable end record before it
        // asks the directory to synchronize. That error is uncertain, so
        // this process must retain the range as a gap rather than hand it to
        // the retrying transaction.
        let dir = temp_dir("row-id-reservation-after-publish");
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let mut failing = ds.begin();
        failing
            .insert(
                RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1_i64]))])
                    .unwrap(),
            )
            .unwrap();
        let fault = strata_storage::datafile::test_support::fail_directory_sync_on_call(
            1,
            std::io::ErrorKind::Other,
        );

        let error = failing.commit().unwrap_err();

        assert!(
            matches!(
                error,
                TxnError::RowIdReservationDurability {
                    end: 1,
                    source: strata_storage::StorageError::Io(ref source),
                } if source.kind() == std::io::ErrorKind::Other
            ),
            "the post-publication failure must retain its typed uncertainty and I/O kind"
        );
        assert!(
            ds.data_files().is_empty(),
            "the failed claim must not publish a row"
        );
        assert!(
            std::fs::read_dir(ds.data_dir()).unwrap().next().is_none(),
            "reservation durability must fail before any row file is written"
        );
        assert_eq!(
            strata_storage::read_row_id_high_water(&dir).unwrap(),
            Some(1),
            "the linked reservation end remains a non-reusable floor"
        );
        drop(fault);

        let mut retry = ds.begin();
        retry
            .insert(
                RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![2_i64]))])
                    .unwrap(),
            )
            .unwrap();
        retry.commit().unwrap();
        assert_eq!(ds.data_files()[0].row_id_range, Some((1, 1)));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(feature = "test-fault-injection")]
    #[test]
    fn create_syncs_only_its_preexisting_parent_bottom_up() {
        // Break caught: synchronizing above the caller-provided immediate
        // parent can fail on inaccessible system directories and incorrectly
        // widens Dataset::create's durability responsibility.
        let root = temp_dir("create-directory-sync-order-root");
        let parent = root.join("one").join("two");
        std::fs::create_dir_all(&parent).unwrap();
        let dir = parent.join("dataset");
        let recorder = strata_storage::datafile::test_support::record_directory_syncs();

        Dataset::create(&dir, test_schema()).unwrap();

        let expected = vec![
            dir.clone(),
            parent.clone(),
            dir.join("_meta").join("row-id-high-water"),
            dir.join("_meta"),
            dir.clone(),
            dir.join("_versions"),
            dir.clone(),
        ];
        let actual = recorder.calls();
        assert_eq!(
            actual, expected,
            "creation must synchronize only the dataset and its immediate pre-existing parent, then the manifest directory and dataset directory"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[cfg(feature = "test-fault-injection")]
    #[test]
    fn create_returns_an_error_when_parent_directory_sync_fails() {
        // Break caught: a creation acknowledgement must depend on syncing the
        // immediate parent that owns the new dataset directory entry.
        let parent = temp_dir("create-parent-directory-sync-failure-parent");
        let dir = parent.join("dataset");
        let _fault = strata_storage::datafile::test_support::fail_directory_sync_on_call(
            2,
            std::io::ErrorKind::Other,
        );

        let result = Dataset::create(&dir, test_schema());
        let Err(error) = result else {
            panic!("Dataset::create must fail when its parent directory sync fails");
        };

        assert!(
            matches!(error, TxnError::Storage(strata_storage::StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::Other),
            "expected the immediate parent directory-sync error, got {error:?}"
        );
        assert!(
            strata_storage::read_current(&dir).unwrap().is_none(),
            "a failed directory durability boundary must not publish the initial manifest"
        );
        std::fs::remove_dir_all(&parent).ok();
    }

    #[cfg(feature = "test-fault-injection")]
    #[test]
    fn create_retry_resyncs_the_bounded_anchor_before_manifest_publication() {
        // Break caught: after the immediate-parent sync fails before
        // publication, the dataset is visible but its entry is not known
        // durable. A retry must re-sync the same bounded anchor pair.
        let root = temp_dir("create-retry-directory-sync-root");
        let parent = root.join("one").join("two");
        std::fs::create_dir_all(&parent).unwrap();
        let dir = parent.join("dataset");
        let fault = strata_storage::datafile::test_support::fail_directory_sync_on_call(
            2,
            std::io::ErrorKind::Other,
        );

        let first = Dataset::create(&dir, test_schema());
        let Err(error) = first else {
            panic!("the pre-publication immediate-parent sync must fail");
        };
        assert!(
            matches!(error, TxnError::Storage(strata_storage::StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::Other),
            "expected the injected pre-publication immediate-parent sync failure, got {error:?}"
        );
        assert!(
            strata_storage::read_current(&dir).unwrap().is_none(),
            "the injected failure must happen before initial-manifest publication"
        );
        drop(fault);

        let recorder = strata_storage::datafile::test_support::record_directory_syncs();
        Dataset::create(&dir, test_schema()).unwrap();

        let expected = vec![
            dir.clone(),
            parent.clone(),
            dir.join("_meta").join("row-id-high-water"),
            dir.join("_meta"),
            dir.clone(),
            dir.join("_versions"),
            dir.clone(),
        ];
        let actual = recorder.calls();
        assert!(
            actual == expected,
            "retry must re-sync the same bounded dataset/parent anchor before publishing; expected {expected:?}, got {actual:?}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[cfg(feature = "test-fault-injection")]
    #[test]
    fn create_returns_an_error_when_manifest_publication_root_sync_fails() {
        // Break caught: LocalFs owns the final dataset-root sync after it
        // publishes the initial manifest. Its failure is uncertain: the
        // manifest was already atomically renamed, so callers must receive
        // an error even though a later open can observe it.
        let parent = temp_dir("create-final-directory-sync-failure-parent");
        let dir = parent.join("dataset");
        let _fault = strata_storage::datafile::test_support::fail_directory_sync_on_call(
            7,
            std::io::ErrorKind::Other,
        );

        let result = Dataset::create(&dir, test_schema());
        let Err(error) = result else {
            panic!("Dataset::create must return the final directory-sync failure");
        };

        assert!(
            matches!(error, TxnError::Storage(strata_storage::StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::Other),
            "the final dataset-directory sync failure must be returned, got {error:?}"
        );
        assert!(
            strata_storage::read_current(&dir).unwrap().is_some(),
            "the error is uncertain: atomic manifest publication may already be visible"
        );
        std::fs::remove_dir_all(&parent).ok();
    }

    /// Proves 8 concurrent claims hand out non-overlapping, contiguous
    /// ranges — no id handed out twice, and none skipped. Uses
    /// `std::thread::scope` rather than `unsafe { transmute }` to borrow
    /// the stack-local safely — see Task 5's brief for why the `transmute`
    /// draft was rejected (this workspace's "safe Rust by default"
    /// convention).
    ///
    /// Previously also asserted that every claim was registered as
    /// in-flight while held, and released once dropped. That registry no
    /// longer exists — see [`crate::row_id`]'s module doc for why it became
    /// redundant once a snapshot's index became exactly its own manifest's
    /// segment list.
    #[test]
    fn concurrent_claims_hand_out_non_overlapping_ranges() {
        use crate::row_id::RowIdAllocator;

        let dir = temp_dir("concurrent-row-id-claims");
        let allocator = RowIdAllocator::new(&dir, 0);
        let claims: Vec<_> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| allocator.claim(10).unwrap()))
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        let mut bases: Vec<u64> = claims.iter().map(super::RowIdRange::base).collect();
        bases.sort_unstable();
        for (i, base) in bases.iter().enumerate() {
            assert_eq!(
                *base,
                (i as u64) * 10,
                "ranges must be contiguous, non-overlapping"
            );
        }

        assert_eq!(
            allocator.next_row_id(),
            80,
            "the counter must cover every id handed out"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// C1: the guard that a transaction's claimed row-id range exactly
    /// matches the rows it lays out must be a *runtime* check, not a
    /// `debug_assert` that vanishes in release builds. `write_pending_batches`
    /// receives `pending` and `claim` as independent parameters, so nothing
    /// in the type system ties the claim's size to the rows about to be
    /// written inside it; if they ever diverge the function would hand out
    /// row-ids *past* the claimed range — ids some other transaction's claim
    /// already covers, which is exactly the reuse spec §8 forbids ("gaps are
    /// safe, reuse is forbidden"). This drives the mismatch directly with a
    /// claim smaller than the batch it's handed alongside and asserts a typed
    /// `TxnError::RowIdRangeMismatch` rather than relying on a debug-only
    /// panic that release builds silently drop.
    #[test]
    fn write_pending_batches_rejects_a_claim_that_does_not_match_the_rows_it_writes() {
        use crate::row_id::RowIdRange;

        let dir = temp_dir("row-id-range-mismatch");
        // A 3-row batch paired with a claim that only covers 2 row-ids — the
        // exact divergence the guard exists to catch.
        let batch = vector_batch(
            vec![1, 2, 3],
            vec![[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [2.0, 0.0, 0.0]],
        );
        let claim = RowIdRange { base: 0, len: 2 };
        let mut data_files = Vec::new();

        let result = Transaction::write_pending_batches(
            std::slice::from_ref(&batch),
            &dir,
            0,
            &claim,
            0,
            &mut data_files,
        );

        assert!(
            matches!(result, Err(TxnError::RowIdRangeMismatch { .. })),
            "expected TxnError::RowIdRangeMismatch for a claim/row divergence, got {result:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    fn temp_dir(label: &str) -> PathBuf {
        tempfile::Builder::new()
            .prefix(&format!("strata-txn-test-{label}-"))
            .tempdir()
            .unwrap()
            .keep()
    }

    /// Every `.seg` file physically present in `ds`'s data directory that
    /// the current manifest does **not** list. A failed commit must leave
    /// exactly one — orphaned, not absent.
    fn orphaned_segment_files(ds: &Dataset) -> Vec<String> {
        let referenced: std::collections::HashSet<String> = ds
            .snapshot()
            .manifest
            .segments
            .iter()
            .map(|s| s.name.clone())
            .collect();
        let mut orphans: Vec<String> = std::fs::read_dir(ds.data_dir())
            .into_iter()
            .flatten()
            .filter_map(std::result::Result::ok)
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| {
                std::path::Path::new(name)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("seg"))
                    && !referenced.contains(name)
            })
            .collect();
        orphans.sort();
        orphans
    }

    /// Asserts base design §5's six-point list for a dataset whose most
    /// recent commit failed, then reopens and asserts (a)-(d) again.
    /// `attempted_query` is the failed commit's own vector, whose
    /// distinctive coordinates make a hit for it unambiguous.
    fn assert_failed_commit_left_no_trace(
        dir: &std::path::Path,
        ds: &Dataset,
        version_before: u64,
        segments_before: usize,
        attempted_query: &[f32],
    ) {
        // (b) the visible version never advanced.
        let snapshot = ds.snapshot();
        assert_eq!(
            snapshot.version, version_before,
            "a failed commit must not advance the visible version"
        );

        // (d) no manifest entry names the orphaned segment, and the
        // snapshot's in-memory segment set agrees with the manifest.
        assert_eq!(
            snapshot.manifest.segments.len(),
            segments_before,
            "a failed commit must publish no SegmentEntry: {:?}",
            snapshot.manifest.segments
        );
        assert_eq!(
            snapshot.index.len(),
            segments_before,
            "the snapshot's segment set must stay in lockstep with the manifest"
        );

        // (f) the orphan really was written -- without this the whole test
        // would pass against an implementation that never wrote a segment.
        let orphans = orphaned_segment_files(ds);
        assert_eq!(
            orphans.len(),
            1,
            "exactly one orphaned .seg file must exist on disk: {orphans:?}"
        );

        // (c) the attempted row is not searchable. Asserted by distance
        // rather than by row-id so it cannot pass vacuously on an empty
        // result set caused by a broken search.
        let hits = snapshot.vector_search(attempted_query, 1, None).unwrap();
        assert!(
            hits.is_empty() || hits[0].squared_distance > 1000.0,
            "the failed commit's vector must never be searchable: {hits:?}"
        );

        // (e) reopening reproduces all of the above. This is the assertion
        // that catches an in-memory-only cleanup that never made it to disk.
        let reopened = Dataset::open(dir).unwrap();
        let reopened_snapshot = reopened.snapshot();
        assert_eq!(reopened_snapshot.version, version_before);
        assert_eq!(reopened_snapshot.manifest.segments.len(), segments_before);
        assert_eq!(reopened_snapshot.index.len(), segments_before);
        assert_eq!(
            orphaned_segment_files(&reopened).len(),
            1,
            "the orphan must survive a reopen -- it is garbage, not corruption"
        );
        let reopened_hits = reopened_snapshot
            .vector_search(attempted_query, 1, None)
            .unwrap();
        assert!(
            reopened_hits.is_empty() || reopened_hits[0].squared_distance > 1000.0,
            "the failed commit's vector must still not be searchable after a \
             reopen: {reopened_hits:?}"
        );
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

    fn fixture_data_file(name: impl Into<String>) -> DataFileEntry {
        DataFileEntry {
            name: name.into(),
            byte_len: 0,
            crc32c: 0,
            row_count: 0,
            row_id_range: None,
            stats: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn dataset_schema_is_persisted_and_rejects_renamed_or_castable_batches() {
        let dir = temp_dir("owned-schema");
        let schema = test_schema();
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

        assert_eq!(ds.schema(), schema);
        assert_eq!(Dataset::open(&dir).unwrap().schema(), schema);

        let renamed = Arc::new(Schema::new(vec![Field::new(
            "renamed",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(renamed, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let mut txn = ds.begin();
        assert!(matches!(
            txn.insert(batch),
            Err(TxnError::BatchSchemaMismatch { .. })
        ));
        assert!(
            txn.pending.is_empty(),
            "schema validation must precede buffering or I/O"
        );

        let castable = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            castable,
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .unwrap();
        assert!(matches!(
            txn.insert(batch),
            Err(TxnError::BatchSchemaMismatch { .. })
        ));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_schema_reopen_scan_preserves_committed_row_count() {
        // Break caught: projecting an empty logical schema discarded every
        // physical column, leaving Arrow unable to preserve the rows that
        // were committed solely through Strata's hidden identity columns.
        let dir = temp_dir("empty-schema-reopen-scan");
        let schema = Arc::new(Schema::empty());
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();

        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
        txn.commit().unwrap();
        drop(ds);

        let reopened = Dataset::open(&dir).unwrap();
        let scanned = reopened.snapshot().scan(&schema).unwrap();
        assert_eq!(scanned.num_columns(), 0);
        assert_eq!(
            scanned.num_rows(),
            3,
            "a zero-column projection must retain every committed row after reopen"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn insert_rejects_swapped_owned_field_order() {
        // Break caught: matching logical fields by name instead of the
        // owned schema's exact ordered identity would permit callers to
        // relabel values when two columns share a physical type.
        let dir = temp_dir("swapped-owned-field-order");
        let schema = Arc::new(Schema::new(vec![
            Field::new("first", DataType::Int64, false),
            Field::new("second", DataType::Int64, false),
        ]));
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
        let swapped = Arc::new(Schema::new(vec![
            Field::new("second", DataType::Int64, false),
            Field::new("first", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            swapped,
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();

        let result = ds.begin().insert(batch);
        assert!(
            matches!(result, Err(TxnError::BatchSchemaMismatch { .. })),
            "the owned schema's field order is part of batch identity: {result:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn insert_rejects_nullability_mismatch_against_owned_schema() {
        // Break caught: accepting a nullable input for a non-nullable owned
        // field would change the dataset contract without changing its name
        // or Arrow data type.
        let dir = temp_dir("owned-schema-nullability");
        let schema = test_schema();
        let ds = Dataset::create(&dir, schema).unwrap();
        let nullable = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(nullable, vec![Arc::new(Int64Array::from(vec![Some(1)]))])
            .unwrap();

        let result = ds.begin().insert(batch);
        assert!(
            matches!(result, Err(TxnError::BatchSchemaMismatch { .. })),
            "nullability is part of the owned schema contract: {result:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn delete_and_update_reject_unowned_dead_duplicate_and_non_single_row_targets() {
        let dir = temp_dir("strict-targets");
        let schema = test_schema();
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

        let mut future = ds.begin();
        assert!(matches!(
            future.delete(0),
            Err(TxnError::RowNotFound { row_id: 0 })
        ));

        let mut seed = ds.begin();
        seed.insert(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .unwrap(),
        )
        .unwrap();
        seed.commit().unwrap();

        let visible = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(visible.num_rows(), 1);
        assert_eq!(
            visible
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1,
            "rejecting a future delete target must not tombstone the later row that claims it"
        );

        let mut delete = ds.begin();
        delete.delete(0).unwrap();
        assert!(matches!(
            delete.delete(0),
            Err(TxnError::DuplicateTarget { row_id: 0 })
        ));
        delete.commit().unwrap();

        let mut dead = ds.begin();
        assert!(matches!(
            dead.delete(0),
            Err(TxnError::RowNotLive { row_id: 0 })
        ));
        let empty = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();
        assert!(matches!(
            dead.update(0, empty),
            Err(TxnError::InvalidUpdateShape { actual_rows: 0 })
        ));
        let multiple = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![2, 3]))],
        )
        .unwrap();
        assert!(matches!(
            dead.update(9, multiple),
            Err(TxnError::InvalidUpdateShape { actual_rows: 2 })
        ));
        assert!(dead.pending.is_empty());
        assert!(dead.pending_tombstones.is_empty());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn committed_data_file_metadata_describes_the_physical_rows() {
        let dir = temp_dir("data-file-metadata");
        let schema = test_schema();
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![4, 5]))]).unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let entry = ds.data_files().pop().unwrap();
        assert!(entry.byte_len > 0);
        assert_ne!(entry.crc32c, 0);
        assert_eq!(entry.row_count, 2);
        assert_eq!(entry.row_id_range, Some((0, 1)));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_rejects_data_file_metadata_that_disagrees_with_the_file() {
        let dir = temp_dir("recovery-row-file-metadata");
        let schema = test_schema();
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![4]))]).unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let mut corrupt = (*ds.snapshot().manifest).clone();
        corrupt.version += 1;
        corrupt.data_files[0].row_count = 2;
        commit_manifest(&dir, &corrupt).unwrap();

        let result = Dataset::open(&dir);
        let Err(error) = result else {
            panic!("recovery must reject a row count that does not describe the actual IPC file");
        };
        assert!(
            matches!(error, TxnError::Storage(strata_storage::StorageError::CorruptDataFile(_, ref reason)) if reason.contains("row_count")),
            "recovery must reject a row count that does not describe the actual IPC file: {error:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_rejects_a_segment_vector_without_a_data_file_owner() {
        let dir = temp_dir("recovery-vector-owner");
        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let ds = Dataset::create(&dir, batch.schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
        txn.commit().unwrap();

        let mut corrupt = (*ds.snapshot().manifest).clone();
        corrupt.version += 1;
        corrupt.data_files.clear();
        commit_manifest(&dir, &corrupt).unwrap();

        let result = Dataset::open(&dir);
        let Err(error) = result else {
            panic!("every vector row-id must be owned by a validated data file");
        };
        assert!(
            matches!(error, TxnError::CorruptSegment(ref reason) if reason.contains("no row-file owner")),
            "every vector row-id must be owned by a validated data file: {error:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn metadata_bearing_dictionary_dataset_reopens_and_scans() {
        // Break caught: dictionary and hidden-column schema reconstruction
        // used to discard Arrow field/schema metadata, so a valid commit
        // became unrecoverable when Dataset::open compared its physical
        // field metadata with the owned logical schema.
        use arrow::array::StringArray;

        let dir = temp_dir("metadata-bearing-dictionary-reopen");
        let field_metadata = HashMap::from([("semantic_type".to_string(), "label".to_string())]);
        let schema_metadata = HashMap::from([("owner".to_string(), "catalog".to_string())]);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("name", DataType::Utf8, false).with_metadata(field_metadata.clone())],
            schema_metadata.clone(),
        ));
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
        let names: Vec<&str> = (0..100)
            .map(|i| if i % 2 == 0 { "alice" } else { "bob" })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(names.clone()))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
        txn.commit().unwrap();

        let on_disk = read_batch(&ds.data_dir().join(&ds.data_files()[0].name)).unwrap();
        assert!(matches!(
            on_disk.schema_ref().field(0).data_type(),
            DataType::Dictionary(_, _)
        ));
        assert_eq!(on_disk.schema_ref().field(0).metadata(), &field_metadata);
        assert_eq!(on_disk.schema_ref().metadata(), &schema_metadata);

        let reopened = Dataset::open(&dir).unwrap();
        assert_eq!(reopened.schema(), schema);
        let scanned = reopened.snapshot().scan(&schema).unwrap();
        let names_on_read = scanned
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names_on_read.value(0), "alice");
        assert_eq!(names_on_read.value(1), "bob");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_rejects_projection_with_altered_owned_schema_metadata() {
        // Break caught: validating only projection fields let callers replace
        // dataset-owned schema metadata in the returned RecordBatch.
        let dir = temp_dir("projection-schema-metadata");
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false)],
            HashMap::from([("owner".to_string(), "dataset".to_string())]),
        ));
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![7]))],
            )
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let altered = Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false)],
            HashMap::from([("owner".to_string(), "caller".to_string())]),
        ));

        assert!(matches!(
            ds.snapshot().scan(&altered),
            Err(TxnError::BatchSchemaMismatch { .. })
        ));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_rejects_a_row_file_whose_crc_no_longer_matches() {
        // Break caught: accepting bytes changed after the manifest checksum
        // was recorded would make a durable catalog point at unverified rows.
        let dir = temp_dir("recovery-row-file-crc");
        let ds = Dataset::create(&dir, test_schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![7]))]).unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let path = ds.data_dir().join(&ds.data_files()[0].name);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        std::fs::write(&path, bytes).unwrap();

        let Err(error) = Dataset::open(&dir) else {
            panic!("recovery must reject a row file changed after its manifest CRC was recorded");
        };
        assert!(
            matches!(error, TxnError::Storage(strata_storage::StorageError::CorruptDataFile(_, ref reason)) if reason.contains("crc32c")),
            "recovery must reject a row file changed after its manifest CRC was recorded: {error:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_rejects_a_row_id_range_that_disagrees_with_physical_rows() {
        // Break caught: trusting a manifest range instead of the physical
        // _row_id sequence could give two files overlapping ownership.
        let dir = temp_dir("recovery-row-id-range");
        let ds = Dataset::create(&dir, test_schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![7]))]).unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let mut corrupt = (*ds.snapshot().manifest).clone();
        corrupt.version += 1;
        corrupt.data_files[0].row_id_range = Some((4, 4));
        commit_manifest(&dir, &corrupt).unwrap();

        let Err(error) = Dataset::open(&dir) else {
            panic!("recovery must reject a row-id range that disagrees with physical row IDs");
        };
        assert!(
            matches!(error, TxnError::Storage(strata_storage::StorageError::CorruptManifest(_, ref reason)) if reason.contains("row_id")),
            "recovery must reject a row-id range that disagrees with physical row IDs: {error:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_classifies_overflowing_catalog_row_id_range_as_corrupt_manifest() {
        // Break caught: an overflow is caused exclusively by hostile catalog
        // metadata here, so surfacing it as an internal allocation error hid
        // the corrupted manifest from callers.
        let dir = temp_dir("recovery-overflowing-row-id-range");
        let ds = Dataset::create(&dir, test_schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![4, 5]))])
                .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let mut corrupt = (*ds.snapshot().manifest).clone();
        corrupt.version += 1;
        corrupt.data_files[0].row_id_range = Some((u64::MAX, u64::MAX));
        commit_manifest(&dir, &corrupt).unwrap();

        let Err(error) = Dataset::open(&dir) else {
            panic!("recovery must reject an overflowing catalog row-id range");
        };
        assert!(
            matches!(error, TxnError::Storage(strata_storage::StorageError::CorruptManifest(_, ref reason)) if reason.contains("row_id_range") && reason.contains("overflow")),
            "catalog row-id range overflow must be typed corruption with field context: {error:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_rejects_duplicate_physical_row_ownership() {
        // Break caught: allowing the same _row_id to be owned by two files
        // makes visibility and update targeting ambiguous.
        let dir = temp_dir("recovery-duplicate-row-owner");
        let ds = Dataset::create(&dir, test_schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![7]))]).unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let mut corrupt = (*ds.snapshot().manifest).clone();
        corrupt.version += 1;
        let mut copied_entry = corrupt.data_files[0].clone();
        copied_entry.name = "duplicate-owner.arrow".to_string();
        std::fs::copy(
            ds.data_dir().join(&corrupt.data_files[0].name),
            ds.data_dir().join(&copied_entry.name),
        )
        .unwrap();
        corrupt.data_files.push(copied_entry);
        commit_manifest(&dir, &corrupt).unwrap();

        let Err(error) = Dataset::open(&dir) else {
            panic!("recovery must reject multiple data-file owners for one row ID");
        };
        assert!(
            matches!(error, TxnError::Storage(strata_storage::StorageError::CorruptManifest(_, ref reason)) if reason.contains("owned by more than one")),
            "recovery must reject multiple data-file owners for one row ID: {error:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_rejects_orphan_tombstones() {
        // Break caught: a tombstone with no physical owner can hide a future
        // row if IDs are ever recovered or allocated incorrectly.
        let dir = temp_dir("recovery-orphan-tombstone");
        let ds = Dataset::create(&dir, test_schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![7]))]).unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let mut corrupt = (*ds.snapshot().manifest).clone();
        corrupt.version += 1;
        corrupt.tombstones.push(99);
        commit_manifest(&dir, &corrupt).unwrap();

        let Err(error) = Dataset::open(&dir) else {
            panic!("recovery must reject a tombstone with no physical row owner");
        };
        assert!(
            matches!(error, TxnError::Storage(strata_storage::StorageError::CorruptManifest(_, ref reason)) if reason.contains("no row-file owner")),
            "recovery must reject a tombstone with no physical row owner: {error:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_rejects_duplicate_tombstones() {
        // Break caught: duplicate tombstones make the manifest's delete set
        // non-canonical and conceal corruption during recovery.
        let dir = temp_dir("recovery-duplicate-tombstone");
        let ds = Dataset::create(&dir, test_schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![7]))]).unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let mut corrupt = (*ds.snapshot().manifest).clone();
        corrupt.version += 1;
        corrupt.tombstones = vec![0, 0];
        commit_manifest(&dir, &corrupt).unwrap();

        let Err(error) = Dataset::open(&dir) else {
            panic!("recovery must reject a duplicated tombstone");
        };
        assert!(
            matches!(error, TxnError::Storage(strata_storage::StorageError::CorruptManifest(_, ref reason)) if reason.contains("appears more than once")),
            "recovery must reject a duplicated tombstone: {error:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_rejects_duplicate_zero_row_data_file_entry() {
        // Break caught: zero-row files contribute no ownership IDs, so row
        // ownership validation alone cannot detect a repeated manifest entry.
        let dir = temp_dir("recovery-duplicate-zero-row-file");
        let schema = test_schema();
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
        let empty =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(Vec::<i64>::new()))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(empty).unwrap();
        txn.commit().unwrap();

        let mut corrupt = (*ds.snapshot().manifest).clone();
        assert_eq!(corrupt.data_files[0].row_count, 0);
        corrupt.version += 1;
        corrupt.data_files.push(corrupt.data_files[0].clone());
        commit_manifest(&dir, &corrupt).unwrap();

        let Err(error) = Dataset::open(&dir) else {
            panic!("recovery must reject duplicate data-file names even when they own zero rows");
        };
        assert!(
            matches!(error, TxnError::Storage(strata_storage::StorageError::CorruptManifest(_, ref reason)) if reason.contains("appears more than once")),
            "recovery must reject duplicate data-file names even when they own zero rows: {error:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_rejects_duplicate_vector_ids_across_distinct_segments() {
        // Break caught: a duplicated vector row ID yields two searchable
        // graph nodes for one physical row, breaking row/vector identity.
        let dir = temp_dir("recovery-duplicate-vector-row-id");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(vector_batch(vec![1], vec![[0.0, 0.0, 0.0]]))
            .unwrap();
        txn.commit().unwrap();

        let mut corrupt = (*ds.snapshot().manifest).clone();
        corrupt.version += 1;
        let mut copied_segment = corrupt.segments[0].clone();
        copied_segment.name = "duplicate-vector-row-id.seg".to_string();
        std::fs::copy(
            ds.data_dir().join(&corrupt.segments[0].name),
            ds.data_dir().join(&copied_segment.name),
        )
        .unwrap();
        corrupt.segments.push(copied_segment);
        commit_manifest(&dir, &corrupt).unwrap();

        let Err(error) = Dataset::open(&dir) else {
            panic!("recovery must reject vector row IDs duplicated across segment files");
        };
        assert!(
            matches!(error, TxnError::CorruptSegment(ref reason) if reason.contains("duplicate vector row_id")),
            "recovery must reject vector row IDs duplicated across segment files: {error:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn every_row_in_one_transaction_shares_the_identical_timestamp() {
        let dir = temp_dir("timestamp-shared-per-txn");
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap(),
        )
        .unwrap();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![3, 4]))])
                .unwrap(),
        )
        .unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();
        let after_insert = ds.snapshot().manifest.commit_time_high_water;
        assert!(after_insert > 0);

        // Sleep-free: two distinct clock reads are only guaranteed distinct
        // if enough wall-clock time actually elapses, which isn't
        // guaranteed in a fast test - so this asserts non-decreasing
        // (`>=`), matching the spec's own "non-decreasing" wording, not
        // strictly increasing.
        let mut txn = ds.begin();
        txn.delete(0).unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let mut last = 0i64;
        for i in 0..5 {
            let mut txn = ds.begin();
            txn.insert(
                RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![i]))])
                    .unwrap(),
            )
            .unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let (claim_point, claimed) = checkpoint_pair();

        // "slow": captures its timestamp first (small - issue_timestamp
        // runs at the very top of commit(), before write_phase, which is
        // what pause_after_row_id_claim pauses after), then parks before
        // acquiring commit_lock.
        let mut slow = ds.begin();
        slow.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        )
        .unwrap();
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
        )
        .unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        )
        .unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();
        let ts_after_commit_1 = ds.snapshot().manifest.commit_time_high_water;

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![2]))]).unwrap(),
        )
        .unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

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
        let result = txn.insert(batch);
        assert!(
            matches!(result, Err(TxnError::BatchSchemaMismatch { .. })),
            "owned-schema validation must reject reserved-field batches before buffering: {result:?}"
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
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

        let names: Vec<&str> = (0..100)
            .map(|i| if i % 2 == 0 { "alice" } else { "bob" })
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(names.clone()))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();
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
        // data/segment filenames a prior session already committed.
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
            let ds = Dataset::create(&dir, test_schema()).unwrap();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let mut txn = ds.begin();
            txn.insert(batch).unwrap();
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
        txn.insert(batch).unwrap();
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
    fn opening_a_legacy_manifest_requires_explicit_migration() {
        let dir = temp_dir("legacy-manifest-migration");
        let mut legacy_manifest = Manifest::empty();
        legacy_manifest.schema_ipc.clear();
        strata_storage::commit_manifest(&dir, &legacy_manifest).unwrap();

        let result = Dataset::open(&dir);
        assert!(
            matches!(
                result,
                Err(TxnError::Storage(
                    strata_storage::StorageError::LegacyFormatNeedsMigration(_)
                ))
            ),
            "a manifest without the owned schema/integrity envelope must require migration"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn insert_then_commit_then_scan_round_trips() {
        let dir = temp_dir("insert-scan");
        let schema = test_schema();
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch.clone()).unwrap();
        txn.commit().unwrap();

        assert_eq!(ds.current_version(), 1);
        let scanned = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(scanned, batch);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn create_twice_errors() {
        let dir = temp_dir("create-twice");
        let _ds = Dataset::create(&dir, test_schema()).unwrap();
        let result = Dataset::create(&dir, test_schema());
        assert!(matches!(result, Err(TxnError::AlreadyExists(_))));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_computes_and_stores_column_stats() {
        let dir = temp_dir("commit-stats");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![30, 10, 20]))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
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
    // deferred it because `Dataset::open`'s old delta-replay open path
    // panicked ("capacity overflow") on such a manifest before `commit`
    // ever ran. Resolved by Batch 1, Task 4: `Dataset::open` now rejects
    // any manifest whose `next_row_id` exceeds
    // `MAX_REASONABLE_ROW_ID_CAPACITY` with a typed
    // `TxnError::UnreasonableCapacity`, checked directly (the check used to
    // live inside that removed open path — see
    // `MAX_REASONABLE_ROW_ID_CAPACITY`'s own doc comment) — covered by
    // `open_errors_instead_of_attempting_a_huge_allocation_on_an_unreasonable_next_row_id`
    // below. The capacity ceiling makes a near-`u64::MAX` `next_row_id`
    // unreachable through `open`, so the originally-specified commit-time
    // wrap test is intentionally subsumed by the open-time guard test.

    #[test]
    fn open_errors_instead_of_attempting_a_huge_allocation_on_an_unreasonable_next_row_id() {
        let dir = temp_dir("unreasonable-capacity");
        let hostile = Manifest {
            version: 0,
            schema_ipc: Manifest::empty_with_schema(test_schema().as_ref()).schema_ipc,
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
            schema_ipc: Manifest::empty_with_schema(test_schema().as_ref()).schema_ipc,
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
        txn.insert(batch).unwrap();
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
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

        let names: Vec<&str> = vec!["x"; 20]; // single distinct value, well under threshold
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap();
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        // Two commits, disjoint id ranges -> two files with non-overlapping stats.
        let low = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(low).unwrap();
        txn.commit().unwrap();

        let high = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![100, 101, 102]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(high).unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let first = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(first).unwrap();
        txn.commit().unwrap();

        let second =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![40, 50]))]).unwrap();
        let mut txn = ds.begin();
        txn.insert(second).unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap(),
        )
        .unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap(),
        )
        .unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap(),
        )
        .unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap(),
        )
        .unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let low = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(low).unwrap();
        txn.commit().unwrap();

        let high = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![100, 101, 102]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(high).unwrap();
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
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let batch = vector_batch(
            vec![1, 2, 3],
            vec![[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [10.0, 10.0, 10.0]],
        );
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
        txn.commit().unwrap();

        let results = ds
            .snapshot()
            .vector_search(&[0.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_id, 0); // row-id 0 is the first committed row (id=1)

        std::fs::remove_dir_all(&dir).ok();
    }

    /// End-to-end coverage for `insert_batch_parallel`'s port onto
    /// `build_and_write_segment` (see that function and
    /// `PARALLEL_INSERT_THREADS` above), gated behind this crate's
    /// `parallel-insert` feature the same way the call site itself is
    /// (`cargo test -p strata-txn --features parallel-insert`) -- without
    /// that feature the parallel path doesn't exist to test. Every other
    /// vector test in this module commits too few rows to ever leave
    /// `HnswIndex::insert_batch_parallel`'s sequential fallback (it only
    /// fans out to real OS threads once the post-first-row remainder
    /// exceeds `MIN_ROWS_PER_CHUNK` (64) in `crates/index/src/hnsw.rs`), so
    /// none of them exercise the parallel path through the REAL
    /// `Dataset::create` -> `insert` -> `commit` -> `vector_search` flow --
    /// only direct `HnswIndex` calls in `crates/index`'s own tests do.
    /// 301 rows in one commit (300 after the mandatory sequential first
    /// insert, chunked into exactly 4 x 75 -- on a host with at least 4
    /// available cores) clears that threshold and spawns all
    /// `PARALLEL_INSERT_THREADS = 4` worker threads -- a smaller N (e.g.
    /// 100) still exercises the parallel path but, because
    /// `insert_batch_parallel`'s chunk size is
    /// `max(ceil(rest/threads), MIN_ROWS_PER_CHUNK)`, would only produce 2
    /// chunks, not 4. `insert_batch_parallel` also clamps `threads` to
    /// `std::thread::available_parallelism()`, so on a <4-core host (e.g.
    /// some CI runners) this still exercises the real parallel path with
    /// real chunking, just with fewer than 4 actual chunks/threads -- the
    /// point of this test (proving the path is wired end-to-end) still
    /// holds either way, but "exactly 4 x 75" is a ≥4-core claim, not a
    /// universal one.
    ///
    /// Uses `widely_separated_rows`-style far-apart points (mirroring
    /// `crates/index/src/hnsw.rs`'s own choice for its analogous
    /// findability test). This asserts EXACT recall (0 misses), not a
    /// bounded miss rate: the clustered-data recall hazard this batch of
    /// work found and fixed (`Graph::insert`'s own doc comment in
    /// `crates/index/src/graph.rs` -- hazard #4, the dominant mechanism)
    /// is now verified closed end to end (0/800 real commits of a
    /// 200-row/20-cluster fixture under heavy concurrent load), and
    /// widely-separated points don't even exercise that hazard's
    /// clustered-topology precondition -- any miss here would be a real
    /// regression, not an expected residual.
    #[test]
    #[cfg(feature = "parallel-insert")]
    #[allow(clippy::cast_precision_loss)] // row indices here are always < 301, far under f32/f64's exact-integer ceiling
    fn a_large_single_commit_is_built_via_the_parallel_insert_path_and_stays_searchable() {
        const N: i64 = 301;

        let dir = temp_dir("large-commit-parallel-insert");
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let ids: Vec<i64> = (0..N).collect();
        let vectors: Vec<[f32; 3]> = (0..N).map(|i| [i as f32 * 1000.0, 0.0, 0.0]).collect();

        let mut txn = ds.begin();
        txn.insert(vector_batch(ids, vectors)).unwrap();
        txn.commit().unwrap();

        assert_eq!(
            ds.snapshot().manifest.segments.len(),
            1,
            "one commit must produce exactly one segment regardless of how many \
             worker threads built it"
        );

        let mut misses = 0u64;
        for i in 0..N {
            let query = [i as f32 * 1000.0, 0.0, 0.0];
            let hits = ds.snapshot().vector_search(&query, 1, None).unwrap();
            if hits.first().map(|m| m.row_id) != u64::try_from(i).ok() {
                misses += 1;
            }
        }
        assert_eq!(
            misses, 0,
            "{misses}/{N} rows were not their own nearest neighbor after a large \
             parallel-insert commit -- the clustered-data recall hazard this fixture \
             predates is now fixed and verified closed, so any miss here is a real \
             regression"
        );

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
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss
    )] // N/CLUSTERS/PER_CLUSTER are small fixed test constants, well within every cast's exact range here
    fn a_large_single_commit_builds_a_correct_segment_and_every_row_is_visible() {
        // Exercises S1's in-commit segment build (Graph construction ->
        // to_segment_bytes -> write_bytes -> SegmentReader load -> fan-out
        // search) at a row count past what every other vector-commit test
        // in this file (a handful of rows) reaches -- the codec itself is
        // covered in isolation at similar scale by
        // crates/index/src/segment_reader.rs's own round-trip tests, but
        // nothing else in this file wires that path end-to-end through the
        // txn layer's commit path at this scale.
        const N: i64 = 200;
        const CLUSTERS: i64 = 20;
        const PER_CLUSTER: usize = 10;

        // 20 well-separated 10-point clusters (2000 units apart), each
        // generated by this file's own established `cluster_vectors`
        // helper -- NOT one giant quasi-collinear line of 200 points.
        // A pure line ([i*1000, 0, 0], or even that with small jitter)
        // hits the exact pathology `cluster_vectors`'s own doc comment
        // above describes: the neighbor-diversification heuristic prunes
        // almost all direct links between near-collinear points, which
        // can make a query miss its own exact match even at generous
        // production parameters.
        let dir = temp_dir("large-single-commit");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let ids: Vec<i64> = (0..N).collect();
        let vectors: Vec<[f32; 3]> = (0..CLUSTERS)
            .flat_map(|c| cluster_vectors(PER_CLUSTER, [c as f32 * 2000.0, 0.0, 0.0], 0.5))
            .collect();
        let mut txn = ds.begin();
        txn.insert(vector_batch(ids, vectors.clone())).unwrap();
        txn.commit().unwrap();

        let snapshot = ds.snapshot();
        assert_eq!(
            snapshot.scan(&vector_test_schema()).unwrap().num_rows() as i64,
            N,
            "every one of the 200 rows must be scannable -- proves the in-commit segment \
             build didn't drop or lose any row"
        );

        // A coarse smoke-level hit rate, not a precise recall-regression
        // detector: this is a pipeline-level check (did the commit-path
        // wiring work end-to-end), not a substitute for
        // segment_recall_bench's production-scale recall measurement.
        let mut hits = 0;
        for i in 0..N {
            let query = vectors[i as usize];
            let results = snapshot.vector_search(&query, 1, None).unwrap();
            if results.first().is_some_and(|r| r.row_id == i as u64) {
                hits += 1;
            }
        }
        let hit_rate = f64::from(hits) / N as f64;
        assert!(
            hit_rate >= 0.9,
            "expected at least 90% of rows findable at their own exact coordinates after a \
             large single commit (a pipeline-level smoke check, not a recall-precision \
             assertion), got {hits}/{N} ({hit_rate:.3})"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn vector_search_with_predicate_only_returns_matching_rows() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("vector-search-filtered");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

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
        txn.insert(batch).unwrap();
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

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
        txn.insert(batch).unwrap();
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
    fn vector_search_with_two_different_predicates_against_one_snapshot_stays_correct_for_both() {
        // Regression coverage for the per-snapshot live-set cache
        // (`crate::live_set_cache`): querying the SAME snapshot with two
        // DIFFERENT predicates, one after the other, must never have the
        // second query's live set contaminated by the first's cached one —
        // exactly the failure mode a mis-keyed cache produces (a wrong but
        // *silent* answer, not an error).
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("vector-search-two-predicates-one-snapshot");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let near_cluster = cluster_vectors(15, [0.0, 0.0, 0.0], 0.01);
        let far_cluster = cluster_vectors(15, [1000.0, 0.0, 0.0], 0.01);
        let mut ids = vec![1i64; 15];
        ids.extend(vec![2i64; 15]);
        let mut vectors = near_cluster;
        vectors.extend(far_cluster);
        let batch = vector_batch(ids, vectors);
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
        txn.commit().unwrap();

        let snapshot = ds.snapshot();
        let predicate_near = Predicate::Eq("id".to_string(), Value::Int64(1));
        let predicate_far = Predicate::Eq("id".to_string(), Value::Int64(2));

        // Query the far predicate FIRST, then the near one, so a cache keyed
        // wrong (or not keyed at all) would hand the near query the far
        // predicate's stale cached live set.
        let far_results = snapshot
            .vector_search(&[1000.0, 0.0, 0.0], 3, Some(&predicate_far))
            .unwrap();
        assert_eq!(far_results.len(), 3, "unexpected results: {far_results:?}");
        assert!(
            far_results.iter().all(|r| r.row_id >= 15),
            "id=2 predicate must only return the far cluster: {far_results:?}"
        );

        let near_results = snapshot
            .vector_search(&[0.0, 0.0, 0.0], 3, Some(&predicate_near))
            .unwrap();
        assert_eq!(
            near_results.len(),
            3,
            "unexpected results: {near_results:?}"
        );
        assert!(
            near_results.iter().all(|r| r.row_id < 15),
            "id=1 predicate must only return the near cluster, not the \
             other predicate's cached result: {near_results:?}"
        );

        // Re-querying the far predicate a second time must still be
        // correct too (proves the cache didn't get overwritten by the
        // intervening near-predicate query).
        let far_again = snapshot
            .vector_search(&[1000.0, 0.0, 0.0], 3, Some(&predicate_far))
            .unwrap();
        assert!(
            far_again.iter().all(|r| r.row_id >= 15),
            "re-querying id=2 after querying id=1 must still return the far \
             cluster: {far_again:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    // --- S1 W4a: SegmentEntry.zone_map compute-and-store tests ---
    //
    // These tests only assert what gets computed and stored at commit time;
    // the pruning consumer (`Snapshot::vector_search`, via
    // `zone_map_permits_scan`) is exercised separately in
    // `crates/txn/src/snapshot.rs` (S1 W4b).

    fn zone_map_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]))
    }

    #[allow(clippy::needless_pass_by_value)]
    fn zone_map_batch(ids: Vec<i64>, categories: Vec<&str>, vectors: Vec<[f32; 3]>) -> RecordBatch {
        let id_arr = Arc::new(Int64Array::from(ids));
        let cat_arr = Arc::new(arrow::array::StringArray::from(categories));
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let flat: Vec<f32> = vectors.iter().flatten().copied().collect();
        let values = Arc::new(arrow::array::Float32Array::from(flat));
        let vec_arr = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field, 3, values, None,
        ));
        RecordBatch::try_new(zone_map_test_schema(), vec![id_arr, cat_arr, vec_arr]).unwrap()
    }

    #[test]
    fn single_batch_commit_populates_the_segments_zone_map() {
        let dir = temp_dir("zone-map-single-batch");
        let ds = Dataset::create(&dir, zone_map_test_schema()).unwrap();

        let batch = zone_map_batch(
            vec![30, 10, 20],
            vec!["banana", "apple", "cherry"],
            cluster_vectors(3, [0.0, 0.0, 0.0], 0.01),
        );
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
        txn.commit().unwrap();

        let snapshot = ds.snapshot();
        assert_eq!(snapshot.manifest.segments.len(), 1);
        let zone_map = &snapshot.manifest.segments[0].zone_map;

        assert_eq!(
            zone_map.get("id"),
            Some(&ColumnStats {
                min: Value::Int64(10),
                max: Value::Int64(30),
            }),
            "must match what compute_stats alone produces for this batch: {zone_map:?}"
        );
        assert_eq!(
            zone_map.get("category"),
            Some(&ColumnStats {
                min: Value::Utf8("apple".to_string()),
                max: Value::Utf8("cherry".to_string()),
            }),
            "must match what compute_stats alone produces for this batch: {zone_map:?}"
        );
        let ts_stats = zone_map
            .get(TIMESTAMP_COLUMN)
            .expect("zone map must include a _timestamp entry");
        assert_eq!(
            ts_stats.min, ts_stats.max,
            "a single-batch commit's _timestamp zone map is always a single point"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn multi_batch_commit_merges_zone_map_across_every_batch_not_just_the_first() {
        let dir = temp_dir("zone-map-multi-batch-merge");
        let ds = Dataset::create(&dir, zone_map_test_schema()).unwrap();

        let mut txn = ds.begin();
        // First batch: mid-range values (45..=60). Neither the true global
        // min nor max comes from here, so a merge that (bug) kept only the
        // first batch's stats, or a merge that never widens past it, would
        // both be caught by the assertions below.
        txn.insert(zone_map_batch(
            vec![50, 60],
            vec!["m1", "m2"],
            cluster_vectors(2, [0.0, 0.0, 0.0], 0.01),
        ))
        .unwrap();
        // Second batch: carries the true global min (10).
        txn.insert(zone_map_batch(
            vec![10, 55],
            vec!["m3", "m4"],
            cluster_vectors(2, [100.0, 100.0, 100.0], 0.01),
        ))
        .unwrap();
        // Third batch: carries the true global max (90).
        txn.insert(zone_map_batch(
            vec![90, 45],
            vec!["m5", "m6"],
            cluster_vectors(2, [200.0, 200.0, 200.0], 0.01),
        ))
        .unwrap();
        txn.commit().unwrap();

        let snapshot = ds.snapshot();
        let zone_map = &snapshot.manifest.segments[0].zone_map;
        let id_stats = zone_map.get("id").unwrap();
        assert_eq!(
            id_stats.min,
            Value::Int64(10),
            "global min (10) lives only in the second batch, not the first (50..=60): {zone_map:?}"
        );
        assert_eq!(
            id_stats.max,
            Value::Int64(90),
            "global max (90) lives only in the third batch, not the first (50..=60): {zone_map:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn multi_batch_commit_merges_float64_zone_map_across_every_batch() {
        // Analogous to
        // `multi_batch_commit_merges_zone_map_across_every_batch_not_just_the_first`
        // but for the `Float64` merge arm, which had no dedicated coverage:
        // a review traced it as safe (NaN operands are discarded by
        // `f64::min`/`f64::max`, so a batch that yields only NaN contributes
        // nothing to the merged range — the correct fail-safe direction),
        // but that reasoning should be encoded as a test, not left implicit.
        let dir = temp_dir("zone-map-multi-batch-float64-merge");
        let schema = Arc::new(Schema::new(vec![
            Field::new("score", DataType::Float64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
        let make_batch = |scores: Vec<f64>, vectors: Vec<[f32; 3]>| -> RecordBatch {
            let score_arr = Arc::new(arrow::array::Float64Array::from(scores));
            let item_field = Arc::new(Field::new("item", DataType::Float32, false));
            let flat: Vec<f32> = vectors.iter().flatten().copied().collect();
            let values = Arc::new(arrow::array::Float32Array::from(flat));
            let vec_arr = Arc::new(arrow::array::FixedSizeListArray::new(
                item_field, 3, values, None,
            ));
            RecordBatch::try_new(schema.clone(), vec![score_arr, vec_arr]).unwrap()
        };

        let mut txn = ds.begin();
        // First batch: mid-range values (50.5..=60.5). Neither the true
        // global min nor max comes from here, same discriminating structure
        // as the Int64 test above.
        txn.insert(make_batch(
            vec![50.5, 60.5],
            cluster_vectors(2, [0.0, 0.0, 0.0], 0.01),
        ))
        .unwrap();
        // Second batch: carries the true global min (10.25).
        txn.insert(make_batch(
            vec![10.25, 55.0],
            cluster_vectors(2, [100.0, 100.0, 100.0], 0.01),
        ))
        .unwrap();
        // Third batch: carries the true global max (90.75).
        txn.insert(make_batch(
            vec![90.75, 45.0],
            cluster_vectors(2, [200.0, 200.0, 200.0], 0.01),
        ))
        .unwrap();
        txn.commit().unwrap();

        let snapshot = ds.snapshot();
        let zone_map = &snapshot.manifest.segments[0].zone_map;
        let score_stats = zone_map.get("score").unwrap();
        assert_eq!(
            score_stats.min,
            Value::Float64(10.25),
            "global min (10.25) lives only in the second batch, not the first \
             (50.5..=60.5): {zone_map:?}"
        );
        assert_eq!(
            score_stats.max,
            Value::Float64(90.75),
            "global max (90.75) lives only in the third batch, not the first \
             (50.5..=60.5): {zone_map:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn insert_rejects_a_batch_missing_an_owned_column() {
        let dir = temp_dir("zone-map-column-absent-in-one-batch");
        let ds = Dataset::create(&dir, zone_map_test_schema()).unwrap();

        let mut txn = ds.begin();
        txn.insert(zone_map_batch(
            vec![1, 2],
            vec!["a", "b"],
            cluster_vectors(2, [0.0, 0.0, 0.0], 0.01),
        ))
        .unwrap();

        // The dataset owns its schema. A batch that omits one owned column
        // must be rejected before it can allocate a row ID or write a file.
        let schema_without_category = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));
        let id_arr = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let values = Arc::new(arrow::array::Float32Array::from(Vec::<f32>::new()));
        let vec_arr = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field, 3, values, None,
        ));
        let batch_without_category =
            RecordBatch::try_new(schema_without_category, vec![id_arr, vec_arr]).unwrap();
        assert!(matches!(
            txn.insert(batch_without_category),
            Err(TxnError::BatchSchemaMismatch { .. })
        ));
        assert_eq!(
            txn.pending.len(),
            1,
            "the rejected batch must not be buffered"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn insert_rejects_a_batch_with_a_mismatched_owned_column_type() {
        let dir = temp_dir("zone-map-type-mismatch-across-batches");
        let schema_int_code = Arc::new(Schema::new(vec![
            Field::new("code", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));
        let ds = Dataset::create(&dir, Arc::clone(&schema_int_code)).unwrap();

        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let vector_field = || {
            Field::new(
                "vector",
                DataType::FixedSizeList(item_field.clone(), 3),
                false,
            )
        };

        let mut txn = ds.begin();

        // First batch: "code" is Int64.
        let vectors_a = cluster_vectors(2, [0.0, 0.0, 0.0], 0.01);
        let flat_a: Vec<f32> = vectors_a.iter().flatten().copied().collect();
        let code_arr_a = Arc::new(Int64Array::from(vec![1, 2]));
        let values_a = Arc::new(arrow::array::Float32Array::from(flat_a));
        let vec_arr_a = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field.clone(),
            3,
            values_a,
            None,
        ));
        let batch_a = RecordBatch::try_new(schema_int_code, vec![code_arr_a, vec_arr_a]).unwrap();
        txn.insert(batch_a).unwrap();

        // Same owned column name, but a different logical type.
        let schema_utf8_code = Arc::new(Schema::new(vec![
            Field::new("code", DataType::Utf8, false),
            vector_field(),
        ]));
        let vectors_b = cluster_vectors(2, [100.0, 100.0, 100.0], 0.01);
        let flat_b: Vec<f32> = vectors_b.iter().flatten().copied().collect();
        let code_arr_b = Arc::new(arrow::array::StringArray::from(vec!["x", "y"]));
        let values_b = Arc::new(arrow::array::Float32Array::from(flat_b));
        let vec_arr_b = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field, 3, values_b, None,
        ));
        let batch_b = RecordBatch::try_new(schema_utf8_code, vec![code_arr_b, vec_arr_b]).unwrap();
        assert!(matches!(
            txn.insert(batch_b),
            Err(TxnError::BatchSchemaMismatch { .. })
        ));
        assert_eq!(
            txn.pending.len(),
            1,
            "the rejected batch must not be buffered"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn segment_zone_map_survives_dataset_reopen() {
        let dir = temp_dir("zone-map-reopen-roundtrip");
        let ds = Dataset::create(&dir, zone_map_test_schema()).unwrap();

        let batch = zone_map_batch(
            vec![30, 10, 20],
            vec!["banana", "apple", "cherry"],
            cluster_vectors(3, [0.0, 0.0, 0.0], 0.01),
        );
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
        txn.commit().unwrap();

        let zone_map_before = ds.snapshot().manifest.segments[0].zone_map.clone();
        assert!(
            !zone_map_before.is_empty(),
            "sanity check: the commit must have actually populated a zone map"
        );
        drop(ds);

        // Force a real load from disk, not an in-memory shortcut - the same
        // pattern used by this file's other reopen tests.
        let reopened = Dataset::open(&dir).unwrap();
        let zone_map_after = &reopened.snapshot().manifest.segments[0].zone_map;
        assert_eq!(
            &zone_map_before, zone_map_after,
            "the zone map computed at commit time must round-trip through Dataset::open unchanged"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn timestamps_and_the_high_water_mark_survive_reopen() {
        let dir = temp_dir("timestamp-survives-reopen");
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(
                test_schema(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap(),
        )
        .unwrap();
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
        )
        .unwrap();
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

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
        )
        .unwrap();
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
        )
        .unwrap();
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
        )
        .unwrap();
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

        // The spec's own §5.4 exit criterion wants an `explain`-shaped
        // assertion over this compound predicate, not just correct results.
        // Each of the three segments corresponds to one commit here (near,
        // far, mid): near's zone map has timestamp < ts_after_commit_2, so
        // the timestamp conjunct alone proves it can't match; mid's zone map
        // has category="b" only, so the category conjunct alone proves it
        // can't match. far's zone map (timestamp==ts_after_commit_2,
        // category="a") satisfies both conjuncts, so it's the only segment
        // that must be scanned - exactly 2 of the 3 segments are skipped.
        let explain = snapshot.explain(&predicate);
        assert_eq!(
            explain.segments_skipped.len(),
            2,
            "the compound predicate must prune the near segment (timestamp too old) and the mid \
             segment (wrong category), leaving only the far segment to scan: {explain:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn last_issued_timestamp_is_seeded_from_the_persisted_high_water_mark_on_reopen() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("timestamp-restart-floor-seeding");
        let ds = Dataset::create(&dir, test_schema()).unwrap();
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        )
        .unwrap();
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
        )
        .unwrap();
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
    fn reopening_a_dataset_loads_the_vector_index_from_the_manifests_segments() {
        let dir = temp_dir("segment-replay");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

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
        txn.insert(batch).unwrap();
        txn.commit().unwrap();
        drop(ds);

        // Force a real load from disk, not an in-memory shortcut -- this is
        // the crash-recovery-equivalent test for the index (a fresh Dataset
        // struct, same process, but the segment set is definitely rebuilt
        // from the .seg file the manifest lists, not carried over).
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
        assert_eq!(
            reopened.snapshot().manifest.segments.len(),
            1,
            "one vector-carrying commit must have produced exactly one segment"
        );
        assert_eq!(
            reopened.snapshot().index.len(),
            1,
            "the loaded segment set must match the manifest's segment list"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reopening_a_dataset_with_multiple_segments_finds_each_segments_own_cluster() {
        // Sibling of `reopening_a_dataset_loads_the_vector_index_from_the_manifests_segments`,
        // which only ever covers exactly one segment -- not enough to catch
        // a regression that mixes up segments, silently drops one, or maps
        // a hit back to the wrong global row-id once more than one segment
        // is in play. This is the direct regression test for "post-reopen
        // results match pre-reopen results" across multiple segments.
        let dir = temp_dir("multi-segment-reopen");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        // Three well-separated commits, each its own segment (one
        // vector-carrying commit -> one segment, per
        // `build_and_write_segment`'s doc comment).
        let mut txn_a = ds.begin();
        txn_a
            .insert(vector_batch(
                vec![1i64, 2i64, 3i64],
                cluster_vectors(3, [0.0, 0.0, 0.0], 0.01),
            ))
            .unwrap();
        txn_a.commit().unwrap();

        let mut txn_b = ds.begin();
        txn_b
            .insert(vector_batch(
                vec![4i64, 5i64, 6i64],
                cluster_vectors(3, [500.0, 500.0, 500.0], 0.01),
            ))
            .unwrap();
        txn_b.commit().unwrap();

        let mut txn_c = ds.begin();
        txn_c
            .insert(vector_batch(
                vec![7i64, 8i64, 9i64],
                cluster_vectors(3, [900.0, 900.0, 900.0], 0.01),
            ))
            .unwrap();
        txn_c.commit().unwrap();

        assert_eq!(ds.snapshot().manifest.segments.len(), 3);
        let row_ids_before: Vec<u64> = [
            [0.0, 0.0, 0.0],
            [500.0, 500.0, 500.0],
            [900.0, 900.0, 900.0],
        ]
        .iter()
        .map(|query| ds.snapshot().vector_search(query, 1, None).unwrap()[0].row_id)
        .collect();

        drop(ds);

        // Force a real load from disk -- the crash-recovery-equivalent path
        // for the index, same as the single-segment sibling test.
        let reopened = Dataset::open(&dir).unwrap();
        assert_eq!(reopened.snapshot().manifest.segments.len(), 3);
        assert_eq!(
            reopened.snapshot().index.len(),
            3,
            "the loaded segment set must match the manifest's segment list"
        );

        // A query near EACH commit's own cluster must return that same
        // commit's own row post-reopen -- not another segment's row, and
        // not an error from a segment mixed up with the wrong one.
        for (query, expected_row_id) in [
            ([0.0, 0.0, 0.0], row_ids_before[0]),
            ([500.0, 500.0, 500.0], row_ids_before[1]),
            ([900.0, 900.0, 900.0], row_ids_before[2]),
        ] {
            let results = reopened.snapshot().vector_search(&query, 1, None).unwrap();
            assert_eq!(
                results.len(),
                1,
                "query {query:?} must find exactly one result post-reopen"
            );
            assert_eq!(
                results[0].row_id, expected_row_id,
                "query {query:?} must find the same row post-reopen as it did \
                 pre-reopen: {results:?}"
            );
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn vector_search_fan_out_matches_brute_force_ground_truth_across_overlapping_segments() {
        // Phase S1 W3's exit criterion
        // (`docs/design.md`):
        // "recall parity with the pre-migration monolithic baseline
        // (integration test, not just the bench)". The fan-out search this
        // validates (`SegmentSet::search` merging results across segments)
        // shipped in S1 W3.2a; this is the exit-criterion test that was
        // never actually written for it, lifted one layer up from
        // `crates/index/src/segment_set.rs`'s
        // `merged_top_k_matches_brute_force_ground_truth_over_the_full_point_set`
        // (which proves `SegmentSet::search`'s merge itself is correct) to
        // `Dataset`/`Snapshot` (which proves the whole commit -> segment ->
        // fan-out path preserves that correctness end to end, through real
        // transactions and real on-disk segments rather than a
        // hand-assembled `SegmentSet`).
        //
        // Unlike `reopening_a_dataset_with_multiple_segments_finds_each_segments_own_cluster`
        // and its siblings, whose clusters sit 500+ units apart specifically
        // so each query trivially resolves to "its own" segment, a test for
        // cross-segment *merging* needs the true top-k to genuinely span
        // more than one segment -- a widely-separated layout can't
        // discriminate a merge bug that only ever consults the first part.
        // So the three 20-point cluster centers below are only 40 units
        // apart, closer than each cluster's own 60-unit spacing: cluster
        // A's points span roughly x=[0,60), cluster B's span x=[40,100),
        // cluster C's span x=[80,140) -- heavily overlapping bounding
        // regions rather than disjoint neighborhoods, with y/z offsets
        // drawn from the identical [0,60) range in every cluster. A query
        // at [64,15,15] with k=10 was verified by direct computation
        // (mirroring `cluster_vectors`' own golden-ratio/sqrt2/sqrt3
        // offset sequence) to draw its true top-10 nearest neighbors 3
        // from commit 0's (row-ids 0..20), 4 from commit 1's (row-ids
        // 20..40), and 3 from commit 2's (row-ids 40..60) ranges -- a
        // genuine 3-way split, so dropping ANY one segment's worth of
        // results measurably drops recall below this test's 0.9
        // threshold. See the accompanying report for the computed
        // neighbor list.
        let dir = temp_dir("recall-parity-overlapping-segments");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let count = 20usize;
        let spacing = 60.0f32;
        let centers = [[0.0f32, 0.0, 0.0], [40.0, 0.0, 0.0], [80.0, 0.0, 0.0]];

        // Every (row_id, vector) pair committed, in commit order, so
        // `brute_force_search`'s `row_index` maps 1:1 onto this Vec's
        // index -- the same care the `crates/index`-level sibling test
        // takes.
        let mut all_points: Vec<(u64, Vec<f32>)> = Vec::new();
        // Disjoint row-id ranges, one per commit (0..20, 20..40, 40..60).
        // Each commit is its own segment (one vector-carrying commit -> one
        // segment), so these ranges double as "which segment did this
        // row-id come from" for the cross-segment assertions below.
        let mut commit_ranges: Vec<std::ops::Range<u64>> = Vec::new();

        for center in &centers {
            let vectors = cluster_vectors(count, *center, spacing);
            let base = i64::try_from(all_points.len()).unwrap();
            let ids: Vec<i64> = (base..base + i64::try_from(count).unwrap()).collect();

            let mut txn = ds.begin();
            txn.insert(vector_batch(ids, vectors.clone())).unwrap();
            txn.commit().unwrap();

            let range_start = u64::try_from(all_points.len()).unwrap();
            for v in &vectors {
                let row_id = u64::try_from(all_points.len()).unwrap();
                all_points.push((row_id, v.to_vec()));
            }
            let range_end = u64::try_from(all_points.len()).unwrap();
            commit_ranges.push(range_start..range_end);
        }
        assert_eq!(
            ds.snapshot().manifest.segments.len(),
            3,
            "three vector-carrying commits must produce three segments"
        );
        assert_eq!(
            ds.snapshot().index.len(),
            3,
            "the loaded segment set must match the manifest's segment list"
        );

        // A FixedSizeListArray built in the SAME order as `all_points`, so
        // `brute_force_search`'s `row_index` maps 1:1 onto
        // `all_points[row_index].0`'s row-id -- identical technique to the
        // `crates/index`-level sibling test.
        let flat: Vec<f32> = all_points
            .iter()
            .flat_map(|(_, v)| v.iter().copied())
            .collect();
        let values = Arc::new(arrow::array::Float32Array::from(flat));
        let field = Arc::new(Field::new("item", DataType::Float32, false));
        let vectors_arr = arrow::array::FixedSizeListArray::new(field, 3, values, None);

        let query = [64.0f32, 15.0, 15.0];
        let k = 10;

        let truth = strata_index::brute_force_search(&vectors_arr, &query, k).unwrap();
        let truth_row_ids: std::collections::HashSet<u64> = truth
            .iter()
            .map(|neighbor| all_points[neighbor.row_index].0)
            .collect();

        // Fixture sanity check: if the true top-k didn't actually span all 3
        // segments, this test would not be exercising what it claims to --
        // a top-k that only ever spans 2 of the 3 ranges can't discriminate
        // a fan-out bug that silently drops the one segment it happens to
        // exclude. The geometry was verified by direct computation before
        // writing this test (see the report); re-asserting it here means a
        // future edit to the centers/spacing/query that accidentally
        // collapses the top-k back into fewer segments fails loudly here,
        // rather than silently degrading this into a weaker test that
        // still passes.
        let truth_ranges_represented = commit_ranges
            .iter()
            .filter(|r| truth_row_ids.iter().any(|id| r.contains(id)))
            .count();
        assert_eq!(
            truth_ranges_represented, 3,
            "fixture regression: the brute-force ground truth top-{k} no longer spans \
             all 3 segments, so this test can no longer discriminate a fan-out bug that \
             drops any single segment; truth row-ids {truth_row_ids:?} against ranges \
             {commit_ranges:?}"
        );

        let hits = ds.snapshot().vector_search(&query, k, None).unwrap();
        assert_eq!(hits.len(), k, "{hits:?}");
        let hit_row_ids: std::collections::HashSet<u64> = hits.iter().map(|m| m.row_id).collect();

        let recall = hit_row_ids.intersection(&truth_row_ids).count() as f64 / k as f64;
        assert!(
            recall >= 0.9,
            "merged top-{k} must closely match exact brute-force ground truth over the \
             full 60-point union spanning 3 overlapping segments; recall@{k} = {recall:.2}. \
             got {hit_row_ids:?}, truth {truth_row_ids:?}"
        );

        // The assertion that actually proves cross-segment fan-out
        // happened, rather than relying on recall alone: a regression that
        // only ever queries a subset of segments could still score
        // deceptively high recall if the segments it does consult happen
        // to dominate the true top-k. This fixture's true top-k spans all
        // 3 commit ranges with a genuine 3/4/3 split (asserted above), so
        // a fan-out that silently dropped ANY one segment could return
        // results from at most 2 ranges here -- this assertion is what
        // would actually catch that regression.
        let hit_ranges_represented = commit_ranges
            .iter()
            .filter(|r| hit_row_ids.iter().any(|id| r.contains(id)))
            .count();
        assert_eq!(
            hit_ranges_represented, 3,
            "results must span all 3 committed segments -- a result set missing any one \
             commit's row-id range would mean the fan-out failed to consult that segment: \
             got {hit_row_ids:?} against ranges {commit_ranges:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reopening_a_dataset_with_a_truncated_segment_file_returns_corrupt_segment() {
        // Zero test coverage existed anywhere in the workspace for
        // `TxnError::CorruptSegment`, despite `load_segments` constructing
        // it -- this is that coverage. Mirrors the truncation technique
        // already used to test `SegmentReader::from_bytes`'s own corruption
        // handling (`crates/index/src/segment_reader.rs`'s
        // `a_truncated_file_is_rejected_rather_than_read_past_its_end`), but
        // exercised through the real commit -> close -> corrupt -> reopen
        // path, since `load_segments`'s `byte_len` cross-check runs *before*
        // `SegmentReader::from_bytes` ever sees the bytes.
        let dir = temp_dir("truncated-segment-on-reopen");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let mut txn = ds.begin();
        txn.insert(vector_batch(
            vec![1i64, 2i64],
            cluster_vectors(2, [0.0, 0.0, 0.0], 0.01),
        ))
        .unwrap();
        txn.commit().unwrap();

        let segment_name = ds.snapshot().manifest.segments[0].name.clone();
        drop(ds);

        let segment_path = data_subdir(&dir).join(&segment_name);
        let bytes = std::fs::read(&segment_path).unwrap();
        assert!(
            bytes.len() > 8,
            "precondition: the real segment must be long enough to truncate meaningfully"
        );
        std::fs::write(&segment_path, &bytes[..bytes.len() - 8]).unwrap();

        match Dataset::open(&dir) {
            Err(TxnError::CorruptSegment(_)) => {}
            Err(other) => panic!("expected CorruptSegment, got a different error: {other}"),
            Ok(_) => panic!("open must not succeed against a truncated segment"),
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn committing_a_batch_with_a_non_finite_vector_component_is_rejected_cleanly() {
        // Regression test for the Phase 4 final-review finding: a
        // non-finite (NaN/Infinity) vector component used to durably
        // commit — the old delta log's JSON encoding silently turned it
        // into `null` — and then permanently brick the dataset, since every
        // future open-time replay would fail to parse that `null` back into
        // an f32. Must now be rejected upfront, before any
        // file for the offending batch is written to disk, leaving no
        // trace: no manifest advance, no orphaned-but-referenced files.
        let dir = temp_dir("non-finite-vector-rejected");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let batch = vector_batch(vec![1, 2], vec![[0.0, 0.0, 0.0], [f32::NAN, 1.0, 1.0]]);
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let first = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .unwrap();
        let second =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![30, 40, 50]))])
                .unwrap();

        let mut txn = ds.begin();
        txn.insert(first).unwrap();
        txn.insert(second).unwrap();
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
        Dataset::create(&dir, test_schema()).unwrap();

        // Simulate a hostile manifest: hand-craft a DataFileEntry whose name
        // tries to escape data/ via a parent-directory component. No real
        // commit can ever produce this - file names are always generated
        // internally - so this is only reachable via a corrupted/hand-edited
        // manifest, which is exactly the threat model this guards against.
        let hostile = Manifest {
            version: 1,
            data_files: vec![fixture_data_file("../../etc/passwd")],
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
            commit_time_high_water: 0,
            segments: Vec::new(),
            ..Manifest::empty()
        };
        strata_storage::commit_manifest(&dir, &hostile).unwrap();
        let result = Dataset::open(&dir);
        assert!(
            matches!(result, Err(TxnError::UnsafeManifestPath(_))),
            "expected UnsafeManifestPath"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn open_errors_instead_of_traversing_outside_data_dir_on_an_unsafe_segment_entry() {
        // `SegmentEntry.name` became a second consumer of `safe_join`'s
        // guard alongside `DataFileEntry.name` (see the sibling test just
        // above), but had zero test coverage of its own. Unlike a hostile
        // `DataFileEntry`, which is only checked lazily on the first
        // `scan`, a hostile `SegmentEntry` is checked eagerly: segments load
        // during `Dataset::open` itself (`load_segments`), not on first
        // use — so the assertion here is on `open`'s own return value, not
        // a later `scan`.
        let dir = temp_dir("segment-path-traversal");
        Dataset::create(&dir, test_schema()).unwrap();

        // Simulate a hostile manifest: hand-craft a SegmentEntry whose name
        // tries to escape data/ via a parent-directory component. No real
        // commit can ever produce this — segment filenames are always
        // generated internally from `write_attempt_counter` — so this is
        // only reachable via a corrupted/hand-edited manifest, exactly the
        // threat model `safe_join` guards against.
        let hostile = Manifest {
            version: 1,
            data_files: Vec::new(),
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
            commit_time_high_water: 0,
            segments: vec![SegmentEntry {
                name: "../../etc/passwd".to_string(),
                format_version: strata_index::SEGMENT_FORMAT_VERSION,
                vector_count: 0,
                dimension: 0,
                row_id_min: 0,
                row_id_max: 0,
                byte_len: 0,
                zone_map: std::collections::HashMap::new(),
            }],
            ..Manifest::empty()
        };
        strata_storage::commit_manifest(&dir, &hostile).unwrap();

        match Dataset::open(&dir) {
            Err(TxnError::UnsafeManifestPath(_)) => {}
            Err(other) => panic!("expected UnsafeManifestPath, got a different error: {other}"),
            Ok(_) => panic!("open must not succeed against a hostile segment path"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn open_errors_with_corrupt_segment_when_two_segments_in_one_manifest_disagree_on_dimension() {
        // `load_segments`'s running `established_dimension` (see the doc
        // comment just above it) is the *second* line of defense against
        // Finding 1's dimension race: the in-lock check in `commit` makes
        // this branch unreachable through the normal write path, so
        // nothing in the existing suite ever exercises it. A future
        // refactor could silently break or delete it with every other test
        // staying green. Same hand-crafted-manifest technique as the
        // sibling test just above (`..._on_an_unsafe_segment_entry`): no
        // real commit path can ever produce two self-consistent segments
        // at different dimensions in the same manifest, so the only way to
        // exercise this branch is to fabricate one directly.
        let dir_a = temp_dir("cross-segment-dimension-a");
        let ds_a = Dataset::create(&dir_a, vector_test_schema()).unwrap();
        let mut txn = ds_a.begin();
        txn.insert(vector_batch(
            vec![1, 2],
            vec![[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]],
        ))
        .unwrap();
        txn.commit().unwrap();

        // A second, wholly separate dataset, committed to independently,
        // whose vectors are 5-dimensional rather than A's 3 — producing a
        // real, valid, self-consistent 5-d `.seg` file and `SegmentEntry`.
        let dir_b = temp_dir("cross-segment-dimension-b");
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 5),
                false,
            ),
        ]));
        let ds_b = Dataset::create(&dir_b, Arc::clone(&schema_b)).unwrap();
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let flat: Vec<f32> = vec![
            0.0, 0.0, 0.0, 0.0, 0.0, // row 1
            1.0, 1.0, 1.0, 1.0, 1.0, // row 2
        ];
        let vec_arr = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field,
            5,
            Arc::new(arrow::array::Float32Array::from(flat)),
            None,
        ));
        let batch_b = RecordBatch::try_new(
            schema_b,
            vec![Arc::new(Int64Array::from(vec![1i64, 2])), vec_arr],
        )
        .unwrap();
        let mut txn_b = ds_b.begin();
        txn_b.insert(batch_b).unwrap();
        txn_b.commit().unwrap();

        let manifest_b = ds_b.snapshot().manifest.as_ref().clone();
        assert_eq!(
            manifest_b.segments.len(),
            1,
            "sanity: B's commit must have produced exactly one segment"
        );
        let mut foreign_entry = manifest_b.segments[0].clone();
        assert_eq!(foreign_entry.dimension, 5);

        // Copy B's real segment bytes into A's data/ dir under a name that
        // doesn't collide with anything A itself ever generates (A's own
        // segment names come from its own `write_attempt_counter`, which
        // starts at 0), then point a hand-crafted `SegmentEntry` at it —
        // dimension/byte_len/etc left exactly as B produced them, since the
        // actual on-disk bytes must match what's declared.
        let foreign_name = "foreign-5d.seg".to_string();
        std::fs::copy(
            data_subdir(&dir_b).join(&foreign_entry.name),
            data_subdir(&dir_a).join(&foreign_name),
        )
        .unwrap();
        foreign_entry.name = foreign_name;

        let mut manifest_a = ds_a.snapshot().manifest.as_ref().clone();
        manifest_a.version += 1;
        manifest_a.segments.push(foreign_entry);
        strata_storage::commit_manifest(&dir_a, &manifest_a).unwrap();

        match Dataset::open(&dir_a) {
            Err(TxnError::CorruptSegment(_)) => {}
            Err(other) => panic!("expected CorruptSegment, got a different error: {other}"),
            Ok(_) => panic!(
                "open must not succeed against a manifest whose segments disagree on dimension"
            ),
        }

        std::fs::remove_dir_all(&dir_a).ok();
        std::fs::remove_dir_all(&dir_b).ok();
    }

    #[test]
    fn scan_errors_on_column_count_mismatch_between_physical_file_and_caller_schema() {
        let dir = temp_dir("schema-mismatch");
        let write_schema = test_schema(); // single "id" column
        let ds = Dataset::create(&dir, Arc::clone(&write_schema)).unwrap();

        let batch = RecordBatch::try_new(
            write_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
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
            matches!(result, Err(TxnError::BatchSchemaMismatch { .. })),
            "expected BatchSchemaMismatch, got {result:?}"
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
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let low = vector_batch(vec![1, 1], vec![[0.0, 0.0, 0.0], [0.01, 0.01, 0.01]]);
        let mut txn = ds.begin();
        txn.insert(low).unwrap();
        txn.commit().unwrap();

        let high = vector_batch(
            vec![2, 2],
            vec![[1000.0, 1000.0, 1000.0], [1000.01, 1000.01, 1000.01]],
        );
        let mut txn = ds.begin();
        txn.insert(high).unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();
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
        assert!(
            ds.snapshot().manifest.segments.is_empty(),
            "an empty commit adds no segments either"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_commit_whose_batch_has_no_vector_column_writes_no_segment_file() {
        // Amendment §3c: a commit that carries rows but no vectors at all
        // (no `"vector"` column on any pending batch) must publish zero
        // segments -- not an empty one. Distinct from the zero-pending-batch
        // case above: this commit writes a real data file, so `write_phase`
        // runs the full path down to `build_and_write_segment`, which must
        // still return `None` because `build_vector_inserts` produced no
        // entries.
        //
        // Task 9 extends this with the delete-only-commit case below: a
        // transaction that only tombstones rows likewise inserts nothing and
        // must build no segment either, made explicit so it can't regress
        // into "we write an empty segment and nobody noticed" (amendment
        // §3c).
        let dir = temp_dir("no-vector-column-no-segment");
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let batch = RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
        txn.commit().unwrap();

        assert_eq!(
            ds.data_files().len(),
            1,
            "the commit's row data must still be written"
        );
        assert!(
            ds.snapshot().manifest.segments.is_empty(),
            "a commit with no vector column must publish no SegmentEntry: {:?}",
            ds.snapshot().manifest.segments
        );
        assert_eq!(
            ds.snapshot().index.len(),
            0,
            "the snapshot's segment set must stay empty too"
        );
        let seg_files: Vec<_> = std::fs::read_dir(ds.data_dir())
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "seg"))
            .collect();
        assert!(
            seg_files.is_empty(),
            "no .seg file must have been written to data_dir: {seg_files:?}"
        );

        // And a delete-only commit likewise: nothing to insert, so no
        // segment is built at all.
        let mut deleting = ds.begin();
        deleting.delete(0).unwrap();
        deleting.commit().unwrap();
        assert!(
            ds.snapshot().manifest.segments.is_empty(),
            "a delete-only commit must publish no SegmentEntry either: {:?}",
            ds.snapshot().manifest.segments
        );
        assert!(
            orphaned_segment_files(&ds).is_empty(),
            "no .seg file may be written at all for a delete-only commit -- \
             not even an unreferenced one"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_errors_cleanly_when_a_manifest_listed_file_is_missing_from_disk() {
        let dir = temp_dir("scan-missing-file");
        let schema = test_schema();
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
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
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

        // First commit: high-cardinality (all-distinct) -> stays plain Utf8.
        let owned: Vec<String> = (0..20).map(|i| format!("name-{i}")).collect();
        let high_card: Vec<&str> = owned.iter().map(String::as_str).collect();
        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(high_card))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch1).unwrap();
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
        txn.insert(batch2).unwrap();
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
    fn build_vector_inserts_skips_null_vector_rows_without_erroring() {
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

        let inserts = build_vector_inserts(&batch, 0).unwrap();
        assert_eq!(
            inserts.len(),
            1,
            "the null-vector row must be skipped, not errored on"
        );
        assert_eq!(inserts[0].row_id, 0);
    }

    #[test]
    fn build_vector_inserts_produces_the_correct_vector_per_row() {
        // Distinct, easily-distinguishable vectors per row -- a flat-buffer
        // indexing bug (e.g. an off-by-one in row * value_length, or
        // accidentally reading a neighboring row's slice) would surface as
        // a row getting the wrong vector, which a row_id-only assertion
        // (as the other build_vector_inserts tests use) would never catch.
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

        let inserts = build_vector_inserts(&batch, 100).unwrap();
        assert_eq!(inserts.len(), 3);
        let as_pair = |i: &VectorInsert| (i.row_id, i.vector.clone());
        assert_eq!(as_pair(&inserts[0]), (100, vec![1.0, 2.0, 3.0]));
        assert_eq!(as_pair(&inserts[1]), (101, vec![4.0, 5.0, 6.0]));
        assert_eq!(as_pair(&inserts[2]), (102, vec![7.0, 8.0, 9.0]));
    }

    #[test]
    fn build_vector_inserts_reads_the_correct_vector_from_a_sliced_batch() {
        // Pins the assumption build_vector_inserts's downcast-hoist rewrite
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

        let inserts = build_vector_inserts(&sliced, 0).unwrap();
        assert_eq!(inserts.len(), 2);
        let as_pair = |i: &VectorInsert| (i.row_id, i.vector.clone());
        assert_eq!(as_pair(&inserts[0]), (0, vec![7.0, 8.0, 9.0]));
        assert_eq!(as_pair(&inserts[1]), (1, vec![10.0, 11.0, 12.0]));
    }

    #[test]
    fn build_vector_inserts_errors_on_wrong_inner_type_even_with_zero_rows() {
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

        let result = build_vector_inserts(&batch, 0);
        assert!(
            result.is_err(),
            "a wrong inner type must error even when every row is null: {result:?}"
        );
    }

    #[test]
    fn build_vector_inserts_errors_when_vector_column_is_not_a_fixed_size_list() {
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

        let result = build_vector_inserts(&batch, 0);
        assert!(result.is_err(), "expected an error, got {result:?}");
    }

    #[test]
    fn build_vector_inserts_errors_when_vector_inner_type_is_not_float32() {
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

        let result = build_vector_inserts(&batch, 0);
        assert!(result.is_err(), "expected an error, got {result:?}");
    }

    #[test]
    fn validate_vector_dimensions_rejects_a_ragged_commit_against_a_plain_established_dimension() {
        // The signature change the W3.2 amendment section 2 requires: the
        // check reads a `usize`, not a live graph handle, because after
        // W3.2a there is no shared graph to ask.
        let ragged = vec![
            VectorInsert {
                row_id: 0,
                vector: vec![1.0, 2.0, 3.0],
            },
            VectorInsert {
                row_id: 1,
                vector: vec![1.0, 2.0],
            },
        ];
        let result = validate_vector_dimensions(&ragged, 0);
        assert!(
            matches!(
                result,
                Err(TxnError::Index(
                    strata_index::IndexError::DimensionMismatch {
                        query_len: 2,
                        expected: 3
                    }
                ))
            ),
            "two pending vectors of different lengths must be rejected even with \
             nothing established yet: {result:?}"
        );
    }

    #[test]
    fn validate_vector_dimensions_rejects_a_commit_disagreeing_with_the_established_dimension() {
        let inserts = vec![VectorInsert {
            row_id: 0,
            vector: vec![1.0, 2.0],
        }];
        let result = validate_vector_dimensions(&inserts, 3);
        assert!(
            matches!(
                result,
                Err(TxnError::Index(
                    strata_index::IndexError::DimensionMismatch {
                        query_len: 2,
                        expected: 3
                    }
                ))
            ),
            "{result:?}"
        );
    }

    #[test]
    fn validate_vector_dimensions_accepts_an_empty_commit_and_a_consistent_one() {
        assert!(validate_vector_dimensions(&[], 3).is_ok());
        assert!(validate_vector_dimensions(&[], 0).is_ok());
        let consistent = vec![
            VectorInsert {
                row_id: 0,
                vector: vec![1.0, 2.0, 3.0],
            },
            VectorInsert {
                row_id: 1,
                vector: vec![4.0, 5.0, 6.0],
            },
        ];
        assert!(validate_vector_dimensions(&consistent, 3).is_ok());
        assert!(validate_vector_dimensions(&consistent, 0).is_ok());
    }

    #[test]
    fn delete_tombstones_a_row_and_it_becomes_invisible() {
        let dir = temp_dir("delete-basic");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
        txn.commit().unwrap();

        let mut txn = ds.begin();
        txn.delete(0).unwrap();
        txn.commit().unwrap();

        assert!(!ds.snapshot().is_visible(0));
    }

    #[test]
    fn redeleting_an_already_tombstoned_row_does_not_duplicate_it_in_the_persisted_manifest() {
        let dir = temp_dir("tombstone-dedup-cross-txn");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(batch).unwrap();
        setup.commit().unwrap();

        let mut txn_a = ds.begin();
        txn_a.delete(0).unwrap();
        txn_a.commit().unwrap();
        assert_eq!(ds.snapshot().manifest.tombstones.len(), 1);

        // A later transaction cannot re-target an already tombstoned row.
        let mut txn_b = ds.begin();
        assert!(matches!(
            txn_b.delete(0),
            Err(TxnError::RowNotLive { row_id: 0 })
        ));
        assert_eq!(
            ds.snapshot().manifest.tombstones.len(),
            1,
            "a rejected re-delete must not change persisted tombstones"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn deleting_the_same_row_twice_in_one_transaction_does_not_duplicate_persisted_tombstone() {
        let dir = temp_dir("tombstone-dedup-intra-txn");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(batch).unwrap();
        setup.commit().unwrap();

        let mut txn = ds.begin();
        txn.delete(0).unwrap();
        assert!(matches!(
            txn.delete(0),
            Err(TxnError::DuplicateTarget { row_id: 0 })
        ));
        txn.commit().unwrap();

        assert_eq!(
            ds.snapshot().manifest.tombstones.len(),
            1,
            "a duplicate delete must not duplicate the persisted tombstone"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn update_tombstones_old_row_and_makes_new_row_visible() {
        let dir = temp_dir("update-basic");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
        txn.commit().unwrap();

        let replacement = vector_batch(vec![1i64], cluster_vectors(1, [5.0, 5.0, 5.0], 0.0));
        let mut txn = ds.begin();
        txn.update(0, replacement).unwrap();
        txn.commit().unwrap();

        let snapshot = ds.snapshot();
        assert!(!snapshot.is_visible(0), "old row must be tombstoned");
        assert!(snapshot.is_visible(1), "replacement row must be visible");
    }

    #[test]
    fn tombstones_persist_across_reopen() {
        let dir = temp_dir("delete-persists");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
        txn.commit().unwrap();

        let mut txn = ds.begin();
        txn.delete(0).unwrap();
        txn.commit().unwrap();
        drop(ds);

        let reopened = Dataset::open(&dir).unwrap();
        assert!(!reopened.snapshot().is_visible(0));
    }

    #[test]
    fn concurrent_delete_of_the_same_row_conflicts() {
        let dir = temp_dir("commit-lock-conflict");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();
        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(batch).unwrap();
        setup.commit().unwrap();

        let mut txn_a = ds.begin();
        txn_a.delete(0).unwrap();
        let mut txn_b = ds.begin();
        txn_b.delete(0).unwrap();

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
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();
        let batch = vector_batch(vec![1i64, 2i64], cluster_vectors(2, [0.0, 0.0, 0.0], 0.01));
        let mut setup = ds.begin();
        setup.insert(batch).unwrap();
        setup.commit().unwrap();

        let mut txn_a = ds.begin();
        txn_a.delete(0).unwrap();
        let mut txn_b = ds.begin();
        txn_b.delete(1).unwrap();

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
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();
        let batch = vector_batch(vec![1i64, 2i64], cluster_vectors(2, [0.0, 0.0, 0.0], 0.01));
        let mut setup = ds.begin();
        setup.insert(batch).unwrap();
        setup.commit().unwrap();
        assert_eq!(ds.current_version(), 1);

        let mut txn_a = ds.begin();
        txn_a.delete(0).unwrap();
        let mut txn_b = ds.begin();
        txn_b.delete(1).unwrap();

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
        let ds = Dataset::create(&dir, test_schema()).unwrap();
        let schema = test_schema();

        let mut txn_a = ds.begin();
        txn_a
            .insert(
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
                    .unwrap(),
            )
            .unwrap();
        let mut txn_b = ds.begin();
        txn_b
            .insert(
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![2]))])
                    .unwrap(),
            )
            .unwrap();

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
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

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
        txn.insert(batch).unwrap();
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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

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
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
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
    fn a_fourth_commit_adds_exactly_one_row_to_three_commits_of_history() {
        let dir = tempfile::Builder::new()
            .prefix("strata-replay-cost-regression-")
            .tempdir()
            .unwrap()
            .keep();
        Dataset::create(&dir, crate::mvp_fixtures::mvp_schema()).unwrap();
        let dataset = Dataset::open(&dir).unwrap();

        // Commit 3 separate single-row batches first, establishing history.
        // `mvp_row(id, name, vector)` builds one row in mvp_schema()'s
        // shape — `id` is the schema's business column, unrelated to the
        // internal system row-id the commit path assigns automatically.
        for i in 0..3i64 {
            let mut txn = dataset.begin();
            txn.insert(crate::mvp_fixtures::mvp_row(i, "row", [i as f32, 0.0, 0.0]).unwrap())
                .unwrap();
            txn.commit().unwrap();
        }

        // The 4th commit's own pending batch has exactly 1 row (1 new
        // segment, keyed 0..1). This only checks the resulting row count —
        // NOT that publishing it left the 3 earlier commits' segment files
        // untouched (this test's name previously overclaimed that; it
        // asserts nothing about segment files at all). A count of exactly
        // "3 history rows + 1 new row" would still be wrong if either this
        // commit's row were lost, or history-dependent logic silently
        // reprocessed old entries into a wrong count — that's the actual
        // discriminating power this assertion has.
        let mut txn = dataset.begin();
        txn.insert(crate::mvp_fixtures::mvp_row(3, "row", [3.0, 0.0, 0.0]).unwrap())
            .unwrap();
        txn.commit().unwrap();

        let snapshot = dataset.snapshot();
        assert_eq!(
            snapshot
                .scan(&crate::mvp_fixtures::mvp_schema())
                .unwrap()
                .num_rows(),
            4,
            "expected exactly 4 rows total (system row-ids 0..=3)"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn insert_rejects_non_owned_vector_dimensions_before_publication() {
        let dir = temp_dir("inconsistent-batch-dimensions");
        let schema = crate::mvp_fixtures::mvp_schema();
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

        let mut seed = ds.begin();
        seed.insert(crate::mvp_fixtures::mvp_row(0, "seed", [0.0, 0.0, 0.0]).unwrap())
            .unwrap();
        seed.commit().unwrap();
        let snapshot_before = ds.snapshot();

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
        assert!(matches!(
            txn.insert(batch_5d),
            Err(TxnError::BatchSchemaMismatch { .. })
        ));
        assert_eq!(txn.pending.len(), 0);
        assert_eq!(ds.snapshot().version, snapshot_before.version);
        assert_eq!(
            ds.snapshot().manifest.segments.len(),
            snapshot_before.manifest.segments.len()
        );

        std::fs::remove_dir_all(&dir).ok();
    }
    #[test]
    fn insert_rejects_non_owned_vector_schema_before_allocation() {
        let dir = temp_dir("non-owned-vector-schema");
        let schema = crate::mvp_fixtures::mvp_schema();
        let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
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
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(arrow::array::FixedSizeListArray::new(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    5,
                    Arc::new(arrow::array::Float32Array::from(vec![
                        9.0, 9.0, 9.0, 9.0, 9.0,
                    ])),
                    None,
                )),
            ],
        )
        .unwrap();

        let mut txn = ds.begin();
        assert!(matches!(
            txn.insert(batch_5d),
            Err(TxnError::BatchSchemaMismatch { .. })
        ));
        assert_eq!(txn.pending.len(), 0);
        assert_eq!(ds.current_version(), 0);
        assert!(ds.snapshot().manifest.data_files.is_empty());

        std::fs::remove_dir_all(&dir).ok();
    }
    #[test]
    fn losing_transactions_vectors_never_become_searchable_when_it_conflicts() {
        // Deterministic, not loom: both transactions begin from the same
        // snapshot, then commit sequentially (not concurrently) so which
        // one wins is fixed by test order, not explored interleavings —
        // there is no concurrency to model here, only a specific sequence
        // to regression-test. This is what actually exercises the
        // graph-mutation-ordering bug (design doc §2): both transactions
        // use `update`, not `delete`, since a delete-only transaction has
        // nothing to insert and can't trigger this bug at all.
        let dir = temp_dir("abort-leaves-no-graph-trace");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();
        let setup_batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(setup_batch).unwrap();
        setup.commit().unwrap();

        // Distinctive, far-apart, never-elsewhere-used coordinates so a
        // vector_search near either one unambiguously reveals whether
        // that specific insert ever reached the graph.
        let winner_batch = vector_batch(vec![2i64], cluster_vectors(1, [500.0, 500.0, 500.0], 0.0));
        let loser_batch = vector_batch(vec![3i64], cluster_vectors(1, [900.0, 900.0, 900.0], 0.0));

        let mut txn_winner = ds.begin();
        txn_winner.update(0, winner_batch).unwrap();
        let mut txn_loser = ds.begin();
        txn_loser.update(0, loser_batch).unwrap();

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
    fn a_failed_commits_vector_is_never_searchable_after_a_later_commit_advances_the_row_id_counter()
     {
        // Regression test for the dangling-search-hit hazard. Before S1
        // W3.2a, `commit` applied this transaction's HNSW `Insert` deltas to
        // a shared `Arc<HnswIndex>` *before* `commit_manifest` made the
        // commit durable. If `commit_manifest` failed (e.g. ENOSPC) —
        // modelled here by `inject_manifest_commit_failure`, injected at
        // exactly that step — the failed transaction's vector was left in
        // the shared graph with no manifest entry backing it, and its
        // row-id was already allocated by `write_phase`. A *later*
        // successful commit then persisted `manifest.next_row_id` past that
        // residue row-id and published `watermark = next_row_id - 1`, so
        // `Snapshot::is_visible` started passing for the residue id. With no
        // manifest-membership cross-check on the search path,
        // `vector_search` would then return the residue as a dangling hit —
        // a row `scan` can never see — violating the flagship "no silently
        // stale vector search results" claim. A guard type used to
        // soft-delete a failed commit's graph inserts on the error path to
        // close this; S1 W3.2b removed it once the guarantee below took
        // over.
        //
        // Since W3.2a this holds *structurally*, not via that compensation:
        // this transaction's vector never touches anything shared until publish
        // — it lives only in this commit's own segment file, built and
        // fsynced in `write_phase`, which the injected failure below
        // discards before it ever reaches a manifest. This test remains the
        // regression test for the property, now guaranteed by construction.
        let dir = temp_dir("failed-commit-no-dangling-search-hit");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        // Seed: one durable row (system row-id 0), far from the residue's
        // distinctive coordinates. Establishes the graph's dimension and
        // gives the row-id counter a meaningful starting point.
        let mut seed = ds.begin();
        seed.insert(vector_batch(
            vec![1i64],
            cluster_vectors(1, [0.0, 0.0, 0.0], 0.0),
        ))
        .unwrap();
        seed.commit().unwrap();

        // T1: insert a vector at distinctive, never-reused coordinates, then
        // fail at the manifest-commit step (after its segment has already
        // been built and fsynced in write_phase). Its row-id (1) is
        // allocated but never committed.
        let mut failing = ds.begin();
        failing
            .insert(vector_batch(
                vec![2i64],
                cluster_vectors(1, [900.0, 900.0, 900.0], 0.0),
            ))
            .unwrap();
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
        // location. It claims row-id 2 and persists `next_row_id = 3`,
        // moving the durable allocation high-water mark past the residue
        // row-id 1 — the condition that used to make the residue pass
        // `is_visible`, and the one this test still reproduces.
        let mut later = ds.begin();
        later
            .insert(vector_batch(
                vec![3i64],
                cluster_vectors(1, [500.0, 500.0, 500.0], 0.0),
            ))
            .unwrap();
        later.commit().unwrap();

        let snapshot = ds.snapshot();
        assert_eq!(
            snapshot.manifest.next_row_id, 3,
            "precondition: the seed (row-id 0), the failed commit (row-id 1) \
             and the later commit (row-id 2) must each have consumed one \
             row-id, so the later commit really did allocate past the residue \
             row-id 1, or this test isn't exercising the hazard"
        );

        // The discriminating assertion: searching at T1's distinctive
        // coordinates must not return its residue vector. Pre-fix, the
        // residue (row-id 1 at [900,900,900]) is both physically in the graph
        // and now visible, so it comes back with ~0 squared distance.
        // Post-fix its segment and data never entered any manifest, so this
        // snapshot cannot reach it at all and the nearest live match is the
        // far-away seed/T2 data.
        let results = snapshot
            .vector_search(&[900.0, 900.0, 900.0], 1, None)
            .unwrap();
        assert!(
            results.is_empty() || results[0].squared_distance > 1000.0,
            "a failed commit's vector must never be searchable, even after a \
             later commit advances the row-id counter past its row-id: {results:?}"
        );

        // Positive controls, so the assertion above can't pass vacuously.
        // Search itself really is working on this snapshot — so "not found"
        // above means *excluded*, not "search is broken".
        assert_eq!(
            snapshot.index.established_dimension(),
            3,
            "the seed commit established dimension 3; the failed commit contributes \
             nothing to it, which is itself the W3.2a improvement -- a failed \
             first-ever vector commit no longer poisons the session's dimension"
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
    fn reopening_after_a_failed_manifest_commit_does_not_reuse_the_abandoned_row_id() {
        // COR-02 / CONC-03: a row-id claim is made before the manifest is
        // published. If that publication fails and the process later
        // restarts, the next committed row must start after the abandoned
        // physical id rather than trusting the stale manifest watermark.
        let dir = temp_dir("row-id-high-water-restart-non-reuse");
        let ds = Dataset::create(&dir, test_schema()).unwrap();

        let mut abandoned = ds.begin();
        abandoned
            .insert(
                RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1_i64]))])
                    .unwrap(),
            )
            .unwrap();
        abandoned.inject_manifest_commit_failure();
        assert!(
            abandoned.commit().is_err(),
            "the injected manifest failure must abandon a claimed range"
        );
        drop(ds);

        let reopened = Dataset::open(&dir).unwrap();
        let mut committed = reopened.begin();
        committed
            .insert(
                RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![2_i64]))])
                    .unwrap(),
            )
            .unwrap();
        committed.commit().unwrap();

        let entries = reopened.data_files();
        assert_eq!(entries.len(), 1, "only the post-restart commit is visible");
        assert_eq!(
            entries[0].row_id_range,
            Some((1, 1)),
            "the durable allocator high-water must keep row-id 0 permanently abandoned"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_concurrent_reader_never_sees_an_in_flight_commits_vector() {
        // Regression test for the snapshot-isolation window spec §2 rules
        // out: "a transaction's writes are never visible to any other
        // transaction until commit succeeds."
        //
        // Row-ids are claimed *before* `commit_lock`, in `write_phase`,
        // while `manifest.next_row_id` is published from the *global* row-id
        // counter inside some *other* transaction's critical section — so an
        // unrelated commit's manifest numerically covers row-ids this
        // transaction has claimed but not committed. Back when visibility
        // was a `row_id <= watermark` bound, that alone was enough to make
        // the uncommitted row-id look visible.
        //
        // Before S1 W3.2a this was a real race: the slow transaction's
        // vector was applied to a shared `Arc<HnswIndex>` before
        // `commit_manifest` made the commit durable, so between that apply
        // and `commit_manifest`, the vector was both physically in the
        // graph and (pre-fix) passing `is_visible` on the currently
        // published snapshot — a search hit for a row no `scan` could see,
        // roughly one `commit_manifest` fsync wide. Since W3.2a, this
        // transaction's segment is built and fsynced entirely in
        // `write_phase` and joins no shared structure until commit's
        // in-lock `latest_snapshot.index.with_appended(...)`, which runs
        // only after `commit_manifest` succeeds — so even though the
        // *other* transaction's published `next_row_id` numerically covers
        // this transaction's claimed row-id, there is nothing for a reader
        // to find: the slow transaction's segment isn't part of any published
        // `SegmentSet` yet. This test is the end-to-end proof that the
        // guarantee holds under exactly the timing that used to expose the
        // race, not just structurally.
        //
        // Unlike `a_failed_commits_vector_is_never_searchable_...` above,
        // this is the *success* path: the slow transaction goes on to
        // commit cleanly. The old graph-residue guard never closed this gap
        // anyway — it only ever handled the permanent-residue case — and is
        // gone now regardless, since W3.2a removed the shared graph it used
        // to compensate for.
        //
        // The window is one fsync wide, so the schedule is made
        // deterministic with `Checkpoint`s rather than raced with sleeps: a
        // sleep-based version would pass vacuously whenever it missed.
        let dir = temp_dir("in-flight-commit-not-visible-to-reader");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        // Seed row-id 0: establishes the graph's dimension and gives the
        // row-id counter a meaningful pre-existing value.
        let mut seed = ds.begin();
        seed.insert(vector_batch(
            vec![1i64],
            cluster_vectors(1, [0.0, 0.0, 0.0], 0.0),
        ))
        .unwrap();
        seed.commit().unwrap();

        let (claim_point, claimed) = checkpoint_pair();
        let (publish_point, ready_to_publish) = checkpoint_pair();

        // The slow transaction: inserts at distinctive, never-reused
        // coordinates so a hit for it is unambiguous.
        let mut slow = ds.begin();
        slow.insert(vector_batch(
            vec![2i64],
            cluster_vectors(1, [900.0, 900.0, 900.0], 0.0),
        ))
        .unwrap();
        slow.pause_after_row_id_claim(claim_point);
        slow.pause_before_manifest_commit(publish_point);
        let slow_thread = std::thread::spawn(move || slow.commit());

        // Step 1: the slow transaction has claimed row-id 1 and written its
        // data files, but holds no lock and has touched nothing shared.
        claimed.wait();

        // Step 2: an unrelated transaction commits. It claims row-id 2 and
        // publishes `manifest.next_row_id = 3` — read from the global
        // counter, which already includes the slow transaction's claim — so
        // under the old `row_id <= watermark` rule its watermark (2) would
        // have covered the slow transaction's uncommitted row-id 1. An
        // insert-only transaction has an empty write-set, so
        // this cannot conflict with the slow one.
        let mut other = ds.begin();
        other
            .insert(vector_batch(
                vec![3i64],
                cluster_vectors(1, [500.0, 500.0, 500.0], 0.0),
            ))
            .unwrap();
        other.commit().unwrap();

        // Step 3: release the slow transaction into `commit_lock` and stop
        // it just before `commit_manifest`. Its `.seg` file is durable but
        // no manifest references it, so -- unlike before W3.2a, when its
        // vector was physically in the shared graph at this instant -- there
        // is nothing for a reader to observe even in principle. The
        // assertion below is now a structural guarantee rather than a race
        // the in-flight registry had to win; it is kept because it is the
        // end-to-end proof that the guarantee actually moved.
        claimed.release();
        ready_to_publish.wait();

        // Step 4: a reader thread reads a fresh snapshot while the slow
        // commit sits paused inside its critical section, just before
        // `commit_manifest`. It takes no `commit_lock`, so it runs freely
        // while the slow commit is parked there.
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
        // than "search is broken" or "this snapshot hides more than the
        // in-flight row". The first is the other direction of the invariant
        // at `Dataset` level:
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
        ready_to_publish.release();
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
    fn a_transaction_whose_history_has_aged_out_of_the_commit_log_reports_retained_range() {
        // Uses TEST_COMMIT_LOG_CAPACITY (not the real, much larger
        // production COMMIT_LOG_CAPACITY) via
        // create_with_commit_log_capacity — see that constant's doc
        // comment for why this is exactly as rigorous a test.
        let dir = temp_dir("commit-log-wraparound-e2e");
        let ds = Dataset::create_with_commit_log_capacity(
            &dir,
            vector_test_schema(),
            TEST_COMMIT_LOG_CAPACITY,
        )
        .unwrap();
        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(batch).unwrap();
        setup.commit().unwrap();

        // txn begins here, before every filler commit below — its
        // base_version stays fixed at whatever ds.current_version()
        // is right now.
        let mut txn = ds.begin();
        txn.delete(0).unwrap();

        // Commit enough disjoint no-op-ish filler transactions to push the
        // CommitLog's oldest retained entry past txn's read-version.
        for i in 0..(TEST_COMMIT_LOG_CAPACITY as i64 + 2) {
            let filler = vector_batch(
                vec![100 + i],
                cluster_vectors(1, [f32::from(i as i16), 0.0, 0.0], 0.0),
            );
            let mut filler_txn = ds.begin();
            filler_txn.insert(filler).unwrap();
            filler_txn.commit().unwrap();
        }

        assert_eq!(
            ds.insufficient_history_conflict_count(),
            0,
            "no InsufficientHistory conflict should have fired yet"
        );

        let result = txn.commit();
        // The disjoint fillers never touched row 0, so this must expose the
        // lost retained-history range rather than inventing a row conflict.
        let Err(TxnError::InsufficientHistory {
            base_version,
            oldest_retained_version,
            latest_version,
        }) = result
        else {
            panic!("expected insufficient history once history aged out, got {result:?}");
        };
        assert_eq!(base_version, 1);
        assert_eq!(oldest_retained_version, 4);
        assert_eq!(latest_version, 11);
        assert_eq!(
            ds.insufficient_history_conflict_count(),
            1,
            "the aged-out commit should have incremented the observability counter exactly once"
        );

        // Same PID-reuse collision risk as
        // `losing_transactions_vectors_never_become_searchable_when_it_conflicts` —
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
        let ds = Dataset::create_with_commit_log_capacity(
            &dir,
            vector_test_schema(),
            TEST_COMMIT_LOG_CAPACITY,
        )
        .unwrap();

        // txn begins here, before every filler commit below — its
        // base_version stays fixed at whatever ds.current_version()
        // is right now.
        let mut txn = ds.begin();
        let insert_only_batch =
            vector_batch(vec![99_999], cluster_vectors(1, [500.0, 500.0, 500.0], 0.0));
        txn.insert(insert_only_batch).unwrap();

        // Commit enough disjoint filler transactions to push the
        // CommitLog's oldest retained entry past txn's read-version.
        for i in 0..(TEST_COMMIT_LOG_CAPACITY as i64 + 2) {
            let filler = vector_batch(
                vec![100 + i],
                cluster_vectors(1, [f32::from(i as i16), 0.0, 0.0], 0.0),
            );
            let mut filler_txn = ds.begin();
            filler_txn.insert(filler).unwrap();
            filler_txn.commit().unwrap();
        }

        let result = txn.commit();
        assert!(
            result.is_ok(),
            "insert-only transactions have an empty write-set and can never \
             conflict, even with aged-out history, but got {result:?}"
        );

        // Same PID-reuse collision risk as
        // `losing_transactions_vectors_never_become_searchable_when_it_conflicts` —
        // see that test's cleanup comment for why this matters.
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_commit_failing_at_the_manifest_step_leaves_an_orphaned_segment_and_nothing_else() {
        // Flavor 1 of base design §5's failed-commit test: a recoverable
        // I/O failure (e.g. ENOSPC) at `commit_manifest`, injected at
        // exactly that step -- after this commit's .seg file is already
        // fsynced. Distinct from
        // `a_failed_commits_vector_is_never_searchable_after_a_later_commit_advances_the_row_id_counter`
        // above, which uses the same injector to regression-test one
        // specific hazard (a dangling search hit surfacing only after a
        // *later* commit advances the row-id counter); this test asserts the
        // full six-point list -- including that the orphan `.seg` file
        // exists on disk and that a reopen reproduces everything -- against
        // the very next observation, with no later commit involved.
        let dir = temp_dir("failed-commit-io-orphan");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let mut seed = ds.begin();
        seed.insert(vector_batch(
            vec![1i64],
            cluster_vectors(1, [0.0, 0.0, 0.0], 0.0),
        ))
        .unwrap();
        seed.commit().unwrap();
        let version_before = ds.snapshot().version;
        let segments_before = ds.snapshot().manifest.segments.len();
        assert_eq!(segments_before, 1, "the seed commit produced one segment");

        let mut failing = ds.begin();
        failing
            .insert(vector_batch(
                vec![2i64],
                cluster_vectors(1, [900.0, 900.0, 900.0], 0.0),
            ))
            .unwrap();
        failing.inject_manifest_commit_failure();
        // (a)
        let result = failing.commit();
        assert!(
            result.is_err(),
            "the injected manifest-commit failure must make this commit fail, \
             else this test proves nothing: {result:?}"
        );

        assert_failed_commit_left_no_trace(
            &dir,
            &ds,
            version_before,
            segments_before,
            &[900.0, 900.0, 900.0],
        );

        // A subsequent commit must still succeed -- a failed commit leaves
        // no state that blocks the next one.
        let mut next = ds.begin();
        next.insert(vector_batch(
            vec![3i64],
            cluster_vectors(1, [500.0, 500.0, 500.0], 0.0),
        ))
        .unwrap();
        next.commit().unwrap();
        assert_eq!(ds.snapshot().manifest.segments.len(), segments_before + 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_conflicting_commit_leaves_an_orphaned_segment_and_nothing_else() {
        // Flavor 2: a typed Conflict. The losing transaction wrote and
        // fsynced its segment in `write_phase`, before the lock, so the
        // orphan exists -- and must never be referenced. Distinct from
        // `losing_transactions_vectors_never_become_searchable_when_it_conflicts`
        // above, which only asserts (a) and (c); this test asserts the full
        // six-point list, including the orphaned `.seg` file's on-disk
        // existence and survival across a reopen.
        let dir = temp_dir("failed-commit-conflict-orphan");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let mut setup = ds.begin();
        setup
            .insert(vector_batch(
                vec![1i64],
                cluster_vectors(1, [0.0, 0.0, 0.0], 0.0),
            ))
            .unwrap();
        setup.commit().unwrap();

        // Both begin from the same snapshot, then commit sequentially, so
        // which one loses is fixed by test order rather than by an explored
        // interleaving -- there is no concurrency to model here, only a
        // specific sequence to regression-test. Both use `update`, since a
        // delete-only transaction inserts nothing and would build no
        // segment at all.
        let mut winner = ds.begin();
        winner
            .update(
                0,
                vector_batch(vec![2i64], cluster_vectors(1, [500.0, 500.0, 500.0], 0.0)),
            )
            .unwrap();
        let mut loser = ds.begin();
        loser
            .update(
                0,
                vector_batch(vec![3i64], cluster_vectors(1, [900.0, 900.0, 900.0], 0.0)),
            )
            .unwrap();

        winner.commit().unwrap();
        let version_before = ds.snapshot().version;
        let segments_before = ds.snapshot().manifest.segments.len();

        // (a)
        let result = loser.commit();
        assert!(
            matches!(result, Err(TxnError::Conflict { .. })),
            "expected the second update to conflict on row 0, got {result:?}"
        );

        assert_failed_commit_left_no_trace(
            &dir,
            &ds,
            version_before,
            segments_before,
            &[900.0, 900.0, 900.0],
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_panic_between_segment_fsync_and_manifest_swap_leaves_an_orphaned_segment_and_nothing_else()
    {
        // Flavor 3: a panic, not an early `?` return. This is the shape
        // that would expose a compensating action wired only into the error
        // path -- and, historically, the shape the old graph-residue
        // guard's `Drop` existed to survive. After W3.2a nothing needs to
        // survive it, because nothing shared was ever touched; this test is
        // what proves that rather than assuming it.
        let dir = temp_dir("failed-commit-panic-orphan");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let mut seed = ds.begin();
        seed.insert(vector_batch(
            vec![1i64],
            cluster_vectors(1, [0.0, 0.0, 0.0], 0.0),
        ))
        .unwrap();
        seed.commit().unwrap();
        let version_before = ds.snapshot().version;
        let segments_before = ds.snapshot().manifest.segments.len();

        // The default panic hook would print a backtrace for a panic this
        // test deliberately causes, which is noise in an otherwise clean
        // run. Suppressed only around the `catch_unwind`, then restored.
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        // (a) -- `Transaction` is not `UnwindSafe` (it holds `Arc`s and an
        // `ArcSwap` handle), and it does not need to be: the panic happens
        // before any shared state is mutated, and the only thing that could
        // observe a torn value -- the manifest -- is never written on this
        // path. `AssertUnwindSafe` records that reasoning explicitly.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut panicking = ds.begin();
            panicking
                .insert(vector_batch(
                    vec![2i64],
                    cluster_vectors(1, [900.0, 900.0, 900.0], 0.0),
                ))
                .unwrap();
            panicking.inject_panic_before_manifest_commit();
            panicking.commit()
        }));
        std::panic::set_hook(previous_hook);
        assert!(
            outcome.is_err(),
            "the injected panic must actually unwind out of commit, else this \
             test proves nothing"
        );

        assert_failed_commit_left_no_trace(
            &dir,
            &ds,
            version_before,
            segments_before,
            &[900.0, 900.0, 900.0],
        );

        // A subsequent commit must still succeed -- the panic must not have
        // left `commit_lock` poisoned in a way that blocks progress. (It
        // does poison it; `commit` recovers a poisoned lock via
        // `PoisonError::into_inner`, and this is what proves that path is
        // still exercised and still correct after the guard went inert.)
        let mut next = ds.begin();
        next.insert(vector_batch(
            vec![3i64],
            cluster_vectors(1, [500.0, 500.0, 500.0], 0.0),
        ))
        .unwrap();
        next.commit().unwrap();
        assert_eq!(ds.snapshot().manifest.segments.len(), segments_before + 1);
        let after = ds
            .snapshot()
            .vector_search(&[500.0, 500.0, 500.0], 1, None)
            .unwrap();
        assert!(
            after.first().is_some_and(|m| m.squared_distance < 0.001),
            "the post-panic commit's vector must be searchable: {after:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn n_vector_carrying_commits_produce_exactly_n_segments_and_all_stay_searchable() {
        // The design doc §4 proof criterion, with amendment §3c's
        // correction applied -- and, because search fans out over every
        // part (this plan's Scope decision), also the end-to-end proof that
        // a row committed in segment 0 is still findable after segment 4
        // lands. Without fan-out this second half fails. Distinct from
        // `reopening_a_dataset_with_multiple_segments_finds_each_segments_own_cluster`,
        // which only checks the *final* segment count and post-reopen
        // fan-out across 3 segments; this test additionally asserts the
        // per-commit invariant (`segments.len() == i + 1`) after each of 5
        // commits, which is the design doc's literal proof criterion, not
        // just a consequence observed at the end.
        let dir = temp_dir("n-commits-n-segments");
        let ds = Dataset::create(&dir, vector_test_schema()).unwrap();

        let centers = [
            [0.0_f32, 0.0, 0.0],
            [1000.0, 0.0, 0.0],
            [0.0, 1000.0, 0.0],
            [0.0, 0.0, 1000.0],
            [1000.0, 1000.0, 1000.0],
        ];
        for (i, center) in centers.iter().enumerate() {
            let mut txn = ds.begin();
            txn.insert(vector_batch(
                vec![i64::try_from(i).unwrap()],
                cluster_vectors(1, *center, 0.0),
            ))
            .unwrap();
            txn.commit().unwrap();
            assert_eq!(
                ds.snapshot().manifest.segments.len(),
                i + 1,
                "one segment per vector-carrying commit"
            );
            assert_eq!(ds.snapshot().index.len(), i + 1);
        }

        for (i, center) in centers.iter().enumerate() {
            let hits = ds.snapshot().vector_search(center, 1, None).unwrap();
            assert_eq!(
                hits.first().map(|m| m.row_id),
                Some(u64::try_from(i).unwrap()),
                "the row committed in segment {i} must still be the nearest match \
                 for its own vector after every later segment landed: {hits:?}"
            );
        }

        // And after a reopen, which loads all five from the manifest.
        let reopened = Dataset::open(&dir).unwrap();
        assert_eq!(reopened.snapshot().index.len(), 5);
        for (i, center) in centers.iter().enumerate() {
            let hits = reopened.snapshot().vector_search(center, 1, None).unwrap();
            assert_eq!(
                hits.first().map(|m| m.row_id),
                Some(u64::try_from(i).unwrap()),
                "segment {i}'s row must survive a reopen: {hits:?}"
            );
        }

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
/// **Research note (Task 7), updated by the Task 10 fix below:** `arc-swap`
/// (resolved to 1.9.2 in `Cargo.lock`) has no documented `loom` integration
/// or feature flag — confirmed against docs.rs/arc-swap/1.9.2, crates.io's
/// listed features (only an optional `serde` feature), and the crate's own
/// upstream `Cargo.toml` (features: `weak`, `internal-test-strategies`,
/// `experimental-strategies`, `experimental-thread-local` — no mention of
/// loom anywhere). `loom` can only explore interleavings of its own
/// instrumented primitives, so it cannot see inside `arc-swap`'s real
/// internal atomics without `arc-swap` itself being loom-aware — the same
/// reason `crates/index`'s earlier loom test (`hnsw.rs`'s
/// `establish_or_check_dimension`) needed a
/// `#[cfg(loom)]`/`#[cfg(not(loom))]` shim swapping in loom's atomic types.
///
/// `one_writer_store_races_safely_with_many_readers_load` below still does
/// **not** instrument the real `Dataset`/`SnapshotCell` type; it models the
/// *shape* of the `Dataset::snapshot()` / `Transaction::commit()` race
/// directly on loom's own `sync::atomic::AtomicUsize` as a fast, minimal
/// baseline sanity check of the swap-then-load pattern in the abstract.
/// It is kept for exactly that reason — it explores the pattern's general
/// safety in isolation from `commit`'s filesystem I/O and its own frame-cost
/// budget.
///
/// The two Dataset-level models below —
/// `a_failed_commits_segment_is_never_visible_to_a_concurrent_reader` and
/// `a_commits_row_and_its_segment_become_visible_as_one_atomic_step` —
/// exist to close the gap that note originally left open: they race a real
/// reader thread (`Dataset::snapshot()`) against a real committer thread
/// (`Transaction::commit()`) on an *actual* `Dataset`, and for loom to see
/// that race at all, `Dataset.current` / `Transaction.current`'s storage
/// (`SnapshotCell` — see its own doc comment above `struct Dataset`) is a
/// `loom::sync::Mutex`-backed shim under `--cfg loom`, following exactly
/// the `#[cfg(loom)]`/`#[cfg(not(loom))]` dual-primitive precedent
/// `row_id.rs` already established, and reverting to the real
/// `arc_swap::ArcSwap` outside it. Without this shim these two models'
/// reader thread performs zero loom-instrumented operations, which is
/// invisible to DPOR: loom's partial-order reduction collapses a thread
/// with no dependent accesses to a single equivalence class, so the
/// documented interleaving windows (the reader's read landing before the
/// committer takes `commit_lock`, between the segment fsync and a failure,
/// or after) were never actually being explored, only replayed once as a
/// single trivial schedule — which is exactly what these two models'
/// suspiciously fast original runtimes (sub-second) were a symptom of.
///
/// **This instrumentation is not free.** Making `SnapshotCell` genuinely
/// loom-visible (so DPOR actually explores the reader-vs-committer
/// interleavings above, instead of collapsing them to one trivial schedule)
/// raised the per-execution cost of every model that touches
/// `Dataset.current` by roughly 30x. Running the whole `loom_tests` module
/// as a single test binary invocation can now exceed ten minutes and, on
/// Windows, can fail with an `ERROR_NO_SYSTEM_RESOURCES` OS error when all
/// ~9 models run in the same process together -- this is an environmental
/// resource-exhaustion symptom, not a correctness failure (each model has
/// been confirmed to pass individually). On Windows especially, prefer
/// running one model at a time: build per the `Run with` instructions above,
/// then invoke the resulting binary with a single test's full path and
/// `--exact` (e.g. `dataset::loom_tests::a_commits_row_and_its_segment_become_visible_as_one_atomic_step
/// --exact`) rather than filtering to the whole `dataset::loom_tests` module
/// in one run.
///
/// **Model 3**, below
/// (`a_reader_never_sees_one_in_flight_commits_row_while_observing_an_unrelated_commits_row_id_counter`),
/// is the regression gate for deleting `RowIdAllocator.active` / `in_flight`
/// / collapsing `Snapshot::is_visible` to the tombstone check — see
/// `docs/design.md`. It was added
/// here FIRST, against the then-current watermark+in-flight implementation,
/// as the "before" half of the required "must pass both before and after the
/// deletion" proof. The deletion has since landed, and this exact test was
/// re-run completely unmodified afterward and passed — the "after" half.
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
    /// exposes no way to change it. So the rule for these models is:
    /// `Dataset::create` and `Transaction::commit` run on sized threads
    /// spawned through [`spawn_committer`]; root models may still take
    /// snapshots, run vector searches, and make assertions.
    ///
    /// Assertions are an empirical boundary, not a safe one. The root can
    /// still run `Snapshot::vector_search` (HNSW candidate heaps at
    /// `EF_SEARCH_DEFAULT`) — the same *class* of work that just overran 32
    /// KiB, only smaller. That work is the next suspect if a model here
    /// ever exits 139 again.
    ///
    /// **Spawning is not free: loom caps threads at 5 *created* per
    /// execution** (`loom::MAX_THREADS`), and terminated threads never free
    /// their slot — `rt::thread::new_thread` asserts against the total ever
    /// created. The cap is not raisable either; it sizes fixed-length arrays
    /// inside loom (`FirstSeen([u16; MAX_THREADS])`), so a larger
    /// `model::Builder::max_threads` indexes out of bounds. The cap is
    /// per `#[test]` (each `loom::model` closure gets its own count, not a
    /// crate-wide total), but every commit-running model here still has to
    /// budget against it independently: the two `two_threads_deleting_*`
    /// models sit at the cap, 5 of 5 (root + create + setup + 2 racing
    /// threads);
    /// `a_failed_commits_segment_is_never_searchable_under_concurrent_commits`
    /// sits at the cap itself, 5 of 5 (root + combined create/seed + failing
    /// + succeeding + final);
    /// `concurrent_first_vector_commits_at_different_dimensions_are_not_both_accepted`
    /// sits at 4 of 5 (root + create + the two racing committers);
    /// `a_failed_commits_segment_is_never_visible_to_a_concurrent_reader` sits
    /// at 4 of 5 (root + combined create/seed + failing + a reader racing it directly, rather
    /// than another committer — the seed was added by Task 10 to make its
    /// search assertion non-vacuous, see that test's own doc comment);
    /// `a_commits_row_and_its_segment_become_visible_as_one_atomic_step` sits
    /// at 4 of 5 (root + create + a committer + a reader racing it directly);
    /// `a_reader_never_sees_one_in_flight_commits_row_while_observing_an_unrelated_commits_row_id_counter`
    /// ("Model 3") sits at the cap, 5 of 5 (root + create + committer_a +
    /// committer_b + reader).
    /// One more `spawn_committer` in any model already at the cap trips an
    /// assert inside loom, so a commit that only needs the stack — not the
    /// concurrency — still costs a hard-capped slot.
    ///
    /// **Exactly one model is preemption-bounded rather than run
    /// exhaustively:** Model 3 above races two full `Transaction::commit`s
    /// across 4 threads and, unbounded, exhausts the machine's commit charge
    /// before it finishes; it runs through `loom::model::Builder` with
    /// `preemption_bound = Some(3)` instead. Every other model here runs
    /// unbounded through `loom::model(...)` and stays that way — this is a
    /// scoped exception, not a precedent for a new model. See that test's own
    /// comment for the measurements behind the bound.
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

    /// The ordinary unit-test fixture is compiled only with `cfg(test)`,
    /// whereas loom models are built with `--cfg loom`. Keep their schema
    /// fixture local so the loom test binary remains self-contained.
    fn test_schema() -> StdArc<arrow::datatypes::Schema> {
        StdArc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]))
    }

    fn loom_vector_schema() -> StdArc<arrow::datatypes::Schema> {
        StdArc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new(
                "vector",
                arrow::datatypes::DataType::FixedSizeList(
                    StdArc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Float32,
                        false,
                    )),
                    3,
                ),
                true,
            ),
        ]))
    }

    /// Creates a dataset on a sized loom thread: `Dataset::create` performs
    /// the same serialization, filesystem, and index setup work that overflows
    /// loom's root coroutine stack.
    fn create_dataset(
        dir: std::path::PathBuf,
        schema: StdArc<arrow::datatypes::Schema>,
    ) -> crate::Dataset {
        spawn_committer(move || crate::Dataset::create(dir, schema))
            .join()
            .expect("loom dataset creation thread panicked")
            .expect("loom dataset creation failed")
    }

    /// Creates the dataset and installs the seed in one sized thread so the
    /// seeded models retain their existing five-thread loom budget.
    fn create_seeded_vector_dataset(dir: std::path::PathBuf) -> crate::Dataset {
        spawn_committer(move || {
            let ds = crate::Dataset::create(dir, loom_vector_schema()).unwrap();
            let mut seed = ds.begin();
            seed.insert(loom_vector_batch(0, &[0.0, 0.0, 0.0])).unwrap();
            seed.commit().unwrap();
            ds
        })
        .join()
        .expect("loom dataset creation and seed thread panicked")
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
            let ds = create_dataset(dir.clone(), test_schema());
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
                setup.insert(batch).unwrap();
                setup.commit()
            })
            .join()
            .unwrap()
            .unwrap();

            let ds_a = ds.clone();
            let ds_b = ds.clone();

            // Both transactions capture the same base snapshot before their
            // commits are spawned. Under the current write-write contract,
            // one commit succeeds and the other reports a typed conflict.
            let mut txn_a = ds_a.begin();
            txn_a.delete(0).unwrap();
            let mut txn_b = ds_b.begin();
            txn_b.delete(0).unwrap();

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
            let ds = create_dataset(dir.clone(), test_schema());
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
                setup.insert(batch).unwrap();
                setup.commit()
            })
            .join()
            .unwrap()
            .unwrap();

            let ds_a = ds.clone();
            let ds_b = ds.clone();
            let thread_a = spawn_committer(move || {
                let mut txn = ds_a.begin();
                txn.delete(0).unwrap();
                txn.commit()
            });
            let thread_b = spawn_committer(move || {
                let mut txn = ds_b.begin();
                txn.delete(1).unwrap();
                txn.commit()
            });

            assert!(thread_a.join().unwrap().is_ok());
            assert!(thread_b.join().unwrap().is_ok());

            std::fs::remove_dir_all(&dir).ok();
        });
    }

    /// One row, one vector of `vector.len()` dimensions, in the shape
    /// `build_vector_inserts` expects. Dimension is inferred from the
    /// slice rather than fixed, so this same helper serves both the
    /// 3-dimensional residue model below and the dimension-race model,
    /// which needs two different dimensions in the same run. Defined
    /// locally rather than reusing `dataset::tests`' `vector_batch` (fixed
    /// at 3 dimensions and not `pub(crate)`) so this module stays
    /// compilable under `--cfg loom` regardless of whether `cfg(test)` is
    /// also set for that build.
    fn loom_vector_batch(id: i64, vector: &[f32]) -> arrow::array::RecordBatch {
        let dim = i32::try_from(vector.len()).unwrap();
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
                arrow::datatypes::DataType::FixedSizeList(item(), dim),
                true,
            ),
        ]));
        let ids = StdArc::new(arrow::array::Int64Array::from(vec![id]));
        let values = StdArc::new(arrow::array::Float32Array::from(vector.to_vec()));
        let vectors = StdArc::new(arrow::array::FixedSizeListArray::new(
            item(),
            dim,
            values,
            None,
        ));
        arrow::array::RecordBatch::try_new(schema, vec![ids, vectors]).unwrap()
    }

    /// One row with an owned-schema nullable `vector` column containing a
    /// null value, so `build_vector_inserts` yields no entries. Validation
    /// passes while the commit does zero HNSW work and still claims a row-id.
    /// Used for the committers whose only job here is to move the counter,
    /// keeping the loom model small enough to explore.
    fn loom_plain_batch(id: i64) -> arrow::array::RecordBatch {
        let item = StdArc::new(arrow::datatypes::Field::new(
            "item",
            arrow::datatypes::DataType::Float32,
            false,
        ));
        let vectors = StdArc::new(arrow::array::FixedSizeListArray::new(
            item,
            3,
            StdArc::new(arrow::array::Float32Array::from(vec![0.0, 0.0, 0.0])),
            Some(arrow::buffer::NullBuffer::from(vec![false])),
        ));
        arrow::array::RecordBatch::try_new(
            loom_vector_schema(),
            vec![
                StdArc::new(arrow::array::Int64Array::from(vec![id])),
                vectors,
            ],
        )
        .unwrap()
    }

    #[test]
    fn a_failed_commits_segment_is_never_searchable_under_concurrent_commits() {
        // The interleaving counterpart to
        // `dataset::tests::a_failed_commits_vector_is_never_searchable_after_a_later_commit_advances_the_row_id_counter`.
        // That test fixes the order (fail, then commit); this one lets loom
        // explore every order in which a *failing* committer and a
        // *succeeding* committer reach and release the commit lock.
        //
        // **Why this needs a seed commit.** Without a vector-carrying commit
        // that lands before the racing pair, the snapshot's `SegmentSet` has
        // zero parts in every interleaving, and `vector_search` returns
        // `Ok(vec![])` unconditionally regardless of whether the code under
        // test is correct — the core assertion could never fail no matter
        // how badly a regression broke this path. The seed commit below
        // gives the assertion something to discriminate: "the failed
        // commit's distinctive vector is absent" (correct) versus "search
        // returns nothing because there is nothing to search" (a vacuous
        // pass), mirroring how the non-loom sibling test seeds a real row
        // before checking the failed commit's vector is unreachable.
        //
        // The property under test is a quiescent one: once both committers
        // have returned, no schedule leaves the failed commit's vector
        // reachable by a search, and the manifest's segment list holds
        // exactly the seed's segment — no orphaned or duplicate entry from
        // either racing committer, under any interleaving. Since S1 W3.2a
        // this holds *structurally*, not via the old graph-residue guard
        // (removed in W3.2b once this guarantee took over): a commit's
        // segment is built and fsynced entirely in `write_phase`, outside
        // `commit_lock`, and is only ever appended to a `SegmentSet` by the
        // in-lock `manifest.segments.push`/`with_appended` pair that runs
        // strictly after `commit_manifest` succeeds. A commit that fails
        // before reaching that point — whichever of the two committers below
        // that turns out to be, under whichever interleaving loom explores —
        // therefore has no path by which its segment could ever be
        // referenced, let alone searched. What this model actually pins
        // down is that this holds under every interleaving of the two
        // committers, not just the one order the deterministic sibling test
        // fixes.
        //
        // The complementary *transient* property — that a row is never
        // visible before its commit succeeds, whether or not that commit
        // eventually does — is now structural (a snapshot's `SegmentSet` is
        // exactly its own manifest's segment list, so an uncommitted
        // transaction's row/segment can never appear in an already-published
        // snapshot regardless of what its row-id counter numerically covers), and
        // is covered by
        // `a_reader_never_sees_one_in_flight_commits_row_while_observing_an_unrelated_commits_row_id_counter`
        // below (interleavings) and
        // `dataset::tests::a_concurrent_reader_never_sees_an_in_flight_commits_vector`
        // (a real reader thread racing the slow commit's still-open critical
        // section end-to-end).
        //
        // Deliberately minimal otherwise: only the failing transaction
        // inserts a vector beyond the seed (one HNSW node), and the
        // concurrent committer uses a vector-free batch. `loom::model`
        // re-runs this closure once per interleaving, and every run does
        // real filesystem I/O, so keeping per-run work down is what makes
        // the model tractable.
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
            let ds = create_seeded_vector_dataset(dir.clone());

            // Seed: one durable, vector-carrying commit, run to completion
            // *before* the racing pair below — see this test's doc comment
            // The helper performs creation and this commit on one sized
            // thread before the racing pair, preserving the five-thread
            // budget without changing the schedule below.

            let ds_failing = ds.clone();
            let ds_ok = ds.clone();

            let failing = spawn_committer(move || {
                let mut txn = ds_failing.begin();
                txn.insert(loom_vector_batch(1, &[900.0, 900.0, 900.0]))
                    .unwrap();
                txn.inject_manifest_commit_failure();
                txn.commit()
            });
            let succeeding = spawn_committer(move || {
                let mut txn = ds_ok.begin();
                txn.insert(loom_plain_batch(2)).unwrap();
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

            // Exercises the row-id allocator's continued progress after a
            // failed commit, under every interleaving -- the failed
            // commit's claimed row-id is never recycled (spec §8), so this
            // commit must still succeed and claim the next one. Unlike the
            // pre-S1 watermark design, the vector_search assertion below no
            // longer depends on this: non-vacuousness now comes structurally
            // from the segment-list invariant (see this test's doc comment
            // above), not from a watermark numerically advancing past the
            // failed commit's row-id.
            //
            // Spawned rather than run inline for the stack, not the
            // concurrency: the root thread's 32 KiB cannot hold a `commit`
            // (see `COMMIT_STACK_SIZE`). It is joined immediately, so the
            // schedule is unaffected.
            let ds_final = ds.clone();
            spawn_committer(move || {
                let mut final_txn = ds_final.begin();
                final_txn.insert(loom_plain_batch(3)).unwrap();
                final_txn.commit()
            })
            .join()
            .unwrap()
            .unwrap();

            let snapshot = ds.snapshot();

            let results = snapshot
                .vector_search(&[900.0, 900.0, 900.0], 1, None)
                .unwrap();
            // Not `results.is_empty()`: with the seed present, a k=1 search
            // always returns *some* match (the seed, being the only node in
            // the index) — the discriminator is that it's the far-away
            // seed, not the failed commit's own near-zero-distance vector.
            assert!(
                results.is_empty() || results[0].squared_distance > 1000.0,
                "the failed commit's vector must never be searchable under any \
                 interleaving: {results:?}"
            );

            // Positive control: the seed's own vector must still be
            // findable, so the empty result above means "excluded", not
            // "search is broken" or "nothing has ever been indexed".
            let seed_hit = snapshot.vector_search(&[0.0, 0.0, 0.0], 1, None).unwrap();
            assert_eq!(
                seed_hit.first().map(|m| m.row_id),
                Some(0),
                "the seed commit's row must still be searchable: {seed_hit:?}"
            );

            // The structural property a leaked failed-commit segment would
            // actually violate: exactly one segment — the seed's — no
            // matter which of the two racing committers reached
            // `commit_lock` first, or in which order they released it. Both
            // `succeeding` and the final committer above are vector-free
            // and therefore publish no segment of their own (see
            // `build_and_write_segment`'s doc comment on why a vector-less
            // commit writes none).
            assert_eq!(
                snapshot.manifest.segments.len(),
                1,
                "exactly one vector-carrying commit (the seed) ever succeeded; the \
                 manifest must list exactly one segment, under every interleaving: {:?}",
                snapshot.manifest.segments
            );
            assert_eq!(
                snapshot.index.len(),
                1,
                "the snapshot's segment set must stay in lockstep with the manifest"
            );

            std::fs::remove_dir_all(&dir).ok();
        });
    }

    #[test]
    fn concurrent_first_vector_commits_at_different_dimensions_are_not_both_accepted() {
        // The model name is retained for CI compatibility. ARCH-01 rejects
        // the non-owned 5D batch during `insert`, before index work or
        // `commit_lock`; this model verifies typed schema rejection and 3D
        // publication, not an index dimension race.
        //
        // Budget: root + create + 2 committers = 4 of loom's
        // 5-created-threads-per-execution cap — see [`spawn_committer`]'s
        loom::model(|| {
            let dir = tempfile::Builder::new()
                .prefix(&format!(
                    "strata-loom-dimension-race-{}-{:?}-",
                    std::process::id(),
                    loom::thread::current().id()
                ))
                .tempdir()
                .unwrap()
                .keep();
            let ds = create_dataset(dir.clone(), loom_vector_schema());
            assert_eq!(
                ds.snapshot().index.established_dimension(),
                0,
                "precondition: a fresh dataset has no established dimension"
            );

            let ds_3d = ds.clone();
            let ds_5d = ds.clone();

            // The valid 3D transaction commits normally. The 5D transaction
            // reaches `insert` concurrently but returns before `commit`,
            // because schema validation rejects its non-owned batch.
            let three_d = spawn_committer(move || {
                let mut txn = ds_3d.begin();
                txn.insert(loom_vector_batch(0, &[1.0, 0.0, 0.0]))?;
                txn.commit()
            });
            let five_d = spawn_committer(move || {
                let mut txn = ds_5d.begin();
                txn.insert(loom_vector_batch(1, &[9.0, 9.0, 9.0, 9.0, 9.0]))?;
                txn.commit()
            });

            let result_3d = three_d.join().unwrap();
            let result_5d = five_d.join().unwrap();

            assert!(
                result_3d.is_ok(),
                "the owned 3-dimensional vector schema must be publishable: \
                 3d={result_3d:?}"
            );
            assert!(
                matches!(result_5d, Err(crate::TxnError::BatchSchemaMismatch { .. })),
                "the non-owned 5-dimensional batch must be rejected before an index \
                 dimension race: 5d={result_5d:?}"
            );

            // Only the owned 3D batch can publish: exactly one segment has
            // the owned schema's established dimension.
            let snapshot = ds.snapshot();
            assert_eq!(
                snapshot.manifest.segments.len(),
                1,
                "only the owned 3D batch may publish a segment: {:?}",
                snapshot.manifest.segments
            );
            assert_eq!(
                snapshot.index.len(),
                1,
                "the snapshot's segment set must stay in lockstep with the manifest"
            );
            assert_eq!(
                snapshot.index.established_dimension(),
                3,
                "the published segment must retain the owned 3D schema's dimension: \
                 3d={result_3d:?}, 5d={result_5d:?}"
            );

            std::fs::remove_dir_all(&dir).ok();
        });
    }

    #[test]
    fn a_failed_commits_segment_is_never_visible_to_a_concurrent_reader() {
        // Base design §5, loom Model 1 -- "failed commit is invisible."
        //
        // **Fixed by Task 10.** Review found that, before `SnapshotCell`
        // (see its doc comment above `struct Dataset`, and the module doc
        // comment above `mod loom_tests`), the reader thread below performed
        // zero loom-instrumented operations: `Dataset::snapshot()` was a
        // bare `arc_swap::ArcSwap::load_full` call, invisible to DPOR. With
        // only one committer thread and nothing loom-instrumented on the
        // reader side to create a race, loom collapsed this model to a
        // single trivial schedule instead of exploring the windows this
        // test's name promises -- which is exactly why its original runtime
        // was suspiciously sub-second. `Dataset.current` now routes through
        // `SnapshotCell`, a `loom::sync::Mutex` shim under `--cfg loom`, so
        // B's `Dataset::snapshot()` and A's own `self.current.load()` /
        // `load_full()` calls are all dependent accesses DPOR can actually
        // branch on. `EXECUTIONS_EXPLORED` below is the honest check that it
        // now does.
        //
        // Thread A (`failing`) commits with `inject_manifest_commit_failure`:
        // it claims row-ids, builds and fsyncs its segment, then returns Err
        // before the manifest swap -- it never reaches `SnapshotCell::store`.
        // Thread B (`reader`) takes a snapshot and searches concurrently.
        // Under every interleaving loom now genuinely explores of B's
        // `SnapshotCell::load_full()` landing (i) before A takes
        // `commit_lock`, (ii) between A's segment fsync and A's Err, or
        // (iii) after A's Err/return, B must observe neither A's row-id nor
        // A's segment file.
        //
        // **Seed commit, added by Task 10.** Review also found the original
        // `hits.is_empty()` assertion vacuous: with no vector-carrying
        // commit landing before the racing pair, B's snapshot has zero
        // segments under every interleaving and `vector_search` returns
        // `Ok(vec![])` unconditionally, regardless of whether the code under
        // test is correct. The seed below (one durable, far-away vector,
        // run to completion first) gives the assertions something to
        // discriminate -- "only the seed's vector is ever findable" versus
        // "search finds nothing because there is nothing to search" --
        // exactly mirroring the seed
        // `a_failed_commits_segment_is_never_searchable_under_concurrent_commits`
        // above already uses for the same reason.
        //
        // **What this model proves, and what it can't.** The property here
        // is *invariance*: B's observed (version, segment count, segment
        // names, hits) is the same value under every interleaving loom
        // explores, because A's failure path never reaches
        // `SnapshotCell::store` at all -- there is no intermediate state to
        // observe, by construction. That is the whole point (unlike Model 2
        // below, whose two valid outcomes differ), but it also means the
        // result itself can't demonstrate multiple schedules were explored.
        // `EXECUTIONS_EXPLORED` is a plain (non-loom) counter, declared
        // outside the modelled closure so it survives across every
        // interleaving `loom::model` runs -- loom only resets state created
        // *inside* the closure, not a `static` sitting outside it -- and
        // asserting it ends up `> 1` afterward is a direct, honest signal
        // that DPOR did not again collapse this model to the single trivial
        // schedule the pre-fix version was actually running.
        //
        // This is the genuinely new interleaving space relative to
        // `a_failed_commits_segment_is_never_searchable_under_concurrent_commits`
        // above: that model races two *committers* against each other and
        // only reads afterward (sequentially, once both have joined). This
        // one races a *reader* directly against the failing committer's own
        // execution, so loom explores schedules where the reader's
        // `snapshot()` lands mid-commit -- something the committer-vs-
        // committer model never exercises.
        //
        // Deliberately minimal per §5's flagged risk: one seed row plus one
        // row from the failing commit, dim 3. Budget: root + combined
        // create/seed + failing + reader = 4 of loom's
        // 5-created-threads-per-execution cap.
        static EXECUTIONS_EXPLORED: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);

        loom::model(|| {
            EXECUTIONS_EXPLORED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let dir = tempfile::Builder::new()
                .prefix(&format!(
                    "strata-loom-failed-segment-{}-{:?}-",
                    std::process::id(),
                    loom::thread::current().id()
                ))
                .tempdir()
                .unwrap()
                .keep();
            let ds = create_seeded_vector_dataset(dir.clone());

            // Seed: one durable, vector-carrying commit, run to completion
            // *before* the racing pair below -- see this test's doc comment
            // The helper performs creation and this commit on one sized
            // thread before the racing pair, preserving the five-thread
            // budget without changing the schedule below.

            let version_before = ds.snapshot().version;

            let ds_failing = ds.clone();
            let failing = spawn_committer(move || {
                let mut txn = ds_failing.begin();
                txn.insert(loom_vector_batch(1, &[900.0, 900.0, 900.0]))
                    .unwrap();
                txn.inject_manifest_commit_failure();
                txn.commit()
            });

            let ds_reader = ds.clone();
            let reader = spawn_committer(move || {
                let snapshot = ds_reader.snapshot();
                let hits = snapshot
                    .vector_search(&[900.0, 900.0, 900.0], 1, None)
                    .unwrap();
                let segment_names: Vec<String> = snapshot
                    .manifest
                    .segments
                    .iter()
                    .map(|s| s.name.clone())
                    .collect();
                (snapshot.version, snapshot.index.len(), hits, segment_names)
            });

            assert!(
                failing.join().unwrap().is_err(),
                "the injected manifest-commit failure must make this commit fail"
            );
            let (observed_version, observed_parts, hits, segment_names) = reader.join().unwrap();

            assert_eq!(
                observed_version, version_before,
                "no snapshot may exist at a version the failed commit never produced"
            );
            assert_eq!(
                segment_names,
                vec![ds.snapshot().manifest.segments[0].name.clone()],
                "a reader must observe exactly the seed's segment, and never the \
                 failed commit's, under any interleaving: {segment_names:?}"
            );
            assert_eq!(
                observed_parts, 1,
                "the observed snapshot's segment set must match its manifest's \
                 seed-only segment list"
            );
            assert_eq!(
                hits.len(),
                1,
                "the seed's vector is the only one ever reachable, under any \
                 interleaving: {hits:?}"
            );
            assert!(
                hits[0].squared_distance > 1000.0,
                "the only searchable vector must be the far-away seed, never the \
                 failed commit's near-zero-distance one: {hits:?}"
            );

            // The root thread's own post-join view, which is the quiescent
            // half of the property.
            assert_eq!(
                ds.snapshot().version,
                version_before,
                "a failed commit must not advance the visible version"
            );
            assert_eq!(
                ds.snapshot().manifest.segments.len(),
                1,
                "only the seed's segment may ever be listed"
            );

            std::fs::remove_dir_all(&dir).ok();
        });

        let executions = EXECUTIONS_EXPLORED.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            executions > 1,
            "loom explored only {executions} execution(s) of this model -- with \
             `SnapshotCell` genuinely instrumented, the reader's read and the \
             failing committer's own current-cell accesses should force DPOR \
             to branch over more than the single trivial schedule this count \
             would otherwise indicate (see the module doc comment above `mod \
             loom_tests`)"
        );
    }

    #[test]
    fn a_commits_row_and_its_segment_become_visible_as_one_atomic_step() {
        // Base design §5, loom Model 2 -- "row + segment publish
        // atomically."
        //
        // **Fixed by Task 10.** Same underlying gap as Model 1 above: before
        // `SnapshotCell` (see the module doc comment above `mod loom_tests`
        // and `struct SnapshotCell`'s own doc comment), B's
        // `Dataset::snapshot()` was a bare `arc_swap::ArcSwap::load_full`
        // call, invisible to loom's DPOR scheduler. That means the
        // `version == 0` branch below -- B's read landing *before* A's
        // `SnapshotCell::store` -- was never actually reachable under
        // exploration; only `version == 1` (B scheduled strictly after A
        // completed) ever ran, silently, and the model's sub-second runtime
        // was the tell. `DISTINCT_VERSIONS_OBSERVED` below is the honest
        // check that both branches are now genuinely hit.
        //
        // A commits successfully; B snapshots and then reads THAT SAME
        // snapshot's data-file count (a proxy for row presence -- no real
        // `scan` is issued here) and also runs a real `vector_search`
        // against it. B must observe either the complete pre-commit state or
        // the complete post-commit state -- never A's row present under the
        // old manifest version, and never the version bumped with A's
        // segment absent.
        //
        // This is close to trivially true once both live in one `Manifest`
        // published by a single atomic swap, but it is the entire
        // justification for deleting the old guard/registry machinery, so
        // §5 requires it be proven rather than assumed. Like the model
        // above, this races a reader directly against a committer's own
        // execution rather than against another committer -- the
        // interleaving space neither
        // `a_failed_commits_segment_is_never_searchable_under_concurrent_commits`
        // nor
        // `concurrent_first_vector_commits_at_different_dimensions_are_not_both_accepted`
        // covers.
        //
        // B reads one snapshot and derives every assertion from it: taking
        // two snapshots would let a commit land in between and make the
        // test assert nothing.
        //
        // `DISTINCT_VERSIONS_OBSERVED` is a plain (non-loom) bitmask,
        // declared outside the modelled closure so it accumulates across
        // every interleaving `loom::model` explores -- loom only resets
        // state created *inside* the closure, not a `static` sitting outside
        // it. Bit 0 is set the first time some interleaving lands B on the
        // `version == 0` branch, bit 1 the first time one lands on
        // `version == 1`; asserting both bits are set after `loom::model`
        // returns is a direct, honest proof that DPOR is genuinely
        // exercising B's read on both sides of A's atomic publish, not just
        // replaying whichever side happens to run first.
        //
        // Budget: root + create + writer + reader = 4 of loom's
        // 5-created-threads-per-execution cap.
        static DISTINCT_VERSIONS_OBSERVED: std::sync::atomic::AtomicU8 =
            std::sync::atomic::AtomicU8::new(0);

        loom::model(|| {
            let dir = tempfile::Builder::new()
                .prefix(&format!(
                    "strata-loom-atomic-publish-{}-{:?}-",
                    std::process::id(),
                    loom::thread::current().id()
                ))
                .tempdir()
                .unwrap()
                .keep();
            let ds = create_dataset(dir.clone(), loom_vector_schema());

            let ds_writer = ds.clone();
            let writer = spawn_committer(move || {
                let mut txn = ds_writer.begin();
                txn.insert(loom_vector_batch(1, &[900.0, 900.0, 900.0]))
                    .unwrap();
                txn.commit()
            });

            let ds_reader = ds.clone();
            let reader = spawn_committer(move || {
                let snapshot = ds_reader.snapshot();
                let hits = snapshot
                    .vector_search(&[900.0, 900.0, 900.0], 1, None)
                    .unwrap();
                (
                    snapshot.version,
                    snapshot.manifest.data_files.len(),
                    snapshot.manifest.segments.len(),
                    snapshot.index.len(),
                    hits.len(),
                )
            });

            writer.join().unwrap().unwrap();
            let (version, data_files, segments, parts, hit_count) = reader.join().unwrap();

            // The in-memory segment set and the manifest's segment list are
            // the two halves that must never disagree, in any observed
            // state.
            assert_eq!(
                parts, segments,
                "a snapshot's segment set must always equal its manifest's segment \
                 list -- observed {parts} parts against {segments} entries at \
                 version {version}"
            );

            match version {
                0 => {
                    DISTINCT_VERSIONS_OBSERVED.fetch_or(0b01, std::sync::atomic::Ordering::Relaxed);
                    assert_eq!(data_files, 0, "the pre-commit state has no data file");
                    assert_eq!(segments, 0, "...and no segment");
                    assert_eq!(hit_count, 0, "...and nothing to find");
                }
                1 => {
                    DISTINCT_VERSIONS_OBSERVED.fetch_or(0b10, std::sync::atomic::Ordering::Relaxed);
                    assert_eq!(data_files, 1, "the post-commit state has A's data file");
                    assert_eq!(segments, 1, "...and A's segment");
                    assert_eq!(
                        hit_count, 1,
                        "...and A's row is findable in it -- a version bump with \
                         the segment absent, or present but unsearchable, is the \
                         partial state this model rules out"
                    );
                }
                other => panic!("no interleaving may produce version {other}"),
            }

            std::fs::remove_dir_all(&dir).ok();
        });

        let observed = DISTINCT_VERSIONS_OBSERVED.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            observed, 0b11,
            "loom must observe the reader landing on both sides of the \
             committer's atomic publish -- version bitmask {observed:#04b}, \
             expected 0b11 (both version 0 and version 1 reachable). Seeing \
             only one side means DPOR collapsed this model to a single \
             trivial schedule again (see the module doc comment above `mod \
             loom_tests`)"
        );
    }

    #[test]
    fn a_reader_never_sees_one_in_flight_commits_row_while_observing_an_unrelated_commits_row_id_counter()
     {
        // Base design §6 ("The snapshot-isolation simplification (benefit +
        // hazard)") / this file's own note above `mod loom_tests` --
        // "Model 3", the regression gate for deleting
        // `RowIdAllocator.active`/`in_flight` and collapsing
        // `Snapshot::is_visible` to the tombstone check.
        //
        // The specific hazard `crate::row_id`'s module doc names, now
        // closed: row-ids are claimed *before* `commit_lock`, from a
        // single *global* counter. Two concurrent, non-conflicting
        // (disjoint-row, insert-only) commits can therefore claim in
        // either order but publish (commit) in either order too. Under
        // the OLD shared-mutable-HNSW-graph design, if transaction A
        // claimed a row-id and was still inside `commit()` when unrelated
        // transaction B claimed a later row-id and fully committed, B's
        // published watermark was read from the SAME global counter A had
        // already advanced by claiming -- so B's watermark numerically
        // covered A's row-id, even though A had not committed, which is
        // exactly why that design needed the in-flight exclusion set this
        // model regression-gates the removal of.
        //
        // What actually prevents a reader from finding A's row in that
        // situation today (proven here, not assumed): B's published
        // snapshot's `SegmentSet`/`manifest.data_files` are built from B's
        // OWN commit only. A's segment/data file are never added to them,
        // regardless of what B's row-id counter numerically covers --
        // there is no shared, eagerly-mutated structure for A's in-flight
        // write to leak into. So a reader's `vector_search` hit for a
        // vector and that same row's presence in the SAME snapshot's
        // `scan()` must always agree, for every writer, independent of
        // which row-id-counter value happens to be current at the moment
        // the reader's snapshot was taken.
        //
        // A commits id=1 at [900,900,900]; B commits id=2 at [100,100,100]
        // -- disjoint rows, so both always succeed regardless of
        // interleaving, and a hit for either is unambiguous.
        //
        // Budget: root + create + committer_a + committer_b + reader = 5 of
        // loom's 5-created-threads-per-execution cap.
        //
        // **Preemption-bounded at 3, deliberately not exhaustive -- a
        // scoped exception for this one model.** Every other model in this
        // module runs unbounded through `loom::model(...)` and stays that
        // way; this is not a signal to bound them. An unbounded run of
        // *this* model explores for ~22 minutes and then dies inside
        // `generator`'s stack allocator with Windows OS error 1455,
        // `ERROR_COMMITMENT_LIMIT` ("Il file di paging e' troppo piccolo
        // per essere completato" -- the paging file is too small to
        // complete the operation): it runs the machine out of commit
        // charge, it does not fail an assertion. That is the same class of
        // environmental exhaustion the module doc comment above already
        // records for running all ~9 models in one process, reached here
        // by a single model -- this one creates 4 threads per execution
        // and races two full `Transaction::commit`s where Model 2 above
        // creates 3 and races one. Measured cost per preemption level was
        // ~10x: bound 1 = 1.3s, bound 2 = 21s, bound 3 = 211s, unbounded =
        // 1364s and then the crash.
        //
        // Bounding costs little here because the schedule this model
        // targets is coarse-grained: one transaction pausing for a long
        // stretch, anywhere inside `commit()`, while an unrelated one runs
        // to completion. Expressing that needs few preemptions, and
        // `OBSERVED` below is the evidence rather than the assumption.
        // Measured per bound: bound 1 reaches only 0b1011 -- it never
        // schedules B ahead of A, so the hazard-adjacent "only B committed
        // while A is still in flight" state goes unexplored; bound 2 is the
        // minimum that reaches all four (0b1111). 3 is therefore one level
        // of genuine margin over the minimum sufficient depth, not a bound
        // tuned down until the test passed. (Bound 1's shortfall is also
        // why `OBSERVED` distinguishes the two single-committed directions
        // instead of folding them into one bit: folded, bound 1 would have
        // looked fully covered while missing the exact schedule this model
        // exists to check.)
        //
        // `Builder::new()` seeds `preemption_bound` from
        // `LOOM_MAX_PREEMPTIONS`; assigning it after overrides that, so
        // this gate explores the same space regardless of the environment.
        static OBSERVED: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

        // Built once, outside the model: the reader only needs the projection
        // schema, and constructing a whole throwaway `RecordBatch` (two Arrow
        // arrays plus a `FixedSizeListArray`) per explored execution just to
        // call `.schema()` on it is pure overhead. `SchemaRef` is an
        // `Arc<Schema>`, so cloning it into the reader thread is free.
        let schema = loom_vector_schema();

        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(move || {
            let dir = tempfile::Builder::new()
                .prefix(&format!(
                    "strata-loom-model-3-{}-{:?}-",
                    std::process::id(),
                    loom::thread::current().id()
                ))
                .tempdir()
                .unwrap()
                .keep();
            let ds = create_dataset(dir.clone(), loom_vector_schema());

            let ds_a = ds.clone();
            let committer_a = spawn_committer(move || {
                let mut txn = ds_a.begin();
                txn.insert(loom_vector_batch(1, &[900.0, 900.0, 900.0]))
                    .unwrap();
                txn.commit()
            });

            let ds_b = ds.clone();
            let committer_b = spawn_committer(move || {
                let mut txn = ds_b.begin();
                txn.insert(loom_vector_batch(2, &[100.0, 100.0, 100.0]))
                    .unwrap();
                txn.commit()
            });

            let ds_reader = ds.clone();
            let reader_schema = schema.clone();
            let reader = spawn_committer(move || {
                let snapshot = ds_reader.snapshot();
                let ids: std::collections::HashSet<i64> = snapshot
                    .scan(&reader_schema)
                    .unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect();
                let a_committed = ids.contains(&1);
                let b_committed = ids.contains(&2);
                // `k=1` always returns the single nearest node once the
                // index holds ANY point -- it is never empty just because
                // the OTHER writer's row is the only one present. A's
                // query for [900,900,900] against an index holding only
                // B's [100,100,100] still returns a hit (B's point, at
                // squared distance 3*800^2 = 1_920_000) -- that is not A
                // being visible, it is HNSW doing exactly its job on the
                // one point that exists. The real question is whether the
                // returned hit IS the queried point itself, which a top-1
                // exact-coordinate query answers by distance: ~0 if A's
                // own row is in the index, ~1_920_000 if only B's is (or
                // no hit at all if neither is). 1.0 is a huge safety
                // margin between those two cases.
                let found_own_point = |hits: &[strata_index::VectorMatch]| {
                    hits.first().is_some_and(|hit| hit.squared_distance < 1.0)
                };
                let a_hit = found_own_point(
                    &snapshot
                        .vector_search(&[900.0, 900.0, 900.0], 1, None)
                        .unwrap(),
                );
                let b_hit = found_own_point(
                    &snapshot
                        .vector_search(&[100.0, 100.0, 100.0], 1, None)
                        .unwrap(),
                );
                (snapshot.version, a_committed, b_committed, a_hit, b_hit)
            });

            committer_a.join().unwrap().unwrap();
            committer_b.join().unwrap().unwrap();
            let (version, a_committed, b_committed, a_hit, b_hit) = reader.join().unwrap();

            assert_eq!(
                a_hit, a_committed,
                "A's vector must be searchable if and only if A's row is durably \
                 committed in THIS snapshot -- version {version}"
            );
            assert_eq!(
                b_hit, b_committed,
                "B's vector must be searchable if and only if B's row is durably \
                 committed in THIS snapshot -- version {version}"
            );

            let bit: u8 = match (a_committed, b_committed) {
                (false, false) => 0b0001,
                (true, false) => 0b0010,
                (false, true) => 0b0100,
                (true, true) => 0b1000,
            };
            OBSERVED.fetch_or(bit, std::sync::atomic::Ordering::Relaxed);

            std::fs::remove_dir_all(&dir).ok();
        });

        let observed = OBSERVED.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            observed, 0b1111,
            "loom must observe all four reachable states -- neither committed, \
             only A, only B, and both committed -- bitmask {observed:#06b}. In \
             particular, 'only B committed while A is still in flight' (0b0100) \
             is the specific hazard-adjacent direction this model exists to \
             check; seeing it unreached would mean DPOR never explored the \
             schedule this test is for. Seeing fewer than all four in general \
             means DPOR collapsed this model to a narrower set of schedules than \
             it should explore (see the module doc comment above `mod \
             loom_tests`)."
        );
    }
}
