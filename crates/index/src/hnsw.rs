//! HNSW vector index — lock-free, from-scratch implementation (replacing
//! `hnsw_rs` as of this rewrite). See `.claude/rules/vector-index.md` and
//! `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md`.

use crate::distance::L2;
use crate::graph::Graph;
use crate::live_set::LiveSet;

/// Runs `f` over every element of `chunks` on its own spawned thread,
/// returning each worker's result (or its panic payload) in input order.
/// Extracted as its own function rather than inlined in
/// [`HnswIndex::insert_batch_parallel`] so this exact fan-out pattern can be
/// tested in isolation (see `run_chunks_in_parallel_actually_runs_concurrently_not_serially`)
/// and reused by any other batch-parallel caller (e.g. a future segment
/// builder).
///
/// Uses two explicit loops -- spawn every worker, THEN join every handle --
/// rather than a single `chunks.into_iter().map(|c| scope.spawn(...)).map(|h| h.join())`
/// chain. `Iterator::map` is lazy, so that chained form would spawn one
/// worker and immediately join it before spawning the next, silently
/// serializing the whole thing: every functional test would still pass
/// (every chunk still gets processed correctly), just with zero real
/// concurrency. Two explicit loops can't be collapsed into that mistake by
/// a future "simplify this iterator chain" refactor the way one chained
/// expression could.
fn run_chunks_in_parallel<T: Send, R: Send>(
    chunks: Vec<T>,
    f: impl Fn(T) -> R + Sync,
) -> Vec<std::thread::Result<R>> {
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            handles.push(scope.spawn(|| f(chunk)));
        }
        handles
            .into_iter()
            .map(std::thread::ScopedJoinHandle::join)
            .collect()
    })
}

/// Reads out the final contents of an `insert_batch_parallel`-style shared
/// applied-row-id collector. A clone rather than `into_inner`: every call
/// site is a plain `&Mutex` (not an owned one it could otherwise consume),
/// which keeps `insert_batch_parallel`'s several early-return branches from
/// having to juggle ownership of `applied` across match arms -- the clone
/// itself is negligible (bounded by one commit's batch size, not a hot
/// per-row cost).
fn into_applied_vec(applied: &std::sync::Mutex<Vec<u64>>) -> Vec<u64> {
    applied
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// One search result: which row-id, and its squared L2 distance to the
/// query vector. `row_id` is the persistent, global identity from
/// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8 — not a
/// position within any particular array, unlike `brute_force::Neighbor`.
///
/// `squared_distance` is the sum of squared per-dimension differences (no
/// square root), the same units as `brute_force::Neighbor::squared_distance`
/// — `hnsw_rs`'s underlying `anndists::DistL2` returns true (non-squared)
/// Euclidean distance, so `to_matches` squares it before constructing this
/// struct.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VectorMatch {
    pub row_id: u64,
    pub squared_distance: f32,
}

/// Result of [`HnswIndex::insert_batch_parallel`]. Every variant carries
/// `applied` -- every row-id that MAY have landed in the shared graph
/// across ALL worker threads, not just whichever one triggered the outcome
/// -- so a caller doing failure cleanup (e.g. `crates/txn`'s
/// `GraphResidueGuard`) always knows the full set to undo, regardless of
/// how this resolved. "May have": a row-id is recorded before its own
/// insert attempt runs (not after it succeeds), since `Graph::insert`
/// publishes a node into the shared table before neighbor-linking
/// completes -- a panic in that window would otherwise leave a row live
/// but unrecorded. This makes `applied` a conservative superset that can
/// include a row whose insert was actually REJECTED (e.g.
/// `DimensionMismatch`, rejected before ever reaching the graph); that's
/// harmless, since undoing a never-inserted row-id is a documented no-op
/// (see `HnswIndex::remove`).
///
/// Deliberately not a bare `Result<Vec<u64>, IndexError>`: that shape lets
/// a caller write `let (_, r) = ...; r?;` and lose the applied-row-ids with
/// zero friction. This enum forces an explicit match, so `applied` can
/// never be silently dropped by an unattended `?`.
#[derive(Debug)]
pub enum BatchInsertOutcome {
    /// Every row was attempted with no error or panic; this is every
    /// attempted row-id.
    Ok(Vec<u64>),
    /// A worker hit `IndexError::DimensionMismatch`/`RowIdOutOfRange`.
    /// `error` is the first such error observed across any worker (in no
    /// particular order across workers).
    IndexError {
        applied: Vec<u64>,
        error: IndexError,
    },
    /// A worker thread itself panicked -- a bug, not an expected/typed
    /// error condition (see `insert_batch_parallel`'s doc comment). The
    /// caller is expected to record `applied` first, then propagate
    /// `payload` (e.g. via `std::panic::resume_unwind`) so a `Drop`-based
    /// cleanup guard still fires during the resulting unwind.
    WorkerPanicked {
        applied: Vec<u64>,
        payload: Box<dyn std::any::Any + Send + 'static>,
    },
}

/// Below this, per-thread overhead (spawn cost, cache-cold start) isn't
/// worth it relative to a few more sequential inserts on one thread -- e.g.
/// a 3-row batch across 8 available cores should not spawn 3 threads for 1
/// row each. Provisional; confirmed/tuned alongside the 1/4/8-thread
/// measurement pass cited in `crates/txn/src/dataset.rs`'s
/// `PARALLEL_INSERT_THREADS`.
const MIN_ROWS_PER_CHUNK: usize = 64;

/// Test-only sentinel row-id that deterministically panics
/// [`HnswIndex::insert_owned_chunk`] -- see that method's own comment on the
/// injection point. `u64::MAX` rather than some arbitrary large constant:
/// this crate's own `slot_array`'s `EMPTY` sentinel already reserves
/// `u64::MAX` as never a real row-id (see its own doc), and `crates/txn`'s
/// row-id allocator independently caps every real row-id at 1e9, so no real
/// production row-id can ever collide with this value.
#[cfg(test)]
const PANIC_TEST_ROW_ID: u64 = u64::MAX;

/// # Examples
///
/// ```
/// use strata_index::IndexError;
///
/// let err = IndexError::MaxConnectionTooLarge(300);
/// assert_eq!(
///     err.to_string(),
///     "max_nb_connection must be <= 256 (hnsw_rs hard limit), got 300"
/// );
/// ```
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("max_nb_connection must be <= 256 (hnsw_rs hard limit), got {0}")]
    MaxConnectionTooLarge(usize),
    #[error("query has {query_len} dimensions, but the index expects {expected}")]
    DimensionMismatch { query_len: usize, expected: usize },
    #[error("row_id {row_id} is beyond the index's addressable capacity of {capacity} rows")]
    RowIdOutOfRange { row_id: u64, capacity: u64 },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("delta log entry serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Maximum number of bidirectional links per node per layer (`hnsw_rs`'s
/// `max_nb_connection`) — hard-capped at 256 by the underlying library, see
/// [`HnswIndex::new`]'s doc comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxConnections(pub usize);

/// Expected/reserved capacity for the graph's internal allocation
/// (`hnsw_rs`'s `max_elements`) — a sizing hint, not a hard cap on how many
/// vectors can be inserted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxElements(pub usize);

/// Maximum number of layers in the graph's hierarchy (`hnsw_rs`'s
/// `max_layer`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxLayers(pub usize);

/// Candidate-list size used while building the graph (`hnsw_rs`'s
/// `ef_construction`) — higher values trade insert time for graph quality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EfConstruction(pub usize);

pub struct HnswIndex {
    graph: Graph<L2>,
    m: usize,
    mmax0: usize,
    mmax: usize,
    ef_construction: usize,
    m_l: f64,
    row_counter: std::sync::atomic::AtomicU64, // supplies a deterministic unif draw per insert; see Self::insert's note below
}

impl HnswIndex {
    /// # Examples
    ///
    /// ```
    /// use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
    ///
    /// let index = HnswIndex::new(
    ///     MaxConnections(16),
    ///     MaxElements(100),
    ///     MaxLayers(16),
    ///     EfConstruction(200),
    /// )?;
    /// index.insert(0, &[0.0, 0.0, 0.0])?;
    ///
    /// let results = index.search(&[0.0, 0.0, 0.0], 1, 50, |_| true)?;
    /// assert_eq!(results.len(), 1);
    /// assert_eq!(results[0].row_id, 0);
    /// # Ok::<(), strata_index::IndexError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::MaxConnectionTooLarge`] if `max_nb_connection`
    /// exceeds 256 — this validation predates the lock-free rewrite (it
    /// used to guard against `hnsw_rs::Hnsw::new`'s uncatchable
    /// `std::process::exit(1)` on that condition) and is retained
    /// unconditionally: 256 remains this crate's own documented connection
    /// ceiling regardless of backing implementation.
    pub fn new(
        max_nb_connection: MaxConnections,
        max_elements: MaxElements,
        max_layer: MaxLayers,
        ef_construction: EfConstruction,
    ) -> Result<Self, IndexError> {
        if max_nb_connection.0 > 256 {
            return Err(IndexError::MaxConnectionTooLarge(max_nb_connection.0));
        }
        let _ = max_layer; // MaxLayers is retained in the public signature for API compatibility; the new design derives level count from mL/unif rather than a hard layer cap — see design doc §2/§3.
        let m = max_nb_connection.0.max(1);
        // `m` is capped at 256 by the `max_nb_connection.0 > 256` check
        // above, so this cast is always exact — never a real precision
        // loss, just a lint that can't see the bound.
        #[allow(clippy::cast_precision_loss)]
        let m_l = 1.0 / (m as f64).ln();
        Ok(Self {
            graph: Graph::new(L2, max_elements.0.max(1)),
            m,
            mmax0: m * 2,
            mmax: m,
            ef_construction: ef_construction.0,
            m_l,
            row_counter: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// # Examples
    ///
    /// ```
    /// use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
    ///
    /// let index = HnswIndex::new(
    ///     MaxConnections(16), MaxElements(100), MaxLayers(16), EfConstruction(200),
    /// )?;
    /// index.insert(0, &[1.0, 2.0, 3.0])?;
    /// assert_eq!(index.established_dimension(), 3);
    /// # Ok::<(), strata_index::IndexError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::DimensionMismatch`] if `vector`'s length
    /// doesn't match the dimensionality of the first vector ever inserted.
    /// Checked upfront (inside `Graph::insert`'s own
    /// `check_or_establish_dimension` call) so a corrupted delta-log entry
    /// with a wrong-length vector can never reach the distance function.
    pub fn insert(&self, row_id: u64, vector: &[f32]) -> Result<(), IndexError> {
        self.insert_owned(row_id, vector.to_vec())
    }

    /// Same as [`Self::insert`], but takes ownership of `vector` and moves
    /// it straight into the graph instead of cloning a borrowed slice.
    /// `crates/txn`'s commit-apply loop and recovery replay both already
    /// own a freshly-deserialized/freshly-built `Vec<f32>` at their call
    /// site — routing through `insert`'s `&[f32]` there would force a
    /// wasted clone of the full 512-dim embedding on every insert, on top
    /// of the one copy already paid getting the vector out of Arrow (or
    /// out of the delta log) in the first place. `Graph::insert` moves
    /// `vector` into `Node::new` from there, not a further copy — so this
    /// takes the vector from two copies down to one, not three to two.
    ///
    /// # Errors
    ///
    /// Same as [`Self::insert`].
    pub fn insert_owned(&self, row_id: u64, vector: Vec<f32>) -> Result<(), IndexError> {
        // A deterministic-but-varying draw per insert, avoiding a new RNG
        // dependency: derived from a monotonically-advancing counter run
        // through a fixed hash, mapped into (0, 1). This is NOT
        // cryptographic or high-quality randomness — HNSW's level
        // assignment only needs a source that varies across inserts to
        // achieve the paper's expected layer-count distribution, and this
        // project's own established precedent (this file's *old* tests)
        // already tolerates non-reproducible layer assignment (see the
        // existing `insert_cluster` test helper's doc comment on
        // hnsw_rs's own unseeded RNG). If a real `rand`-crate dependency
        // is preferred instead, swap this for one — flagged here as an
        // explicit, deliberate choice for the implementer/reviewer to
        // confirm, not a silent placeholder. Safe to call concurrently:
        // `fetch_add` guarantees each call sees a distinct `n`, so
        // `insert_batch_parallel`'s worker threads can call this directly
        // without any extra synchronization.
        let n = self
            .row_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut x = n.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        x ^= x >> 33;
        x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        x ^= x >> 33;
        #[allow(clippy::cast_precision_loss)]
        let unif = ((x >> 11) as f64 / (1u64 << 53) as f64).clamp(f64::EPSILON, 1.0 - f64::EPSILON);

        self.graph.insert(
            row_id,
            vector,
            self.m,
            self.mmax0,
            self.mmax,
            self.ef_construction,
            self.m_l,
            1.0,
            unif,
        )
    }

    /// Soft-deletes `row_id`: it is excluded from every subsequent
    /// [`Self::search`]/[`Self::search_filtered`] result by the graph's own
    /// deleted-flag check, independent of any caller-supplied visibility
    /// predicate. Its node physically remains as a traversal waypoint, so
    /// other rows stay reachable through it, until Phase 8 compaction. A
    /// no-op if `row_id` was never inserted, and irreversible — nothing
    /// clears the flag.
    ///
    /// **Sole intended use: undoing an insert that never became durable.**
    /// `crates/txn`'s commit path calls this to drop a transaction's
    /// vectors back out of the shared graph when that transaction failed
    /// before its manifest commit (see that crate's `GraphResidueGuard`).
    /// That is sound precisely because such a row-id was never committed in
    /// *any* version, so no snapshot should ever observe it, and because
    /// row-ids are never reused
    /// (`.claude/docs/design/phase-0-transaction-and-format-spec.md` §8) —
    /// a soft-deleted id can never legitimately reappear.
    ///
    /// **Do not use this to implement a user-level DELETE.** That is
    /// `crates/txn`'s versioned `Snapshot::tombstones` set, which is
    /// per-version and replayed from the manifest. This flag is global and
    /// unversioned: applying it to a committed row would hide that row from
    /// *already-open* snapshots taken before the delete, breaking the
    /// snapshot isolation those readers are promised.
    ///
    /// # Examples
    ///
    /// ```
    /// use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
    ///
    /// let index = HnswIndex::new(
    ///     MaxConnections(16), MaxElements(100), MaxLayers(16), EfConstruction(200),
    /// )?;
    /// index.insert(0, &[0.0, 0.0, 0.0])?;
    /// index.insert(1, &[10.0, 10.0, 10.0])?;
    ///
    /// index.remove(0);
    /// let results = index.search(&[0.0, 0.0, 0.0], 1, 50, |_| true)?;
    /// assert_eq!(results[0].row_id, 1, "the removed row is never returned");
    /// # Ok::<(), strata_index::IndexError>(())
    /// ```
    pub fn remove(&self, row_id: u64) {
        self.graph.delete(row_id);
    }

    /// Inserts every `(row_id, vector)` pair in `rows` across up to
    /// `threads` worker threads into the shared lock-free graph — the
    /// parallel-construction primitive from the 2026-07-24 ingest+recovery
    /// performance audit. Order across rows is NOT guaranteed and doesn't
    /// need to be for correctness: HNSW is already an approximate
    /// algorithm, and which thread processes which row can produce a
    /// different graph *shape* than a strictly sequential insert of the
    /// same rows would (every row still ends up inserted and findable).
    ///
    /// **One accepted, explicit behavior change**: today, the live graph's
    /// per-row level assignment (via the internal `row_counter` atomic)
    /// happens in the same sequential order `crates/txn`'s `replay_index`
    /// replays the delta log in, so a live graph and its post-restart
    /// replayed graph are the same shape. Parallelizing insertion means
    /// `row_counter.fetch_add` calls interleave nondeterministically across
    /// worker threads, so the live graph's shape can differ from what a
    /// restart would replay. Both are valid, complete HNSW graphs and
    /// every row is findable in either — this is a deliberate cut, not an
    /// oversight, and doesn't affect Phase 7's chaos harness (its
    /// assertions are structural — no crash, no data loss, no corruption —
    /// not pinned to an exact search result).
    ///
    /// **Known recall risk, not a safety one**: `Graph::insert`'s shrink
    /// step (read `occupied`, compute what to keep, `clear_matching`) isn't
    /// atomic across those three steps, and a full neighbor layer silently
    /// drops a claimed edge. Both pre-exist this method, but concurrent
    /// inserts make both fire more often than a sequential insert would —
    /// every row still ends up inserted and part of a connected graph, but
    /// recall can be very slightly lower than an equivalent sequential
    /// insert. Not asserted away here; see `bench/`'s recall benchmarks for
    /// the production-scale measurement this trades against the ingest
    /// speedup.
    ///
    /// `rows.len() < 2` or `threads <= 1` degrades to a plain sequential
    /// loop on the calling thread (no `thread::scope` overhead for tiny
    /// batches). `threads` is clamped to
    /// `std::thread::available_parallelism()`, and chunk size is floored
    /// at `MIN_ROWS_PER_CHUNK`, so a small batch never oversplits into
    /// more worker threads than useful.
    ///
    /// Chunking is contiguous (`rows[0..k]`, `rows[k..2k]`, ...), not
    /// round-robin: if the input batch has any vector-space locality
    /// (e.g. rows inserted in embedding-similarity order), contiguous
    /// chunking tends to put different workers in different regions of
    /// the graph, reducing both `SlotArray` CAS contention and how often
    /// two workers race to shrink the SAME neighbor's list. Round-robin
    /// would scatter every worker across the whole batch, maximizing both.
    /// The shared entry-point CAS is contended identically either way (it's
    /// global, not per-region).
    ///
    /// Bounded by construction: `crates/txn`'s `commit_lock` serializes
    /// commits, so at most one call to this method is ever in flight at a
    /// time — `threads` workers here, never `threads` x concurrent-commits
    /// workers.
    pub fn insert_batch_parallel(
        &self,
        rows: Vec<(u64, Vec<f32>)>,
        threads: usize,
    ) -> BatchInsertOutcome {
        // A single shared collector every worker pushes into AS EACH ROW
        // LANDS, rather than each worker accumulating its own local Vec and
        // returning it at the end. This is load-bearing for panic safety:
        // design review round 1 fixed the case where bailing out on the
        // first error/panic mid-join-loop would lose every OTHER worker's
        // fully-successful applied rows, but an EARLIER version of this
        // fix still had a gap -- if a worker panicked partway through ITS
        // OWN chunk (or if `std::thread::Scope::spawn` itself panicked,
        // e.g. the OS refusing to create another thread), that worker's
        // own already-applied rows died with its unwinding stack, since
        // they only existed in a local Vec never returned. Writing
        // directly into a `Mutex` that lives in THIS function's frame
        // (not any worker's) means a row that lands is recorded
        // regardless of what happens to it, or any other worker, or the
        // fan-out itself, afterward.
        let applied: std::sync::Mutex<Vec<u64>> =
            std::sync::Mutex::new(Vec::with_capacity(rows.len()));

        if rows.len() < 2 {
            return self.run_sequential(rows, &applied);
        }
        // Queried after the tiny/delete-only-batch early return above, not
        // before: no reason to pay this syscall for a batch that's about
        // to take the sequential path regardless of core count.
        let threads = threads
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        if threads <= 1 {
            return self.run_sequential(rows, &applied);
        }

        let chunk_size = rows.len().div_ceil(threads).max(MIN_ROWS_PER_CHUNK);
        // Contiguous, zero-clone chunking: `Vec::drain` MOVES elements out
        // of `rows` (no `Clone` bound, no per-row Vec<f32> copy) -- unlike
        // `rows.chunks(n).map(<[_]>::to_vec)`, which would deep-clone every
        // row's vector via `(u64, Vec<f32>)`'s `Clone` impl and silently
        // defeat the entire point of taking `rows` by value.
        let mut rows = rows;
        let mut chunks = Vec::new();
        while !rows.is_empty() {
            let take = chunk_size.min(rows.len());
            chunks.push(rows.drain(0..take).collect::<Vec<_>>());
        }
        if chunks.len() == 1 {
            // MIN_ROWS_PER_CHUNK folded everything into one chunk despite
            // threads > 1 -- run it inline rather than paying a thread
            // spawn (and a cold, freshly-allocated SEARCH_SCRATCH) for
            // zero actual parallelism. `unwrap_or_default` rather than
            // `expect`/`unwrap`: provably non-empty by the `len() == 1`
            // check just above, but falls back to a no-op empty chunk
            // instead of panicking if that invariant were ever violated,
            // matching this codebase's "fails safe rather than panicking"
            // convention for provably-unreachable conditions.
            let chunk = chunks.into_iter().next().unwrap_or_default();
            return self.run_sequential(chunk, &applied);
        }

        // Catches a panic from the fan-out ITSELF -- e.g.
        // `std::thread::Scope::spawn` panicking because the OS refused to
        // create another thread mid-loop -- as distinct from an
        // individual WORKER's own panic, which `run_chunks_in_parallel`
        // already converts into an `Err` inside its returned `Vec` (via
        // `ScopedJoinHandle::join`) without unwinding past it. Without this
        // outer catch, that rare fan-out-level panic would propagate
        // straight out of this function with no `BatchInsertOutcome` at
        // all, dropping `applied` -- and every row any worker had already
        // recorded into it -- along with it. `AssertUnwindSafe` is sound
        // here: nothing after this catch reads `self`/`chunks`/`applied` to
        // make a decision based on their possibly-mid-panic state -- the
        // `Err` arm below only reads `applied` (a `Mutex`, whose contents
        // are never torn even when poisoned, since every mutation is a
        // single `Vec::push`) and returns a `WorkerPanicked` outcome; it
        // never touches `self`/`chunks` again.
        let fan_out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_chunks_in_parallel(chunks, |chunk| self.insert_owned_chunk(chunk, &applied))
        }));
        let worker_results = match fan_out {
            Ok(results) => results,
            Err(payload) => {
                return BatchInsertOutcome::WorkerPanicked {
                    applied: into_applied_vec(&applied),
                    payload,
                };
            }
        };

        // Collect EVERY worker's result before deciding anything -- do not
        // bail out on the first error or panic. A worker's failure must
        // never cost the caller visibility into what every OTHER worker
        // already committed to the shared graph: `crates/txn`'s
        // `GraphResidueGuard` needs the FULL applied-row-id list regardless
        // of how this outcome resolves, to undo exactly what landed (not
        // guess, and not silently leave live-but-unrecorded rows behind —
        // see `BatchInsertOutcome`'s own doc comment).
        let mut worker_panic = None;
        let mut index_error = None;
        for result in worker_results {
            match result {
                Ok(error) => {
                    if index_error.is_none() {
                        index_error = error;
                    }
                }
                Err(payload) => {
                    if worker_panic.is_none() {
                        worker_panic = Some(payload);
                    }
                }
            }
        }
        let applied = into_applied_vec(&applied);
        if let Some(payload) = worker_panic {
            BatchInsertOutcome::WorkerPanicked { applied, payload }
        } else if let Some(error) = index_error {
            BatchInsertOutcome::IndexError { applied, error }
        } else {
            BatchInsertOutcome::Ok(applied)
        }
    }

    /// Runs `rows` through [`Self::insert_owned_chunk`] on the CALLING
    /// thread (the sequential-degenerate path: `rows.len() < 2`,
    /// `threads <= 1`, or `MIN_ROWS_PER_CHUNK` folding everything into one
    /// chunk) -- but still under the same `catch_unwind` protection as the
    /// multi-worker fan-out in [`Self::insert_batch_parallel`]. Without this, a panic from
    /// `insert_owned` here would unwind straight out of
    /// `insert_batch_parallel`, dropping `applied` -- and every row already
    /// recorded into it -- along with it, exactly the hazard the fan-out's
    /// own `catch_unwind` exists to close. This path is not the rare case:
    /// every commit under `MIN_ROWS_PER_CHUNK` rows and every single-core
    /// host takes it, so leaving it uncaught would silently reopen the gap
    /// for the common case while only fixing the multi-worker one.
    fn run_sequential(
        &self,
        rows: Vec<(u64, Vec<f32>)>,
        applied: &std::sync::Mutex<Vec<u64>>,
    ) -> BatchInsertOutcome {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.insert_owned_chunk(rows, applied)
        }));
        match result {
            Ok(error) => Self::outcome_from(applied, error),
            Err(payload) => BatchInsertOutcome::WorkerPanicked {
                applied: into_applied_vec(applied),
                payload,
            },
        }
    }

    fn outcome_from(
        applied: &std::sync::Mutex<Vec<u64>>,
        error: Option<IndexError>,
    ) -> BatchInsertOutcome {
        let applied = into_applied_vec(applied);
        match error {
            None => BatchInsertOutcome::Ok(applied),
            Some(error) => BatchInsertOutcome::IndexError { applied, error },
        }
    }

    // Shared by both the sequential-degenerate path and each parallel
    // worker inside insert_batch_parallel. Pushes each successfully
    // inserted row-id into `applied` AS IT LANDS (not accumulated locally
    // and returned at the end) -- see insert_batch_parallel's own comment
    // on `applied` for why this matters for panic safety.
    fn insert_owned_chunk(
        &self,
        rows: Vec<(u64, Vec<f32>)>,
        applied: &std::sync::Mutex<Vec<u64>>,
    ) -> Option<IndexError> {
        for (row_id, vector) in rows {
            // Recorded BEFORE insert_owned runs, not after it returns Ok:
            // Graph::insert publishes a node into the shared NodeTable (making
            // it live and potentially reachable as a neighbor) before
            // neighbor-linking/entry-point-advance complete, so a panic in
            // that window would otherwise leave a row live but unrecorded
            // here. Marking a row_id "applied" even when insert_owned goes on
            // to return an error is harmless: an error means the row was
            // REJECTED before ever reaching the graph (e.g. a dimension
            // mismatch, checked before any node is published), and
            // HnswIndex::remove -- what GraphResidueGuard calls for every
            // applied row-id -- is a documented no-op for a row-id that was
            // never actually inserted.
            applied
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(row_id);
            // Test-only panic-injection hook (mirrors `crates/txn`'s
            // `inject_manifest_commit_failure`): a real caller can never
            // legitimately insert `PANIC_TEST_ROW_ID` (`u64::MAX`), since
            // `crates/txn`'s row-id allocator never hands it out (see
            // `MAX_ROW_ID_CAPACITY`'s doc). This is the only way to
            // deterministically exercise `insert_batch_parallel`'s
            // `WorkerPanicked` outcome and the `catch_unwind` wrapping
            // both the sequential path and the multi-worker fan-out --
            // `insert_owned` itself has no reachable panic path from bad
            // input (a dimension/row-id problem is a typed `IndexError`,
            // not a panic), so there is no way to trigger this from real
            // data.
            #[cfg(test)]
            assert_ne!(
                row_id, PANIC_TEST_ROW_ID,
                "intentional test panic for insert_batch_parallel's WorkerPanicked coverage"
            );
            if let Err(error) = self.insert_owned(row_id, vector) {
                return Some(error);
            }
        }
        None
    }

    /// The vector dimension established by the first-ever [`Self::insert`]
    /// call, or `0` if no vector has been inserted yet. Read-only — never
    /// establishes a dimension itself. Exposed so callers (e.g.
    /// `crates/txn`'s `Transaction::commit`) can pre-validate a batch of
    /// pending inserts' dimensions against this index *before* applying
    /// any of them, rather than discovering a mismatch mid-application.
    #[must_use]
    pub fn established_dimension(&self) -> usize {
        self.graph.established_dimension()
    }

    /// # Examples
    ///
    /// ```
    /// use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
    ///
    /// let index = HnswIndex::new(
    ///     MaxConnections(16), MaxElements(100), MaxLayers(16), EfConstruction(200),
    /// )?;
    /// index.insert(0, &[0.0, 0.0, 0.0])?;
    /// index.insert(1, &[10.0, 10.0, 10.0])?;
    ///
    /// let results = index.search(&[0.0, 0.0, 0.0], 1, 50, |_| true)?;
    /// assert_eq!(results[0].row_id, 0);
    /// # Ok::<(), strata_index::IndexError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::DimensionMismatch`] if `query`'s length
    /// doesn't match the dimensionality of the first vector ever inserted —
    /// checked upfront rather than silently truncating, matching
    /// `brute_force_search`'s existing Phase 1 behavior.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        let raw = self.graph.k_nn_search(query, k, ef_search, is_visible)?;
        Ok(raw
            .into_iter()
            .map(|(row_id, dist)| VectorMatch {
                row_id,
                squared_distance: dist * dist,
            })
            .collect())
    }

    /// Builds a [`LiveSet`] from `live_ids` and delegates to
    /// [`Self::search_filtered_live`]. Sizing note: the bitset is
    /// `max_row_id / 8` bytes — proportional to the largest live row-id,
    /// *not* to `live_ids.len()`. For a dense low-selectivity filter that is
    /// a large win over the ~24-bytes/entry `HashSet` it replaced (~12KB vs
    /// ~1MB at 100k live ids). For a *highly selective* predicate over a very
    /// large dataset it can be the other way round — a handful of matches
    /// with a max row-id near 1e8 still allocates ~12MB here where a
    /// `HashSet` would have been tiny. Both are dwarfed by the in-memory
    /// graph and by the whole-file re-read `crates/txn`'s `row_ids_matching`
    /// pays to resolve `live_ids` in the first place, so this is not the term
    /// that matters — but it is not unconditionally smaller.
    ///
    /// Callers that resolve the same live set across many queries (e.g. a
    /// per-snapshot cache keyed by predicate) should build a [`LiveSet`] once
    /// with [`LiveSet::from_row_ids`] and call [`Self::search_filtered_live`]
    /// directly instead, to avoid rebuilding the bitset on every call.
    ///
    /// # Errors
    ///
    /// Same as [`Self::search`].
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        live_ids: &[usize],
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        self.search_filtered_live(
            query,
            k,
            ef_search,
            &LiveSet::from_row_ids(live_ids),
            is_visible,
        )
    }

    /// Like [`Self::search_filtered`], but takes an already-built
    /// [`LiveSet`] rather than rebuilding one from a raw `&[usize]`.
    ///
    /// # Errors
    ///
    /// Same as [`Self::search`].
    pub fn search_filtered_live(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        live: &LiveSet,
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        // live-set membership and is_visible are composed into ONE
        // predicate passed straight into k_nn_search -> search_layer,
        // applied during traversal-time result-set construction — not a
        // post-filter over an already-capped top-k. This matches
        // hnsw_rs's original FilterT-based behavior exactly: a live row
        // deep in the graph is never missed just because it fell outside
        // some pre-guessed widened candidate window, since the predicate is
        // evaluated as part of the same search, not after it.
        let filter = move |id: u64| live.contains(id) && is_visible(id);
        let raw = self.graph.k_nn_search(query, k, ef_search, filter)?;
        Ok(raw
            .into_iter()
            .map(|(row_id, dist)| VectorMatch {
                row_id,
                squared_distance: dist * dist,
            })
            .collect())
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    // Empirically validated (10000-trial repro, zero failures) against a
    // 30-point, two-cluster fixture: enough connections/candidate budget
    // that `hnsw_rs`'s neighbor-diversification pruning can't leave any
    // near-cluster member unreachable during search. See `insert_cluster`'s
    // doc comment for why a lower `max_nb_connection` was still measurably
    // (if rarely) flaky on this same fixture.
    const TEST_MAX_NB_CONNECTION: usize = 200;
    const TEST_MAX_LAYER: usize = 16;
    const TEST_EF_CONSTRUCTION: usize = 1600;
    const TEST_EF_SEARCH: usize = 500;

    #[test]
    fn run_chunks_in_parallel_actually_runs_concurrently_not_serially() {
        // Proves genuine concurrency, not just eventual correctness: every
        // worker blocks on a Barrier until every OTHER worker has also
        // started. If run_chunks_in_parallel silently serialized (e.g. a
        // `.map(spawn).map(join)` chain collapsed the spawn/join loops back
        // together in some future refactor), the first worker would block
        // forever waiting for a second worker that never gets spawned
        // until the first returns -- a deadlock, not a wrong answer, which
        // a purely correctness-focused test (e.g. "every row inserted")
        // would never catch. Wrapped in a watchdog thread with a timeout
        // so a real regression fails this test cleanly instead of hanging
        // the whole suite.
        const WORKERS: usize = 4;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(WORKERS));
        let chunks: Vec<usize> = (0..WORKERS).collect();

        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let results = run_chunks_in_parallel(chunks, move |i| {
                barrier.wait();
                i
            });
            let _ = tx.send(results);
        });

        let results = rx.recv_timeout(std::time::Duration::from_secs(10)).expect(
            "run_chunks_in_parallel deadlocked -- workers are not actually running \
                 concurrently (every worker is blocked on a Barrier waiting for the others)",
        );
        let mut values: Vec<usize> = results
            .into_iter()
            .map(std::thread::Result::unwrap)
            .collect();
        values.sort_unstable();
        assert_eq!(values, vec![0, 1, 2, 3]);
    }

    #[test]
    fn run_chunks_in_parallel_propagates_a_worker_panic_as_a_join_error() {
        let chunks = vec![1, 2, 3];
        let results = run_chunks_in_parallel(chunks, |i| {
            assert_ne!(i, 2, "intentional test panic");
            i
        });
        let mut ok_values: Vec<i32> = Vec::new();
        let mut panicked = 0;
        for r in results {
            match r {
                Ok(v) => ok_values.push(v),
                Err(_) => panicked += 1,
            }
        }
        ok_values.sort_unstable();
        assert_eq!(ok_values, vec![1, 3]);
        assert_eq!(panicked, 1);
    }

    /// Inserts `count` points scattered within a small cube of side
    /// `spacing` around `center`, with row-ids `start_id..start_id + count`.
    ///
    /// `hnsw_rs::Hnsw::new` seeds its RNG from OS entropy with no seed
    /// exposed anywhere in the public API (verified against the installed
    /// `hnsw_rs-0.3.4` source), so unlucky random layer assignment can make
    /// greedy search miss the true nearest neighbor on tiny (2-3 point)
    /// fixtures. Using many points arranged in clusters that are far apart
    /// relative to their own radius makes "which cluster is nearest"
    /// unambiguous regardless of layer-assignment luck, without needing the
    /// library to expose a seed.
    ///
    /// Offsets come from an irrational-multiplier equidistribution
    /// sequence (fractional parts of `i * golden ratio`, etc.) rather than
    /// a regular line or grid. A 2000-trial repro showed that a line or
    /// axis-aligned grid of near-duplicate points lets `hnsw_rs`'s
    /// neighbor-diversification heuristic prune almost all direct links
    /// between them (they all point the same direction from any given
    /// node), occasionally leaving parts of the near cluster unreachable
    /// during search even with `ef_search` well above the point count.
    /// Quasi-random, non-collinear offsets avoid that degenerate case.
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    fn insert_cluster(
        index: &HnswIndex,
        start_id: u64,
        count: u64,
        center: [f32; 3],
        spacing: f32,
    ) {
        const PHI: f64 = 0.618_033_988_749_895; // fractional part of the golden ratio
        const SQRT2: f64 = 0.414_213_562_373_095; // fractional part of sqrt(2)
        const SQRT3: f64 = 0.732_050_807_568_877; // fractional part of sqrt(3)
        for i in 0..count {
            let n = i as f64;
            let frac = |mult: f64| (n * mult).fract();
            let dx = (frac(PHI) as f32) * spacing;
            let dy = (frac(SQRT2) as f32) * spacing;
            let dz = (frac(SQRT3) as f32) * spacing;
            index
                .insert(
                    start_id + i,
                    &[center[0] + dx, center[1] + dy, center[2] + dz],
                )
                .unwrap();
        }
    }

    #[test]
    fn insert_then_search_finds_the_true_nearest_neighbor() {
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        // Near cluster: row-ids 0..15, within a 0.01-wide cube around
        // (0,0,0). Far cluster: row-ids 15..30, within a 0.01-wide cube
        // around (1000,0,0). Clusters are ~100000x farther apart than
        // their own radius, so which cluster is nearest is unambiguous
        // even under hnsw_rs's approximate search.
        insert_cluster(&index, 0, 15, [0.0, 0.0, 0.0], 0.01);
        insert_cluster(&index, 15, 15, [1000.0, 0.0, 0.0], 0.01);

        // Row 0 is an exact match for the query (offset 0 in the near
        // cluster) — the unambiguous true nearest neighbor.
        let results = index
            .search(&[0.0, 0.0, 0.0], 3, TEST_EF_SEARCH, |_| true)
            .unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].row_id, 0);
        #[allow(clippy::float_cmp)]
        {
            assert_eq!(results[0].squared_distance, 0.0);
        }
        assert!(
            results[1].row_id < 15 && results[2].row_id < 15,
            "the next-nearest neighbors must come from the near cluster, not the far one: {results:?}"
        );
        assert!(
            results[0].squared_distance <= results[1].squared_distance
                && results[1].squared_distance <= results[2].squared_distance,
            "results must be ranked by increasing distance: {results:?}"
        );
    }

    #[test]
    fn insert_owned_makes_a_vector_findable_by_search() {
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        index.insert_owned(0, vec![0.0, 0.0, 0.0]).unwrap();
        index.insert_owned(1, vec![1000.0, 0.0, 0.0]).unwrap();

        let results = index
            .search(&[0.0, 0.0, 0.0], 1, TEST_EF_SEARCH, |_| true)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_id, 0);
        #[allow(clippy::float_cmp)]
        {
            assert_eq!(results[0].squared_distance, 0.0);
        }
    }

    #[test]
    fn insert_owned_errors_on_dimension_mismatch_with_previously_inserted_vectors() {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();
        index.insert_owned(0, vec![0.0, 0.0, 0.0]).unwrap();

        let result = index.insert_owned(1, vec![0.0, 0.0]);
        assert!(matches!(
            result,
            Err(IndexError::DimensionMismatch {
                query_len: 2,
                expected: 3
            })
        ));
    }

    /// Widely-separated points (1000 units apart, `TEST_EF_CONSTRUCTION`-
    /// scale graphs use sub-unit clusters elsewhere in this file), so a
    /// query at row `i`'s exact coordinates is unambiguously nearest to row
    /// `i` regardless of graph *shape* -- which is exactly what varies
    /// between a sequential and a parallel insert of the same rows.
    #[allow(clippy::cast_precision_loss)] // row-ids here are always < 300, far under f32's exact-integer ceiling
    fn widely_separated_rows(count: u64) -> Vec<(u64, Vec<f32>)> {
        (0..count)
            .map(|i| (i, vec![i as f32 * 1000.0, 0.0, 0.0]))
            .collect()
    }

    #[test]
    #[allow(clippy::cast_precision_loss)] // row-ids here are always < 300, far under f32's exact-integer ceiling
    fn insert_batch_parallel_makes_every_row_findable_across_multiple_threads() {
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(300),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        let rows = widely_separated_rows(300);

        let outcome = index.insert_batch_parallel(rows, 4);
        let BatchInsertOutcome::Ok(_) = outcome else {
            panic!("expected Ok, got {outcome:?}");
        };

        for i in 0..300u64 {
            let query = vec![i as f32 * 1000.0, 0.0, 0.0];
            let results = index.search(&query, 1, TEST_EF_SEARCH, |_| true).unwrap();
            assert_eq!(
                results[0].row_id, i,
                "row {i} must be findable at its own exact coordinates"
            );
        }
    }

    #[test]
    fn insert_batch_parallel_returns_every_applied_row_id_on_success() {
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(300),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        let rows = widely_separated_rows(300);

        let BatchInsertOutcome::Ok(applied) = index.insert_batch_parallel(rows, 4) else {
            panic!("expected Ok");
        };
        let mut applied_set: HashSet<u64> = applied.into_iter().collect();
        let expected: HashSet<u64> = (0..300u64).collect();
        assert_eq!(applied_set.len(), 300, "no row-id should be reported twice");
        assert_eq!(
            std::mem::take(&mut applied_set),
            expected,
            "every input row-id must appear in the applied set exactly once"
        );
    }

    #[test]
    fn insert_batch_parallel_with_fewer_than_two_rows_runs_sequentially() {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(10),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();

        let BatchInsertOutcome::Ok(applied) =
            index.insert_batch_parallel(vec![(0, vec![1.0, 2.0, 3.0])], 8)
        else {
            panic!("expected Ok");
        };
        assert_eq!(applied, vec![0]);
        let results = index.search(&[1.0, 2.0, 3.0], 1, 50, |_| true).unwrap();
        assert_eq!(results[0].row_id, 0);
    }

    #[test]
    fn insert_batch_parallel_with_one_thread_runs_sequentially() {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(10),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();
        let rows = widely_separated_rows(10);

        let BatchInsertOutcome::Ok(applied) = index.insert_batch_parallel(rows, 1) else {
            panic!("expected Ok");
        };
        let applied_set: HashSet<u64> = applied.into_iter().collect();
        assert_eq!(applied_set, (0..10u64).collect());
    }

    #[test]
    fn insert_batch_parallel_reports_every_other_workers_applied_rows_on_a_dimension_mismatch() {
        // 300 rows across 4 threads with MIN_ROWS_PER_CHUNK=64 gives chunk
        // sizes of 75 each (contiguous) -- the LAST row (299) gets a wrong
        // dimension, so it's the last element processed by the last chunk:
        // every other row (0..299), across every chunk/thread, must still
        // show up in `applied` even though the whole batch's outcome is an
        // error. This is the property design review round 1's critical
        // finding was about: a naive implementation that bails out on the
        // first error/panic during the join loop would lose every OTHER
        // worker's fully-successful applied rows too, not just row 299's.
        //
        // Row 299 itself is ALSO in `applied` here, per a follow-up review
        // finding: a row-id is recorded before its own insert is attempted
        // (not after it succeeds), so `applied` is a conservative superset
        // that can include a row that was actually rejected -- harmless,
        // since `GraphResidueGuard` undoing a never-inserted row-id is a
        // documented no-op (see `BatchInsertOutcome`'s own doc comment).
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(300),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        let mut rows = widely_separated_rows(300);
        rows[299].1 = vec![1.0, 2.0]; // wrong dimension (established dim is 3)

        let outcome = index.insert_batch_parallel(rows, 4);
        let BatchInsertOutcome::IndexError { applied, error } = outcome else {
            panic!("expected IndexError, got {outcome:?}");
        };
        assert!(
            matches!(
                error,
                IndexError::DimensionMismatch {
                    query_len: 2,
                    expected: 3
                }
            ),
            "unexpected error: {error:?}"
        );
        let applied_set: HashSet<u64> = applied.into_iter().collect();
        let expected: HashSet<u64> = (0..300u64).collect();
        assert_eq!(
            applied_set, expected,
            "every row, including the one that failed dimension validation, must be recorded \
             applied (a conservative superset -- see the comment above)"
        );
    }

    #[test]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss
    )] // N/DIM/K are small fixed test constants, well within every cast's exact range here
    fn insert_batch_parallel_recall_matches_sequential_insert_within_tolerance() {
        // Not a safety assertion (every row is findable regardless, per the
        // tests above) -- this is the recall-quality check
        // insert_batch_parallel's own doc comment flags as a known,
        // measured-not-asserted-away risk: Graph::insert's shrink step
        // isn't atomic across its read-compute-clear_matching steps, and a
        // full neighbor layer silently drops a claimed edge, both of which
        // concurrent inserts can trigger more often than a sequential
        // insert. Builds the SAME clustered fixture two ways (sequential
        // insert_owned calls vs. insert_batch_parallel) and compares
        // recall@10 against exact brute-force ground truth for each.
        use crate::brute_force::brute_force_search;
        use arrow::array::{FixedSizeListArray, Float32Array};
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;

        const N: u64 = 500;
        const DIM: usize = 8;
        const K: usize = 10;

        // Deterministic pseudo-random points via a fixed-seed LCG -- no
        // `rand` dependency, matches this module's existing precedent
        // (`insert_cluster`'s golden-ratio/sqrt fractional-part generator)
        // of avoiding a new RNG dependency for test-fixture generation.
        let mut seed: u64 = 0x9E37_79B9;
        let mut next = move || {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            #[allow(clippy::cast_precision_loss)]
            let unit = ((seed >> 11) as f64 / (1u64 << 53) as f64) as f32;
            unit * 10.0 - 5.0
        };
        let vectors: Vec<Vec<f32>> = (0..N).map(|_| (0..DIM).map(|_| next()).collect()).collect();
        let rows: Vec<(u64, Vec<f32>)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, v.clone()))
            .collect();

        let queries: Vec<Vec<f32>> = vectors.iter().take(50).cloned().collect();

        // Ground truth via brute force over a FixedSizeListArray built from
        // the same vectors.
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let flat: Vec<f32> = vectors.iter().flatten().copied().collect();
        let values = Arc::new(Float32Array::from(flat));
        #[allow(clippy::cast_possible_truncation)]
        let vectors_array = FixedSizeListArray::new(item_field, DIM as i32, values, None);
        let ground_truth: Vec<HashSet<usize>> = queries
            .iter()
            .map(|q| {
                brute_force_search(&vectors_array, q, K)
                    .unwrap()
                    .into_iter()
                    .map(|n| n.row_index)
                    .collect()
            })
            .collect();

        let recall = |index: &HnswIndex| -> f64 {
            let mut hits = 0usize;
            for (qi, q) in queries.iter().enumerate() {
                let got: HashSet<usize> = index
                    .search(q, K, TEST_EF_SEARCH, |_| true)
                    .unwrap()
                    .into_iter()
                    .map(|m| m.row_id as usize)
                    .collect();
                hits += got.intersection(&ground_truth[qi]).count();
            }
            hits as f64 / (queries.len() * K) as f64
        };

        let sequential = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(N as usize),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        for (row_id, vector) in rows.clone() {
            sequential.insert_owned(row_id, vector).unwrap();
        }
        let sequential_recall = recall(&sequential);

        let parallel = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(N as usize),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        let BatchInsertOutcome::Ok(_) = parallel.insert_batch_parallel(rows, 4) else {
            panic!("expected Ok");
        };
        let parallel_recall = recall(&parallel);

        // Generous tolerance: this is a smoke check that concurrent
        // shrink/claim races don't grossly degrade recall, not a tight
        // regression pin -- production-scale recall numbers belong in
        // bench/, not a unit test with N=500 8-dim points.
        assert!(
            parallel_recall >= sequential_recall - 0.15,
            "parallel insert's recall ({parallel_recall:.3}) dropped more than the 0.15 \
             tolerance below sequential insert's ({sequential_recall:.3})"
        );
    }

    #[test]
    fn insert_batch_parallel_reports_worker_panicked_and_every_other_applied_row_on_the_multi_worker_path()
     {
        // Exercises the fan-out's own catch_unwind (rows.len() >= 2,
        // threads > 1, multiple chunks) -- the path
        // run_chunks_in_parallel_propagates_a_worker_panic_as_a_join_error
        // already covers the join-conversion step of, but nothing before
        // this test ever drove insert_batch_parallel itself into
        // WorkerPanicked. PANIC_TEST_ROW_ID is the last row of the last
        // chunk (same contiguous-chunking reasoning as the dimension-
        // mismatch test above), so every other row across every chunk must
        // still appear in applied.
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(300),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        let mut rows = widely_separated_rows(300);
        rows[299].0 = PANIC_TEST_ROW_ID;

        let outcome = index.insert_batch_parallel(rows, 4);
        let BatchInsertOutcome::WorkerPanicked {
            applied,
            payload: _,
        } = outcome
        else {
            panic!("expected WorkerPanicked, got {outcome:?}");
        };
        let applied_set: HashSet<u64> = applied.into_iter().collect();
        let mut expected: HashSet<u64> = (0..299u64).collect();
        expected.insert(PANIC_TEST_ROW_ID);
        assert_eq!(
            applied_set, expected,
            "every row from every chunk, including the one that panicked, must still be \
             recorded applied -- a worker's panic must never cost visibility into what every \
             OTHER worker already committed to the shared graph"
        );
    }

    #[test]
    fn insert_batch_parallel_reports_worker_panicked_on_the_sequential_degenerate_path() {
        // Exercises run_sequential's catch_unwind, the fix for the gap
        // design review found in the reconciled commit: rows.len() < 2 (and
        // by the same code path, threads <= 1 and the MIN_ROWS_PER_CHUNK
        // single-chunk fold) run insert_owned_chunk directly on the calling
        // thread, with no fan-out at all. Before the fix, a panic here
        // unwound straight out of insert_batch_parallel and dropped
        // `applied` -- exactly the hazard GraphResidueGuard exists to
        // prevent -- for what is actually the COMMON case (every commit
        // under MIN_ROWS_PER_CHUNK rows).
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(10),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();

        let outcome =
            index.insert_batch_parallel(vec![(PANIC_TEST_ROW_ID, vec![1.0, 2.0, 3.0])], 8);
        let BatchInsertOutcome::WorkerPanicked {
            applied,
            payload: _,
        } = outcome
        else {
            panic!("expected WorkerPanicked, got {outcome:?}");
        };
        assert_eq!(
            applied,
            vec![PANIC_TEST_ROW_ID],
            "the panicking row must still be recorded applied -- it's marked applied \
             BEFORE insert_owned runs, precisely so a panic mid-insert can't lose it"
        );
    }

    #[test]
    fn invisible_row_is_never_returned_even_as_the_true_nearest_neighbor() {
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        // Near cluster: row-ids 0..15, within a 0.01-wide cube around
        // (0,0,0). Far cluster: row-ids 15..30, within a 0.01-wide
        // cube around (1000,0,0).
        insert_cluster(&index, 0, 15, [0.0, 0.0, 0.0], 0.01);
        insert_cluster(&index, 15, 15, [1000.0, 0.0, 0.0], 0.01);
        // Row 0 is the exact-match true nearest neighbor; mark it invisible
        // (the caller-side equivalent of tombstoning it).
        let invisible: HashSet<u64> = HashSet::from([0]);

        // Visibility exclusion happens inside hnsw_rs's own traversal-level
        // filter (not a Rust-side post-filter on an already-capped top-k),
        // so asking for exactly 5 candidates is enough to get 5 live
        // results even though the true nearest neighbor is invisible — no
        // "ask for one extra" compensation needed.
        let results = index
            .search(&[0.0, 0.0, 0.0], 5, TEST_EF_SEARCH, |id| {
                !invisible.contains(&id)
            })
            .unwrap();
        assert_eq!(
            results.len(),
            5,
            "the near cluster has 14 live rows left after excluding row 0, all vastly \
             closer than the far cluster, so the top 5 must still be fully populated: {results:?}"
        );
        assert!(
            results.iter().all(|r| r.row_id != 0),
            "the invisible row must be excluded, not just re-ranked: {results:?}"
        );
        assert!(
            results.iter().all(|r| r.row_id < 15),
            "every returned row must still be a genuine near-cluster neighbor, \
             not a fallback to the far cluster: {results:?}"
        );
    }

    #[test]
    fn invisibility_of_the_single_nearest_neighbor_still_returns_k_live_results_for_small_k() {
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        // Near cluster: row-ids 0..15, within a 0.01-wide cube around
        // (0,0,0). Far cluster: row-ids 15..30, within a 0.01-wide
        // cube around (1000,0,0).
        insert_cluster(&index, 0, 15, [0.0, 0.0, 0.0], 0.01);
        insert_cluster(&index, 15, 15, [1000.0, 0.0, 0.0], 0.01);
        // Row 0 is the exact-match true nearest neighbor; mark it
        // invisible. Under the old design, `hnsw_rs`'s unfiltered
        // `Hnsw::search(query, 1, ef)` would return exactly one raw
        // candidate — row 0, the unambiguous nearest — and post-filtering
        // it out afterward would leave *zero* results even though 14 live
        // near-cluster rows exist. Pushing the exclusion into hnsw_rs's own
        // traversal-level filter (via `search_filter`) means row 0 is never
        // considered a candidate in the first place, so the true
        // next-nearest *live* neighbor is found instead.
        let invisible: HashSet<u64> = HashSet::from([0]);

        let results = index
            .search(&[0.0, 0.0, 0.0], 1, TEST_EF_SEARCH, |id| {
                !invisible.contains(&id)
            })
            .unwrap();
        assert_eq!(
            results.len(),
            1,
            "an invisible true-nearest-neighbor must not shrink the result \
             count below k when enough live candidates exist deeper in the \
             graph: {results:?}"
        );
        assert_ne!(
            results[0].row_id, 0,
            "the invisible row must never be returned: {results:?}"
        );
        assert!(
            results[0].row_id < 15,
            "the returned row must be a genuine near-cluster neighbor, not a \
             fallback to the far cluster: {results:?}"
        );
    }

    #[test]
    fn search_filtered_only_returns_ids_in_the_live_set() {
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        // Near cluster: row-ids 0..15, within a 0.01-wide cube around
        // (0,0,0) — much closer to the query than the far cluster, but
        // excluded from the live set below.
        insert_cluster(&index, 0, 15, [0.0, 0.0, 0.0], 0.01);
        // Far cluster: row-ids 15..30, within a 0.01-wide cube around
        // (1000,0,0).
        insert_cluster(&index, 15, 15, [1000.0, 0.0, 0.0], 0.01);

        // Only the far cluster is "live" per the caller's predicate, even
        // though every near-cluster row is far closer to the query.
        let live_ids: Vec<usize> = (15..30).collect();
        let results = index
            .search_filtered(&[0.0, 0.0, 0.0], 3, TEST_EF_SEARCH, &live_ids, |_| true)
            .unwrap();
        assert_eq!(results.len(), 3, "unexpected results: {results:?}");
        assert!(
            results.iter().all(|r| r.row_id >= 15),
            "search_filtered must only return ids from the live set, even when \
             closer points exist outside it: {results:?}"
        );
    }

    #[test]
    fn search_filtered_live_with_a_prebuilt_live_set_matches_search_filtered() {
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        insert_cluster(&index, 0, 15, [0.0, 0.0, 0.0], 0.01);
        insert_cluster(&index, 15, 15, [1000.0, 0.0, 0.0], 0.01);

        let live_ids: Vec<usize> = (15..30).collect();
        let via_ids = index
            .search_filtered(&[0.0, 0.0, 0.0], 3, TEST_EF_SEARCH, &live_ids, |_| true)
            .unwrap();
        let live_set = crate::live_set::LiveSet::from_row_ids(&live_ids);
        let via_live_set = index
            .search_filtered_live(&[0.0, 0.0, 0.0], 3, TEST_EF_SEARCH, &live_set, |_| true)
            .unwrap();
        assert_eq!(
            via_ids, via_live_set,
            "search_filtered and search_filtered_live must agree given an \
             equivalent live set"
        );
    }

    #[test]
    fn search_filtered_excludes_invisible_rows_even_for_the_single_nearest_live_id() {
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        // Near cluster: row-ids 0..15, within a 0.01-wide cube around
        // (0,0,0). Far cluster: row-ids 15..30, within a 0.01-wide
        // cube around (1000,0,0).
        insert_cluster(&index, 0, 15, [0.0, 0.0, 0.0], 0.01);
        insert_cluster(&index, 15, 15, [1000.0, 0.0, 0.0], 0.01);
        // Row 0 is the exact-match true nearest neighbor among the
        // near-cluster live set; mark it invisible. Visibility exclusion
        // is composed into the same `FilterT` predicate as the `live_ids`
        // membership check, so both are applied during hnsw_rs's own
        // traversal — not as a Rust-side post-filter that could silently
        // return fewer than k live results.
        let invisible: HashSet<u64> = HashSet::from([0]);

        // Every near-cluster row is "live" per the caller's predicate;
        // only the invisibility marker should exclude row 0.
        let live_ids: Vec<usize> = (0..15).collect();
        let results = index
            .search_filtered(&[0.0, 0.0, 0.0], 1, TEST_EF_SEARCH, &live_ids, |id| {
                !invisible.contains(&id)
            })
            .unwrap();
        assert_eq!(
            results.len(),
            1,
            "an invisible true-nearest live id must not shrink the result \
             count below k when enough other live candidates exist: {results:?}"
        );
        assert_ne!(
            results[0].row_id, 0,
            "the invisible row must never be returned, even though it is \
             in the live set: {results:?}"
        );
        assert!(
            results[0].row_id < 15,
            "the returned row must be a genuine near-cluster neighbor, not a \
             fallback to the far cluster: {results:?}"
        );
    }

    #[test]
    fn search_reports_squared_l2_distance_not_plain_l2() {
        // `anndists::DistL2::eval` returns true (non-squared) Euclidean
        // distance for f32 — verified against the installed
        // `anndists-0.1.5` source. A single point lets us hand-compute the
        // exact expected value and catch a regression to plain L2, which a
        // relative-ordering-only test (as above) cannot: a 3-4-5 triangle
        // gives distance 5.0 but squared distance 25.0, and those two
        // values are different enough that a `sqrt` vs. no-`sqrt` bug
        // can't accidentally pass.
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]).unwrap();

        let results = index
            .search(&[3.0, 4.0, 0.0], 1, TEST_EF_SEARCH, |_| true)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_id, 0);
        #[allow(clippy::float_cmp)]
        {
            assert_eq!(
                results[0].squared_distance, 25.0,
                "expected squared L2 distance (3^2 + 4^2 = 25), not plain L2 \
                 distance (sqrt(25) = 5): {results:?}"
            );
        }
    }

    #[test]
    fn new_rejects_max_nb_connection_above_256() {
        let result = HnswIndex::new(
            MaxConnections(257),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(200),
        );
        assert!(matches!(
            result,
            Err(IndexError::MaxConnectionTooLarge(257))
        ));
    }

    #[test]
    fn search_errors_on_dimension_mismatch() {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]).unwrap();

        let result = index.search(&[0.0, 0.0], 1, 50, |_| true);
        assert!(matches!(
            result,
            Err(IndexError::DimensionMismatch {
                query_len: 2,
                expected: 3
            })
        ));
    }

    #[test]
    fn insert_errors_on_dimension_mismatch_with_previously_inserted_vectors() {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]).unwrap();

        let result = index.insert(1, &[0.0, 0.0]);
        assert!(matches!(
            result,
            Err(IndexError::DimensionMismatch {
                query_len: 2,
                expected: 3
            })
        ));
    }

    #[test]
    fn established_dimension_is_zero_before_any_insert() {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();
        assert_eq!(index.established_dimension(), 0);
    }

    #[test]
    fn established_dimension_reflects_the_first_inserted_vectors_length() {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]).unwrap();
        assert_eq!(index.established_dimension(), 3);
    }
}
