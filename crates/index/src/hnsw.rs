//! HNSW vector index — lock-free, from-scratch implementation (replacing
//! `hnsw_rs` as of this rewrite). See `.claude/rules/vector-index.md` and
//! `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md`.

use crate::distance::L2;
use crate::graph::Graph;
use crate::live_set::LiveSet;

/// Below this, per-thread overhead (spawn cost, cache-cold start) isn't
/// worth it relative to a few more sequential inserts on one thread -- e.g.
/// a 3-row batch across 8 available cores should not spawn 3 threads for 1
/// row each. Inherited from this crate's pre-S1 parallel-insert design
/// (measured/tuned there, not re-derived here); see
/// `crates/txn/src/dataset.rs`'s call site for the current thread-count
/// policy and its own tuning notes.
const MIN_ROWS_PER_CHUNK: usize = 64;

/// Outcome of attempting to run one chunk on [`run_chunks_in_parallel`]'s
/// spawned-thread fan-out: either a thread was created and ran to
/// completion (successfully, or via a caught panic), or the OS refused to
/// create the thread at all. The latter is reported separately rather than
/// folded into `std::thread::Result`'s panic-payload shape -- there is no
/// real unwind to represent, so synthesizing one would be misleading.
enum ChunkOutcome<R> {
    Ran(std::thread::Result<R>),
    // Never constructed by the `#[cfg(loom)]` variant of
    // `run_chunks_in_parallel` (it runs everything inline, nothing to
    // spawn), only by the real-thread `#[cfg(not(loom))]` one.
    #[cfg_attr(loom, allow(dead_code))]
    SpawnFailed(std::io::Error),
}

/// Runs `f` over every element of `chunks`, returning each one's outcome in
/// input order. Extracted as its own function (rather than inlined in
/// [`HnswIndex::insert_batch_parallel`]) so this exact fan-out pattern can
/// be reused by any other batch-parallel caller.
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
///
/// Uses `std::thread::Builder::spawn_scoped` (which returns `io::Result`),
/// not `Scope::spawn` (which unwraps internally and panics if the OS
/// refuses to create the thread) -- `.claude/rules/concurrency-txn-layer.md`
/// documents `ERROR_NO_SYSTEM_RESOURCES` under thread pressure as a real,
/// observed risk in this project's own dev environment, and this runs on
/// the commit path. A refused spawn is reported as [`ChunkOutcome::SpawnFailed`]
/// rather than panicking or silently dropping the chunk.
///
/// Real OS threads are unsupported here under `--cfg loom`: `Graph::insert`
/// (reached through `f`) uses `loom::sync::atomic`/`loom::thread_local!`
/// internals in a loom-instrumented build, which only function correctly
/// inside loom's own deterministic scheduler -- a real OS thread executing
/// them outside it is undefined. This crate's loom coverage of concurrent
/// `Graph::insert` lives in `graph.rs`'s own `loom_tests` module, calling
/// the primitive directly rather than through this orchestration layer;
/// see that module for the shrink-step-race model. Gated structurally with
/// `#[cfg(loom)]` here rather than left to `MIN_ROWS_PER_CHUNK` sizing to
/// make it incidentally unreachable.
#[cfg(not(loom))]
fn run_chunks_in_parallel<T: Send, R: Send>(
    chunks: Vec<T>,
    f: impl Fn(T) -> R + Sync,
) -> Vec<ChunkOutcome<R>> {
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            handles.push(std::thread::Builder::new().spawn_scoped(scope, || f(chunk)));
        }
        handles
            .into_iter()
            .map(|h| match h {
                Ok(handle) => ChunkOutcome::Ran(handle.join()),
                Err(e) => ChunkOutcome::SpawnFailed(e),
            })
            .collect()
    })
}

#[cfg(loom)]
fn run_chunks_in_parallel<T: Send, R: Send>(
    chunks: Vec<T>,
    f: impl Fn(T) -> R + Sync,
) -> Vec<ChunkOutcome<R>> {
    chunks
        .into_iter()
        .map(|chunk| ChunkOutcome::Ran(Ok(f(chunk))))
        .collect()
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
    // Produced by `insert_batch_parallel` when the OS refuses to spawn a
    // worker thread for a chunk (`ChunkOutcome::SpawnFailed`) -- segment
    // (de)serialization itself is still entirely in-memory and has no I/O
    // of its own.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("cannot build a segment with no vectors")]
    SegmentEmpty,
    #[error("segment exceeds the format's size limits: {0}")]
    SegmentTooLarge(String),
    #[error("segment is corrupt or was written by an incompatible writer: {0}")]
    SegmentCorrupt(String),
    // Should be structurally unreachable -- see `Graph::insert`'s own doc
    // comment and `run_shrink_retry_loop`'s doc comment in `graph.rs` for
    // the invariant this guards. Surfaced as a typed error (not a panic)
    // so a caller on the commit path gets a normal `Result` to propagate,
    // matching this crate's "typed errors over panics" convention, even
    // though hitting this in practice would mean filing a bug, not
    // handling an expected condition.
    #[error(
        "row {row_id}'s neighbor {neighbor_id} at layer {layer} did not converge to capacity \
         {capacity} after {attempts} shrink attempts -- this should be structurally \
         unreachable, please file a bug"
    )]
    NeighborShrinkDidNotConverge {
        row_id: u64,
        neighbor_id: u64,
        layer: usize,
        capacity: usize,
        attempts: u32,
    },
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
    pub(crate) graph: Graph<L2>,
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
    /// `check_or_establish_dimension` call) so a wrong-length vector can
    /// never reach the distance function. Also propagates
    /// [`IndexError::RowIdOutOfRange`] and (believed unreachable in
    /// practice) [`IndexError::NeighborShrinkDidNotConverge`] straight from
    /// `Graph::insert` — see that method's own `# Errors` section for what
    /// each one means and, for the latter, its partial-mutation semantics.
    pub fn insert(&self, row_id: u64, vector: &[f32]) -> Result<(), IndexError> {
        self.insert_owned(row_id, vector.to_vec())
    }

    /// Same as [`Self::insert`], but takes ownership of `vector` and moves
    /// it straight into the graph instead of cloning a borrowed slice.
    /// `crates/txn`'s per-commit segment builder already owns a
    /// freshly-built `Vec<f32>` at its call site — routing through
    /// `insert`'s `&[f32]` there would force a wasted clone of the full
    /// 512-dim embedding on every insert, on top of the one copy already
    /// paid getting the vector out of Arrow in the first place.
    /// `Graph::insert` moves `vector` into `Node::new` from there, not a
    /// further copy — so this takes the vector from two copies down to one,
    /// not three to two.
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
        // concurrent callers inserting into distinct `HnswIndex` instances
        // (e.g. concurrent commits each building their own per-commit
        // segment) never need any extra synchronization here.
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

    /// Inserts every row in `rows` (`(row_id, vector)` pairs), parallelizing
    /// across up to `threads` worker threads once there's enough work to
    /// make it worth it. Intended for `crates/txn`'s per-commit segment
    /// builder, which owns a brand-new, private `HnswIndex` outside
    /// `commit_lock` and never shares it with a reader until it's fully
    /// built and serialized — see `.claude/rules/vector-index.md`.
    ///
    /// **`rows[0]` is always inserted sequentially, before any worker
    /// thread is spawned for the rest.** `Graph::insert`'s "first node in
    /// the graph" fast path is no longer a bare `EntryPoint::get() == None`
    /// check that two racing threads could both pass — it's
    /// `EntryPoint::claim_if_empty`, a single atomic claim that guarantees
    /// at most one caller ever takes the zero-connections path even when
    /// several threads race a genuinely empty graph concurrently (see that
    /// method's own doc comment). So the empty-graph race this comment used
    /// to describe is now structurally impossible regardless of insertion
    /// order, not just avoided by going first. What inserting `rows[0]`
    /// sequentially still buys is establishing a real, connected entry
    /// point before any worker thread runs, instead of every one of the
    /// `threads` worker threads below independently racing `claim_if_empty`
    /// against each other on their own first row -- fewer wasted claim
    /// attempts, and none of them ever need to handle the `Ok(())`
    /// (first-node) branch of `Graph::insert` at all. A loom model isn't
    /// the right tool for proving this method's own chunking/fan-out
    /// directly: loom explores every interleaving of everything it's
    /// given, and modelling many-rows-per-thread across several threads
    /// would multiply the state space combinatorially — this project's own
    /// experience
    /// elsewhere in this crate is that far smaller additions already blow
    /// loom's exploration budget. The right level of proof is: the
    /// underlying primitive (`Graph::insert`, called concurrently on a
    /// non-empty graph) is loom-proven; this method's own chunking is
    /// covered by ordinary tests instead, the same way this crate's pre-S1
    /// parallel-insert code was.
    ///
    /// Unlike that pre-S1 design, no residue-tracking/undo machinery is
    /// needed here: this index is never reader-visible until it's fully
    /// built, so a failed or panicked build is simply discarded (`NodeTable`'s
    /// `Drop` reclaims everything already inserted) rather than needing to
    /// be surgically unwound row-by-row.
    ///
    /// # Errors
    ///
    /// Returns the first [`IndexError`] observed, from whichever row or
    /// chunk hit it first (no defined order across concurrent chunks) —
    /// takes priority over a worker panic if both occurred in the same call
    /// (a typed, informative error is more useful than a possibly-confusing
    /// panic backtrace when both are available).
    ///
    /// # Panics
    ///
    /// If a worker thread itself panics (a bug, not an expected error
    /// condition) and no `IndexError` was also observed, that panic is
    /// propagated to the caller via [`std::panic::resume_unwind`] once
    /// every other worker has finished joining.
    pub fn insert_batch_parallel(
        &self,
        rows: Vec<(u64, Vec<f32>)>,
        threads: usize,
    ) -> Result<(), IndexError> {
        // A single pass over `rows`' owned `IntoIter` throughout this
        // method (never `Vec::remove(0)`/`Vec::drain(0..take)` in a loop) --
        // both shift every remaining element down on each call, making the
        // original approach O(n) per removal and O(n * chunks) overall.
        let mut rows = rows.into_iter();
        // See this method's own doc comment: this specific row must never
        // race with any other insert on a possibly-still-empty graph.
        let Some((first_id, first_vector)) = rows.next() else {
            return Ok(());
        };
        self.insert_owned(first_id, first_vector)?;
        let rest: Vec<(u64, Vec<f32>)> = rows.collect();
        if rest.is_empty() {
            return Ok(());
        }

        let threads = threads
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        let target_chunk_size = rest.len().div_ceil(threads.max(1)).max(MIN_ROWS_PER_CHUNK);
        if threads <= 1 || target_chunk_size >= rest.len() {
            for (row_id, vector) in rest {
                self.insert_owned(row_id, vector)?;
            }
            return Ok(());
        }

        // Exactly `num_chunks` chunks, each `base` or `base + 1` rows, same
        // as a contiguous split would give -- but MEMBERSHIP is round-robin
        // (row `i` goes to `chunks[i % num_chunks]`), not contiguous
        // blocks. `crates/txn`'s callers hand rows to this method in
        // whatever order a commit's own writes arrived in, which for
        // topic-clustered embeddings (the fixture that originally surfaced
        // hazard #4 above) tends to group same-cluster rows together --
        // contiguous chunking would then hand one whole cluster to one
        // worker, so each worker builds its own region's structure against
        // a graph that's missing every OTHER region for as long as its
        // sibling workers are still concurrently inserting theirs,
        // maximizing exactly the kind of half-built-neighborhood exposure
        // hazard #4 exploits. Round-robin interleaves clusters across
        // workers instead, so no single worker's chunk is one cluster's
        // rows in isolation. This is a defense-in-depth measure alongside
        // the publication-barrier fix above, not a replacement for it --
        // the fix is what makes the hazard structurally impossible;
        // round-robin only reduces how often a caller's row ordering makes
        // it more likely to matter.
        let total = rest.len();
        let num_chunks = total.div_ceil(target_chunk_size).max(1);
        let base = total / num_chunks;
        let remainder = total % num_chunks;
        let mut chunks: Vec<Vec<(u64, Vec<f32>)>> = (0..num_chunks)
            .map(|i| Vec::with_capacity(base + usize::from(i < remainder)))
            .collect();
        for (i, row) in rest.into_iter().enumerate() {
            chunks[i % num_chunks].push(row);
        }

        let results = run_chunks_in_parallel(chunks, |chunk| {
            for (row_id, vector) in chunk {
                self.insert_owned(row_id, vector)?;
            }
            Ok::<(), IndexError>(())
        });

        // Join every worker before deciding anything -- an `IndexError`
        // from one chunk must not be masked by a panic surfacing from a
        // different chunk's join first, and vice versa `IndexError` takes
        // priority when both occurred (see this method's own `# Errors`).
        // A refused thread spawn (`ChunkOutcome::SpawnFailed`) is folded
        // into the same `IndexError` precedence via `IndexError::Io`,
        // rather than panicking or silently dropping that chunk's rows.
        let mut first_index_error = None;
        let mut first_panic = None;
        for result in results {
            match result {
                ChunkOutcome::Ran(Ok(Ok(()))) => {}
                ChunkOutcome::Ran(Ok(Err(e))) => {
                    if first_index_error.is_none() {
                        first_index_error = Some(e);
                    }
                }
                ChunkOutcome::Ran(Err(payload)) => {
                    if first_panic.is_none() {
                        first_panic = Some(payload);
                    }
                }
                ChunkOutcome::SpawnFailed(io_err) => {
                    if first_index_error.is_none() {
                        first_index_error = Some(IndexError::Io(io_err));
                    }
                }
            }
        }
        if let Some(e) = first_index_error {
            return Err(e);
        }
        if let Some(payload) = first_panic {
            std::panic::resume_unwind(payload);
        }
        Ok(())
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
    /// Before S1 W3.2, `crates/txn`'s commit path called this to drop a
    /// transaction's vectors back out of a shared graph when that
    /// transaction failed before its manifest commit. That guarantee is now
    /// provided structurally instead — a commit's vectors live only in its
    /// own per-commit segment, which a failed commit never gets to publish
    /// — so nothing in `crates/txn`'s commit path calls this method
    /// anymore. It remains index-internal API, sound on the same terms it
    /// always was: such a row-id was never committed in *any* version, so
    /// no snapshot should ever observe it, and because row-ids are never
    /// reused (`.claude/docs/design/phase-0-transaction-and-format-spec.md`
    /// §8) — a soft-deleted id can never legitimately reappear.
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
        let filter = build_live_filter_from_live_set(live, is_visible);
        let raw = self.graph.k_nn_search(query, k, ef_search, filter)?;
        Ok(raw
            .into_iter()
            .map(|(row_id, dist)| VectorMatch {
                row_id,
                squared_distance: dist * dist,
            })
            .collect())
    }

    /// Serializes this index into a complete on-disk segment image, in
    /// memory — no file I/O (see `crate::segment_writer`'s module doc for
    /// why the write/fsync lives in `crates/txn` instead).
    ///
    /// This index must be a **fresh, per-commit index keyed by
    /// segment-local ordinals `0..row_ids.len()`**, built by calling
    /// [`Self::insert_owned`] once per vector with `local` as the key —
    /// *not* the dataset's global row-ids. `row_ids[local]` supplies the
    /// global row-id each ordinal stands for, and must be strictly
    /// ascending. See
    /// `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §3b.
    ///
    /// # Errors
    ///
    /// See `crate::segment_writer::encode_segment`: [`IndexError::SegmentEmpty`]
    /// for an empty `row_ids` or an index with no vectors,
    /// [`IndexError::SegmentCorrupt`] for a graph that is not a well-formed
    /// `0..N` keying, [`IndexError::DimensionMismatch`] for a ragged vector,
    /// and [`IndexError::SegmentTooLarge`] if the image would overflow the
    /// format's `u32` fields.
    pub fn to_segment_bytes(&self, row_ids: &[u64]) -> Result<Box<[u8]>, IndexError> {
        crate::segment_writer::encode_segment(
            &self.graph,
            row_ids,
            crate::segment_format::SegmentParams {
                m: self.m,
                mmax0: self.mmax0,
                mmax: self.mmax,
                ef_construction: self.ef_construction,
                m_l: self.m_l,
            },
        )
    }
}

/// Builds the combined live-ids/visibility predicate used by
/// `segment_set::SegmentSet::search_filtered`/`search_filtered_pruned` (the
/// sibling module), which call `k_nn_search_generic` directly rather than
/// through [`HnswIndex`]'s methods — see that module's doc comments for why.
/// Delegates to [`LiveSet`] (the same bitset [`HnswIndex::search_filtered`]
/// builds) rather than duplicating it, so there is exactly one
/// live-row-id-membership implementation in this crate.
///
/// `live_ids` membership and `is_visible` are composed into ONE predicate
/// passed straight into `k_nn_search` -> `search_layer`, applied during
/// traversal-time result-set construction — not a post-filter over an
/// already-capped top-k. This matches `hnsw_rs`'s original FilterT-based
/// behavior exactly: a `live_ids` row deep in the graph is never missed just
/// because it fell outside some pre-guessed widened candidate window, since
/// the predicate is evaluated as part of the same search, not after it.
///
/// `live_ids` need not be sorted: the bitset is order-insensitive.
pub(crate) fn build_live_filter(
    live_ids: &[usize],
    is_visible: impl Fn(u64) -> bool,
) -> impl Fn(u64) -> bool {
    let live = LiveSet::from_row_ids(live_ids);
    move |id: u64| live.contains(id) && is_visible(id)
}

/// As [`build_live_filter`], but for a caller that already has a built
/// [`LiveSet`] in hand (e.g. `crates/txn`'s per-`(Snapshot, Predicate)`
/// cache) and wants to avoid rebuilding the bitset from a raw `&[usize]` on
/// every call — see `HnswIndex::search_filtered_live`'s doc comment for the
/// same tradeoff on the `HnswIndex`-method side.
pub(crate) fn build_live_filter_from_live_set<'a>(
    live: &'a LiveSet,
    is_visible: impl Fn(u64) -> bool + 'a,
) -> impl Fn(u64) -> bool + 'a {
    move |id: u64| live.contains(id) && is_visible(id)
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

    /// Widely-separated points (1000 units apart, `TEST_EF_CONSTRUCTION`-scale
    /// graphs use sub-unit clusters elsewhere in this file), so a query at
    /// row `i`'s exact coordinates is unambiguously nearest to row `i`
    /// regardless of graph *shape* -- which is exactly what varies between a
    /// sequential and a parallel insert of the same rows.
    #[allow(clippy::cast_precision_loss)] // row-ids here are always < 300, far under f32's exact-integer ceiling
    fn widely_separated_rows(count: u64) -> Vec<(u64, Vec<f32>)> {
        (0..count)
            .map(|i| (i, vec![i as f32 * 1000.0, 0.0, 0.0]))
            .collect()
    }

    #[test]
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)] // row-ids/miss-counts/N here are always small (<= 500), far under any of these casts' precision ceiling
    fn insert_batch_parallel_keeps_every_row_findable_across_multiple_threads() {
        // PRODUCTION parameters (`HNSW_MAX_NB_CONNECTION`/`HNSW_EF_CONSTRUCTION`
        // in `crates/txn/src/dataset.rs`), not this file's generous
        // `TEST_*` constants -- this is the one test in this module meant
        // to reflect what actually ships, not a best-case config. An
        // earlier version of this test tolerated a nonzero miss rate here,
        // attributed at the time to `Graph::insert`'s neighbor-shrink step.
        // That attribution turned out to be wrong: instrumentation over
        // 1120 real commits showed the shrink body barely executes
        // (0.171/commit) and its retry path never fires, while the actual
        // dominant mechanism -- a node published to `NodeTable` before its
        // own edges exist, picked as a concurrent insert's descent entry --
        // is now fixed (`Node::is_published`/`mark_published` plus the
        // publication guard on both of `Graph::insert`'s descent loops, see
        // that method's own doc comment) and verified end to end (0/800
        // real commits of a clustered fixture through the actual commit
        // path). This test now asserts EXACT recall (0 misses), not a
        // bounded rate: fresh measurement post-fix (50 runs of this exact
        // fixture -- 30 idle, 20 under 4x concurrent CPU load) landed at
        // 0/500 every single time, so any miss here is a real regression,
        // not an expected residual.
        //
        // A prior version of this test used `widely_separated_rows` and
        // asserted a fixed 15% ceiling based on a 5-run sample. Review
        // (independently reproduced: ~215 runs on a loaded 12-core
        // machine) found that fixture's miss-count distribution is
        // heavy-tailed -- typically 0-14/300 but occasionally 50-59/300
        // (16.7%-19.7%), because widely-separated points get pruned to
        // very few edges each by the diversity heuristic, so losing even
        // one edge can fully disconnect a node with no redundant path
        // back to it. That made the assertion genuinely flaky (~4% of
        // runs on a busy machine), not just conservative. Switched to the
        // same realistic uniform-random fixture the recall test below
        // uses (matching this crate's pre-S1 methodology, commit
        // `3697ba8`): dense, non-degenerate points give every node several
        // redundant edges, matching the realistic-data behavior
        // `insert_batch_parallel_recall_matches_sequential_insert_within_tolerance`
        // measures via ground-truth recall@10 instead of self-retrieval.
        fn test_unif(seed: u64) -> f64 {
            let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
            x ^= x >> 33;
            x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
            x ^= x >> 33;
            ((x >> 11) as f64 / (1u64 << 53) as f64).clamp(f64::EPSILON, 1.0 - f64::EPSILON)
        }
        fn dense_rows(count: u64, dim: usize) -> Vec<(u64, Vec<f32>)> {
            (0..count)
                .map(|i| {
                    let vector = (0..dim)
                        .map(|d| {
                            let seed = i.wrapping_mul(dim as u64).wrapping_add(d as u64);
                            (test_unif(seed.wrapping_add(7)) * 10.0) as f32
                        })
                        .collect();
                    (i, vector)
                })
                .collect()
        }

        const N: u64 = 500;
        const DIM: usize = 8;
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(N as usize),
            MaxLayers(16),
            EfConstruction(100),
        )
        .unwrap();
        let rows = dense_rows(N, DIM);

        index.insert_batch_parallel(rows.clone(), 4).unwrap();

        let mut misses = 0u64;
        for (row_id, vector) in &rows {
            let results = index.search(vector, 1, 32, |_| true).unwrap();
            if results.first().map(|m| m.row_id) != Some(*row_id) {
                misses += 1;
            }
        }
        assert_eq!(
            misses, 0,
            "{misses}/{N} rows were not their own nearest neighbor after a production- \
             parameter parallel-insert build -- the clustered-data recall hazard this \
             fixture predates is now fixed and verified closed (see this test's own \
             doc comment), so any miss here is a real regression"
        );
    }

    /// The specific hazard a naive fan-out (no mandatory sequential first
    /// insert) would risk: on a genuinely empty graph, two+ threads racing
    /// to be first could both take `Graph::insert`'s "first node" fast path
    /// and skip connection-building entirely, leaving all but one of them
    /// permanently unreachable via graph traversal. This is now
    /// structurally impossible regardless of insertion order --
    /// `EntryPoint::claim_if_empty` makes "am I first" a single atomic
    /// claim (see that method's own doc comment) -- but `insert_batch_parallel`
    /// still inserts the first row sequentially before spawning any worker
    /// (see that method's own doc comment for why: fewer wasted claim
    /// attempts, not correctness). So this is a stress/regression test for
    /// both layers together, not a proof the way a loom model would be --
    /// see the method's own doc comment for why a loom model isn't the
    /// right tool here (the existing `graph.rs` loom coverage, including
    /// `concurrent_inserts_into_a_genuinely_empty_graph_never_strand_a_node_loom`,
    /// already proves the underlying primitive sound). Every one of many
    /// threads' rows must be reachable, not just present in the table.
    ///
    /// Uses generous-but-NOT-this-file's-`TEST_*`-scale connectivity
    /// parameters, and deliberately NOT production ones either -- unlike
    /// `insert_batch_parallel_keeps_every_row_findable_across_multiple_threads`
    /// above (which now also asserts exact 0 misses, both hazards it used
    /// to tolerate being fixed), this test isolates the empty-graph race
    /// from ordinary construction noise by using connectivity generous
    /// enough that no other cause could plausibly produce a miss, so any
    /// miss here specifically implicates the empty-graph-race fix. Review
    /// measured this file's shared `TEST_MAX_NB_CONNECTION`/
    /// `TEST_EF_CONSTRUCTION` (200/1600) at 500 rows costing 64s of this
    /// crate's ~8s otherwise -- neither is load-bearing for the empty-graph
    /// race itself, which only needs (a) enough rows for the parallel path
    /// to actually fan out across multiple real chunks (`MIN_ROWS_PER_CHUNK
    /// = 64`, so >128 rows guarantees at least 2 chunks after the mandatory
    /// first sequential insert) and (b) connectivity generous enough to
    /// guarantee zero misses (this test asserts EXACT reachability for
    /// every single row, not a bounded miss rate) -- not 500 rows' worth.
    /// A first attempt at cutting cost by ALSO dialing connectivity down to
    /// this file's "production-realistic" scale (`MaxConnections(16)`/
    /// `EfConstruction(100)`, used by the recall/findability tests above)
    /// broke correctness outright at 150 widely-separated rows -- widely-
    /// separated points get pruned to very few edges each by the diversity
    /// heuristic, so a single lost edge can fully disconnect a node with no
    /// redundant path back to it, which is no longer specific to the
    /// empty-graph race this test isolates. Keeping this file's original
    /// generous `TEST_*` connectivity constants (proven to guarantee zero
    /// misses) but cutting the row count from 500 to 150 (the minimum
    /// multiple of `MIN_ROWS_PER_CHUNK` that still guarantees >1 real
    /// chunk) keeps that guarantee while measured at 1.40s vs. the
    /// original's 64s -- HNSW insert cost isn't linear in row count at a
    /// fixed `ef_construction` (each new row's own construction-time search
    /// scales with the graph's current size), so the cost drop from a
    /// ~3.3x row-count cut is much more than 3.3x.
    ///
    /// Skips outright below 2 available cores rather than asserting
    /// nothing -- with 1 core, `insert_batch_parallel` degrades to its
    /// sequential fallback and this test would pass without exercising any
    /// real concurrency at all.
    #[test]
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)] // row-ids here are always < 150, far under either cast's precision ceiling
    fn insert_batch_parallel_from_a_genuinely_empty_graph_leaves_no_row_unreachable() {
        const N: u64 = 150;

        if std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get) < 2 {
            eprintln!("skipping: needs >=2 available cores to exercise real concurrency");
            return;
        }
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(N as usize),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        let rows = widely_separated_rows(N);

        index.insert_batch_parallel(rows, 8).unwrap();

        for i in 0..N {
            let query = vec![i as f32 * 1000.0, 0.0, 0.0];
            let results = index.search(&query, 1, TEST_EF_SEARCH, |_| true).unwrap();
            assert_eq!(
                results[0].row_id, i,
                "row {i} must be reachable via graph traversal from the entry point, \
                 not just present in the node table"
            );
        }
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

        index
            .insert_batch_parallel(vec![(0, vec![1.0, 2.0, 3.0])], 8)
            .unwrap();
        let results = index.search(&[1.0, 2.0, 3.0], 1, 50, |_| true).unwrap();
        assert_eq!(results[0].row_id, 0);
    }

    #[test]
    #[allow(clippy::cast_precision_loss)] // row-ids here are always < 10, far under f32's exact-integer ceiling
    fn insert_batch_parallel_with_one_thread_runs_sequentially() {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(10),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();
        let rows = widely_separated_rows(10);

        index.insert_batch_parallel(rows, 1).unwrap();
        for i in 0..10u64 {
            let query = vec![i as f32 * 1000.0, 0.0, 0.0];
            let results = index.search(&query, 1, TEST_EF_SEARCH, |_| true).unwrap();
            assert_eq!(results[0].row_id, i);
        }
    }

    #[test]
    fn insert_batch_parallel_with_an_empty_batch_is_a_no_op() {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(10),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();
        index.insert_batch_parallel(Vec::new(), 8).unwrap();
        assert_eq!(index.established_dimension(), 0);
    }

    #[test]
    fn insert_batch_parallel_surfaces_a_dimension_mismatch_from_any_chunk() {
        // 300 rows across 4 threads (299 after the mandatory first
        // sequential insert) gives 4 chunks of sizes 75/75/75/74 (the even
        // `base`/`remainder` split -- see `insert_batch_parallel`'s own
        // comment) on a host with at least 4 available cores;
        // `insert_batch_parallel` clamps `threads` to
        // `available_parallelism()`, so a <4-core host gets fewer,
        // differently-sized chunks. Chunk MEMBERSHIP is round-robin (rest-
        // index `i` goes to `chunks[i % num_chunks]`), not contiguous, but
        // rows are still pushed onto each chunk's `Vec` in ascending
        // original-index order -- so the LAST row (299, rest-index 298)
        // lands in whichever chunk owns residue `298 % num_chunks`, and is
        // always the LAST element within that chunk's own `Vec` (298 is
        // the largest index in its residue class), regardless of chunk
        // count. That proves a chunk-local error surfaces even when every
        // other chunk succeeds, regardless of core count or chunking
        // scheme.
        let index = HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(300),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
        .unwrap();
        let mut rows = widely_separated_rows(300);
        rows[299].1 = vec![0.0, 0.0]; // wrong dimension (2, not 3)

        let err = index.insert_batch_parallel(rows, 4).unwrap_err();
        assert!(
            matches!(err, IndexError::DimensionMismatch { .. }),
            "expected DimensionMismatch, got {err:?}"
        );
    }

    #[test]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )] // N/DIM/K are small fixed test constants, well within every cast's exact range here
    fn insert_batch_parallel_recall_matches_sequential_insert_within_tolerance() {
        // A prior version of this test measured self-retrieval (query =
        // the row's own vector, check it's in its own top-k) instead of
        // real recall@k against exact ground truth. With exact distance 0
        // that's a reachability check, not a recall measurement, and it
        // saturates at 1.0000 for both arms regardless of any real
        // regression -- review caught that this made the tolerance below
        // untestable. Restored to this crate's pre-S1 methodology (see
        // commit `3697ba8`): brute-force ground truth via
        // `crate::brute_force::brute_force_search` over a genuine
        // `FixedSizeListArray`, then recall@k = fraction of (query, k)
        // ground-truth hits each index's own `search` reproduces -- a
        // metric that can actually move when the shrink-step race drops a
        // real edge, not just when a row becomes fully unreachable.
        use crate::brute_force::brute_force_search;
        use arrow::array::{FixedSizeListArray, Float32Array};
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;

        const N: u64 = 500;
        const DIM: usize = 8;
        const K: usize = 10;
        const EF_SEARCH: usize = 32;

        // Deterministic pseudo-random points via a fixed-seed LCG -- no
        // `rand` dependency, matches this module's existing precedent
        // (`insert_cluster`'s golden-ratio/sqrt fractional-part generator)
        // of avoiding a new RNG dependency for test-fixture generation.
        let mut seed: u64 = 0x9E37_79B9;
        let mut next = move || {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
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
        #[allow(clippy::cast_possible_wrap)]
        // DIM is a fixed test constant (8), nowhere near i32::MAX
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

        // Production parameters end to end (`HNSW_MAX_NB_CONNECTION`/
        // `HNSW_EF_CONSTRUCTION` in `crates/txn/src/dataset.rs`, and
        // `EF_SEARCH_DEFAULT` in `crates/txn/src/snapshot.rs`), not this
        // file's generous `TEST_*` constants -- this test's whole point is
        // to measure what actually ships.
        let recall = |index: &HnswIndex| -> f64 {
            let mut hits = 0usize;
            for (qi, q) in queries.iter().enumerate() {
                let got: HashSet<usize> = index
                    .search(q, K, EF_SEARCH, |_| true)
                    .unwrap()
                    .into_iter()
                    .map(|m| m.row_id as usize)
                    .collect();
                hits += got.intersection(&ground_truth[qi]).count();
            }
            hits as f64 / (queries.len() * K) as f64
        };

        let sequential = HnswIndex::new(
            MaxConnections(16),
            MaxElements(N as usize),
            MaxLayers(16),
            EfConstruction(100),
        )
        .unwrap();
        for (row_id, vector) in rows.clone() {
            sequential.insert_owned(row_id, vector).unwrap();
        }
        let sequential_recall = recall(&sequential);

        let parallel = HnswIndex::new(
            MaxConnections(16),
            MaxElements(N as usize),
            MaxLayers(16),
            EfConstruction(100),
        )
        .unwrap();
        parallel.insert_batch_parallel(rows, 8).unwrap();
        let parallel_recall = recall(&parallel);

        // Measured on this exact configuration (500 rows, 8 dims, k=10, 5
        // runs): sequential recall@10 is a deterministic 1.0000 every run
        // (fixed-seed fixture, sequential order is itself deterministic);
        // parallel recall@10 ranges 0.9900-1.0000, i.e. 0-1 percentage
        // points below sequential. `bench/benches/vector_search_bench.rs`
        // is this crate's dedicated recall-at-realistic-scale benchmark
        // (100K real embeddings, floor asserted at recall@10 > 0.8) -- see
        // `.claude/rules/vector-index.md` for how index-quality tradeoffs
        // like this one are meant to be tracked. The 8-point tolerance
        // below has real headroom above this test's own measured range
        // specifically so this stays a regression *gate*, not a bound this
        // fixture is expected
        // to approach.
        assert!(
            parallel_recall >= sequential_recall - 0.08,
            "parallel-insert recall {parallel_recall:.4} regressed more than 8 \
             percentage points vs sequential-insert recall {sequential_recall:.4}"
        );
    }
}
