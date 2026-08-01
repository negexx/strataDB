# Phase 1 performance audit

**Date:** 2026-08-01

**Lane:** Sol — manifest/recovery scaling, immutable-segment fan-out and residency,
scan/projection/pruning I/O, and benchmark evidence

**Scope:** Current working tree, including its existing dirty changes. This audit read the current
Rust paths and benchmark sources but ran no benchmark or long test suite. It changed no Rust, tests,
dependencies, or configuration. Historical measurements are labelled as such and are not treated as
fresh measurements of this working tree.

## Verdict

**BLOCKED ON PERFORMANCE EVIDENCE — Phase 1 should not exit this lane yet.**

The current mechanisms predict three independent growth curves: each commit deep-clones and rewrites
the accumulated manifest; recovery lists all retained manifest versions and eagerly reads, copies,
and validates every referenced segment; and vector search sequentially visits every unpruned segment
at full per-segment search effort. Historical measurements confirm manifest growth and Arrow IPC read
amplification, but the current segmented implementation has no attributable, retained measurement of
commit latency versus both file and segment count, reopen latency/RSS versus version and segment
count, or production `SegmentSet` latency/RSS versus segment count. That directly misses the roadmap's
Phase 1 exit requirement that manifest/segment growth be measured
(`docs/roadmap.md`, `## Phase 1 — Correctness and durability baseline`; `docs/status.md`,
`Manifest/segment growth and cleanup obligations`).

This is primarily an **evidence and bounding blocker**, not a demand to implement compaction in Phase
1. Phase 1 should record reproducible curves, workload/environment metadata, and an explicit operating
bound. Compaction, retention/GC, lazy or mapped segment residency, and sub-file pruning remain Phase 3
or Phase 2 work as assigned below.

## Findings

### PERF-01 — Current segmented growth has harnesses but no current, retained evidence

- **Measured vs predicted:** Evidence gap. Historical measurements exist; current-tree behavior is
  otherwise statically predicted.
- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 1 exit evidence
- **Disposition:** **Phase 1 blocker.** Run and retain a short controlled matrix before exit; do not
  infer current segmented bounds from pre-segment or unversioned local output.
- **Evidence:**
  - Phase 1 explicitly requires manifest/segment growth to be measured, while the status ledger still
    marks that obligation Partial (`docs/roadmap.md`, `## Phase 1 — Correctness and durability baseline`;
    `docs/status.md`, `Manifest/segment growth and cleanup obligations`).
  - `manifest_growth_bench` is correctly designed as a monotonic sequence and reports bucket latency
    plus newest-manifest bytes, but it is id-only and deliberately creates no index segment
    (`bench/benches/manifest_growth_bench.rs:1-32`, `:43-46`, `:74-101`, `:116-160`).
  - `lifecycle_bench` times commit, reopen, full scan, and filtered scan, but its default ingest is only
    five 5,000-row commits, so it is not a segment/version-count sweep
    (`bench/benches/lifecycle_bench.rs:206-214`, `:227-300`).
  - `segment_recall_bench` sweeps 1–64 graphs, but it builds and searches `HnswIndex` values through a
    benchmark-local merge rather than the production `SegmentReader`/`SegmentSet` path
    (`bench/benches/segment_recall_bench.rs:150-180`, `:202-234`). Its “exactly what Strata builds
    today” wording is stale under the immutable-segment baseline (`:10-17`, `:192-195`).
  - The only inspected local Criterion vector-search artifacts are dated 2026-07-25 under
    `target/criterion/vector_search/...`; no manifest/recovery/segment-count Criterion artifact exists
    there, and generated `target/` output is neither attributable to this dirty tree nor retained
    project evidence.

Minimum useful evidence is a small matrix over commit count and segment count that records p50/p95
commit latency, newest and total manifest bytes, reopen wall time, steady/transient RSS, and query
latency. Include the exact revision/dirty state, hardware, filesystem, row count, vector dimension,
batch size, and warm/cold-cache condition.

### PERF-02 — Manifest publication is O(history) per commit and O(commits²) cumulatively

- **Measured vs predicted:** **Historically measured; current segmented magnitude predicted.**
- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 1 bound/evidence; Phase 3 lifecycle implementation
- **Disposition:** **Phase 1 blocker for a current segmented curve and documented operating bound.**
  Keep incremental manifests/checkpoints and retained-version GC in Phase 3 unless the measurement
  forces an earlier design decision.
- **Evidence:**
  - `Manifest` owns accumulated `Vec`s for every data file, tombstone, and immutable segment
    (`crates/storage/src/manifest.rs:110-165`).
  - Under the global commit lock, each successful commit deep-clones the latest manifest, appends its
    entries, serializes the complete structure to JSON, and durably writes the complete new version
    (`crates/txn/src/dataset.rs:945-986`, `:1034-1082`, `:1113-1136`;
    `crates/storage/src/manifest.rs:193-216`).
  - The in-memory segment-list append separately copies all existing part handles, explicitly
    documenting O(parts) per commit and O(parts²) per session
    (`crates/index/src/segment_set.rs:103-134`).
  - The retained historical id-only benchmark measured mean commit latency rising from 12.2 ms at
    commits 0–299 to 39.5 ms at 5700–5999 and a newest manifest of 867,869 bytes at 6,000 files
    (`docs/history/analysis/2026-07-23-complexity-audit.md:410-441`).
  - That measurement omitted `SegmentEntry` and its zone map; the later segmented audit therefore
    labelled the real vector-workload crossover an estimate, not a measurement
    (`docs/history/analysis/2026-07-26-full-pipeline-performance-audit.md:441-461`).

The historical result confirms the complexity mechanism, not the current scale threshold. Segment
metadata, tombstones, filesystem behavior, and this dirty source can move that threshold materially.

### PERF-03 — Recovery scales with both retained versions and total resident segment bytes

- **Measured vs predicted:** **Static prediction; no current N-version/N-segment reopen curve.**
- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 1 recovery bound/evidence; Phase 3 retention and residency
- **Disposition:** **Phase 1 blocker for measurement and a supported bound.** Put manifest retention,
  lazy/mapped loading, parallel read/validation, and cleanup implementation in Phase 3.
- **Evidence:**
  - `read_current` lists and parses every `_versions/*.manifest` name before reading and deserializing
    the largest version (`crates/storage/src/manifest.rs:219-256`). With no version GC, opener metadata
    work is O(retained versions).
  - `Dataset::open` immediately calls `load_segments`, before returning a usable handle
    (`crates/txn/src/dataset.rs:426-445`).
  - `load_segments` visits segment entries serially, reads every complete file, validates it, creates a
    resident reader, and retains all readers in the returned set
    (`crates/txn/src/dataset.rs:1721-1807`).
  - `SegmentReader::from_bytes` copies each `fs::read` buffer into its own 64-byte-aligned owned
    allocation before its linear validation passes (`crates/index/src/segment_reader.rs:69-120`,
    `:169-230`). Steady index residency is therefore at least the complete segment images; transient
    open memory additionally includes the current raw read buffer and aligned copy.
  - `lifecycle_bench` correctly identifies reopen as segment load without graph reconstruction, but
    measures one default five-segment point rather than a scaling curve
    (`bench/benches/lifecycle_bench.rs:227-269`).

The S1 recovery improvement is real—there is no HNSW rebuild—but “O(bytes)” is not yet an operational
bound. Version-directory enumeration, segment count, total bytes, validation CPU, open-file I/O, and
peak RSS all need separate columns in the Phase 1 evidence.

### PERF-04 — One segment per vector commit makes unpruned query work grow with commit count

- **Measured vs predicted:** **Current static mechanism; historical fan-out measurements are not on
  the production reader path.**
- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 1 growth bound; Phase 3 compaction/index lifecycle
- **Disposition:** **Phase 1 blocker for production-path measurement and an explicit maximum supported
  fan-out.** Compaction is Phase 3; do not batch or defer acknowledgement to hide this curve.
- **Evidence:**
  - The production contract is one immutable segment per vector-carrying commit, and every unpruned
    segment is queried for its own top-k at the caller's full `ef_search`
    (`crates/index/src/segment_set.rs:1-17`).
  - Production `fan_out` is a serial loop. It accumulates up to segment_count × k candidates, then
    sorts, hashes for deduplication, and truncates (`crates/index/src/segment_set.rs:163-217`).
  - `with_appended` states that segment count is tolerated now and expected to be bounded by later
    compaction; it also correctly warns that deferred/batched publication would violate the
    no-silent-buffering invariant (`crates/index/src/segment_set.rs:103-134`).
  - The roadmap places compaction, bounded history, and index lifecycle in Phase 3, while Phase 1 must
    measure and document the obligation (`docs/roadmap.md`, `## Phase 1 — Correctness and durability baseline`;
    `## Phase 3 — Operational lifecycle`).
  - The existing sweep uses `HnswIndex` plus benchmark-local merge, not serialized
    `SegmentReader`/production `SegmentSet`, so it cannot bound parser layout, resident bytes, zone-map
    payloads, production dedup, or current inner-loop costs
    (`bench/benches/segment_recall_bench.rs:150-168`, `:202-234`).

Report both all-segments and pruned-segment cases. A single total-row-count sweep is insufficient:
hold rows fixed while varying segments, then hold rows/segment fixed while increasing segments.

### PERF-05 — Segment residency is eager, snapshot-pinned, and not represented by current benchmarks

- **Measured vs predicted:** **Static prediction.**
- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Phase 1 memory bound; Phase 3 lifecycle
- **Disposition:** Measure steady and peak RSS in Phase 1; evaluate lazy load/mmap and snapshot-aware
  reclamation with Phase 3 compaction/retention.
- **Evidence:**
  - Every `IndexPart::Sealed` retains an `Arc<SegmentReader>` and zone-map payload; `SegmentSet` retains
    the entire parts slice behind an `Arc` (`crates/index/src/segment_set.rs:60-77`).
  - A `SegmentReader` owns the complete aligned file image for its lifetime
    (`crates/index/src/segment_reader.rs:69-93`).
  - Opening eagerly populates every part (`crates/txn/src/dataset.rs:1721-1807`), and appending a commit
    preserves all old readers while adding the new one (`crates/index/src/segment_set.rs:103-134`).
  - The benchmark closest to a residency sweep measures live `HnswIndex` construction, not sealed
    reader residency (`bench/benches/segment_recall_bench.rs:202-220`); the lifecycle harness reports
    only one default five-segment point (`bench/benches/lifecycle_bench.rs:206-269`).

Old snapshots intentionally pin old segment-set views. That is a correctness strength, but any future
cleanup policy must account for those pins; disk compaction alone does not define when resident or
source segment images can be reclaimed.

### PERF-06 — Public scans have file pruning but no projection pushdown and no sub-file pruning

- **Measured vs predicted:** **Static prediction for public scan; benchmark coverage gap.**
- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Phase 2 query/usability; Phase 3 physical layout if sub-file pruning is adopted
- **Disposition:** Later-phase implementation, but add a truthful Phase 1/2 baseline now. Benchmark
  projected, fully-pruned, partially-pruned, and unprunable scans separately.
- **Evidence:**
  - `read_surviving_files` can prune whole files from manifest stats and has an internal projected-read
    branch (`crates/txn/src/snapshot.rs:156-197`).
  - Public `scan` always reads every file and all columns, while `scan_with_predicate` prunes files but
    still reads all columns, casts the complete surviving batch, and only then filters rows
    (`crates/txn/src/snapshot.rs:233-249`, `:285-304`). No public projected scan routes to the internal
    `columns` branch.
  - Data files contain one record batch, so the stats decision has file/commit granularity only
    (`crates/storage/src/datafile.rs:21-36`, `:86-143`).
  - The lifecycle “filtered scan” does not exercise pruning under its default data shape: category is
    `id % 10`, each data file is a contiguous 5,000-row chunk, and the predicate is category = 3, so
    every file's min/max spans the predicate
    (`bench/benches/lifecycle_bench.rs:120-150`, `:207-214`, `:227-300`).

The existing pruning mechanism is useful and safe, but the current lifecycle timing labelled
“predicate pushdown + row filter” is effectively an all-files-read filter case. It cannot establish
pruning benefit or a projected-scan bound.

### PERF-07 — Arrow IPC projection avoids array construction, not the dominant file-body read

- **Measured vs predicted:** **Historically measured and current-source-confirmed; documentation is
  internally contradictory.**
- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Documentation correction now; Phase 2 query path; Phase 3 physical layout
- **Disposition:** Correct the `datafile.rs` claim independently of any optimization. Keep the live-set
  cache; benchmark cold distinct predicates. Treat true byte-range projection as a format/layout task,
  not as an accomplished property.
- **Evidence:**
  - `read_batch_columns` opens the Arrow IPC file twice and passes a column projection to
    `FileReader` (`crates/storage/src/datafile.rs:156-199`). Its comment claims unselected column
    bodies are never touched and contrasts ~204 MB with ~1.6 MB (`:159-171`).
  - The current caller documents the opposite measured behavior: Arrow reads the whole contiguous
    record-batch body before projection; projection saved only ~2 ms of ~109 ms while ~205 MB was read
    on each uncached call (`crates/txn/src/snapshot.rs:409-439`).
  - The retained measurement found 3.90 s / 2559.5 MB for 50 repeated filtered queries before caching,
    then 79.82 ms / 51.3 MB after caching; nine distinct predicates cost 457.42 ms / 460.7 MB, matching
    one full body read per cold predicate
    (`docs/history/analysis/2026-07-25-filtered-vector-search-memory-audit.md:168-192`).
  - The current per-snapshot predicate cache correctly amortizes identical predicates but does not
    change the cold-read amplification (`crates/txn/src/snapshot.rs:359-428`).

This is the clearest measured-versus-predicted lesson in the lane: logical column projection was real,
but the static prediction that it would proportionally reduce I/O was false. Current decisions should
use the measured whole-body behavior until a different reader or physical layout proves otherwise.

## Strengths

- **Recovery no longer rebuilds HNSW.** `Dataset::open` validates immutable segment images without
  distance evaluation or graph construction (`crates/txn/src/dataset.rs:394-403`, `:1721-1807`). This
  removes the former algorithmic recovery bottleneck even though open remains unbounded in bytes.
- **Publication correctness is not traded for throughput.** Segment creation occurs before the lock,
  but manifest clone/check/publish remains serialized and acknowledgement follows successful
  namespace publication; durability remains subject to the Phase 1 audit (`crates/txn/src/dataset.rs:915-945`, `:1113-1136`). The code explicitly rejects silent
  buffering as a segment-count workaround (`crates/index/src/segment_set.rs:103-111`).
- **Pruning is metadata-only and observable.** `Snapshot::explain` reports data files and segments
  scanned/skipped without opening their bodies (`crates/txn/src/snapshot.rs:251-282`), making truthful
  pruning benchmarks straightforward.
- **Filtered vector-search caching has measured value.** The per-snapshot predicate cache converts
  repeated full-body reads into one cold read per predicate, and the retained measurements track the
  predicted miss count closely (`crates/txn/src/snapshot.rs:359-428`;
  `docs/history/analysis/2026-07-25-filtered-vector-search-memory-audit.md:168-192`).
- **The benchmark sources already contain useful building blocks.** `manifest_growth_bench` isolates
  accumulated-manifest cost, `lifecycle_bench` records wall and allocator phases, and
  `segment_recall_bench` supplies a segment-count sweep. The missing step is a short production-path
  matrix with retained, attributable results—not a new long benchmark framework.

## Phase disposition summary

1. **Before Phase 1 exit:** run and retain the bounded matrix described in PERF-01; state supported
   commit/segment/version bounds and the observed curve; correct the contradictory projection claim.
2. **Phase 2:** define projected scan behavior and benchmark scan/filter/pruning cases that genuinely
   prune zero, some, and all files.
3. **Phase 3:** design compaction, retained-version/manifest GC, snapshot-aware reclamation, and any
   lazy/mapped/parallel segment loading or finer-grained physical pruning. Preserve per-commit durable
   visibility; do not use buffering to disguise fan-out.
