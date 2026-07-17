# Phase 5 Spec — Single-Writer MVCC & Snapshot Isolation

**Status:** Implemented. See `docs/superpowers/specs/2026-07-17-phase-5-mvcc-snapshot-isolation-design.md` for the full design rationale and the `llm-council` deliberation that shaped it (`.superpowers/council/council-transcript-20260717-201037.md`).

## Summary

`Dataset` is `Arc<ArcSwap<Snapshot>>`-backed. `Snapshot` bundles an immutable manifest, the one shared ever-growing `HnswIndex` graph, a row-id watermark, and a frozen tombstone set. `Dataset::snapshot()` returns a cheap, point-in-time-consistent view; `Transaction::commit()` applies only its own new delta entries to the shared graph (no full historical replay per commit) and swaps in a new `Snapshot` only after the on-disk manifest commit succeeds.

## 12. Known limitations

Carried forward from `phase-4-vector-index-spec.md §12`, plus one new Phase-5-specific item:

- `hnsw_rs` has no node-removal API — a tombstoned row's graph node is never physically reclaimed until Phase 8 compaction rebuilds the graph from only-live rows. Phase 5 does not change this.
- **New:** a `Snapshot` stays alive for exactly as long as any reader holds a clone of it (ordinary `Arc` refcounting). A pathological long-lived reader holding an old `Snapshot` indefinitely pins that version's `Arc<Manifest>` — and therefore the data/delta-log files it references — from ever being reclaimed by Phase 8 compaction, whenever that lands. No enforcement (max-snapshot-age, warnings) exists yet; this is an open question for Phase 8's design, not solved here.
- Time-travel / arbitrary-version snapshot reconstruction is explicitly out of scope — `Dataset::snapshot()` always returns the *current* version only; there is no API to retrieve a version whose `Snapshot` object has already been dropped.
