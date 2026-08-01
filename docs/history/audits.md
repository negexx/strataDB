# Historical audits and measurements

The old analysis tree contained useful findings but repeated the same conclusions in unstable,
line-number-heavy prose. Their themes are retained here; the active seven-lane review is
[phase-1-audit.md](../phase-1-audit.md).

## July 2026 audit chronology

- The complexity audit identified manifest growth, recovery cost, and the difference between per-commit
  work and accumulated history. Its measurements are old-baseline evidence, not current segmented
  performance proof.
- The OCC proposal review examined conflict windows, commit serialization, and the limits of the
  single-handle model.
- The ingest/recovery audit covered crash boundaries, abandoned files, and restart behavior.
- The filtered-vector-search audit examined pruning, live-set filtering, and memory behavior.
- The full-pipeline performance audit connected manifest size, segment fan-out, and query cost.

## Measurements and provenance

Historical references worth retaining:

- PR #10 corrected the reported recall from 0.9890 to 0.9940; this was an experiment, not universal
  ANN proof.
- PR #36 added zone-map metadata and pruning evidence.
- PR #47 added chaos-worker coverage and documented follow-up corrections.
- A 2,000-seed historical chaos run covered its recorded workload only.
- The benchmark dataset recipe was `Qdrant/dbpedia-entities-openai3-text-embedding-3-small-512-100K`.
- BranchBench, CIDR, and Sig2Model are research context, not verified implementation evidence.

Any future benchmark must identify the code revision, workload, seed count, hardware, and whether the
result measures the current immutable-segment implementation.
