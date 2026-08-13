# Phase 2 memory audit

**Run:** 2026-08-13.

No P0/P1 memory defect was confirmed. Python tabular results are serialized to Arrow IPC and vector
results are converted to bounded dictionaries, but the branch does not establish a portable RSS or
peak-allocation bound for wide projections, large groups, or large `k` values.

| ID | Severity | Finding | Disposition |
|---|---|---|---|
| MEM2-01 | Implemented evidence path | Query-result allocation and Python conversion were not measured under a named workload. | The Phase 2 workflow captures `/usr/bin/time -v` maximum RSS for the bounded shared-reader benchmark; it is evidence, not a supported RSS budget. |
| MEM2-02 | Resolved | The public documentation did not centralize memory expectations for IPC and hydration. | The evidence matrix and workflow now state the bounded measurement policy and non-SLO limitation. |

Run [31652917305](https://github.com/negexx/strataDB/actions/runs/31652917305) recorded 9,152 KB
maximum RSS for the benchmark executable alone, after separating compilation from measurement.
