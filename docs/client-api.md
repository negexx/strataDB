# Supported client and administration API

Strata is an embedded, single-node engine. The supported engine surface is
`strata-txn::Dataset`, `Snapshot`, and `Transaction`, used by multiple agents
sharing one handle in one process. Reads use immutable snapshots; writes use
write-write optimistic conflict detection. The isolation ceiling is snapshot
isolation, and the current transaction API does not provide a general staged
vector read or full serializability.

The Python package exposes the documented Dataset/Snapshot/Transaction facade,
typed query and migration operations, planner explain output, and typed error
categories. Blocking engine work releases the Python GIL. The package does not
provide cross-process coordination or distributed transactions.

The `strata` CLI provides local administration and evidence commands including
`inspect --json`, `schema`, `explain --json`, `migration validate|run|status`,
`manifest-status`, `recovery-status`, and `evidence`. It also provides the
supported typed query, scan, lookup, and group-by commands. Exit categories are
stable: operational failure `1`, usage `2`, conflict `3`, unsupported `4`, and
corruption `5`. Commands operate on one local dataset and do not coordinate
independently opened processes.

Schema changes and manifest-format changes require an explicit compatibility
decision, forward/recovery tests, and documentation. Unsupported or corrupt
formats are rejected; they are not guessed or silently upgraded.
