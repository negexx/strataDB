---
paths:
  - "crates/txn/**/*.rs"
  - "crates/storage/**/*.rs"
  - "tests/sim/**/*.rs"
---

# Transaction & Conflict Resolution Layer

This is Strata's flagship subsystem — the whole project exists to make these guarantees hold under real concurrent load. These are correctness invariants, not style preferences.

**Read `.claude/docs/design/phase-0-transaction-and-format-spec.md` before writing anything here.** It has the precise, reviewed definitions of "conflict," "transaction boundary," and the exact commit protocol steps — this file's bullets are a summary and a set of guardrails, not a substitute for the spec. If something here and the spec ever disagree, the spec wins; fix this file.

- **No write is acknowledged to the caller until it is fsynced, conflict-checked, and durably committed.** Never add a buffering/batching path that acknowledges before that point, even for throughput. If throughput needs improving, that's a design conversation, not a quiet code change here.
- **Isolation level is snapshot isolation, not serializability.** Don't add serializability machinery (e.g. read-set validation beyond what OCC needs) — it's an explicit, documented cut. See `.claude/docs/decisions/`.
- **Commit is a single compare-and-swap of the manifest pointer.** A transaction records the manifest version it read; conflict detection runs before the CAS is attempted. Conflict detection is row/key-range granularity — two transactions touching disjoint rows must never spuriously conflict.
- **Conflicts are surfaced via a typed error identifying the contested rows/keys, never silently resolved.** A last-writer-wins convenience mode may exist but must be opt-in, never the default path. Use a real `enum` error type (`thiserror` or hand-rolled), not a stringly-typed error.
- **A transaction that writes a row and updates the vector index commits both atomically.** A crash or conflict mid-transaction must leave neither behind — see `.claude/rules/vector-index.md` for the index side of this.
- **The borrow checker rules out data races in safe code — it does not prove your OCC/conflict-detection logic is correct.** Every change here needs a `loom` test exercising the interleavings that matter (concurrent conflicting writers, concurrent non-conflicting writers, crash mid-commit), in addition to the normal `cargo test` unit tests. This is the whole reason Rust was chosen over C++ for this project — don't treat loom coverage as optional.
- Any `unsafe` block in this crate needs a `// SAFETY:` comment stating the invariant it relies on, and should be treated as a signal to double-check whether safe Rust could express the same thing — `unsafe_op_in_unsafe_fn = "deny"` is set workspace-wide so these can't slip in silently.
- Prefer lock-free/OCC-style designs already established in this layer over introducing new locking; if a new lock (`Mutex`/`RwLock`) is genuinely required, document the lock order in a comment at the acquisition site, and note the `Ordering` chosen for any raw atomics and why.
- Test every change here with a concurrency/conflict scenario, not just a single-threaded happy path — a passing `cargo test` alone is not sufficient evidence this layer is correct.
