---
paths:
  - "crates/bindings/**/*.rs"
  - "crates/bindings/**/*.py"
---

# Python Bindings (PyO3)

- Use the modern `#[pymodule] mod strata_ext { #[pymodule_export] use super::...; }` form, not the older function-based `#[pymodule] fn strata_ext(py: Python, m: &PyModule)` API — this project standardized on the module form; don't mix styles across files.
- Release the GIL (`py.allow_threads(...)`) around any call that does blocking I/O or holds Strata's internal locks — otherwise concurrent Python callers serialize on the GIL even though the Rust engine itself supports concurrent access.
- The transaction API (`begin`/`commit`/retry-on-conflict helpers) is exposed directly to Python callers, not hidden behind an auto-commit-only wrapper — callers need to reason about transaction boundaries explicitly. Don't add an auto-commit convenience mode that silently starts/ends transactions.
- Typed conflict errors from `crates/txn/` must map to a distinct Python exception type (via `#[derive(FromPyObject)]`/a custom `PyErr` conversion), not collapse into a generic `RuntimeError` — callers need to catch and retry on conflict specifically. Implement `From<StrataError> for PyErr`, don't `.to_string()` errors into a generic exception at the boundary.
- Keep the binding layer thin: validation and business logic belong in `crates/txn`/`crates/storage`/etc., not duplicated in `crates/bindings`.
- Any `unwrap()`/`expect()` at the FFI boundary that could plausibly panic on bad Python input is a bug, not a shortcut — a Rust panic across the FFI boundary is undefined behavior in PyO3, not just an ugly Python traceback. Convert to `PyResult` and propagate with `?`.
