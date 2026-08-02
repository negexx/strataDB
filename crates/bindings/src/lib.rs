//! Placeholder Python extension module proving `PyO3` links correctly. This
//! is not a stable client facade. The supported engine surface is currently
//! the Rust `Dataset`/`Snapshot`/`Transaction` API in `strata-txn`; Python
//! bindings remain deferred — see `docs/architecture.md`.

use pyo3::prelude::*;

#[pyfunction]
fn placeholder_version() -> &'static str {
    "0.1.0"
}

#[pymodule]
mod strata_ext {
    #[pymodule_export]
    use super::placeholder_version;
}
