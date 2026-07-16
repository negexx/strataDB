//! Placeholder Python extension module proving `PyO3` links correctly. The
//! real transaction API (begin/commit/retry-on-conflict) lands once
//! `strata-txn` has real logic — see `.claude/rules/python-bindings.md`.

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
