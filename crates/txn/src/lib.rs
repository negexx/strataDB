//! Transaction & conflict resolution — Strata's flagship subsystem. See
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` and
//! `.claude/rules/concurrency-txn-layer.md` before editing anything here for
//! real.

pub mod dataset;
pub mod error;

pub use dataset::{Dataset, Transaction};
pub use error::{Result, TxnError};
