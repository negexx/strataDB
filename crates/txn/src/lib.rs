//! Transaction & conflict resolution — Strata's flagship subsystem. See
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` and
//! `.claude/rules/concurrency-txn-layer.md` before editing anything here for
//! real.

pub mod commit_log;
pub mod dataset;
pub mod error;
pub mod mvp_fixtures;
pub mod snapshot;

pub use arrow;
pub use dataset::{Dataset, ROW_ID_COLUMN, Transaction};
pub use error::{Result, TxnError};
pub use snapshot::Snapshot;
