//! Transaction & conflict resolution — Strata's flagship subsystem. See
//! `docs/design/phase-0-transaction-and-format-spec.md` and
//! `.opencode/rules/concurrency-txn-layer.md` before editing anything here for
//! real.
//!
//! `Dataset`, `Snapshot`, and `Transaction` are the supported engine surface;
//! storage, index, and query crates are internal implementation layers.

pub mod commit_log;
pub mod dataset;
pub mod error;
pub(crate) mod live_set_cache;
pub mod mvp_fixtures;
pub(crate) mod row_id;
pub mod snapshot;

pub use arrow;
pub use dataset::{Dataset, ROW_ID_COLUMN, TIMESTAMP_COLUMN, Transaction};
pub use error::{Result, TxnError};
pub use snapshot::Snapshot;
