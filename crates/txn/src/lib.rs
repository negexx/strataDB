//! Transaction & conflict resolution — Strata's flagship subsystem. See
//! `docs/design.md` and `docs/audit/phase-1/audit.md` before editing anything here
//! for real.
//!
//! `Dataset`, `Snapshot`, and `Transaction` are the supported engine surface;
//! storage, index, and query crates are internal implementation layers.

#![forbid(unsafe_code)]

pub mod commit_log;
pub mod compaction;
pub mod dataset;
pub mod error;
pub mod facade;
mod filter_key;
mod lifecycle;
mod lifecycle_coordination;
pub(crate) mod live_set_cache;
pub mod maintenance;
pub mod mvp_fixtures;
pub mod observability;
pub mod query;
pub(crate) mod retention;
mod retention_executor;
pub(crate) mod row_id;
pub mod snapshot;
pub mod vacuum;

pub use arrow;
pub use compaction::{CompactionPolicy, CompactionReport};
#[cfg(feature = "test-fault-injection")]
#[doc(hidden)]
pub use dataset::test_support;
pub use dataset::{Dataset, ROW_ID_COLUMN, TIMESTAMP_COLUMN, Transaction};
pub use error::{Result, TxnError};
pub use facade::{DataFileInfo, SegmentInfo};
pub use lifecycle::LifecycleReport;
pub use maintenance::{LifecycleMaintenancePolicy, LifecycleMaintenanceReport};
pub use observability::{
    OperationalEvent, OperationalEventFilter, OperationalEventKind, OperationalEventLog,
    OperationalEventOutcome,
};
pub use query::{
    Aggregate, AggregateFunction, AggregateOutput, Comparison, ComparisonOperator,
    FilterExpression, FilterLiteral, GroupByRequest, GroupByResult, GroupedRow, HydrationError,
    LogicalType, ProjectedField, ProjectedRow, Projection, QueryError, QueryExecutionError,
    QueryResult, QueryValidationError, ResultValue, RowId, RowLookupOutcome, RowLookupRequest,
    RowLookupResult, ScanRequest, ScanResult, VectorHit, VectorHydration, VectorHydrationState,
    VectorSearchRequest, VectorSearchResult,
};
pub use retention::{AgeRetentionPolicy, RetentionCandidate, RetentionPlan, RetentionPolicy};
pub use retention_executor::ManifestPruneReport;
pub use snapshot::Snapshot;
pub use strata_query::{LogicalOperator, PhysicalOperator, PhysicalPlan};
pub use strata_storage::{SchemaMigration, SchemaMigrationResult, StorageError};
pub use vacuum::VacuumReport;
