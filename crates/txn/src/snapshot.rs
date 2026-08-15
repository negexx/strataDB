//! A point-in-time, immutable view of a [`Dataset`](crate::Dataset) — see
//! `docs/design.md`.
//! `Snapshot` itself is never cloned — callers hold it behind
//! [`Dataset::snapshot`](crate::dataset::Dataset::snapshot)'s `Arc<Snapshot>`,
//! and cloning *that* `Arc` is cheap and never touches the data it points to.
//! Every field except `live_set_cache` is `Copy` or `Arc`-wrapped (`index:
//! SegmentSet` is itself just an `Arc<[_]>` internally); `live_set_cache` is
//! neither (it owns a `Mutex`-guarded cache — see `crate::live_set_cache`'s
//! module doc), which is fine precisely because nothing clones a `Snapshot`
//! by value, only the surrounding `Arc`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch,
    StringArray, UInt64Array,
};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use strata_index::LiveSet;
use strata_query::{
    LogicalOperator, LogicalPlan, PhysicalPlan, PlanObservations, Planner, Predicate, PredicateKey,
    filter, mask, should_scan_file,
};
#[cfg(test)]
use strata_storage::read_batch;
use strata_storage::{
    DataFileEntry, Manifest, StorageOwner, Value, read_batch_columns_with, read_batch_with,
};

use crate::facade::{DataFileInfo, SegmentInfo};

use crate::dataset::{ROW_ID_COLUMN, cast_batch_to_schema};
use crate::error::{Result, TxnError};
use crate::query::{
    Aggregate, AggregateFunction, AggregateOutput, Comparison, ComparisonOperator, DatasetSchema,
    FilterExpression, FilterLiteral, GroupByRequest, GroupByResult, GroupedRow, LogicalColumn,
    LogicalType, ProjectedField, ProjectedRow, QueryExecutionError, QueryResult, ResultValue,
    RowId, RowLookupOutcome, RowLookupRequest, RowLookupResult, ScanRequest, ScanResult, VectorHit,
    VectorHydrationState, VectorSearchRequest, VectorSearchResult,
};

use crate::live_set_cache::LiveSetCache;
pub use crate::live_set_cache::LiveSetCacheAccounting;
use crate::retention::SnapshotLease;

/// Downcasts a `SegmentSet` part's opaque zone-map payload back to the
/// concrete type `crates/txn` is the only crate that knows it really is
/// (`HashMap<String, ColumnStats>`), and applies the existing
/// `should_scan_file` evaluator — S1 W4b. `crates/index` never sees a
/// `Predicate` or a `ColumnStats`; see
/// `docs/design.md`.
///
/// The `None` arm (a payload that isn't a `HashMap<String, ColumnStats>`)
/// is unreachable in practice — `crates/txn` is the only code that ever
/// constructs one of these payloads (`Transaction::commit`, `load_segments`)
/// and it always constructs exactly this type — but fails open (must scan)
/// rather than panicking, matching `should_scan_file`'s own "absent means
/// must scan" contract for a payload it cannot interpret.
fn zone_map_permits_scan(
    zone_map: &(dyn std::any::Any + Send + Sync),
    predicate: &Predicate,
) -> bool {
    match zone_map.downcast_ref::<std::collections::HashMap<String, strata_storage::ColumnStats>>()
    {
        Some(map) => should_scan_file(map, predicate),
        None => true,
    }
}

fn filter_expression_pruning_predicate(filter: &FilterExpression) -> Option<Predicate> {
    match filter {
        FilterExpression::Compare(Comparison {
            column,
            operator,
            value,
        }) => {
            let value = match value {
                FilterLiteral::Int64(value) => Value::Int64(*value),
                FilterLiteral::Float64(value) => Value::Float64(*value),
                FilterLiteral::Utf8(value) => Value::Utf8(value.clone()),
                FilterLiteral::Boolean(_) | FilterLiteral::UInt64(_) => return None,
            };
            match operator {
                ComparisonOperator::Equal => Some(Predicate::Eq(column.clone(), value)),
                ComparisonOperator::LessThan => Some(Predicate::Lt(column.clone(), value)),
                ComparisonOperator::LessThanOrEqual => Some(Predicate::LtEq(column.clone(), value)),
                ComparisonOperator::GreaterThan => Some(Predicate::Gt(column.clone(), value)),
                ComparisonOperator::GreaterThanOrEqual => {
                    Some(Predicate::GtEq(column.clone(), value))
                }
                ComparisonOperator::NotEqual => None,
            }
        }
        FilterExpression::And(left, right) => Some(Predicate::And(
            Box::new(filter_expression_pruning_predicate(left)?),
            Box::new(filter_expression_pruning_predicate(right)?),
        )),
        FilterExpression::Or(left, right) => Some(Predicate::Or(
            Box::new(filter_expression_pruning_predicate(left)?),
            Box::new(filter_expression_pruning_predicate(right)?),
        )),
        FilterExpression::Not(_) => None,
    }
}

pub struct Snapshot {
    #[allow(dead_code)]
    pub(crate) dir: PathBuf,
    pub(crate) storage: Arc<StorageOwner>,
    pub(crate) version: u64,
    pub(crate) lease: Arc<SnapshotLease>,
    pub(crate) schema: SchemaRef,
    pub(crate) manifest: Arc<Manifest>,
    pub(crate) index: strata_index::SegmentSet,
    pub(crate) tombstones: Arc<imbl::HashSet<u64>>,
    /// Per-predicate resolved-row-id cache — see
    /// `docs/phase-1-performance.md`
    /// and `crate::live_set_cache`'s module doc for why this is sound (this
    /// `Snapshot` is immutable) and how it's bounded (a byte budget, not an
    /// entry count).
    pub(crate) live_set_cache: LiveSetCache,
}

/// Soft cap on a single `Snapshot`'s resolved-live-set cache — see
/// `crate::live_set_cache`'s module doc for why a byte budget rather than an
/// entry-count LRU. 64 MiB is a few thousand 25k-row-scale bitsets or a
/// handful of very large ones; revisit with real workload data, not a guess,
/// if it ever needs tuning.
pub(crate) const LIVE_SET_CACHE_BYTE_BUDGET: usize = 64 * 1024 * 1024;

/// The outcome of [`Snapshot::explain`] — which files and segments a
/// predicate would require scanning, without actually reading any file
/// bodies or evaluating a single vector distance.
///
/// The `segments_*` fields were added in S1 W4b, additively: they never
/// changed the meaning of `total_files`/`scanned`/`skipped`, which describe
/// row data files only and are read directly by `widen_ef` — merging
/// segment counts into them would silently change `ef_search` width across
/// every filtered vector search in the system (see the S1 W4 design
/// amendment §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainResult {
    pub total_files: usize,
    pub scanned: Vec<String>,
    pub skipped: Vec<String>,
    /// How many index segments this snapshot's manifest lists, regardless
    /// of whether any of them carries a vector-search-relevant predicate
    /// column in its zone map.
    pub segments_total: usize,
    /// Names of segments `should_scan_file` says could match the
    /// predicate, evaluated against each segment's own
    /// `SegmentEntry.zone_map` (S1 W4a).
    pub segments_scanned: Vec<String>,
    /// Names of segments `should_scan_file` proves cannot match — these
    /// are the segments `Snapshot::vector_search`'s predicate path skips
    /// entirely via `SegmentSet::search_filtered_pruned`.
    pub segments_skipped: Vec<String>,
}

// HNSW search-widening parameters — see `widen_ef`'s doc comment.
const EF_SEARCH_DEFAULT: usize = 32;
const MIN_SELECTIVITY_FLOOR: f64 = 0.01;
const MAX_EF_SCALE: f64 = 20.0;

/// Widens `base_ef` using `Snapshot::explain`'s scanned/total file ratio as
/// a coarse, file-granularity *upper bound* on selectivity — see
/// `docs/design.md`. Erring toward a
/// wider `ef` costs search time, never correctness, so an overestimate of
/// how many rows survive is the safe direction.
fn widen_ef(base_ef: usize, snapshot: &Snapshot, predicate: &Predicate) -> usize {
    let explain = snapshot.explain(predicate);
    #[allow(clippy::cast_precision_loss)]
    let selectivity_upper_bound = explain.scanned.len() as f64 / explain.total_files.max(1) as f64;
    let scale = (1.0 / selectivity_upper_bound.max(MIN_SELECTIVITY_FLOOR)).min(MAX_EF_SCALE);
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )]
    let widened = ((base_ef as f64) * scale).round() as usize;
    widened
}

impl Snapshot {
    /// The immutable manifest version this snapshot represents.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Returns the current per-snapshot live-set-cache observation.
    ///
    /// This is a read-only diagnostic for retained-footprint measurement.
    /// Its charged-byte value is approximate accounting, not process RSS or
    /// exact allocator residency; see [`LiveSetCacheAccounting`].
    #[must_use]
    pub fn live_set_cache_accounting(&self) -> LiveSetCacheAccounting {
        self.live_set_cache.accounting()
    }

    /// The immutable logical schema owned by this dataset.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub(crate) fn owns_row(&self, row_id: u64) -> bool {
        self.manifest.data_files.iter().any(|entry| {
            entry
                .row_id_range
                .is_some_and(|(first, last)| first <= row_id && row_id <= last)
        })
    }

    pub(crate) fn owns_live_row(&self, row_id: u64) -> bool {
        self.owns_row(row_id) && self.is_visible(row_id)
    }

    fn validate_projection_schema(&self, requested: &SchemaRef) -> Result<()> {
        if requested.metadata() != self.schema.metadata() {
            return Err(TxnError::BatchSchemaMismatch {
                expected: format!("owned schema metadata {:?}", self.schema.metadata()),
                actual: format!("requested schema metadata {:?}", requested.metadata()),
            });
        }
        for field in requested.fields() {
            let expected = match field.name().as_str() {
                ROW_ID_COLUMN => {
                    Field::new(ROW_ID_COLUMN, arrow::datatypes::DataType::UInt64, false)
                }
                crate::dataset::TIMESTAMP_COLUMN => Field::new(
                    crate::dataset::TIMESTAMP_COLUMN,
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
                name => self
                    .schema
                    .field_with_name(name)
                    .map_err(|_| TxnError::BatchSchemaMismatch {
                        expected: format!("owned field named {name:?}"),
                        actual: format!("requested field {field:?}"),
                    })?
                    .as_ref()
                    .clone(),
            };
            if field.as_ref() != &expected {
                return Err(TxnError::BatchSchemaMismatch {
                    expected: format!("{expected:?}"),
                    actual: format!("{field:?}"),
                });
            }
        }
        Ok(())
    }

    /// Whether `row_id` is visible under this snapshot: not tombstoned as
    /// of this snapshot's version. No per-tombstone version needs to be
    /// stored for this to be correct — the version boundary comes from
    /// *when* a `Snapshot` was built (immediately after the commit that
    /// produced it), and tombstones are just row-ids in
    /// `Manifest.tombstones` as of that version.
    ///
    /// This used to also check a `row_id <= watermark` bound and a
    /// transaction-in-flight exclusion set — both removed. See
    /// `crate::row_id`'s module doc for why a segment/data-file publish
    /// being atomic with the manifest swap makes both checks redundant:
    /// every row-id this method is ever called with comes from candidates
    /// this snapshot's own `index`/`manifest.data_files` produced, which by
    /// construction can never include a row-id this snapshot's own commit
    /// didn't allocate, and can never include an in-flight (not-yet-durable)
    /// transaction's row at all. The named loom model is the regression gate
    /// for this invariant; its post-change execution remains separately
    /// tracked when host resource limits prevent completion.
    ///
    /// This runs once per candidate during HNSW graph traversal.
    pub(crate) fn is_visible(&self, row_id: u64) -> bool {
        !self.tombstones.contains(&row_id)
    }

    /// Data file entries (name + per-column stats) belonging to this
    /// snapshot's version. Exposed for tests that need to inspect the raw
    /// on-disk representation directly.
    #[must_use]
    pub fn data_files(&self) -> &[DataFileEntry] {
        &self.manifest.data_files
    }

    /// Returns facade-owned metadata without exposing storage manifest types.
    #[must_use]
    pub fn data_file_info(&self) -> Vec<DataFileInfo> {
        self.manifest
            .data_files
            .iter()
            .map(DataFileInfo::from_entry)
            .collect()
    }

    /// Returns facade-owned metadata for the immutable vector segments.
    #[must_use]
    pub fn segment_info(&self) -> Vec<SegmentInfo> {
        self.manifest
            .segments
            .iter()
            .map(SegmentInfo::from_entry)
            .collect()
    }

    /// Iterates `self.manifest.data_files`, keeping only entries
    /// `should_scan_file` says could match `predicate` (or every entry, if
    /// `predicate` is `None`), reads and joins each surviving file's path
    /// via [`safe_join`], removes any row this snapshot's tombstone set
    /// covers (see [`Self::filter_tombstoned_rows`]), and applies `process`
    /// to each resulting raw batch. Shared by [`Snapshot::scan`],
    /// [`Snapshot::scan_with_predicate`], and [`Snapshot::row_ids_matching`]
    /// — one enforcement point for tombstone visibility, rather than three
    /// call sites each independently responsible for remembering it. Per
    /// `docs/design.md`: `scan` must treat a tombstoned row-id as
    /// dead regardless of whether its bytes are still on disk.
    ///
    /// `columns`, when `Some`, restricts the Arrow IPC read to those columns
    /// so the rest are never decoded. Callers that need whole rows pass
    /// `None`; callers that only need a couple of scalar columns out of a
    /// table carrying wide embeddings should not. **Precondition:** when
    /// `Some`, the slice must include [`ROW_ID_COLUMN`] — tombstone
    /// filtering needs it, and [`Self::filter_tombstoned_rows`] returns a
    /// typed error (not a panic, not a silent skip) if it's missing.
    fn read_surviving_files<T>(
        &self,
        predicate: Option<&Predicate>,
        columns: Option<&[&str]>,
        mut process: impl FnMut(RecordBatch) -> Result<T>,
    ) -> Result<Vec<T>> {
        self.manifest
            .data_files
            .iter()
            .filter(|entry| predicate.is_none_or(|p| should_scan_file(&entry.stats, p)))
            .map(|entry| {
                let key = self
                    .storage
                    .data_object_key(&entry.name)
                    .map_err(TxnError::Storage)?;
                let batch = match columns {
                    Some(cols) => read_batch_columns_with(&self.storage, &key, cols)?,
                    None => read_batch_with(&self.storage, &key)?,
                };
                let batch = self.filter_tombstoned_rows(batch)?;
                process(batch)
            })
            .collect()
    }

    pub(crate) fn read_surviving_physical_batches(&self) -> Result<Vec<RecordBatch>> {
        self.read_surviving_files(None, None, Ok)
    }

    /// Removes every row whose `ROW_ID_COLUMN` value is in
    /// `self.tombstones` from `batch`. A no-op that returns `batch`
    /// unchanged (no allocation, no column scan) when this snapshot has no
    /// tombstones at all — the common case for a dataset that has never
    /// deleted or updated a row.
    ///
    /// # Errors
    ///
    /// Returns an error if `batch` has no `ROW_ID_COLUMN`, or if that
    /// column isn't `UInt64` — both indicate a caller violated
    /// [`Self::read_surviving_files`]'s `columns` precondition, not a data
    /// problem.
    fn filter_tombstoned_rows(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if self.tombstones.is_empty() {
            return Ok(batch);
        }
        let row_id_idx = batch.schema_ref().index_of(ROW_ID_COLUMN)?;
        let row_ids = batch
            .column(row_id_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                TxnError::Arrow(arrow::error::ArrowError::CastError(format!(
                    "{ROW_ID_COLUMN} column must be UInt64"
                )))
            })?;
        let keep: BooleanArray = row_ids
            .values()
            .iter()
            .map(|&id| !self.tombstones.contains(&id))
            .collect();
        Ok(filter_record_batch(&batch, &keep)?)
    }

    /// Reads every row committed as of this snapshot's version, as a
    /// single `RecordBatch` cast back to `schema` — the caller's logical
    /// schema, not necessarily the physical on-disk representation. For
    /// predicate-pushdown pruning, see [`Snapshot::scan_with_predicate`]
    /// and [`Snapshot::explain`] below — this method always reads every
    /// file this snapshot's manifest lists.
    ///
    /// # Errors
    ///
    /// Returns an error if any committed data file fails to read, if a
    /// column can't be cast to `schema`'s corresponding field type, or if
    /// the cast batches can't be concatenated against `schema`.
    pub fn scan(&self, schema: &SchemaRef) -> Result<RecordBatch> {
        self.validate_projection_schema(schema)?;
        let batches =
            self.read_surviving_files(None, None, |batch| cast_batch_to_schema(&batch, schema))?;
        Ok(concat_batches(schema, &batches)?)
    }

    /// Reports which of this snapshot's files `predicate` would require
    /// scanning, without opening any file body — pure introspection over
    /// stats already loaded in the manifest. See `docs/design.md`.
    #[must_use]
    pub fn explain(&self, predicate: &Predicate) -> ExplainResult {
        let mut scanned = Vec::new();
        let mut skipped = Vec::new();
        for entry in &self.manifest.data_files {
            if should_scan_file(&entry.stats, predicate) {
                scanned.push(entry.name.clone());
            } else {
                skipped.push(entry.name.clone());
            }
        }
        let mut segments_scanned = Vec::new();
        let mut segments_skipped = Vec::new();
        for entry in &self.manifest.segments {
            if should_scan_file(&entry.zone_map, predicate) {
                segments_scanned.push(entry.name.clone());
            } else {
                segments_skipped.push(entry.name.clone());
            }
        }
        ExplainResult {
            total_files: self.manifest.data_files.len(),
            scanned,
            skipped,
            segments_total: self.manifest.segments.len(),
            segments_scanned,
            segments_skipped,
        }
    }

    /// Builds the stable logical/physical explain DTO for a typed scan.
    ///
    /// The DTO reports only the immutable snapshot captured by this value;
    /// it never incorporates staged transaction rows.
    ///
    /// # Errors
    ///
    /// Returns the same typed validation error as [`Self::scan_query`] for an
    /// invalid projection or filter.
    pub fn explain_scan_query(&self, request: &ScanRequest) -> QueryResult<PhysicalPlan> {
        self.explain_scan_query_with_overlay(request, false)
    }

    pub(crate) fn explain_scan_query_with_overlay(
        &self,
        request: &ScanRequest,
        transaction_overlay: bool,
    ) -> QueryResult<PhysicalPlan> {
        let projection = self.query_schema()?.validate_scan(request)?;
        let mut operators = vec![LogicalOperator::Source];
        if let Some(filter) = request.filter.as_ref() {
            operators.push(LogicalOperator::Predicate {
                zone_map_eligible: filter_expression_pruning_predicate(filter).is_some(),
            });
        }
        operators.push(LogicalOperator::Projection {
            columns: projection,
        });
        operators.push(LogicalOperator::Materialize);
        self.plan_query(operators, request.filter.as_ref(), transaction_overlay)
    }

    /// Executes the physical path selected for a typed scan.
    ///
    /// The selected path delegates to [`Self::scan_query`], retaining its
    /// projection ordering, null, tombstone, and snapshot semantics.
    ///
    /// # Errors
    ///
    /// Returns planner or typed scan-contract errors.
    pub fn execute_planned_scan_query(&self, request: &ScanRequest) -> QueryResult<ScanResult> {
        let _plan = self.explain_scan_query(request)?;
        self.scan_query(request)
    }

    /// Builds the stable logical/physical explain DTO for grouped aggregation.
    ///
    /// # Errors
    ///
    /// Returns the same typed validation error as [`Self::group_by_query`].
    pub fn explain_group_by_query(&self, request: &GroupByRequest) -> QueryResult<PhysicalPlan> {
        self.explain_group_by_query_with_overlay(request, false)
    }

    pub(crate) fn explain_group_by_query_with_overlay(
        &self,
        request: &GroupByRequest,
        transaction_overlay: bool,
    ) -> QueryResult<PhysicalPlan> {
        let _outputs = self.query_schema()?.validate_group_by(request)?;
        let mut operators = vec![LogicalOperator::Source];
        if let Some(filter) = request.filter.as_ref() {
            operators.push(LogicalOperator::Predicate {
                zone_map_eligible: filter_expression_pruning_predicate(filter).is_some(),
            });
        }
        operators.push(LogicalOperator::Grouping {
            keys: request.group_by.clone(),
            aggregate_count: request.aggregates.len(),
        });
        operators.push(LogicalOperator::Materialize);
        self.plan_query(operators, request.filter.as_ref(), transaction_overlay)
    }

    /// Executes the physical path selected for grouped aggregation.
    ///
    /// # Errors
    ///
    /// Returns planner or typed grouped-query errors.
    pub fn execute_planned_group_by_query(
        &self,
        request: &GroupByRequest,
    ) -> QueryResult<GroupByResult> {
        let _plan = self.explain_group_by_query(request)?;
        self.group_by_query(request)
    }

    /// Builds the stable logical/physical explain DTO for vector search.
    ///
    /// # Errors
    ///
    /// Returns the same typed validation error as [`Self::vector_search_query`].
    pub fn explain_vector_search_query(
        &self,
        request: &VectorSearchRequest,
    ) -> QueryResult<PhysicalPlan> {
        let hydration = self
            .query_schema()?
            .validate_vector_search(request)?
            .is_some();
        let has_filter = request.filter.is_some();
        let mut operators = vec![LogicalOperator::Source];
        if has_filter {
            operators.push(LogicalOperator::Predicate {
                zone_map_eligible: request
                    .filter
                    .as_ref()
                    .and_then(filter_expression_pruning_predicate)
                    .is_some(),
            });
        }
        operators.push(LogicalOperator::VectorSearch {
            vector_column: request.vector_column.clone(),
            k: request.k,
            has_filter,
            hydration,
        });
        operators.push(LogicalOperator::Materialize);
        self.plan_query(operators, request.filter.as_ref(), false)
    }

    /// Executes the physical path selected for vector search.
    ///
    /// # Errors
    ///
    /// Returns planner or typed vector-search errors.
    pub fn execute_planned_vector_search_query(
        &self,
        request: &VectorSearchRequest,
    ) -> QueryResult<VectorSearchResult> {
        let _plan = self.explain_vector_search_query(request)?;
        self.vector_search_query(request)
    }

    fn plan_query(
        &self,
        operators: Vec<LogicalOperator>,
        predicate: Option<&FilterExpression>,
        transaction_overlay: bool,
    ) -> QueryResult<PhysicalPlan> {
        let logical = LogicalPlan::new(operators).map_err(QueryExecutionError::Planner)?;
        let observations =
            self.plan_observations(logical.operators(), predicate, transaction_overlay);
        Planner::plan(logical, observations)
            .map_err(|error| QueryExecutionError::Planner(error).into())
    }

    fn plan_observations(
        &self,
        operators: &[LogicalOperator],
        predicate: Option<&FilterExpression>,
        transaction_overlay: bool,
    ) -> PlanObservations {
        let pruning = predicate
            .and_then(filter_expression_pruning_predicate)
            .map(|predicate| self.explain(&predicate));
        let data_files_total = self.manifest.data_files.len();
        let index_segments_total = self.manifest.segments.len();
        let data_file_selection = || {
            pruning.as_ref().map_or((data_files_total, 0), |explain| {
                (explain.scanned.len(), explain.skipped.len())
            })
        };
        let index_segment_selection = || {
            pruning
                .as_ref()
                .map_or((index_segments_total, 0), |explain| {
                    (
                        explain.segments_scanned.len(),
                        explain.segments_skipped.len(),
                    )
                })
        };

        let result_operator = operators.iter().find(|operator| {
            matches!(
                operator,
                LogicalOperator::Projection { .. }
                    | LogicalOperator::Grouping { .. }
                    | LogicalOperator::VectorSearch { .. }
            )
        });
        let (data_files_scanned, data_files_pruned, index_segments_scanned, index_segments_pruned) =
            match result_operator {
                Some(LogicalOperator::Projection { .. } | LogicalOperator::Grouping { .. }) => {
                    let (data_files_scanned, data_files_pruned) = data_file_selection();
                    (data_files_scanned, data_files_pruned, 0, 0)
                }
                Some(LogicalOperator::VectorSearch { has_filter, .. }) => {
                    let (data_files_scanned, data_files_pruned) = if *has_filter {
                        data_file_selection()
                    } else {
                        (0, 0)
                    };
                    let (index_segments_scanned, index_segments_pruned) = index_segment_selection();
                    (
                        data_files_scanned,
                        data_files_pruned,
                        index_segments_scanned,
                        index_segments_pruned,
                    )
                }
                Some(
                    LogicalOperator::Source
                    | LogicalOperator::Predicate { .. }
                    | LogicalOperator::Materialize,
                )
                | None => (0, 0, 0, 0),
            };

        PlanObservations {
            data_files_total,
            data_files_scanned,
            data_files_pruned,
            index_segments_total,
            index_segments_scanned,
            index_segments_pruned,
            transaction_overlay,
        }
    }

    /// Like [`Snapshot::scan`], but skips any file `predicate` provably
    /// can't match (per [`Snapshot::explain`]'s decision) and row-filters
    /// the rest.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`Snapshot::scan`],
    /// plus if any of `predicate`'s columns doesn't exist or its value's
    /// type doesn't match the column's Arrow type.
    pub fn scan_with_predicate(
        &self,
        schema: &SchemaRef,
        predicate: &Predicate,
    ) -> Result<RecordBatch> {
        self.validate_projection_schema(schema)?;
        let batches = self.read_surviving_files(Some(predicate), None, |batch| {
            let cast = cast_batch_to_schema(&batch, schema)?;
            Ok(filter(&cast, predicate)?)
        })?;
        Ok(concat_batches(schema, &batches)?)
    }

    /// Executes a typed scan against this immutable snapshot.
    ///
    /// The request is validated against this dataset's owned logical schema.
    /// Physical columns stay internal: `_row_id` is read to enforce
    /// tombstone visibility, while filter-only columns are read but never
    /// returned. Rows and fields retain the request's explicit projection
    /// order.
    ///
    /// # Errors
    ///
    /// Returns a validation error for an incompatible query contract or an
    /// execution error wrapping any underlying engine failure.
    pub fn scan_query(&self, request: &ScanRequest) -> QueryResult<ScanResult> {
        let projection = self.query_schema()?.validate_scan(request)?;
        let columns = scan_columns(&projection, request.filter.as_ref());
        let query_schema = self.query_schema_for_columns(&columns)?;
        let pruning_predicate = request
            .filter
            .as_ref()
            .and_then(filter_expression_pruning_predicate);
        let rows = self
            .read_surviving_files(pruning_predicate.as_ref(), Some(&columns), |batch| {
                let batch = cast_batch_to_schema(&batch, &query_schema)?;
                let filtered = match &request.filter {
                    Some(filter) => filter_record_batch(&batch, &filter_mask(&batch, filter)?)?,
                    None => batch,
                };
                projected_rows(&filtered, &projection)
            })?
            .into_iter()
            .flatten()
            .collect();
        Ok(ScanResult { projection, rows })
    }

    pub(crate) fn visible_logical_batches_excluding(
        &self,
        excluded_row_ids: &[u64],
    ) -> Result<Vec<RecordBatch>> {
        self.read_surviving_physical_batches()?
            .into_iter()
            .map(|batch| {
                let batch = filter_row_ids(batch, excluded_row_ids)?;
                cast_batch_to_schema(&batch, &self.schema)
            })
            .collect()
    }

    pub(crate) fn scan_query_overlay(
        &self,
        request: &ScanRequest,
        batches: &[RecordBatch],
    ) -> QueryResult<ScanResult> {
        let projection = self.query_schema()?.validate_scan(request)?;
        let rows = batches
            .iter()
            .map(|batch| {
                let filtered = match &request.filter {
                    Some(filter) => filter_record_batch(batch, &filter_mask(batch, filter)?)
                        .map_err(TxnError::from)?,
                    None => batch.clone(),
                };
                projected_rows(&filtered, &projection)
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        Ok(ScanResult { projection, rows })
    }

    /// Executes a grouped aggregate query against this immutable snapshot.
    ///
    /// Files are processed in manifest order. Tombstones are removed before
    /// filters are evaluated, and groups retain the order in which their key
    /// first appears among the remaining rows.
    ///
    /// # Errors
    ///
    /// Returns a validation error for an incompatible query contract, an
    /// [`QueryExecutionError::Int64SumOverflow`] for an overflowing integer
    /// sum, or an execution error wrapping an underlying engine failure.
    pub fn group_by_query(&self, request: &GroupByRequest) -> QueryResult<GroupByResult> {
        let outputs = self.query_schema()?.validate_group_by(request)?;
        let columns = group_by_columns(request);
        let query_schema = self.query_schema_for_columns(&columns)?;
        let pruning_predicate = request
            .filter
            .as_ref()
            .and_then(filter_expression_pruning_predicate);
        let batches =
            self.read_surviving_files(pruning_predicate.as_ref(), Some(&columns), |batch| {
                let batch = cast_batch_to_schema(&batch, &query_schema)?;
                match &request.filter {
                    Some(filter) => Ok(filter_record_batch(&batch, &filter_mask(&batch, filter)?)?),
                    None => Ok(batch),
                }
            })?;

        let aggregate_templates = request
            .aggregates
            .iter()
            .zip(&outputs)
            .map(|(aggregate, output)| AggregateState::new(aggregate, output))
            .collect::<QueryResult<Vec<_>>>()?;
        let mut group_indices = HashMap::new();
        let mut groups = Vec::new();

        for batch in batches {
            let mut partial_indices = HashMap::new();
            let mut partial_groups = Vec::new();
            for row in 0..batch.num_rows() {
                let keys = request
                    .group_by
                    .iter()
                    .map(|column| {
                        let index = batch
                            .schema_ref()
                            .index_of(column)
                            .map_err(TxnError::Arrow)?;
                        result_value(batch.column(index), row)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let key = GroupKey::new(&keys)?;
                let group_index = if let Some(index) = partial_indices.get(&key) {
                    *index
                } else {
                    let index = partial_groups.len();
                    partial_indices.insert(key, index);
                    partial_groups.push(GroupState {
                        keys,
                        aggregates: aggregate_templates.clone(),
                    });
                    index
                };

                for (aggregate, state) in request
                    .aggregates
                    .iter()
                    .zip(&mut partial_groups[group_index].aggregates)
                {
                    let index = batch
                        .schema_ref()
                        .index_of(&aggregate.column)
                        .map_err(TxnError::Arrow)?;
                    state.update(&result_value(batch.column(index), row)?, &aggregate.alias)?;
                }
            }

            for partial_group in partial_groups {
                let key = GroupKey::new(&partial_group.keys)?;
                let group_index = if let Some(index) = group_indices.get(&key) {
                    *index
                } else {
                    let index = groups.len();
                    group_indices.insert(key, index);
                    groups.push(GroupState {
                        keys: partial_group.keys,
                        aggregates: aggregate_templates.clone(),
                    });
                    index
                };
                for ((aggregate, partial_state), state) in request
                    .aggregates
                    .iter()
                    .zip(partial_group.aggregates)
                    .zip(&mut groups[group_index].aggregates)
                {
                    state.merge(partial_state, &aggregate.alias)?;
                }
            }
        }

        let rows = groups
            .into_iter()
            .map(|group| GroupedRow {
                keys: group.keys,
                aggregates: group
                    .aggregates
                    .into_iter()
                    .map(AggregateState::finish)
                    .collect(),
            })
            .collect();
        GroupByResult::new(request.group_by.clone(), outputs, rows)
    }

    pub(crate) fn group_by_query_overlay(
        &self,
        request: &GroupByRequest,
        batches: &[RecordBatch],
    ) -> QueryResult<GroupByResult> {
        let outputs = self.query_schema()?.validate_group_by(request)?;
        let aggregate_templates = request
            .aggregates
            .iter()
            .zip(&outputs)
            .map(|(aggregate, output)| AggregateState::new(aggregate, output))
            .collect::<QueryResult<Vec<_>>>()?;
        let mut group_indices = HashMap::new();
        let mut groups = Vec::new();

        for batch in batches {
            let batch = match &request.filter {
                Some(filter) => filter_record_batch(batch, &filter_mask(batch, filter)?)
                    .map_err(TxnError::from)?,
                None => batch.clone(),
            };
            for row in 0..batch.num_rows() {
                let keys = request
                    .group_by
                    .iter()
                    .map(|column| {
                        let index = batch
                            .schema_ref()
                            .index_of(column)
                            .map_err(TxnError::Arrow)?;
                        result_value(batch.column(index), row)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let key = GroupKey::new(&keys)?;
                let group_index = if let Some(index) = group_indices.get(&key) {
                    *index
                } else {
                    let index = groups.len();
                    group_indices.insert(key, index);
                    groups.push(GroupState {
                        keys,
                        aggregates: aggregate_templates.clone(),
                    });
                    index
                };

                for (aggregate, state) in request
                    .aggregates
                    .iter()
                    .zip(&mut groups[group_index].aggregates)
                {
                    let index = batch
                        .schema_ref()
                        .index_of(&aggregate.column)
                        .map_err(TxnError::Arrow)?;
                    state.update(&result_value(batch.column(index), row)?, &aggregate.alias)?;
                }
            }
        }

        let rows = groups
            .into_iter()
            .map(|group| GroupedRow {
                keys: group.keys,
                aggregates: group
                    .aggregates
                    .into_iter()
                    .map(AggregateState::finish)
                    .collect(),
            })
            .collect();
        GroupByResult::new(request.group_by.clone(), outputs, rows)
    }

    /// Looks up one physical row as of this immutable snapshot.
    ///
    /// The result distinguishes a row tombstoned in this snapshot from an
    /// ID that this snapshot has no manifest-visible row for. Live rows use
    /// the same dataset-owned projection validation and value conversion as
    /// [`Self::scan_query`].
    ///
    /// # Errors
    ///
    /// Returns a validation error for an incompatible projection or an
    /// execution error wrapping an underlying engine failure.
    pub fn lookup_row(&self, request: &RowLookupRequest) -> QueryResult<RowLookupResult> {
        let projection = self.query_schema()?.validate_row_lookup(request)?;
        let row_id = request.row_id.0;
        if !self.owns_row(row_id) {
            return Ok(RowLookupResult {
                row_id: request.row_id,
                projection,
                outcome: RowLookupOutcome::NotFound,
            });
        }

        let mut columns = projection.iter().map(String::as_str).collect::<Vec<_>>();
        push_unique_column(&mut columns, ROW_ID_COLUMN);
        let query_schema = self.query_schema_for_columns(&columns)?;
        for entry in self.manifest.data_files.iter().filter(|entry| {
            entry
                .row_id_range
                .is_some_and(|(first, last)| first <= row_id && row_id <= last)
        }) {
            let key = self
                .storage
                .data_object_key(&entry.name)
                .map_err(TxnError::Storage)?;
            let raw_batch = read_batch_columns_with(&self.storage, &key, &columns)
                .map_err(TxnError::Storage)?;
            let batch = cast_batch_to_schema(&raw_batch, &query_schema)?;
            let row_ids = batch
                .column(
                    batch
                        .schema_ref()
                        .index_of(ROW_ID_COLUMN)
                        .map_err(TxnError::Arrow)?,
                )
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| query_cast_error(ROW_ID_COLUMN, "UInt64"))?;
            if let Some(row) = (0..row_ids.len()).find(|&row| row_ids.value(row) == row_id) {
                let outcome = if self.is_visible(row_id) {
                    RowLookupOutcome::Live(projected_row(&batch, row, &projection)?)
                } else {
                    RowLookupOutcome::Tombstoned
                };
                return Ok(RowLookupResult {
                    row_id: request.row_id,
                    projection,
                    outcome,
                });
            }
        }
        Ok(RowLookupResult {
            row_id: request.row_id,
            projection,
            outcome: RowLookupOutcome::NotFound,
        })
    }

    pub(crate) fn lookup_projection(&self, request: &RowLookupRequest) -> QueryResult<Vec<String>> {
        self.query_schema()?.validate_row_lookup(request)
    }

    pub(crate) fn project_logical_row(
        batch: &RecordBatch,
        projection: &[String],
    ) -> Result<ProjectedRow> {
        projected_row(batch, 0, projection)
    }

    fn query_schema(&self) -> QueryResult<DatasetSchema> {
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                Ok(LogicalColumn::new(
                    field.name(),
                    logical_type(field.data_type())?,
                    field.is_nullable(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        DatasetSchema::new(columns)
    }

    fn query_schema_for_columns(&self, columns: &[&str]) -> Result<SchemaRef> {
        let fields = columns
            .iter()
            .map(|column| {
                if *column == ROW_ID_COLUMN {
                    Ok(Field::new(ROW_ID_COLUMN, DataType::UInt64, false))
                } else {
                    self.schema
                        .field_with_name(column)
                        .map(|field| field.as_ref().clone())
                        .map_err(|_| TxnError::BatchSchemaMismatch {
                            expected: format!("owned field named {column:?}"),
                            actual: format!("query field list missing {column:?}"),
                        })
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Schema::new_with_metadata(
            fields,
            self.schema.metadata().clone(),
        )))
    }

    /// Executes nearest-neighbor search against this immutable snapshot.
    ///
    /// The current physical index has one vector column. Filters are resolved
    /// from the same immutable row files before candidates are admitted to the
    /// index search. Optional hydration uses this snapshot's point lookup, so
    /// it cannot observe a later manifest.
    ///
    /// # Errors
    ///
    /// Returns typed query validation errors for an invalid request and wraps
    /// missing or unreadable committed data as a typed engine execution error.
    pub fn vector_search_query(
        &self,
        request: &VectorSearchRequest,
    ) -> QueryResult<VectorSearchResult> {
        let hydration_projection = self.query_schema()?.validate_vector_search(request)?;
        let mut matches = match &request.filter {
            Some(filter) => {
                let live_set = self.vector_filter_live_set(filter)?;
                let pruning_predicate = filter_expression_pruning_predicate(filter);
                self.index
                    .search_filtered_pruned_live(
                        &request.query,
                        request.k,
                        EF_SEARCH_DEFAULT,
                        &live_set,
                        |id| self.is_visible(id),
                        |zone_map| {
                            pruning_predicate
                                .as_ref()
                                .is_none_or(|predicate| zone_map_permits_scan(zone_map, predicate))
                        },
                    )
                    .map_err(TxnError::from)?
            }
            None => self
                .index
                .search(&request.query, request.k, EF_SEARCH_DEFAULT, |id| {
                    self.is_visible(id)
                })
                .map_err(TxnError::from)?,
        };
        matches.sort_by(|left, right| {
            left.squared_distance
                .total_cmp(&right.squared_distance)
                .then_with(|| left.row_id.cmp(&right.row_id))
        });

        let mut hits = Vec::with_capacity(matches.len());
        for vector_match in matches {
            let row_id = RowId(vector_match.row_id);
            let hydration = match &hydration_projection {
                None => VectorHydrationState::NotRequested,
                Some(projection) => match self.lookup_row(&RowLookupRequest {
                    row_id,
                    projection: crate::query::Projection::Columns(projection.clone()),
                })? {
                    RowLookupResult {
                        outcome: RowLookupOutcome::Live(row),
                        ..
                    } => VectorHydrationState::Hydrated(row),
                    RowLookupResult {
                        outcome: RowLookupOutcome::Tombstoned,
                        ..
                    } => VectorHydrationState::Unresolved(crate::query::HydrationError::Tombstoned),
                    RowLookupResult {
                        outcome: RowLookupOutcome::NotFound,
                        ..
                    } => VectorHydrationState::Unresolved(crate::query::HydrationError::NotFound),
                },
            };
            hits.push(VectorHit {
                row_id,
                squared_l2_distance: vector_match.squared_distance,
                hydration,
            });
        }
        VectorSearchResult::new(request.k, hydration_projection, hits)
    }

    fn vector_filter_live_set(&self, filter: &FilterExpression) -> QueryResult<LiveSet> {
        let mut columns = Vec::new();
        filter_columns(filter, &mut columns);
        push_unique_column(&mut columns, ROW_ID_COLUMN);
        let query_schema = self.query_schema_for_columns(&columns)?;
        let pruning_predicate = filter_expression_pruning_predicate(filter);
        let row_ids = self
            .read_surviving_files(pruning_predicate.as_ref(), Some(&columns), |batch| {
                let batch = cast_batch_to_schema(&batch, &query_schema)?;
                let selection = filter_mask(&batch, filter)?;
                let row_ids = batch
                    .column(batch.schema_ref().index_of(ROW_ID_COLUMN)?)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| query_cast_error(ROW_ID_COLUMN, "UInt64"))?;
                (0..row_ids.len())
                    .filter(|&row| selection.is_valid(row) && selection.value(row))
                    .map(|row| usize::try_from(row_ids.value(row)).map_err(TxnError::from))
                    .collect::<Result<Vec<_>>>()
            })?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        Ok(LiveSet::from_row_ids(&row_ids))
    }

    /// Approximate nearest-neighbor search over the vector column, as of
    /// this snapshot's version, optionally narrowed to rows matching
    /// `predicate`. Visibility (the tombstone set) is enforced by passing
    /// `Self::is_visible` into
    /// [`SegmentSet::search`](strata_index::SegmentSet::search)/
    /// [`SegmentSet::search_filtered_pruned_live`](strata_index::SegmentSet::search_filtered_pruned_live)
    /// — see `docs/design.md`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::{Float32Array, Int64Array, RecordBatch};
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use strata_txn::Dataset;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = std::env::temp_dir()
    ///     .join(format!("strata-doctest-vector-search-{}", std::process::id()));
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int64, false),
    ///     Field::new(
    ///         "vector",
    ///         DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
    ///         false,
    ///     ),
    /// ]));
    /// let dataset = Dataset::create(&dir, Arc::clone(&schema))?;
    /// let ids = Arc::new(Int64Array::from(vec![1, 2]));
    /// let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    /// let values = Arc::new(Float32Array::from(vec![0.0, 0.0, 0.0, 9.0, 9.0, 9.0]));
    /// let vectors = Arc::new(arrow::array::FixedSizeListArray::new(item_field, 3, values, None));
    /// let batch = RecordBatch::try_new(schema, vec![ids, vectors])?;
    ///
    /// let mut txn = dataset.begin();
    /// txn.insert(batch)?;
    /// txn.commit()?;
    ///
    /// let results = dataset.snapshot().vector_search(&[0.0, 0.0, 0.0], 1, None)?;
    /// assert_eq!(results.len(), 1);
    /// assert_eq!(results[0].row_id, 0); // row-id 0 is the true nearest match
    /// # std::fs::remove_dir_all(&dir).ok();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if `predicate` is supplied and any of its columns
    /// doesn't exist or its value's type doesn't match the column's Arrow
    /// type, or if `query`'s dimensionality doesn't match the indexed
    /// vectors'.
    pub fn vector_search(
        &self,
        query: &[f32],
        k: usize,
        predicate: Option<&Predicate>,
    ) -> Result<Vec<strata_index::VectorMatch>> {
        let Some(predicate) = predicate else {
            return Ok(self
                .index
                .search(query, k, EF_SEARCH_DEFAULT, |id| self.is_visible(id))?);
        };

        // `row_ids_matching` re-reads the whole surviving data file per
        // call (see its own doc comment) — ~51 MB/query at 25k rows x
        // 512-dim, the single largest allocation source in the lifecycle
        // benchmark. `resolve_live_set` (below) resolves it through a
        // per-snapshot cache keyed by predicate identity instead of calling
        // it directly, so a live `Snapshot` queried with the same predicate
        // more than once pays that cost at most once. See
        // `docs/phase-1-performance.md`.
        //
        // Zone-map pruning (S1 W4b) shrinks the fan-out/search side of that
        // cost only — it does nothing for `row_ids_matching`/
        // `resolve_live_set` — so it is not expected to move this
        // end-to-end split; its proof of "working" is `Snapshot::explain`
        // reporting fewer segments scanned, not a wall-clock win (design
        // amendment §6).
        let live_set = self.resolve_live_set(predicate)?;
        let ef = widen_ef(EF_SEARCH_DEFAULT, self, predicate);
        Ok(self.index.search_filtered_pruned_live(
            query,
            k,
            ef,
            &live_set,
            |id| self.is_visible(id),
            |zone_map| zone_map_permits_scan(zone_map, predicate),
        )?)
    }

    /// Resolves `predicate`'s live set via `self.live_set_cache`, computing
    /// it with [`Snapshot::row_ids_matching`] on a miss. See
    /// `crate::live_set_cache`'s module doc for the caching/locking policy.
    fn resolve_live_set(&self, predicate: &Predicate) -> Result<Arc<LiveSet>> {
        let key = PredicateKey::from(predicate);
        self.live_set_cache.get_or_try_compute(key, || {
            let ids = self.row_ids_matching(predicate)?;
            Ok(LiveSet::from_row_ids(&ids))
        })
    }

    /// Resolves the row-ids of every row matching `predicate`, reading each
    /// surviving (per `should_scan_file`) file's raw on-disk batch
    /// directly — not through the public `scan_with_predicate`. Called at
    /// most once per `(Snapshot, Predicate)` pair via
    /// [`Snapshot::resolve_live_set`]'s cache; call this directly only if a
    /// caller genuinely needs an uncached read.
    fn row_ids_matching(&self, predicate: &Predicate) -> Result<Vec<usize>> {
        // Decode only the predicate's own columns and the row-id column, so
        // the embedding column is never turned into an Arrow array.
        //
        // Be clear about what this does *not* buy: Arrow IPC stores a record
        // batch as one contiguous message body, and `FileReader` reads that
        // whole body off disk before decoding anything. Projection therefore
        // skips array *construction*, not the read. Measured, this was worth
        // only ~2ms of a ~109ms call — the remaining ~105ms is re-reading
        // ~205MB (100k rows x 512-dim f32) from the page cache on *every*
        // uncached call, at ~1.5GB/s. `resolve_live_set`'s cache is what
        // amortizes this now for a snapshot queried with the same predicate
        // more than once — see
        // `docs/phase-1-performance.md`.
        //
        // Eliminating the underlying per-call cost needs a genuinely
        // column-chunked file format so a single column can be read without
        // its neighbours — the format change `datafile.rs`'s module doc
        // already defers. Not a drive-by change.
        let mut projection: Vec<&str> = predicate.columns();
        projection.push(ROW_ID_COLUMN);
        projection.sort_unstable();
        projection.dedup();
        let per_file_ids =
            self.read_surviving_files(Some(predicate), Some(&projection), |batch| {
                // Apply the selection mask to the row-id column *only*. Using
                // `filter` here instead would materialise a whole filtered
                // `RecordBatch` — every column, including the embedding column —
                // and then read one `u64` from it. At 512 dimensions that is
                // ~2KB copied and discarded per matched row; on a 100k-row
                // dataset with a 1-in-10 predicate it was ~20MB per query, and
                // it dominated filtered `vector_search` end to end.
                let selection = mask(&batch, predicate)?;
                let row_id_idx = batch.schema_ref().index_of(ROW_ID_COLUMN)?;
                let matched = arrow::compute::filter(batch.column(row_id_idx), &selection)?;
                let row_ids = matched
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        TxnError::Arrow(arrow::error::ArrowError::CastError(format!(
                            "{ROW_ID_COLUMN} column must be UInt64"
                        )))
                    })?;
                // Bulk-read the values buffer rather than calling `value(i)` per
                // row, which re-bounds-checks each access.
                #[allow(clippy::cast_possible_truncation)]
                let ids: Vec<usize> = row_ids.values().iter().map(|&id| id as usize).collect();
                Ok(ids)
            })?;
        Ok(per_file_ids.into_iter().flatten().collect())
    }
}

fn filter_row_ids(batch: RecordBatch, excluded_row_ids: &[u64]) -> Result<RecordBatch> {
    if excluded_row_ids.is_empty() {
        return Ok(batch);
    }
    let row_id_idx = batch.schema_ref().index_of(ROW_ID_COLUMN)?;
    let row_ids = batch
        .column(row_id_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| query_cast_error(ROW_ID_COLUMN, "UInt64"))?;
    let keep: BooleanArray = row_ids
        .values()
        .iter()
        .map(|row_id| !excluded_row_ids.contains(row_id))
        .collect();
    Ok(filter_record_batch(&batch, &keep)?)
}

fn logical_type(data_type: &DataType) -> Result<LogicalType> {
    let logical_type = match data_type {
        DataType::Boolean => LogicalType::Boolean,
        DataType::Int64 => LogicalType::Int64,
        DataType::UInt64 => LogicalType::UInt64,
        DataType::Float64 => LogicalType::Float64,
        DataType::Utf8 => LogicalType::Utf8,
        DataType::FixedSizeList(field, dimensions) if field.data_type() == &DataType::Float32 => {
            LogicalType::Vector {
                dimensions: usize::try_from(*dimensions)?,
            }
        }
        _ => {
            return Err(TxnError::Arrow(arrow::error::ArrowError::CastError(
                format!("unsupported query column type {data_type:?}"),
            )));
        }
    };
    Ok(logical_type)
}

fn scan_columns<'a>(
    projection: &'a [String],
    filter: Option<&'a FilterExpression>,
) -> Vec<&'a str> {
    let mut columns = Vec::with_capacity(projection.len() + 1);
    for column in projection {
        push_unique_column(&mut columns, column);
    }
    if let Some(filter) = filter {
        filter_columns(filter, &mut columns);
    }
    push_unique_column(&mut columns, ROW_ID_COLUMN);
    columns
}

fn group_by_columns(request: &GroupByRequest) -> Vec<&str> {
    let mut columns = Vec::with_capacity(
        request.group_by.len()
            + request.aggregates.len()
            + usize::from(request.filter.is_some())
            + 1,
    );
    for column in &request.group_by {
        push_unique_column(&mut columns, column);
    }
    for aggregate in &request.aggregates {
        push_unique_column(&mut columns, &aggregate.column);
    }
    if let Some(filter) = &request.filter {
        filter_columns(filter, &mut columns);
    }
    push_unique_column(&mut columns, ROW_ID_COLUMN);
    columns
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupKeyValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(u64),
    Utf8(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey(Vec<GroupKeyValue>);

impl GroupKey {
    fn new(values: &[ResultValue]) -> QueryResult<Self> {
        let values = values
            .iter()
            .map(|value| match value {
                ResultValue::Null => Ok(GroupKeyValue::Null),
                ResultValue::Boolean(value) => Ok(GroupKeyValue::Boolean(*value)),
                ResultValue::Int64(value) => Ok(GroupKeyValue::Int64(*value)),
                ResultValue::UInt64(value) => Ok(GroupKeyValue::UInt64(*value)),
                ResultValue::Float64(value) => Ok(GroupKeyValue::Float64(value.to_bits())),
                ResultValue::Utf8(value) => Ok(GroupKeyValue::Utf8(value.clone())),
                ResultValue::Vector(_) => {
                    Err(query_cast_error("group key", "scalar query type").into())
                }
            })
            .collect::<QueryResult<Vec<_>>>()?;
        Ok(Self(values))
    }
}

#[derive(Debug, Clone)]
enum AggregateState {
    Count(u64),
    Int64Sum(Option<i64>),
    Int64Minimum(Option<i64>),
    Int64Maximum(Option<i64>),
    Float64Sum(Option<f64>),
    Float64Minimum(Option<f64>),
    Float64Maximum(Option<f64>),
    Float64Average { sum: f64, count: u64 },
}

impl AggregateState {
    fn new(aggregate: &Aggregate, output: &AggregateOutput) -> QueryResult<Self> {
        let state = match (aggregate.function, output.data_type()) {
            (AggregateFunction::Count, LogicalType::UInt64) => Self::Count(0),
            (AggregateFunction::Sum, LogicalType::Int64) => Self::Int64Sum(None),
            (AggregateFunction::Minimum, LogicalType::Int64) => Self::Int64Minimum(None),
            (AggregateFunction::Maximum, LogicalType::Int64) => Self::Int64Maximum(None),
            (AggregateFunction::Sum, LogicalType::Float64) => Self::Float64Sum(None),
            (AggregateFunction::Minimum, LogicalType::Float64) => Self::Float64Minimum(None),
            (AggregateFunction::Maximum, LogicalType::Float64) => Self::Float64Maximum(None),
            (AggregateFunction::Average, LogicalType::Float64) => {
                Self::Float64Average { sum: 0.0, count: 0 }
            }
            _ => {
                return Err(
                    query_cast_error(&aggregate.column, "validated aggregate input").into(),
                );
            }
        };
        Ok(state)
    }

    fn update(&mut self, value: &ResultValue, alias: &str) -> QueryResult<()> {
        if matches!(value, ResultValue::Null) {
            return Ok(());
        }
        match self {
            Self::Count(count) => *count += 1,
            Self::Int64Sum(sum) => {
                let ResultValue::Int64(value) = value else {
                    return Err(query_cast_error(alias, "Int64").into());
                };
                *sum = Some(match *sum {
                    Some(current) => current.checked_add(*value).ok_or_else(|| {
                        QueryExecutionError::Int64SumOverflow {
                            alias: alias.to_owned(),
                        }
                    })?,
                    None => *value,
                });
            }
            Self::Int64Minimum(minimum) => {
                let ResultValue::Int64(value) = value else {
                    return Err(query_cast_error(alias, "Int64").into());
                };
                *minimum = Some(minimum.map_or(*value, |current| current.min(*value)));
            }
            Self::Int64Maximum(maximum) => {
                let ResultValue::Int64(value) = value else {
                    return Err(query_cast_error(alias, "Int64").into());
                };
                *maximum = Some(maximum.map_or(*value, |current| current.max(*value)));
            }
            Self::Float64Sum(sum) => {
                let value = float_value(value, alias)?;
                *sum = Some(sum.map_or(value, |current| current + value));
            }
            Self::Float64Minimum(minimum) => {
                let value = float_value(value, alias)?;
                *minimum = Some(minimum.map_or(value, |current| current.min(value)));
            }
            Self::Float64Maximum(maximum) => {
                let value = float_value(value, alias)?;
                *maximum = Some(maximum.map_or(value, |current| current.max(value)));
            }
            Self::Float64Average { sum, count } => {
                *sum += float_value(value, alias)?;
                *count += 1;
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: Self, alias: &str) -> QueryResult<()> {
        match (self, other) {
            (Self::Count(count), Self::Count(other)) => {
                *count = count
                    .checked_add(other)
                    .ok_or_else(|| query_cast_error(alias, "UInt64 count"))?;
            }
            (Self::Int64Sum(sum), Self::Int64Sum(other)) => {
                *sum = match (*sum, other) {
                    (Some(current), Some(other)) => {
                        Some(current.checked_add(other).ok_or_else(|| {
                            QueryExecutionError::Int64SumOverflow {
                                alias: alias.to_owned(),
                            }
                        })?)
                    }
                    (None, value) | (value, None) => value,
                };
            }
            (Self::Int64Minimum(minimum), Self::Int64Minimum(other)) => {
                *minimum = match (*minimum, other) {
                    (Some(current), Some(other)) => Some(current.min(other)),
                    (None, value) | (value, None) => value,
                };
            }
            (Self::Int64Maximum(maximum), Self::Int64Maximum(other)) => {
                *maximum = match (*maximum, other) {
                    (Some(current), Some(other)) => Some(current.max(other)),
                    (None, value) | (value, None) => value,
                };
            }
            (Self::Float64Sum(sum), Self::Float64Sum(other)) => {
                *sum = match (*sum, other) {
                    (Some(current), Some(other)) => Some(current + other),
                    (None, value) | (value, None) => value,
                };
            }
            (Self::Float64Minimum(minimum), Self::Float64Minimum(other)) => {
                *minimum = match (*minimum, other) {
                    (Some(current), Some(other)) => Some(current.min(other)),
                    (None, value) | (value, None) => value,
                };
            }
            (Self::Float64Maximum(maximum), Self::Float64Maximum(other)) => {
                *maximum = match (*maximum, other) {
                    (Some(current), Some(other)) => Some(current.max(other)),
                    (None, value) | (value, None) => value,
                };
            }
            (
                Self::Float64Average { sum, count },
                Self::Float64Average {
                    sum: other_sum,
                    count: other_count,
                },
            ) => {
                *sum += other_sum;
                *count = count
                    .checked_add(other_count)
                    .ok_or_else(|| query_cast_error(alias, "UInt64 count"))?;
            }
            (state, _) => {
                return Err(query_cast_error(alias, aggregate_state_type(state)).into());
            }
        }
        Ok(())
    }

    fn finish(self) -> ResultValue {
        match self {
            Self::Count(count) => ResultValue::UInt64(count),
            Self::Int64Sum(value) | Self::Int64Minimum(value) | Self::Int64Maximum(value) => {
                value.map_or(ResultValue::Null, ResultValue::Int64)
            }
            Self::Float64Sum(value) | Self::Float64Minimum(value) | Self::Float64Maximum(value) => {
                value.map_or(ResultValue::Null, ResultValue::Float64)
            }
            Self::Float64Average { sum, count } => {
                if count == 0 {
                    ResultValue::Null
                } else {
                    #[allow(clippy::cast_precision_loss)]
                    let count = count as f64;
                    ResultValue::Float64(sum / count)
                }
            }
        }
    }
}

fn aggregate_state_type(state: &AggregateState) -> &'static str {
    match state {
        AggregateState::Count(_) => "Count",
        AggregateState::Int64Sum(_) => "Int64Sum",
        AggregateState::Int64Minimum(_) => "Int64Minimum",
        AggregateState::Int64Maximum(_) => "Int64Maximum",
        AggregateState::Float64Sum(_) => "Float64Sum",
        AggregateState::Float64Minimum(_) => "Float64Minimum",
        AggregateState::Float64Maximum(_) => "Float64Maximum",
        AggregateState::Float64Average { .. } => "Float64Average",
    }
}

fn float_value(value: &ResultValue, alias: &str) -> QueryResult<f64> {
    match value {
        ResultValue::Int64(value) =>
        {
            #[allow(clippy::cast_precision_loss)]
            Ok(*value as f64)
        }
        ResultValue::Float64(value) => Ok(*value),
        _ => Err(query_cast_error(alias, "numeric query type").into()),
    }
}

#[derive(Debug)]
struct GroupState {
    keys: Vec<ResultValue>,
    aggregates: Vec<AggregateState>,
}

fn filter_columns<'a>(filter: &'a FilterExpression, columns: &mut Vec<&'a str>) {
    match filter {
        FilterExpression::Compare(comparison) => push_unique_column(columns, &comparison.column),
        FilterExpression::And(left, right) | FilterExpression::Or(left, right) => {
            filter_columns(left, columns);
            filter_columns(right, columns);
        }
        FilterExpression::Not(expression) => filter_columns(expression, columns),
    }
}

fn push_unique_column<'a>(columns: &mut Vec<&'a str>, column: &'a str) {
    if !columns.contains(&column) {
        columns.push(column);
    }
}

fn filter_mask(batch: &RecordBatch, filter: &FilterExpression) -> Result<BooleanArray> {
    match filter {
        FilterExpression::Compare(comparison) => comparison_mask(batch, comparison),
        FilterExpression::And(left, right) => {
            let left = filter_mask(batch, left)?;
            let right = filter_mask(batch, right)?;
            Ok(left
                .iter()
                .zip(right.iter())
                .map(|(left, right)| match (left, right) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                })
                .collect())
        }
        FilterExpression::Or(left, right) => {
            let left = filter_mask(batch, left)?;
            let right = filter_mask(batch, right)?;
            Ok(left
                .iter()
                .zip(right.iter())
                .map(|(left, right)| match (left, right) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                })
                .collect())
        }
        FilterExpression::Not(expression) => Ok(filter_mask(batch, expression)?
            .iter()
            .map(|value| value.map(|value| !value))
            .collect()),
    }
}

fn comparison_mask(batch: &RecordBatch, comparison: &Comparison) -> Result<BooleanArray> {
    let column_index = batch.schema_ref().index_of(&comparison.column)?;
    let array = batch.column(column_index);
    match &comparison.value {
        FilterLiteral::Boolean(value) => Ok(comparison_mask_for(
            &array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| query_cast_error(&comparison.column, "Boolean"))?,
            *value,
            comparison.operator,
        )),
        FilterLiteral::Int64(value) => Ok(comparison_mask_for(
            &array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| query_cast_error(&comparison.column, "Int64"))?,
            *value,
            comparison.operator,
        )),
        FilterLiteral::UInt64(value) => Ok(comparison_mask_for(
            &array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| query_cast_error(&comparison.column, "UInt64"))?,
            *value,
            comparison.operator,
        )),
        FilterLiteral::Float64(value) => Ok(comparison_mask_for(
            &array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| query_cast_error(&comparison.column, "Float64"))?,
            *value,
            comparison.operator,
        )),
        FilterLiteral::Utf8(value) => Ok(comparison_mask_for(
            &array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| query_cast_error(&comparison.column, "Utf8"))?,
            value.as_str(),
            comparison.operator,
        )),
    }
}

fn comparison_mask_for<T>(
    array: &impl arrow::array::ArrayAccessor<Item = T>,
    value: T,
    operator: ComparisonOperator,
) -> BooleanArray
where
    T: PartialEq + PartialOrd + Copy,
{
    (0..array.len())
        .map(|index| {
            (!array.is_null(index)).then(|| {
                let actual = array.value(index);
                match operator {
                    ComparisonOperator::Equal => actual == value,
                    ComparisonOperator::NotEqual => actual != value,
                    ComparisonOperator::LessThan => actual < value,
                    ComparisonOperator::LessThanOrEqual => actual <= value,
                    ComparisonOperator::GreaterThan => actual > value,
                    ComparisonOperator::GreaterThanOrEqual => actual >= value,
                }
            })
        })
        .collect()
}

fn projected_rows(batch: &RecordBatch, projection: &[String]) -> Result<Vec<ProjectedRow>> {
    (0..batch.num_rows())
        .map(|row| projected_row(batch, row, projection))
        .collect()
}

fn projected_row(batch: &RecordBatch, row: usize, projection: &[String]) -> Result<ProjectedRow> {
    let fields = projection
        .iter()
        .map(|name| {
            let column = batch.column(batch.schema_ref().index_of(name)?);
            Ok(ProjectedField::new(name, result_value(column, row)?))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ProjectedRow { fields })
}

fn result_value(array: &arrow::array::ArrayRef, row: usize) -> Result<ResultValue> {
    if array.is_null(row) {
        return Ok(ResultValue::Null);
    }
    let value = if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        ResultValue::Boolean(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        ResultValue::Int64(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        ResultValue::UInt64(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        ResultValue::Float64(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        ResultValue::Utf8(array.value(row).to_owned())
    } else if let Some(array) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        let values = array.value(row);
        let values = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| query_cast_error("vector", "FixedSizeList<Float32>"))?;
        ResultValue::Vector((0..values.len()).map(|index| values.value(index)).collect())
    } else {
        return Err(query_cast_error("projected column", "supported query type"));
    };
    Ok(value)
}

fn query_cast_error(column: &str, expected: &str) -> TxnError {
    TxnError::Arrow(arrow::error::ArrowError::CastError(format!(
        "query column {column:?} must be {expected}"
    )))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    type GroupByTestRow<'a> = (Option<&'a str>, Option<i64>, bool, Option<f64>);

    fn query_test_dataset(
        rows: &[(&str, Option<i64>, bool, u64)],
    ) -> (tempfile::TempDir, crate::Dataset) {
        use crate::Dataset;
        use arrow::array::{BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array};
        use arrow::datatypes::{DataType, Field, Schema};

        let temp = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("title", DataType::Utf8, false),
            Field::new("score", DataType::Int64, true),
            Field::new("active", DataType::Boolean, false),
            Field::new("rank", DataType::UInt64, false),
        ]));
        let dataset = Dataset::create(temp.path().join("dataset"), Arc::clone(&schema)).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|row| row.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut transaction = dataset.begin();
        transaction.insert(batch).unwrap();
        transaction.commit().unwrap();
        (temp, dataset)
    }

    #[test]
    fn snapshot_version_accessor_remains_bound_to_captured_snapshot() {
        let (_temp, dataset) = query_test_dataset(&[("before", Some(1), true, 1)]);
        let snapshot = dataset.snapshot();

        let mut transaction = dataset.begin();
        transaction.delete(0).unwrap();
        transaction.commit().unwrap();

        assert_eq!(snapshot.version(), 1);
        assert_eq!(dataset.current_version(), 2);
    }

    fn group_by_test_dataset() -> (tempfile::TempDir, crate::Dataset, SchemaRef) {
        use crate::Dataset;
        use arrow::datatypes::{DataType, Field, Schema};

        let temp = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
            Field::new("selected", DataType::Boolean, false),
            Field::new("ratio", DataType::Float64, true),
        ]));
        let dataset = Dataset::create(temp.path().join("dataset"), Arc::clone(&schema)).unwrap();
        (temp, dataset, schema)
    }

    fn append_group_by_rows(
        dataset: &crate::Dataset,
        schema: &SchemaRef,
        rows: &[GroupByTestRow<'_>],
    ) {
        use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};

        let batch = RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|row| row.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut transaction = dataset.begin();
        transaction.insert(batch).unwrap();
        transaction.commit().unwrap();
    }

    fn test_snapshot(tombstoned: &[u64]) -> Snapshot {
        Snapshot {
            dir: PathBuf::from("unused-in-these-tests"),
            storage: Arc::new(StorageOwner::local("unused-in-these-tests")),
            version: 1,
            lease: SnapshotLease::unregistered(1),
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            manifest: Arc::new(Manifest::empty()),
            // This test exercises `is_visible`'s tombstone check only and
            // never searches, so an empty segment set is exactly right and
            // avoids building an index nothing queries.
            index: strata_index::SegmentSet::empty(),
            tombstones: Arc::new(tombstoned.iter().copied().collect()),
            live_set_cache: LiveSetCache::new(LIVE_SET_CACHE_BYTE_BUDGET),
        }
    }

    #[test]
    fn a_non_tombstoned_row_is_visible() {
        let snapshot = test_snapshot(&[5]);
        assert!(snapshot.is_visible(0));
        assert!(snapshot.is_visible(6));
    }

    #[test]
    fn a_tombstoned_row_is_not_visible() {
        let snapshot = test_snapshot(&[5]);
        assert!(!snapshot.is_visible(5));
    }

    #[test]
    fn explain_gains_segment_fields_without_changing_the_existing_file_fields() {
        use crate::dataset::Dataset;
        use arrow::array::{Float32Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir =
            std::env::temp_dir().join(format!("strata-w4b-explain-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    StdArc::new(Field::new("item", DataType::Float32, false)),
                    3,
                ),
                false,
            ),
        ]));
        let dataset = Dataset::create(&dir, StdArc::clone(&schema)).unwrap();

        // Segment 0: every row tagged "a", clustered near the origin.
        let mut txn = dataset.begin();
        txn.insert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    StdArc::new(Int64Array::from(vec![1, 2])),
                    StdArc::new(StringArray::from(vec!["a", "a"])),
                    StdArc::new(arrow::array::FixedSizeListArray::new(
                        StdArc::new(Field::new("item", DataType::Float32, false)),
                        3,
                        StdArc::new(Float32Array::from(vec![0.0, 0.0, 0.0, 1.0, 1.0, 1.0])),
                        None,
                    )),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        // Segment 1: every row tagged "b", clustered far away.
        let mut txn = dataset.begin();
        txn.insert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    StdArc::new(Int64Array::from(vec![3, 4])),
                    StdArc::new(StringArray::from(vec!["b", "b"])),
                    StdArc::new(arrow::array::FixedSizeListArray::new(
                        StdArc::new(Field::new("item", DataType::Float32, false)),
                        3,
                        StdArc::new(Float32Array::from(vec![
                            9_000.0, 9_000.0, 9_000.0, 9_001.0, 9_001.0, 9_001.0,
                        ])),
                        None,
                    )),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let snapshot = dataset.snapshot();
        assert_eq!(snapshot.manifest.segments.len(), 2, "sanity: two segments");

        let predicate = Predicate::Eq("category".to_string(), Value::Utf8("b".to_string()));
        let explain = snapshot.explain(&predicate);

        // New fields: segment 0's zone map is category == "a" (min==max),
        // which cannot satisfy category == "b" -- it must be skipped.
        // Segment 1's zone map is category == "b" -- it must be scanned.
        assert_eq!(explain.segments_total, 2);
        assert_eq!(explain.segments_scanned.len(), 1, "{explain:?}");
        assert_eq!(explain.segments_skipped.len(), 1, "{explain:?}");
        assert_ne!(
            explain.segments_scanned[0], explain.segments_skipped[0],
            "the scanned and skipped segment must not be the same one"
        );

        // Existing fields must be completely unaffected by the new ones --
        // `category` also appears in each row file's own per-file stats
        // (segment 0's file only ever holds "a", segment 1's only "b"), so
        // file-level pruning was already skipping file 0 for this predicate
        // before this task added the segment fields at all: file-level
        // pruning being unaffected by segment-level pruning means the file
        // that survives (file 1) and the segment that survives
        // (segments_scanned[0]) still agree, not that every file is
        // scan-worthy.
        assert_eq!(explain.total_files, 2);
        assert_eq!(explain.scanned.len(), 1, "{explain:?}");
        assert_eq!(explain.skipped.len(), 1, "{explain:?}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn vector_search_with_a_selective_predicate_returns_correct_results_when_a_segment_is_pruned() {
        use crate::dataset::Dataset;
        use arrow::array::{Float32Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = std::env::temp_dir().join(format!(
            "strata-w4b-vector-search-pruning-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    StdArc::new(Field::new("item", DataType::Float32, false)),
                    3,
                ),
                false,
            ),
        ]));
        let dataset = Dataset::create(&dir, StdArc::clone(&schema)).unwrap();

        // Segment 0 ("a"): clustered at the origin -- nearest to the query
        // below, but must never be returned once filtered to category "b".
        let mut txn = dataset.begin();
        txn.insert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    StdArc::new(Int64Array::from(vec![1])),
                    StdArc::new(StringArray::from(vec!["a"])),
                    StdArc::new(arrow::array::FixedSizeListArray::new(
                        StdArc::new(Field::new("item", DataType::Float32, false)),
                        3,
                        StdArc::new(Float32Array::from(vec![0.0, 0.0, 0.0])),
                        None,
                    )),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        // Segment 1 ("b"): far from the query, but the only segment that
        // can satisfy `category == "b"`.
        let mut txn = dataset.begin();
        txn.insert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    StdArc::new(Int64Array::from(vec![2])),
                    StdArc::new(StringArray::from(vec!["b"])),
                    StdArc::new(arrow::array::FixedSizeListArray::new(
                        StdArc::new(Field::new("item", DataType::Float32, false)),
                        3,
                        StdArc::new(Float32Array::from(vec![9_000.0, 9_000.0, 9_000.0])),
                        None,
                    )),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let snapshot = dataset.snapshot();
        let predicate = Predicate::Eq("category".to_string(), Value::Utf8("b".to_string()));

        // Confirm pruning actually has something to prune here, exactly
        // like the `explain`-shaped assertion the design amendment
        // requires as W4b's proof of "working" (amendment §6).
        let explain = snapshot.explain(&predicate);
        assert_eq!(explain.segments_skipped.len(), 1);

        let results = snapshot
            .vector_search(&[0.0, 0.0, 0.0], 5, Some(&predicate))
            .unwrap();
        assert_eq!(results.len(), 1, "{results:?}");
        assert_eq!(
            results[0].row_id, 1,
            "row-id 1 is category \"b\"'s only row, even though it is far \
             from the query and row-id 0 (category \"a\", pruned) is much \
             nearer: {results:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn vector_search_prunes_correctly_after_reopening_the_dataset_through_load_segments() {
        use crate::dataset::Dataset;
        use arrow::array::{Float32Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = std::env::temp_dir().join(format!(
            "strata-w4b-load-segments-reopen-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    StdArc::new(Field::new("item", DataType::Float32, false)),
                    3,
                ),
                false,
            ),
        ]));
        let dataset = Dataset::create(&dir, StdArc::clone(&schema)).unwrap();

        // Segment 0 ("a"): clustered at the origin -- nearest to the query
        // below, but must never be returned once filtered to category "b".
        let mut txn = dataset.begin();
        txn.insert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    StdArc::new(Int64Array::from(vec![1])),
                    StdArc::new(StringArray::from(vec!["a"])),
                    StdArc::new(arrow::array::FixedSizeListArray::new(
                        StdArc::new(Field::new("item", DataType::Float32, false)),
                        3,
                        StdArc::new(Float32Array::from(vec![0.0, 0.0, 0.0])),
                        None,
                    )),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        // Segment 1 ("b"): far from the query, but the only segment that
        // can satisfy `category == "b"`.
        let mut txn = dataset.begin();
        txn.insert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    StdArc::new(Int64Array::from(vec![2])),
                    StdArc::new(StringArray::from(vec!["b"])),
                    StdArc::new(arrow::array::FixedSizeListArray::new(
                        StdArc::new(Field::new("item", DataType::Float32, false)),
                        3,
                        StdArc::new(Float32Array::from(vec![9_000.0, 9_000.0, 9_000.0])),
                        None,
                    )),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        // Drop the in-memory handle entirely so the reopened dataset's
        // `SegmentSet` is built exclusively by `load_segments` reading the
        // manifest back off disk, never `with_appended`.
        drop(dataset);

        let reopened = Dataset::open(&dir).unwrap();
        let snapshot = reopened.snapshot();
        let predicate = Predicate::Eq("category".to_string(), Value::Utf8("b".to_string()));

        // `Snapshot::explain` reads `self.manifest.segments` directly, never
        // the `SegmentSet` payload `load_segments` builds - so this proves
        // the zone map survives the manifest's own serde round-trip, not
        // that `load_segments`'s re-pairing into the `SegmentSet` is
        // correct. The `vector_search` call below is what actually
        // exercises that pairing.
        let explain = snapshot.explain(&predicate);
        assert_eq!(
            explain.segments_skipped.len(),
            1,
            "the zone map must survive the manifest round-trip and still prove segment \
             \"a\" cannot match after `Dataset::open`: {explain:?}"
        );

        let results = snapshot
            .vector_search(&[0.0, 0.0, 0.0], 5, Some(&predicate))
            .unwrap();
        assert_eq!(results.len(), 1, "{results:?}");
        assert_eq!(
            results[0].row_id, 1,
            "row-id 1 is category \"b\"'s only row, even though it is far \
             from the query and row-id 0 (category \"a\", pruned) is much \
             nearer, and even after the zone map was reloaded from disk: \
             {results:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Direct unit test of `zone_map_permits_scan` itself, using the real
    /// per-segment zone maps a commit actually produces (not hand-built
    /// stand-ins). Closes the gap the existing `explain`/`vector_search`
    /// pruning tests leave open: both of those narrow `live_ids` down to
    /// just the matching row before the zone-map gate ever runs, so they
    /// would still pass even if `zone_map_permits_scan` were replaced with
    /// `|_, _| true`, or if either construction site paired segments with an
    /// empty zone map. This test calls the gate directly and would catch
    /// exactly that regression.
    #[test]
    fn zone_map_permits_scan_prunes_segment_a_and_permits_segment_b_for_category_b() {
        use crate::dataset::Dataset;
        use arrow::array::{Float32Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = std::env::temp_dir().join(format!(
            "strata-w4b-zone-map-permits-scan-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    StdArc::new(Field::new("item", DataType::Float32, false)),
                    3,
                ),
                false,
            ),
        ]));
        let dataset = Dataset::create(&dir, StdArc::clone(&schema)).unwrap();

        // Segment 0: every row tagged "a".
        let mut txn = dataset.begin();
        txn.insert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    StdArc::new(Int64Array::from(vec![1])),
                    StdArc::new(StringArray::from(vec!["a"])),
                    StdArc::new(arrow::array::FixedSizeListArray::new(
                        StdArc::new(Field::new("item", DataType::Float32, false)),
                        3,
                        StdArc::new(Float32Array::from(vec![0.0, 0.0, 0.0])),
                        None,
                    )),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        // Segment 1: every row tagged "b".
        let mut txn = dataset.begin();
        txn.insert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    StdArc::new(Int64Array::from(vec![2])),
                    StdArc::new(StringArray::from(vec!["b"])),
                    StdArc::new(arrow::array::FixedSizeListArray::new(
                        StdArc::new(Field::new("item", DataType::Float32, false)),
                        3,
                        StdArc::new(Float32Array::from(vec![9_000.0, 9_000.0, 9_000.0])),
                        None,
                    )),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let snapshot = dataset.snapshot();
        assert_eq!(snapshot.manifest.segments.len(), 2, "sanity: two segments");

        // `manifest.segments` is append-only (`Transaction::commit` pushes
        // its own new entry), so segment 0 is the "a" commit and segment 1
        // is the "b" commit.
        let zone_map_of_a = snapshot.manifest.segments[0].zone_map.clone();
        let zone_map_of_b = snapshot.manifest.segments[1].zone_map.clone();

        let predicate = Predicate::Eq("category".to_string(), Value::Utf8("b".to_string()));

        let segment_a: Arc<dyn std::any::Any + Send + Sync> = Arc::new(zone_map_of_a);
        let segment_b: Arc<dyn std::any::Any + Send + Sync> = Arc::new(zone_map_of_b);

        assert!(
            !zone_map_permits_scan(&*segment_a, &predicate),
            "segment a's zone map is category \"a\" only, which cannot \
             satisfy category == \"b\""
        );
        assert!(
            zone_map_permits_scan(&*segment_b, &predicate),
            "segment b's zone map is category \"b\", which satisfies the \
             predicate"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn zone_map_permits_scan_fails_open_for_a_payload_that_is_not_a_column_stats_map() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let predicate = Predicate::Eq("x".to_string(), Value::Int64(1));
        let not_a_zone_map: Arc<dyn std::any::Any + Send + Sync> = Arc::new(42u32);

        assert!(
            zone_map_permits_scan(&*not_a_zone_map, &predicate),
            "a payload that isn't a `HashMap<String, ColumnStats>` must fail \
             open (must scan), never panic"
        );
    }

    /// Closes the gap the other pruning tests leave open: they only prove
    /// pruning produces correct *results*, which would also happen if the
    /// gate were fed an empty zone map, since `live_ids` already narrows
    /// results at the node level independent of the gate. This test spies
    /// on `search_filtered_pruned`'s own gate closure to prove the *actual*
    /// per-segment `zone_map` reaches it, at both construction sites:
    /// `with_appended` (an in-memory snapshot straight off `Transaction::commit`)
    /// and `from_segments` (`load_segments`, after a full `drop` + `Dataset::open`).
    #[test]
    #[allow(clippy::too_many_lines)]
    fn search_filtered_pruned_feeds_the_actual_zone_map_payload_to_its_gate() {
        use crate::dataset::Dataset;
        use arrow::array::{Float32Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::cell::RefCell;
        use std::sync::Arc as StdArc;

        fn assert_spy_sees_both_zone_maps(snapshot: &Snapshot) {
            let expected_a = snapshot.manifest.segments[0].zone_map.clone();
            let expected_b = snapshot.manifest.segments[1].zone_map.clone();

            let seen: RefCell<Vec<std::collections::HashMap<String, strata_storage::ColumnStats>>> =
                RefCell::new(Vec::new());
            let hits = snapshot
                .index
                .search_filtered_pruned(
                    &[0.0, 0.0, 0.0],
                    10,
                    64,
                    &[0, 1],
                    |_| true,
                    |zone_map| {
                        let map = zone_map
                            .downcast_ref::<std::collections::HashMap<String, strata_storage::ColumnStats>>()
                            .expect("every part in this test carries a real zone map")
                            .clone();
                        seen.borrow_mut().push(map);
                        true
                    },
                )
                .unwrap();
            assert_eq!(hits.len(), 2, "{hits:?}");

            let seen = seen.into_inner();
            assert_eq!(
                seen.len(),
                2,
                "the gate must be consulted exactly once per part: {seen:?}"
            );
            assert!(
                !expected_a.is_empty() && !expected_b.is_empty(),
                "sanity: both segments must have a non-empty zone map to make this test \
                 meaningful"
            );
            assert!(
                seen.contains(&expected_a),
                "one recorded payload must equal segment a's real zone map from the manifest: \
                 {seen:?} vs {expected_a:?}"
            );
            assert!(
                seen.contains(&expected_b),
                "one recorded payload must equal segment b's real zone map from the manifest: \
                 {seen:?} vs {expected_b:?}"
            );
        }

        let dir = std::env::temp_dir().join(format!(
            "strata-w4b-zone-map-payload-spy-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    StdArc::new(Field::new("item", DataType::Float32, false)),
                    3,
                ),
                false,
            ),
        ]));
        let dataset = Dataset::create(&dir, StdArc::clone(&schema)).unwrap();

        // Segment 0 ("a"), row-id 0.
        let mut txn = dataset.begin();
        txn.insert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    StdArc::new(Int64Array::from(vec![1])),
                    StdArc::new(StringArray::from(vec!["a"])),
                    StdArc::new(arrow::array::FixedSizeListArray::new(
                        StdArc::new(Field::new("item", DataType::Float32, false)),
                        3,
                        StdArc::new(Float32Array::from(vec![0.0, 0.0, 0.0])),
                        None,
                    )),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        // Segment 1 ("b"), row-id 1.
        let mut txn = dataset.begin();
        txn.insert(
            RecordBatch::try_new(
                schema,
                vec![
                    StdArc::new(Int64Array::from(vec![2])),
                    StdArc::new(StringArray::from(vec!["b"])),
                    StdArc::new(arrow::array::FixedSizeListArray::new(
                        StdArc::new(Field::new("item", DataType::Float32, false)),
                        3,
                        StdArc::new(Float32Array::from(vec![9_000.0, 9_000.0, 9_000.0])),
                        None,
                    )),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        // In-memory snapshot: exercises `with_appended` (`Transaction::commit`).
        assert_spy_sees_both_zone_maps(&dataset.snapshot());

        // Drop and reopen entirely: exercises `from_segments` (`load_segments`).
        drop(dataset);
        let reopened = Dataset::open(&dir).unwrap();
        assert_spy_sees_both_zone_maps(&reopened.snapshot());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_never_returns_a_tombstoned_row() {
        use crate::dataset::Dataset;
        use crate::mvp_fixtures::{mvp_batch, mvp_schema};

        let dir =
            std::env::temp_dir().join(format!("strata-scan-tombstone-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dataset = Dataset::create(&dir, mvp_schema()).unwrap();

        // Row-ids 0, 1, 2 in commit order.
        let mut txn = dataset.begin();
        txn.insert(
            mvp_batch(&[
                (0, "a", [0.0, 0.0, 0.0]),
                (1, "b", [1.0, 0.0, 0.0]),
                (2, "c", [2.0, 0.0, 0.0]),
            ])
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        let mut delete_txn = dataset.begin();
        delete_txn.delete(1).unwrap();
        delete_txn.commit().unwrap();

        let batch = dataset.snapshot().scan(&mvp_schema()).unwrap();
        assert_eq!(
            batch.num_rows(),
            2,
            "the tombstoned row must not appear in scan()'s row count"
        );
        let names = batch
            .column(batch.schema_ref().index_of("name").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert!(
            !(0..names.len()).any(|i| names.value(i) == "b"),
            "row-id 1 (name \"b\") was tombstoned and must not appear: {names:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_old_snapshot_still_sees_a_row_a_later_delete_tombstones_via_scan() {
        use crate::dataset::Dataset;
        use crate::mvp_fixtures::{mvp_batch, mvp_schema};

        let dir = std::env::temp_dir().join(format!(
            "strata-scan-tombstone-isolation-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let dataset = Dataset::create(&dir, mvp_schema()).unwrap();

        let mut txn = dataset.begin();
        txn.insert(
            mvp_batch(&[
                (0, "a", [0.0, 0.0, 0.0]),
                (1, "b", [1.0, 0.0, 0.0]),
                (2, "c", [2.0, 0.0, 0.0]),
            ])
            .unwrap(),
        )
        .unwrap();
        txn.commit().unwrap();

        // Take a snapshot BEFORE the delete.
        let old_snapshot = dataset.snapshot();

        let mut delete_txn = dataset.begin();
        delete_txn.delete(1).unwrap();
        delete_txn.commit().unwrap();

        let old_count = old_snapshot.scan(&mvp_schema()).unwrap().num_rows();
        assert_eq!(
            old_count, 3,
            "a Snapshot taken before a later delete must still see the deleted row via scan() \
             — tombstones are scoped to the snapshot that observes them, never applied \
             retroactively to a snapshot taken earlier"
        );

        let new_count = dataset.snapshot().scan(&mvp_schema()).unwrap().num_rows();
        assert_eq!(
            new_count, 2,
            "a freshly-taken snapshot must reflect the delete"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_with_predicate_never_returns_a_tombstoned_row() {
        use crate::dataset::Dataset;
        use crate::mvp_fixtures::{mvp_batch, mvp_schema};
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = std::env::temp_dir().join(format!(
            "strata-scan-with-predicate-tombstone-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let dataset = Dataset::create(&dir, mvp_schema()).unwrap();

        let mut txn = dataset.begin();
        txn.insert(mvp_batch(&[(0, "a", [0.0, 0.0, 0.0]), (1, "b", [1.0, 0.0, 0.0])]).unwrap())
            .unwrap();
        txn.commit().unwrap();

        let mut delete_txn = dataset.begin();
        delete_txn.delete(1).unwrap(); // deletes the row named "b"
        delete_txn.commit().unwrap();

        let predicate = Predicate::Eq("name".to_string(), Value::Utf8("b".to_string()));
        let batch = dataset
            .snapshot()
            .scan_with_predicate(&mvp_schema(), &predicate)
            .unwrap();
        assert_eq!(
            batch.num_rows(),
            0,
            "the row matching this predicate was tombstoned and must not be returned: {batch:?}"
        );

        // Sanity/positive control: a predicate matching the SURVIVING row
        // still works normally, so the zero-rows result above is "excluded
        // by the tombstone", not "scan_with_predicate returns nothing now".
        let surviving_predicate = Predicate::Eq("name".to_string(), Value::Utf8("a".to_string()));
        let surviving_batch = dataset
            .snapshot()
            .scan_with_predicate(&mvp_schema(), &surviving_predicate)
            .unwrap();
        assert_eq!(surviving_batch.num_rows(), 1, "{surviving_batch:?}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_update_makes_only_the_new_row_visible_via_scan_not_both() {
        // Regression test for the concrete consequence of the bug this task
        // fixes: `Transaction::update` is `delete(row_id)` + `insert(batch)`
        // committed atomically. Before this fix, `scan()` ignored tombstones
        // entirely, so an update's OLD row stayed visible forever alongside
        // its replacement — a silent duplicate-row bug, not just a
        // stale-delete one.
        use crate::dataset::Dataset;
        use crate::mvp_fixtures::{mvp_batch, mvp_row, mvp_schema};

        let dir = std::env::temp_dir().join(format!(
            "strata-update-no-duplicate-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let dataset = Dataset::create(&dir, mvp_schema()).unwrap();

        let mut txn = dataset.begin();
        txn.insert(mvp_batch(&[(0, "original", [0.0, 0.0, 0.0])]).unwrap())
            .unwrap();
        txn.commit().unwrap();

        let mut update_txn = dataset.begin();
        update_txn
            .update(0, mvp_row(0, "replacement", [1.0, 0.0, 0.0]).unwrap())
            .unwrap();
        update_txn.commit().unwrap();

        let batch = dataset.snapshot().scan(&mvp_schema()).unwrap();
        assert_eq!(
            batch.num_rows(),
            1,
            "an update must leave exactly one row visible, not both the pre- and \
             post-update version: {batch:?}"
        );
        let names = batch
            .column(batch.schema_ref().index_of("name").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "replacement", "{batch:?}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn filter_tombstoned_rows_errors_typed_when_row_id_column_is_missing() {
        // The `columns: Some(..)` precondition `read_surviving_files`
        // documents -- the projection must include `ROW_ID_COLUMN` -- is
        // unreachable through any real caller today, but the error path
        // that guards it is still real code and deserves direct coverage:
        // it must return a typed error, never panic.
        let snapshot = test_snapshot(&[5]);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap();

        let err = snapshot
            .filter_tombstoned_rows(batch)
            .expect_err("a batch with no ROW_ID_COLUMN must return a typed error, never panic");
        let message = err.to_string();
        assert!(
            !message.contains("must be UInt64"),
            "this must fail at the missing-column lookup, not the wrong-type downcast: {message}"
        );
    }

    #[test]
    fn filter_tombstoned_rows_errors_typed_when_row_id_column_is_the_wrong_type() {
        let snapshot = test_snapshot(&[5]);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(ROW_ID_COLUMN, arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap();

        let err = snapshot
            .filter_tombstoned_rows(batch)
            .expect_err("a non-UInt64 ROW_ID_COLUMN must return a typed error, never panic");
        assert!(
            err.to_string().contains("must be UInt64"),
            "this must fail at the wrong-type downcast specifically, matching \
             filter_tombstoned_rows's own CastError message: {err}"
        );
    }

    #[test]
    fn scan_handles_a_fully_tombstoned_file_alongside_a_live_one() {
        // `filter_tombstoned_rows` can reduce a whole file's batch to zero
        // rows -- confirms that flows cleanly through `concat_batches`
        // alongside a live file's non-empty batch, rather than erroring or
        // silently dropping the live file's rows too.
        use crate::dataset::Dataset;
        use crate::mvp_fixtures::{mvp_batch, mvp_schema};

        let dir = std::env::temp_dir().join(format!(
            "strata-scan-fully-tombstoned-file-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let dataset = Dataset::create(&dir, mvp_schema()).unwrap();

        // First commit -- its own data file, one row, row-id 0.
        let mut txn = dataset.begin();
        txn.insert(mvp_batch(&[(0, "a", [0.0, 0.0, 0.0])]).unwrap())
            .unwrap();
        txn.commit().unwrap();

        // Second commit -- a different data file, two rows, row-ids 1, 2.
        let mut txn2 = dataset.begin();
        txn2.insert(mvp_batch(&[(1, "b", [1.0, 0.0, 0.0]), (2, "c", [2.0, 0.0, 0.0])]).unwrap())
            .unwrap();
        txn2.commit().unwrap();

        // Delete the ONLY row in the first commit's file -- that file's
        // batch filters down to zero rows; the second file's is untouched.
        let mut delete_txn = dataset.begin();
        delete_txn.delete(0).unwrap();
        delete_txn.commit().unwrap();

        let snapshot = dataset.snapshot();
        assert_eq!(
            snapshot.data_files().len(),
            2,
            "this test's premise is two SEPARATE data files, one of which the delete above must \
             fully empty -- if a future change ever coalesced both commits into one file, this \
             test would keep passing at 2 rows below while no longer covering the \
             empty-batch-through-concat_batches case it exists for"
        );

        let batch = snapshot.scan(&mvp_schema()).unwrap();
        assert_eq!(
            batch.num_rows(),
            2,
            "a fully-tombstoned file must contribute zero rows without erroring, while a \
             live file alongside it is unaffected: {batch:?}"
        );
        let names = batch
            .column(batch.schema_ref().index_of("name").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert!(
            (0..names.len()).all(|i| names.value(i) != "a"),
            "the fully-tombstoned file's row must not appear: {names:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_query_reads_filter_columns_omitted_from_the_projection() {
        use crate::{
            Comparison, ComparisonOperator, FilterExpression, FilterLiteral, Projection,
            ResultValue, ScanRequest,
        };

        let (_temp, dataset) =
            query_test_dataset(&[("low", Some(3), true, 1), ("high", Some(11), false, 2)]);

        let result = dataset
            .snapshot()
            .scan_query(&ScanRequest {
                projection: Projection::Columns(vec!["title".into()]),
                filter: Some(FilterExpression::Compare(Comparison {
                    column: "score".into(),
                    operator: ComparisonOperator::GreaterThan,
                    value: FilterLiteral::Int64(10),
                })),
            })
            .unwrap();

        assert_eq!(result.projection, vec!["title"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].fields.len(), 1);
        assert_eq!(result.rows[0].fields[0].name, "title");
        assert_eq!(
            result.rows[0].fields[0].value,
            ResultValue::Utf8("high".into())
        );
    }

    #[cfg(feature = "test-fault-injection")]
    #[test]
    fn scan_query_accounts_for_only_projection_filter_and_row_id_columns() {
        use crate::{
            Comparison, ComparisonOperator, FilterExpression, FilterLiteral, Projection,
            ResultValue, ScanRequest,
        };
        use strata_storage::datafile::test_support::{
            ProjectedRead, record_directory_syncs, record_projected_reads,
        };

        // Break caught: widening scan-query's Arrow projection to every physical
        // column would silently decode unrelated data (for example vectors).
        let (_temp, dataset) =
            query_test_dataset(&[("low", Some(3), true, 1), ("high", Some(11), false, 2)]);
        let accounting = record_projected_reads();
        let _directory_syncs = record_directory_syncs();

        let result = dataset
            .snapshot()
            .scan_query(&ScanRequest {
                projection: Projection::Columns(vec!["title".into()]),
                filter: Some(FilterExpression::Compare(Comparison {
                    column: "score".into(),
                    operator: ComparisonOperator::GreaterThan,
                    value: FilterLiteral::Int64(10),
                })),
            })
            .unwrap();

        assert_eq!(
            accounting.projected_reads(),
            vec![ProjectedRead::new(["title", "score", "_row_id"])],
            "the storage projection must include only result, predicate, and tombstone columns"
        );
        assert_eq!(result.projection, vec!["title"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].fields[0].name, "title");
        assert_eq!(
            result.rows[0].fields[0].value,
            ResultValue::Utf8("high".into())
        );
    }

    #[test]
    fn scan_query_preserves_projection_order_and_keeps_only_true_comparisons() {
        use crate::{
            Comparison, ComparisonOperator, FilterExpression, FilterLiteral, Projection,
            ResultValue, ScanRequest,
        };

        let (_temp, dataset) = query_test_dataset(&[
            ("null", None, true, 1),
            ("low", Some(3), true, 2),
            ("high", Some(11), false, 3),
        ]);

        let result = dataset
            .snapshot()
            .scan_query(&ScanRequest {
                projection: Projection::Columns(vec!["rank".into(), "title".into()]),
                filter: Some(FilterExpression::Compare(Comparison {
                    column: "score".into(),
                    operator: ComparisonOperator::GreaterThan,
                    value: FilterLiteral::Int64(10),
                })),
            })
            .unwrap();

        assert_eq!(result.projection, vec!["rank", "title"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].fields,
            vec![
                crate::ProjectedField::new("rank", ResultValue::UInt64(3)),
                crate::ProjectedField::new("title", ResultValue::Utf8("high".into())),
            ]
        );
    }

    #[test]
    fn scan_query_filters_tombstones_before_returning_zero_column_rows() {
        use crate::{Projection, ScanRequest};

        let (_temp, dataset) =
            query_test_dataset(&[("deleted", Some(11), true, 1), ("live", Some(3), false, 2)]);
        let mut transaction = dataset.begin();
        transaction.delete(0).unwrap();
        transaction.commit().unwrap();

        let result = dataset
            .snapshot()
            .scan_query(&ScanRequest {
                projection: Projection::Columns(Vec::new()),
                filter: None,
            })
            .unwrap();

        assert!(result.projection.is_empty());
        assert_eq!(result.rows.len(), 1);
        assert!(result.rows[0].fields.is_empty());
    }

    #[test]
    fn scan_query_remains_bound_to_the_captured_snapshot() {
        use crate::{Projection, ResultValue, ScanRequest};

        let (_temp, dataset) = query_test_dataset(&[("before", Some(1), true, 1)]);
        let snapshot = dataset.snapshot();
        let schema = dataset.snapshot().schema();
        let appended = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["after"])),
                Arc::new(arrow::array::Int64Array::from(vec![Some(2)])),
                Arc::new(arrow::array::BooleanArray::from(vec![false])),
                Arc::new(arrow::array::UInt64Array::from(vec![2])),
            ],
        )
        .unwrap();
        let mut transaction = dataset.begin();
        transaction.insert(appended).unwrap();
        transaction.commit().unwrap();

        let result = snapshot
            .scan_query(&ScanRequest {
                projection: Projection::Columns(vec!["title".into()]),
                filter: None,
            })
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].fields[0].value,
            ResultValue::Utf8("before".into())
        );
    }

    #[test]
    fn scan_query_wraps_storage_failures_as_typed_engine_errors() {
        use crate::{Projection, QueryError, QueryExecutionError, ScanRequest, TxnError};

        let (temp, dataset) = query_test_dataset(&[("present", Some(1), true, 1)]);
        let snapshot = dataset.snapshot();
        let data_file = &snapshot.data_files()[0].name;
        std::fs::remove_file(temp.path().join("dataset").join("data").join(data_file)).unwrap();

        let error = snapshot
            .scan_query(&ScanRequest {
                projection: Projection::All,
                filter: None,
            })
            .expect_err("a missing committed file must surface a typed engine error");

        assert!(matches!(
            error,
            QueryError::Execution(QueryExecutionError::Engine(source))
                if matches!(source.as_ref(), TxnError::Storage(strata_storage::StorageError::Io(_)))
        ));
    }

    #[test]
    fn scan_query_decodes_dictionary_columns_after_reopen_before_filtering() {
        use crate::{
            Comparison, ComparisonOperator, FilterExpression, FilterLiteral, Projection,
            ResultValue, ScanRequest,
        };
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};

        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("dataset");
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let dataset = crate::Dataset::create(&dir, Arc::clone(&schema)).unwrap();
        let names: Vec<&str> = (0..100)
            .map(|index| if index % 2 == 0 { "alice" } else { "bob" })
            .collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap();
        let mut transaction = dataset.begin();
        transaction.insert(batch).unwrap();
        transaction.commit().unwrap();
        drop(dataset);

        let reopened = crate::Dataset::open(&dir).unwrap();
        let result = reopened
            .snapshot()
            .scan_query(&ScanRequest {
                projection: Projection::Columns(vec!["name".into()]),
                filter: Some(FilterExpression::Compare(Comparison {
                    column: "name".into(),
                    operator: ComparisonOperator::Equal,
                    value: FilterLiteral::Utf8("alice".into()),
                })),
            })
            .unwrap();

        assert_eq!(result.rows.len(), 50);
        assert!(
            result
                .rows
                .iter()
                .all(|row| { row.fields[0].value == ResultValue::Utf8("alice".into()) })
        );
    }

    #[test]
    fn scan_query_composite_filters_keep_only_true_under_three_valued_null_semantics() {
        use crate::{
            Comparison, ComparisonOperator, FilterExpression, FilterLiteral, Projection,
            ResultValue, ScanRequest,
        };

        let (_temp, dataset) = query_test_dataset(&[
            ("null", None, true, 1),
            ("low", Some(5), false, 2),
            ("middle", Some(15), true, 3),
            ("high", Some(25), false, 4),
        ]);
        let compare = |operator, value| {
            FilterExpression::Compare(Comparison {
                column: "score".into(),
                operator,
                value: FilterLiteral::Int64(value),
            })
        };
        let scan = |filter| {
            dataset
                .snapshot()
                .scan_query(&ScanRequest {
                    projection: Projection::Columns(vec!["title".into()]),
                    filter: Some(filter),
                })
                .unwrap()
                .rows
                .into_iter()
                .map(|row| row.fields[0].value.clone())
                .collect::<Vec<_>>()
        };

        let and_rows = scan(FilterExpression::And(
            Box::new(compare(ComparisonOperator::GreaterThan, 10)),
            Box::new(compare(ComparisonOperator::LessThan, 20)),
        ));
        assert_eq!(and_rows, vec![ResultValue::Utf8("middle".into())]);

        let or_rows = scan(FilterExpression::Or(
            Box::new(compare(ComparisonOperator::Equal, 5)),
            Box::new(compare(ComparisonOperator::GreaterThan, 20)),
        ));
        assert_eq!(
            or_rows,
            vec![
                ResultValue::Utf8("low".into()),
                ResultValue::Utf8("high".into()),
            ]
        );

        let not_rows = scan(FilterExpression::Not(Box::new(compare(
            ComparisonOperator::Equal,
            5,
        ))));
        assert_eq!(
            not_rows,
            vec![
                ResultValue::Utf8("middle".into()),
                ResultValue::Utf8("high".into()),
            ]
        );
    }

    #[test]
    fn row_lookup_returns_a_vectorless_live_row_in_requested_projection_order() {
        use crate::{Projection, ResultValue, RowId, RowLookupOutcome, RowLookupRequest};

        let (_temp, dataset) = query_test_dataset(&[("live", Some(7), true, 9)]);

        let result = dataset
            .snapshot()
            .lookup_row(&RowLookupRequest {
                row_id: RowId(0),
                projection: Projection::Columns(vec!["rank".into(), "title".into()]),
            })
            .unwrap();

        assert_eq!(result.row_id, RowId(0));
        assert_eq!(result.projection, vec!["rank", "title"]);
        assert_eq!(
            result.outcome,
            RowLookupOutcome::Live(ProjectedRow {
                fields: vec![
                    ProjectedField::new("rank", ResultValue::UInt64(9)),
                    ProjectedField::new("title", ResultValue::Utf8("live".into())),
                ],
            })
        );
    }

    #[test]
    fn row_lookup_keeps_an_old_snapshot_live_and_reports_a_newer_delete_as_tombstoned() {
        use crate::{Projection, ResultValue, RowId, RowLookupOutcome, RowLookupRequest};

        let (_temp, dataset) = query_test_dataset(&[("before-delete", Some(1), true, 1)]);
        let before_delete = dataset.snapshot();

        let mut transaction = dataset.begin();
        transaction.delete(0).unwrap();
        transaction.commit().unwrap();

        let request = RowLookupRequest {
            row_id: RowId(0),
            projection: Projection::Columns(vec!["title".into()]),
        };
        let old_result = before_delete.lookup_row(&request).unwrap();
        let new_result = dataset.snapshot().lookup_row(&request).unwrap();

        assert_eq!(
            old_result.outcome,
            RowLookupOutcome::Live(ProjectedRow {
                fields: vec![ProjectedField::new(
                    "title",
                    ResultValue::Utf8("before-delete".into()),
                )],
            })
        );
        assert_eq!(new_result.projection, vec!["title"]);
        assert_eq!(new_result.outcome, RowLookupOutcome::Tombstoned);
    }

    #[test]
    fn row_lookup_returns_not_found_for_never_allocated_gap_and_future_ids() {
        use crate::{Dataset, Projection, RowId, RowLookupOutcome, RowLookupRequest};

        let (temp, dataset) = query_test_dataset(&[("first", Some(1), true, 1)]);
        let dir = temp.path().join("dataset");
        drop(dataset);

        strata_storage::persist_row_id_high_water_at_least(&dir, 2).unwrap();
        let dataset = Dataset::open(&dir).unwrap();
        let schema = dataset.snapshot().schema();
        let appended = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["after-gap"])),
                Arc::new(Int64Array::from(vec![Some(2)])),
                Arc::new(BooleanArray::from(vec![false])),
                Arc::new(UInt64Array::from(vec![2])),
            ],
        )
        .unwrap();
        let mut transaction = dataset.begin();
        transaction.insert(appended).unwrap();
        transaction.commit().unwrap();

        let snapshot = dataset.snapshot();
        for row_id in [RowId(1), RowId(3), RowId(99)] {
            let result = snapshot
                .lookup_row(&RowLookupRequest {
                    row_id,
                    projection: Projection::Columns(vec!["title".into()]),
                })
                .unwrap();
            assert_eq!(result.outcome, RowLookupOutcome::NotFound, "{row_id:?}");
        }
    }

    #[test]
    fn row_lookup_uses_the_same_projection_validation_as_scan_query() {
        use crate::{Projection, QueryError, QueryValidationError, RowId, RowLookupRequest};

        let (_temp, dataset) = query_test_dataset(&[("live", Some(1), true, 1)]);
        let snapshot = dataset.snapshot();
        for projection in [
            Projection::Columns(vec!["missing".into()]),
            Projection::Columns(vec![ROW_ID_COLUMN.into()]),
            Projection::Columns(vec!["title".into(), "title".into()]),
        ] {
            let error = snapshot
                .lookup_row(&RowLookupRequest {
                    row_id: RowId(0),
                    projection,
                })
                .expect_err("invalid projections must remain typed query errors");
            assert!(
                matches!(
                    error,
                    QueryError::Validation(
                        QueryValidationError::UnknownColumn { .. }
                            | QueryValidationError::ReservedColumn { .. }
                            | QueryValidationError::DuplicateProjection { .. }
                    )
                ),
                "{error:?}"
            );
        }
    }

    #[test]
    fn row_lookup_decodes_dictionary_encoded_values_after_reopen() {
        use crate::{Dataset, Projection, ResultValue, RowId, RowLookupOutcome, RowLookupRequest};
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};

        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("dataset");
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let dataset = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["alice"; 100]))],
        )
        .unwrap();
        let mut transaction = dataset.begin();
        transaction.insert(batch).unwrap();
        transaction.commit().unwrap();
        drop(dataset);

        let result = Dataset::open(&dir)
            .unwrap()
            .snapshot()
            .lookup_row(&RowLookupRequest {
                row_id: RowId(0),
                projection: Projection::Columns(vec!["name".into()]),
            })
            .unwrap();

        assert_eq!(
            result.outcome,
            RowLookupOutcome::Live(ProjectedRow {
                fields: vec![ProjectedField::new(
                    "name",
                    ResultValue::Utf8("alice".into()),
                )],
            })
        );
    }

    #[test]
    fn row_lookup_wraps_missing_committed_data_as_a_typed_engine_error() {
        use crate::{
            Projection, QueryError, QueryExecutionError, RowId, RowLookupRequest, TxnError,
        };

        let (temp, dataset) = query_test_dataset(&[("present", Some(1), true, 1)]);
        let snapshot = dataset.snapshot();
        let data_file = &snapshot.data_files()[0].name;
        std::fs::remove_file(temp.path().join("dataset").join("data").join(data_file)).unwrap();

        let error = snapshot
            .lookup_row(&RowLookupRequest {
                row_id: RowId(0),
                projection: Projection::All,
            })
            .expect_err("a missing committed file must surface a typed engine error");

        assert!(matches!(
            error,
            QueryError::Execution(QueryExecutionError::Engine(source))
                if matches!(source.as_ref(), TxnError::Storage(strata_storage::StorageError::Io(_)))
        ));
    }

    #[test]
    fn group_by_query_merges_groups_across_files_in_manifest_order() {
        use crate::{Aggregate, AggregateFunction, GroupByRequest, ResultValue};

        let (_temp, dataset, schema) = group_by_test_dataset();
        append_group_by_rows(
            &dataset,
            &schema,
            &[
                (Some("a"), Some(1), true, Some(0.5)),
                (Some("b"), Some(2), true, Some(2.0)),
            ],
        );
        append_group_by_rows(&dataset, &schema, &[(Some("a"), Some(4), true, Some(1.5))]);

        let result = dataset
            .snapshot()
            .group_by_query(&GroupByRequest {
                group_by: vec!["category".into()],
                aggregates: vec![
                    Aggregate::new("amount", AggregateFunction::Count, "count"),
                    Aggregate::new("amount", AggregateFunction::Sum, "sum"),
                    Aggregate::new("amount", AggregateFunction::Average, "average"),
                    Aggregate::new("ratio", AggregateFunction::Sum, "ratio_sum"),
                ],
                filter: None,
            })
            .unwrap();

        assert_eq!(result.group_by(), ["category"]);
        assert_eq!(
            result
                .aggregates()
                .iter()
                .map(crate::AggregateOutput::alias)
                .collect::<Vec<_>>(),
            vec!["count", "sum", "average", "ratio_sum"]
        );
        assert_eq!(
            result.rows(),
            [
                crate::GroupedRow {
                    keys: vec![ResultValue::Utf8("a".into())],
                    aggregates: vec![
                        ResultValue::UInt64(2),
                        ResultValue::Int64(5),
                        ResultValue::Float64(2.5),
                        ResultValue::Float64(2.0),
                    ],
                },
                crate::GroupedRow {
                    keys: vec![ResultValue::Utf8("b".into())],
                    aggregates: vec![
                        ResultValue::UInt64(1),
                        ResultValue::Int64(2),
                        ResultValue::Float64(2.0),
                        ResultValue::Float64(2.0),
                    ],
                },
            ]
        );
    }

    #[test]
    fn group_by_query_preserves_null_keys_and_all_null_aggregate_contracts() {
        use crate::{Aggregate, AggregateFunction, GroupByRequest, ResultValue};

        let (_temp, dataset, schema) = group_by_test_dataset();
        append_group_by_rows(
            &dataset,
            &schema,
            &[
                (None, None, true, None),
                (Some("all-null"), None, true, None),
            ],
        );

        let result = dataset
            .snapshot()
            .group_by_query(&GroupByRequest {
                group_by: vec!["category".into()],
                aggregates: vec![
                    Aggregate::new("amount", AggregateFunction::Count, "count"),
                    Aggregate::new("amount", AggregateFunction::Sum, "sum"),
                    Aggregate::new("amount", AggregateFunction::Minimum, "minimum"),
                    Aggregate::new("amount", AggregateFunction::Maximum, "maximum"),
                    Aggregate::new("amount", AggregateFunction::Average, "average"),
                    Aggregate::new("ratio", AggregateFunction::Sum, "ratio_sum"),
                ],
                filter: None,
            })
            .unwrap();

        assert_eq!(
            result.rows(),
            [
                crate::GroupedRow {
                    keys: vec![ResultValue::Null],
                    aggregates: vec![
                        ResultValue::UInt64(0),
                        ResultValue::Null,
                        ResultValue::Null,
                        ResultValue::Null,
                        ResultValue::Null,
                        ResultValue::Null,
                    ],
                },
                crate::GroupedRow {
                    keys: vec![ResultValue::Utf8("all-null".into())],
                    aggregates: vec![
                        ResultValue::UInt64(0),
                        ResultValue::Null,
                        ResultValue::Null,
                        ResultValue::Null,
                        ResultValue::Null,
                        ResultValue::Null,
                    ],
                },
            ]
        );
    }

    #[test]
    fn group_by_query_returns_no_rows_when_the_snapshot_has_no_input() {
        use crate::{Aggregate, AggregateFunction, GroupByRequest};

        let (_temp, dataset, _schema) = group_by_test_dataset();
        let result = dataset
            .snapshot()
            .group_by_query(&GroupByRequest {
                group_by: vec!["category".into()],
                aggregates: vec![Aggregate::new("amount", AggregateFunction::Count, "count")],
                filter: None,
            })
            .unwrap();

        assert!(result.rows().is_empty());
    }

    #[test]
    fn group_by_query_reads_filter_only_columns_after_tombstone_filtering() {
        use crate::{
            Aggregate, AggregateFunction, Comparison, ComparisonOperator, FilterExpression,
            FilterLiteral, GroupByRequest, ResultValue,
        };

        let (_temp, dataset, schema) = group_by_test_dataset();
        append_group_by_rows(
            &dataset,
            &schema,
            &[
                (Some("deleted"), Some(100), true, Some(1.0)),
                (Some("kept"), Some(3), true, Some(1.0)),
                (Some("filtered"), Some(9), false, Some(1.0)),
            ],
        );
        let mut transaction = dataset.begin();
        transaction.delete(0).unwrap();
        transaction.commit().unwrap();

        let result = dataset
            .snapshot()
            .group_by_query(&GroupByRequest {
                group_by: vec!["category".into()],
                aggregates: vec![Aggregate::new("amount", AggregateFunction::Sum, "sum")],
                filter: Some(FilterExpression::Compare(Comparison {
                    column: "selected".into(),
                    operator: ComparisonOperator::Equal,
                    value: FilterLiteral::Boolean(true),
                })),
            })
            .unwrap();

        assert_eq!(
            result.rows(),
            [crate::GroupedRow {
                keys: vec![ResultValue::Utf8("kept".into())],
                aggregates: vec![ResultValue::Int64(3)],
            }]
        );
    }

    #[test]
    fn group_by_query_decodes_dictionary_values_after_reopen() {
        use crate::{Aggregate, AggregateFunction, Dataset, GroupByRequest, ResultValue};
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};

        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("dataset");
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let dataset = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
        let names = (0..100)
            .map(|index| if index % 2 == 0 { "alice" } else { "bob" })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap();
        let mut transaction = dataset.begin();
        transaction.insert(batch).unwrap();
        transaction.commit().unwrap();
        drop(dataset);

        let result = Dataset::open(&dir)
            .unwrap()
            .snapshot()
            .group_by_query(&GroupByRequest {
                group_by: vec!["name".into()],
                aggregates: vec![Aggregate::new("name", AggregateFunction::Count, "count")],
                filter: None,
            })
            .unwrap();

        assert_eq!(
            result.rows(),
            [
                crate::GroupedRow {
                    keys: vec![ResultValue::Utf8("alice".into())],
                    aggregates: vec![ResultValue::UInt64(50)],
                },
                crate::GroupedRow {
                    keys: vec![ResultValue::Utf8("bob".into())],
                    aggregates: vec![ResultValue::UInt64(50)],
                },
            ]
        );
    }

    #[test]
    fn group_by_query_rejects_invalid_group_contracts() {
        use crate::{
            Aggregate, AggregateFunction, GroupByRequest, QueryError, QueryValidationError,
        };

        let (_temp, dataset, _schema) = group_by_test_dataset();
        for request in [
            GroupByRequest {
                group_by: Vec::new(),
                aggregates: Vec::new(),
                filter: None,
            },
            GroupByRequest {
                group_by: vec![ROW_ID_COLUMN.into()],
                aggregates: Vec::new(),
                filter: None,
            },
            GroupByRequest {
                group_by: vec!["category".into(), "category".into()],
                aggregates: Vec::new(),
                filter: None,
            },
            GroupByRequest {
                group_by: vec!["missing".into()],
                aggregates: Vec::new(),
                filter: None,
            },
            GroupByRequest {
                group_by: vec!["category".into()],
                aggregates: vec![Aggregate::new("amount", AggregateFunction::Sum, "category")],
                filter: None,
            },
        ] {
            let error = dataset
                .snapshot()
                .group_by_query(&request)
                .expect_err("invalid group-by contracts must remain typed errors");
            assert!(
                matches!(
                    error,
                    QueryError::Validation(
                        QueryValidationError::EmptyGroupBy
                            | QueryValidationError::ReservedColumn { .. }
                            | QueryValidationError::DuplicateGroupColumn { .. }
                            | QueryValidationError::UnknownColumn { .. }
                            | QueryValidationError::DuplicateAggregateAlias { .. }
                    )
                ),
                "{error:?}"
            );
        }

        let temp = tempfile::tempdir().unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                "embedding",
                DataType::FixedSizeList(
                    Arc::new(arrow::datatypes::Field::new(
                        "item",
                        DataType::Float32,
                        false,
                    )),
                    2,
                ),
                false,
            ),
        ]));
        let vector_dataset =
            crate::Dataset::create(temp.path().join("vector-dataset"), schema).unwrap();
        assert!(matches!(
            vector_dataset.snapshot().group_by_query(&GroupByRequest {
                group_by: vec!["embedding".into()],
                aggregates: Vec::new(),
                filter: None,
            }),
            Err(QueryError::Validation(
                QueryValidationError::NonScalarGroupColumn { .. }
            ))
        ));
    }

    #[test]
    fn group_by_query_stays_bound_to_its_snapshot() {
        use crate::{Aggregate, AggregateFunction, GroupByRequest, ResultValue};

        let (_temp, dataset, schema) = group_by_test_dataset();
        append_group_by_rows(
            &dataset,
            &schema,
            &[(Some("before"), Some(1), true, Some(1.0))],
        );
        let snapshot = dataset.snapshot();
        append_group_by_rows(
            &dataset,
            &schema,
            &[(Some("after"), Some(2), true, Some(2.0))],
        );

        let result = snapshot
            .group_by_query(&GroupByRequest {
                group_by: vec!["category".into()],
                aggregates: vec![Aggregate::new("amount", AggregateFunction::Sum, "sum")],
                filter: None,
            })
            .unwrap();

        assert_eq!(
            result.rows(),
            [crate::GroupedRow {
                keys: vec![ResultValue::Utf8("before".into())],
                aggregates: vec![ResultValue::Int64(1)],
            }]
        );
    }

    #[test]
    fn group_by_query_returns_a_typed_error_for_int64_sum_overflow() {
        use crate::{
            Aggregate, AggregateFunction, GroupByRequest, QueryError, QueryExecutionError,
        };

        let (_temp, dataset, schema) = group_by_test_dataset();
        append_group_by_rows(
            &dataset,
            &schema,
            &[
                (Some("overflow"), Some(i64::MAX), true, Some(1.0)),
                (Some("overflow"), Some(1), true, Some(1.0)),
            ],
        );

        assert!(matches!(
            dataset.snapshot().group_by_query(&GroupByRequest {
                group_by: vec!["category".into()],
                aggregates: vec![Aggregate::new("amount", AggregateFunction::Sum, "sum")],
                filter: None,
            }),
            Err(QueryError::Execution(QueryExecutionError::Int64SumOverflow { alias })) if alias == "sum"
        ));
    }

    #[test]
    fn aggregate_states_merge_independent_partials_with_checked_and_null_semantics() {
        use crate::{Aggregate, AggregateFunction, AggregateOutput, LogicalType, ResultValue};

        let count = Aggregate::new("amount", AggregateFunction::Count, "count");
        let sum = Aggregate::new("amount", AggregateFunction::Sum, "sum");
        let average = Aggregate::new("amount", AggregateFunction::Average, "average");
        let count_output = AggregateOutput::new("count", LogicalType::UInt64);
        let sum_output = AggregateOutput::new("sum", LogicalType::Int64);
        let average_output = AggregateOutput::new("average", LogicalType::Float64);

        let mut first = vec![
            AggregateState::new(&count, &count_output).unwrap(),
            AggregateState::new(&sum, &sum_output).unwrap(),
            AggregateState::new(&average, &average_output).unwrap(),
        ];
        let mut second = first.clone();
        for state in &mut first {
            state.update(&ResultValue::Null, "aggregate").unwrap();
        }
        for state in &mut second {
            state.update(&ResultValue::Int64(2), "aggregate").unwrap();
            state.update(&ResultValue::Int64(4), "aggregate").unwrap();
        }

        for (left, right) in first.iter_mut().zip(second) {
            left.merge(right, "aggregate").unwrap();
        }

        assert_eq!(first[0].clone().finish(), ResultValue::UInt64(2));
        assert_eq!(first[1].clone().finish(), ResultValue::Int64(6));
        assert_eq!(first[2].clone().finish(), ResultValue::Float64(3.0));
    }

    #[test]
    fn vector_search_query_returns_nearest_hits_with_row_id_tie_order() {
        use crate::{VectorHydration, VectorHydrationState, VectorSearchRequest};
        use arrow::array::{Float32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let temp = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2),
                false,
            ),
        ]));
        let dataset =
            crate::Dataset::create(temp.path().join("dataset"), Arc::clone(&schema)).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["exact", "left", "right"])),
                Arc::new(FixedSizeListArray::new(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    2,
                    Arc::new(Float32Array::from(vec![0.0, 0.0, -1.0, 0.0, 1.0, 0.0])),
                    None,
                )),
            ],
        )
        .unwrap();
        let mut transaction = dataset.begin();
        transaction.insert(batch).unwrap();
        transaction.commit().unwrap();

        let result = dataset
            .snapshot()
            .vector_search_query(&VectorSearchRequest {
                vector_column: "vector".into(),
                query: vec![0.0, 0.0],
                k: 3,
                filter: None,
                hydration: VectorHydration::NotRequested,
            })
            .unwrap();

        assert_eq!(
            result
                .hits()
                .iter()
                .map(|hit| (hit.row_id.0, hit.squared_l2_distance))
                .collect::<Vec<_>>(),
            vec![(0, 0.0), (1, 1.0), (2, 1.0)]
        );
        assert!(
            result
                .hits()
                .iter()
                .all(|hit| matches!(hit.hydration, VectorHydrationState::NotRequested))
        );
    }

    fn vector_query_dataset(
        rows: &[(&str, bool, Option<[f32; 2]>)],
    ) -> (tempfile::TempDir, crate::Dataset, SchemaRef) {
        use arrow::datatypes::{DataType, Field, Schema};

        let temp = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("selected", DataType::Boolean, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2),
                true,
            ),
        ]));
        let dataset =
            crate::Dataset::create(temp.path().join("dataset"), Arc::clone(&schema)).unwrap();
        append_vector_query_rows(&dataset, &schema, rows);
        (temp, dataset, schema)
    }

    fn append_vector_query_rows(
        dataset: &crate::Dataset,
        schema: &SchemaRef,
        rows: &[(&str, bool, Option<[f32; 2]>)],
    ) {
        use arrow::array::{Float32Array, StringArray};

        let values = rows
            .iter()
            .flat_map(|row| row.2.unwrap_or([0.0, 0.0]))
            .collect::<Vec<_>>();
        let vectors = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            2,
            Arc::new(Float32Array::from(values)),
            Some(arrow::buffer::NullBuffer::from(
                rows.iter().map(|row| row.2.is_some()).collect::<Vec<_>>(),
            )),
        );
        let batch = RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(vectors),
            ],
        )
        .unwrap();
        let mut transaction = dataset.begin();
        transaction.insert(batch).unwrap();
        transaction.commit().unwrap();
    }

    #[test]
    fn vector_search_query_rejects_zero_k_dimension_mismatch_and_non_finite_queries() {
        use crate::{QueryError, QueryValidationError, VectorHydration, VectorSearchRequest};

        let (_temp, dataset, _schema) = vector_query_dataset(&[("row", true, Some([0.0, 0.0]))]);
        for request in [
            VectorSearchRequest {
                vector_column: "vector".into(),
                query: vec![0.0, 0.0],
                k: 0,
                filter: None,
                hydration: VectorHydration::NotRequested,
            },
            VectorSearchRequest {
                vector_column: "vector".into(),
                query: vec![0.0],
                k: 1,
                filter: None,
                hydration: VectorHydration::NotRequested,
            },
            VectorSearchRequest {
                vector_column: "vector".into(),
                query: vec![0.0, f32::NAN],
                k: 1,
                filter: None,
                hydration: VectorHydration::NotRequested,
            },
        ] {
            assert!(
                matches!(
                    dataset.snapshot().vector_search_query(&request),
                    Err(QueryError::Validation(
                        QueryValidationError::InvalidVectorK
                            | QueryValidationError::VectorDimensionMismatch { .. }
                            | QueryValidationError::NonFiniteVectorComponent { .. }
                    ))
                ),
                "{request:?}"
            );
        }
    }

    #[test]
    fn vector_search_query_filters_before_accepting_hits_and_allows_underfill() {
        use crate::{
            Comparison, ComparisonOperator, FilterExpression, FilterLiteral, VectorHydration,
            VectorSearchRequest,
        };

        let (_temp, dataset, _schema) = vector_query_dataset(&[
            ("near-but-filtered", false, Some([0.0, 0.0])),
            ("far-and-selected", true, Some([10.0, 10.0])),
        ]);
        let result = dataset
            .snapshot()
            .vector_search_query(&VectorSearchRequest {
                vector_column: "vector".into(),
                query: vec![0.0, 0.0],
                k: 4,
                filter: Some(FilterExpression::Compare(Comparison {
                    column: "selected".into(),
                    operator: ComparisonOperator::Equal,
                    value: FilterLiteral::Boolean(true),
                })),
                hydration: VectorHydration::NotRequested,
            })
            .unwrap();

        assert_eq!(result.requested_k(), 4);
        assert_eq!(result.hits().len(), 1);
        assert_eq!(result.hits()[0].row_id.0, 1);
        assert!((result.hits()[0].squared_l2_distance - 200.0).abs() < f32::EPSILON);
    }

    #[test]
    fn vector_search_query_excludes_null_vector_rows() {
        use crate::{VectorHydration, VectorSearchRequest};

        let (_temp, dataset, _schema) = vector_query_dataset(&[
            ("indexed", true, Some([1.0, 1.0])),
            ("null-vector", true, None),
        ]);
        let result = dataset
            .snapshot()
            .vector_search_query(&VectorSearchRequest {
                vector_column: "vector".into(),
                query: vec![0.0, 0.0],
                k: 2,
                filter: None,
                hydration: VectorHydration::NotRequested,
            })
            .unwrap();

        assert_eq!(result.hits().len(), 1);
        assert_eq!(result.hits()[0].row_id.0, 0);
    }

    #[test]
    fn vector_search_query_hydrates_requested_projection_from_the_same_snapshot() {
        use crate::{
            ProjectedField, Projection, ResultValue, VectorHydration, VectorHydrationState,
            VectorSearchRequest,
        };

        let (_temp, dataset, _schema) =
            vector_query_dataset(&[("hydrated", true, Some([0.0, 0.0]))]);
        let result = dataset
            .snapshot()
            .vector_search_query(&VectorSearchRequest {
                vector_column: "vector".into(),
                query: vec![0.0, 0.0],
                k: 1,
                filter: None,
                hydration: VectorHydration::Projection(Projection::Columns(vec!["name".into()])),
            })
            .unwrap();

        assert_eq!(
            result.hydration_projection(),
            Some(["name".into()].as_slice())
        );
        assert!(matches!(
            &result.hits()[0].hydration,
            VectorHydrationState::Hydrated(ProjectedRow { fields })
                if fields == &[ProjectedField::new("name", ResultValue::Utf8("hydrated".into()))]
        ));
    }

    #[test]
    fn vector_search_query_keeps_unresolved_index_hits_in_the_result() {
        use crate::{
            HydrationError, Projection, VectorHydration, VectorHydrationState, VectorSearchRequest,
        };

        let (_temp, dataset, _schema) =
            vector_query_dataset(&[("indexed", true, Some([0.0, 0.0]))]);
        let indexed_snapshot = dataset.snapshot();
        let snapshot = Snapshot {
            dir: indexed_snapshot.dir.clone(),
            storage: Arc::clone(&indexed_snapshot.storage),
            version: indexed_snapshot.version,
            lease: SnapshotLease::unregistered(indexed_snapshot.version),
            schema: Arc::clone(&indexed_snapshot.schema),
            manifest: Arc::new(Manifest::empty()),
            index: indexed_snapshot.index.clone(),
            tombstones: Arc::clone(&indexed_snapshot.tombstones),
            live_set_cache: LiveSetCache::new(LIVE_SET_CACHE_BYTE_BUDGET),
        };

        let result = snapshot
            .vector_search_query(&VectorSearchRequest {
                vector_column: "vector".into(),
                query: vec![0.0, 0.0],
                k: 1,
                filter: None,
                hydration: VectorHydration::Projection(Projection::Columns(vec!["name".into()])),
            })
            .unwrap();

        assert_eq!(result.hits().len(), 1);
        assert!(matches!(
            result.hits()[0].hydration,
            VectorHydrationState::Unresolved(HydrationError::NotFound)
        ));
    }

    #[test]
    fn vector_search_query_decodes_dictionary_hydration_after_reopen() {
        use crate::{
            Dataset, Projection, ResultValue, VectorHydration, VectorHydrationState,
            VectorSearchRequest,
        };
        use arrow::array::{Float32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("dataset");
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2),
                false,
            ),
        ]));
        let dataset = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
        let vector_values = (0_u8..100)
            .flat_map(|value| [f32::from(value), 0.0])
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["repeated"; 100])),
                Arc::new(FixedSizeListArray::new(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    2,
                    Arc::new(Float32Array::from(vector_values)),
                    None,
                )),
            ],
        )
        .unwrap();
        let mut transaction = dataset.begin();
        transaction.insert(batch).unwrap();
        transaction.commit().unwrap();
        let data_file = dataset.snapshot().data_files()[0].name.clone();
        let on_disk = read_batch(&dir.join("data").join(data_file)).unwrap();
        assert!(matches!(
            on_disk
                .schema_ref()
                .field_with_name("name")
                .unwrap()
                .data_type(),
            DataType::Dictionary(_, _)
        ));
        drop(dataset);

        let result = Dataset::open(&dir)
            .unwrap()
            .snapshot()
            .vector_search_query(&VectorSearchRequest {
                vector_column: "vector".into(),
                query: vec![0.0, 0.0],
                k: 1,
                filter: None,
                hydration: VectorHydration::Projection(Projection::Columns(vec!["name".into()])),
            })
            .unwrap();

        assert!(matches!(
            &result.hits()[0].hydration,
            VectorHydrationState::Hydrated(ProjectedRow { fields })
                if fields[0].value == ResultValue::Utf8("repeated".into())
        ));
    }

    #[test]
    fn vector_search_query_keeps_old_snapshot_bound_to_its_original_segments_and_rows() {
        use crate::{
            Projection, ResultValue, VectorHydration, VectorHydrationState, VectorSearchRequest,
        };

        let (_temp, dataset, schema) = vector_query_dataset(&[("before", true, Some([0.0, 0.0]))]);
        let old_snapshot = dataset.snapshot();
        append_vector_query_rows(&dataset, &schema, &[("after", true, Some([10.0, 10.0]))]);

        let result = old_snapshot
            .vector_search_query(&VectorSearchRequest {
                vector_column: "vector".into(),
                query: vec![10.0, 10.0],
                k: 2,
                filter: None,
                hydration: VectorHydration::Projection(Projection::Columns(vec!["name".into()])),
            })
            .unwrap();

        assert_eq!(result.hits().len(), 1);
        assert!(matches!(
            &result.hits()[0].hydration,
            VectorHydrationState::Hydrated(ProjectedRow { fields })
                if fields[0].value == ResultValue::Utf8("before".into())
        ));
    }

    #[test]
    fn vector_search_query_reports_missing_hydration_data_as_a_typed_engine_error() {
        use crate::{
            Projection, QueryError, QueryExecutionError, VectorHydration, VectorSearchRequest,
        };

        let (temp, dataset, _schema) = vector_query_dataset(&[("present", true, Some([0.0, 0.0]))]);
        let snapshot = dataset.snapshot();
        let data_file = &snapshot.data_files()[0].name;
        std::fs::remove_file(temp.path().join("dataset").join("data").join(data_file)).unwrap();

        let error = snapshot
            .vector_search_query(&VectorSearchRequest {
                vector_column: "vector".into(),
                query: vec![0.0, 0.0],
                k: 1,
                filter: None,
                hydration: VectorHydration::Projection(Projection::All),
            })
            .expect_err("missing committed hydration data must be a typed engine error");

        assert!(matches!(
            error,
            QueryError::Execution(QueryExecutionError::Engine(source))
                if matches!(source.as_ref(), TxnError::Storage(strata_storage::StorageError::Io(_)))
        ));
    }
}
