//! A thin Python facade over Strata's immutable snapshot query API.
//!
//! The extension remains embedded and supports one process sharing one
//! [`strata_txn::Dataset`] handle. It provides bounded read/write transactions:
//! scans read the immutable base snapshot plus the transaction's own overlay,
//! while vector search is rejected when staged writes would make base-index
//! results stale. It does not provide cross-process coordination, general
//! transactional reads, or stronger isolation.

use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict};
use strata_txn::arrow::array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array,
    StringArray, UInt64Array,
};
use strata_txn::arrow::buffer::NullBuffer;
use strata_txn::arrow::datatypes::{DataType, Field, Schema};
use strata_txn::arrow::ipc::reader::StreamReader;
use strata_txn::arrow::ipc::writer::StreamWriter;
use strata_txn::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use strata_txn::{
    Aggregate, AggregateFunction, Comparison, ComparisonOperator, FilterExpression, FilterLiteral,
    GroupByRequest, GroupByResult, HydrationError, LogicalOperator, LogicalType, PhysicalOperator,
    PhysicalPlan, ProjectedRow, Projection, QueryError, QueryExecutionError, ResultValue, RowId,
    RowLookupOutcome, RowLookupRequest, ScanRequest, ScanResult, SchemaMigration, Snapshot,
    StorageError, Transaction, TxnError, VectorHydration, VectorHydrationState,
    VectorSearchRequest, VectorSearchResult,
};

const PYTHON_API_VERSION: &str = "1.0";

create_exception!(strata_ext, StrataError, pyo3::exceptions::PyException);
create_exception!(strata_ext, ValidationError, StrataError);
create_exception!(strata_ext, ExecutionError, StrataError);
create_exception!(strata_ext, ConflictError, StrataError);
create_exception!(strata_ext, InsufficientHistoryError, StrataError);
create_exception!(strata_ext, SchemaMigrationError, ValidationError);
create_exception!(strata_ext, InvalidQueryError, ValidationError);
create_exception!(strata_ext, UnsupportedTransactionReadError, ExecutionError);
create_exception!(strata_ext, StorageDurabilityError, ExecutionError);
create_exception!(strata_ext, CorruptionError, ExecutionError);

/// A Python handle to one embedded Strata dataset.
#[pyclass(name = "Dataset", module = "strata_ext")]
struct PyDataset {
    inner: strata_txn::Dataset,
}

#[pymethods]
impl PyDataset {
    /// Creates a dataset with the supplied stable Python schema descriptor.
    ///
    /// Each field is `(name, type_name, nullable)`. Supported `type_name`
    /// values are `bool`, `int64`, `uint64`, `float64`, `utf8`, and
    /// `vector[N]` for a fixed-size Float32 vector.
    #[staticmethod]
    fn create(
        py: Python<'_>,
        path: PathBuf,
        fields: Vec<(String, String, bool)>,
    ) -> PyResult<Self> {
        let schema = schema_from_python(fields)?;
        py.detach(move || strata_txn::Dataset::create(path, schema))
            .map(|inner| Self { inner })
            .map_err(|error| map_txn_error(&error))
    }

    /// Returns the stable Python API major/minor marker for this handle.
    #[allow(clippy::unused_self)]
    fn api_version(&self) -> &'static str {
        PYTHON_API_VERSION
    }

    /// Returns the current durable manifest version.
    fn version(&self) -> u64 {
        self.inner.snapshot().version()
    }

    fn schema_version(&self) -> u32 {
        self.inner.schema_version()
    }

    /// Adds one nullable column through the only supported explicit migration.
    fn migrate_add_nullable_column(
        &self,
        py: Python<'_>,
        name: String,
        type_name: &str,
    ) -> PyResult<Py<PyDict>> {
        let data_type = python_type_to_arrow(type_name)?;
        let migration = SchemaMigration::add_nullable_column(
            self.inner.schema_version(),
            self.inner.schema_version().saturating_add(1),
            Field::new(name, data_type, true),
        );
        let result = py
            .detach(|| self.inner.migrate_schema(&migration))
            .map_err(|error| map_txn_error(&error))?;
        migration_result_to_python(py, &result)
    }

    /// Opens an existing embedded dataset at `path`.
    ///
    /// The resulting handle is intended for sharing inside one Python process.
    /// Opening the same path independently does not provide cross-process
    /// coordination.
    #[staticmethod]
    fn open(py: Python<'_>, path: PathBuf) -> PyResult<Self> {
        py.detach(move || strata_txn::Dataset::open(path))
            .map(|inner| Self { inner })
            .map_err(|error| map_txn_error(&error))
    }

    /// Captures an immutable point-in-time snapshot for query methods.
    fn snapshot(&self, py: Python<'_>) -> PySnapshot {
        let dataset = self.inner.clone();
        let inner = py.detach(move || dataset.snapshot());
        PySnapshot { inner }
    }

    /// Begins a transaction with a private read-your-writes overlay.
    fn begin(&self) -> PyTransaction {
        let transaction = self.inner.begin();
        PyTransaction {
            schema: transaction.schema(),
            inner: Some(transaction),
            state: TransactionState::Active,
        }
    }
}

fn schema_from_python(fields: Vec<(String, String, bool)>) -> PyResult<Arc<Schema>> {
    fields
        .into_iter()
        .map(|(name, type_name, nullable)| {
            python_type_to_arrow(&type_name).map(|data_type| Field::new(name, data_type, nullable))
        })
        .collect::<PyResult<Vec<_>>>()
        .map(Schema::new)
        .map(Arc::new)
}

fn python_type_to_arrow(type_name: &str) -> PyResult<DataType> {
    match type_name {
        "bool" => Ok(DataType::Boolean),
        "int64" => Ok(DataType::Int64),
        "uint64" => Ok(DataType::UInt64),
        "float64" => Ok(DataType::Float64),
        "utf8" => Ok(DataType::Utf8),
        _ => {
            let Some(dimensions) = type_name
                .strip_prefix("vector[")
                .and_then(|value| value.strip_suffix(']'))
            else {
                return Err(ValidationError::new_err(
                    "field type must be bool, int64, uint64, float64, utf8, or vector[N]",
                ));
            };
            let dimensions = dimensions.parse::<i32>().map_err(|_| {
                ValidationError::new_err("vector field type must be written as vector[N] for N > 0")
            })?;
            if dimensions <= 0 {
                return Err(ValidationError::new_err(
                    "vector field type must be written as vector[N] for N > 0",
                ));
            }
            Ok(DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                dimensions,
            ))
        }
    }
}

/// An immutable snapshot that executes typed read queries.
#[pyclass(name = "Snapshot", module = "strata_ext")]
struct PySnapshot {
    inner: Arc<Snapshot>,
}

#[pymethods]
impl PySnapshot {
    /// Returns the immutable manifest version captured by this snapshot.
    fn version(&self) -> u64 {
        self.inner.version()
    }

    #[pyo3(signature = (projection=None, filter=None))]
    fn explain_scan(
        &self,
        py: Python<'_>,
        projection: Option<Vec<String>>,
        filter: Option<(String, String, Py<PyAny>)>,
    ) -> PyResult<Py<PyDict>> {
        let request = ScanRequest {
            projection: projection_from_python(projection),
            filter: filter_from_python(py, self.inner.schema().as_ref(), filter)?,
        };
        let plan = py
            .detach(|| self.inner.explain_scan_query(&request))
            .map_err(map_query_error)?;
        plan_to_python(py, &plan)
    }
    /// Returns an Arrow IPC stream containing the snapshot scan result.
    ///
    /// `projection` is `None` for all user columns or a list of user column
    /// names. `filter`, when supplied, is `(column, operator, value)` where
    /// `operator` is one of `==`, `!=`, `<`, `<=`, `>`, or `>=`.
    #[pyo3(signature = (projection=None, filter=None))]
    fn scan<'py>(
        &self,
        py: Python<'py>,
        projection: Option<Vec<String>>,
        filter: Option<(String, String, Py<PyAny>)>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let request = ScanRequest {
            projection: projection_from_python(projection),
            filter: filter_from_python(py, &self.inner.schema(), filter)?,
        };
        let snapshot = Arc::clone(&self.inner);
        let bytes = py.detach(move || {
            let result = snapshot
                .scan_query(&request)
                .map_err(BindingFailure::Query)?;
            scan_result_to_ipc(&snapshot, &result).map_err(BindingFailure::Execution)
        });
        let bytes = bytes.map_err(map_binding_failure)?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Looks up one physical row ID and returns its live row as an Arrow IPC
    /// stream. Tombstoned and never-allocated IDs return `None`.
    #[pyo3(signature = (row_id, projection=None))]
    fn lookup<'py>(
        &self,
        py: Python<'py>,
        row_id: u64,
        projection: Option<Vec<String>>,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let request = RowLookupRequest {
            row_id: RowId(row_id),
            projection: projection_from_python(projection),
        };
        let snapshot = Arc::clone(&self.inner);
        let bytes = py.detach(move || {
            let result = snapshot
                .lookup_row(&request)
                .map_err(BindingFailure::Query)?;
            match result.outcome {
                RowLookupOutcome::Live(row) => projected_rows_to_ipc(
                    snapshot.schema().as_ref(),
                    &result.projection,
                    std::slice::from_ref(&row),
                )
                .map(Some)
                .map_err(BindingFailure::Execution),
                RowLookupOutcome::Tombstoned | RowLookupOutcome::NotFound => Ok(None),
            }
        });
        let bytes = bytes.map_err(map_binding_failure)?;
        Ok(bytes.map(|bytes| PyBytes::new(py, &bytes)))
    }

    /// Executes a grouped aggregate and returns its rows as an Arrow IPC stream.
    ///
    /// Every aggregate is `(column, function, alias)`, where `function` is one
    /// of `count`, `sum`, `min`, `max`, or `avg`.
    #[pyo3(signature = (group_by, aggregates, filter=None))]
    fn group_by<'py>(
        &self,
        py: Python<'py>,
        group_by: Vec<String>,
        aggregates: Vec<(String, String, String)>,
        filter: Option<(String, String, Py<PyAny>)>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let request = GroupByRequest {
            group_by,
            aggregates: aggregates_from_python(aggregates)?,
            filter: filter_from_python(py, &self.inner.schema(), filter)?,
        };
        let snapshot = Arc::clone(&self.inner);
        let bytes = py.detach(move || {
            let result = snapshot
                .group_by_query(&request)
                .map_err(BindingFailure::Query)?;
            group_by_result_to_ipc(&snapshot, &result).map_err(BindingFailure::Execution)
        });
        let bytes = bytes.map_err(map_binding_failure)?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Returns nearest-neighbor hits as Python dictionaries, not Arrow IPC.
    ///
    /// Each dictionary contains `row_id`, `squared_l2_distance`, `row`, and
    /// `hydration_error`. `row` is a projected dictionary when `projection` is
    /// supplied and hydration succeeds; otherwise it is `None`. Results are
    /// ordered by ascending squared-L2 distance, then ascending `row_id` for
    /// ties, and may contain fewer than `k` live hits. Distances are squared-L2
    /// values. When hydration is unresolved, `hydration_error` is a dictionary
    /// with a typed `category` and an optional `message`, rather than a
    /// flattened error string.
    #[pyo3(signature = (vector_column, query, k, filter=None, projection=None))]
    fn vector_search(
        &self,
        py: Python<'_>,
        vector_column: String,
        query: Vec<f32>,
        k: usize,
        filter: Option<(String, String, Py<PyAny>)>,
        projection: Option<Vec<String>>,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let request = VectorSearchRequest {
            vector_column,
            query,
            k,
            filter: filter_from_python(py, &self.inner.schema(), filter)?,
            hydration: projection
                .map(Projection::Columns)
                .map_or(VectorHydration::NotRequested, VectorHydration::Projection),
        };
        let snapshot = Arc::clone(&self.inner);
        let result = py.detach(move || snapshot.vector_search_query(&request));
        let result = result.map_err(map_query_error)?;
        vector_result_to_python(py, &result)
    }
}

enum TransactionState {
    Active,
    Committed,
    Aborted,
}

impl TransactionState {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Committed => "committed",
            Self::Aborted => "aborted",
        }
    }
}

/// A private transaction overlay. Dropping an active handle aborts it.
#[pyclass(name = "Transaction", module = "strata_ext", unsendable)]
struct PyTransaction {
    inner: Option<Transaction>,
    schema: Arc<Schema>,
    state: TransactionState,
}

#[pymethods]
impl PyTransaction {
    /// Returns `active`, `committed`, or `aborted`.
    fn state(&self) -> &'static str {
        self.state.as_str()
    }

    /// Discards staged writes. An aborted transaction cannot be reused.
    fn abort(&mut self) {
        self.inner = None;
        if matches!(self.state, TransactionState::Active) {
            self.state = TransactionState::Aborted;
        }
    }

    /// Durably publishes staged writes or returns their typed failure.
    fn commit(&mut self, py: Python<'_>) -> PyResult<()> {
        let transaction = self.take_active()?;
        let result = py.detach(move || transaction.commit());
        self.state = TransactionState::Aborted;
        result.map_err(|error| map_txn_error(&error))?;
        self.state = TransactionState::Committed;
        Ok(())
    }

    /// Stages exactly one Arrow IPC record batch for this transaction.
    fn insert(&mut self, batch: &Bound<'_, PyBytes>) -> PyResult<()> {
        let batch = record_batch_from_ipc(batch.as_bytes())?;
        self.active_mut()?
            .insert(batch)
            .map_err(|error| map_txn_error(&error))
    }

    /// Returns Arrow IPC rows from the base snapshot plus this transaction's
    /// private staged-write overlay.
    #[pyo3(signature = (projection=None, filter=None))]
    fn scan<'py>(
        &mut self,
        py: Python<'py>,
        projection: Option<Vec<String>>,
        filter: Option<(String, String, Py<PyAny>)>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let request = ScanRequest {
            projection: projection_from_python(projection),
            filter: filter_from_python(py, &self.schema, filter)?,
        };
        let schema = Arc::clone(&self.schema);
        let transaction = self.take_active()?;
        let (transaction, result) = py.detach(move || {
            let result = transaction
                .scan_query(&request)
                .map_err(BindingFailure::Query)
                .and_then(|result| {
                    scan_result_to_ipc_with_schema(&schema, &result)
                        .map_err(BindingFailure::Execution)
                });
            (transaction, result)
        });
        self.inner = Some(transaction);
        let bytes = result.map_err(map_binding_failure)?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Returns nearest-neighbor hits from the transaction base snapshot.
    ///
    /// When this transaction has staged inserts, replacements, or deletes,
    /// the immutable vector index cannot represent the overlay and this
    /// raises `UnsupportedTransactionReadError` instead of returning stale
    /// base-snapshot hits.
    #[pyo3(signature = (vector_column, query, k, filter=None, projection=None))]
    fn vector_search(
        &mut self,
        py: Python<'_>,
        vector_column: String,
        query: Vec<f32>,
        k: usize,
        filter: Option<(String, String, Py<PyAny>)>,
        projection: Option<Vec<String>>,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let request = VectorSearchRequest {
            vector_column,
            query,
            k,
            filter: filter_from_python(py, &self.schema, filter)?,
            hydration: projection
                .map(Projection::Columns)
                .map_or(VectorHydration::NotRequested, VectorHydration::Projection),
        };
        let transaction = self.take_active()?;
        let (transaction, result) = py.detach(move || {
            let result = transaction.vector_search_query(&request);
            (transaction, result)
        });
        self.inner = Some(transaction);
        let result = result.map_err(map_query_error)?;
        vector_result_to_python(py, &result)
    }
}

impl PyTransaction {
    fn active_mut(&mut self) -> PyResult<&mut Transaction> {
        self.inner.as_mut().ok_or_else(|| {
            ExecutionError::new_err(format!(
                "transaction is {}; only active transactions accept operations",
                self.state.as_str()
            ))
        })
    }

    fn take_active(&mut self) -> PyResult<Transaction> {
        self.inner.take().ok_or_else(|| {
            ExecutionError::new_err(format!(
                "transaction is {}; only active transactions accept operations",
                self.state.as_str()
            ))
        })
    }
}

fn record_batch_from_ipc(bytes: &[u8]) -> PyResult<RecordBatch> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|error| ValidationError::new_err(error.to_string()))?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|error| ValidationError::new_err(error.to_string()))?
        .ok_or_else(|| ValidationError::new_err("Arrow IPC input must contain one record batch"))?;
    if reader
        .next()
        .transpose()
        .map_err(|error| ValidationError::new_err(error.to_string()))?
        .is_some()
    {
        return Err(ValidationError::new_err(
            "Arrow IPC input must contain exactly one record batch",
        ));
    }
    Ok(batch)
}

fn projection_from_python(projection: Option<Vec<String>>) -> Projection {
    projection.map_or(Projection::All, Projection::Columns)
}

fn filter_from_python(
    py: Python<'_>,
    schema: &strata_txn::arrow::datatypes::Schema,
    filter: Option<(String, String, Py<PyAny>)>,
) -> PyResult<Option<FilterExpression>> {
    let Some((column, operator, value)) = filter else {
        return Ok(None);
    };
    let operator = comparison_operator(&operator)?;
    let value = filter_literal_for_schema(value.bind(py), schema, &column)?;
    Ok(Some(FilterExpression::Compare(Comparison {
        column,
        operator,
        value,
    })))
}

fn comparison_operator(operator: &str) -> PyResult<ComparisonOperator> {
    match operator {
        "==" => Ok(ComparisonOperator::Equal),
        "!=" => Ok(ComparisonOperator::NotEqual),
        "<" => Ok(ComparisonOperator::LessThan),
        "<=" => Ok(ComparisonOperator::LessThanOrEqual),
        ">" => Ok(ComparisonOperator::GreaterThan),
        ">=" => Ok(ComparisonOperator::GreaterThanOrEqual),
        _ => Err(ValidationError::new_err(
            "filter operator must be one of ==, !=, <, <=, >, or >=",
        )),
    }
}

fn aggregates_from_python(aggregates: Vec<(String, String, String)>) -> PyResult<Vec<Aggregate>> {
    aggregates
        .into_iter()
        .map(|(column, function, alias)| {
            let function = match function.as_str() {
                "count" => AggregateFunction::Count,
                "sum" => AggregateFunction::Sum,
                "min" => AggregateFunction::Minimum,
                "max" => AggregateFunction::Maximum,
                "avg" => AggregateFunction::Average,
                _ => {
                    return Err(ValidationError::new_err(
                        "aggregate function must be one of count, sum, min, max, or avg",
                    ));
                }
            };
            Ok(Aggregate::new(column, function, alias))
        })
        .collect()
}

fn filter_literal_for_schema(
    value: &Bound<'_, PyAny>,
    schema: &strata_txn::arrow::datatypes::Schema,
    column: &str,
) -> PyResult<FilterLiteral> {
    if schema
        .field_with_name(column)
        .is_ok_and(|field| matches!(field.data_type(), DataType::UInt64))
    {
        if let Ok(value) = value.extract::<bool>() {
            return Ok(FilterLiteral::Boolean(value));
        }
        if let Ok(value) = value.extract::<u64>() {
            return Ok(FilterLiteral::UInt64(value));
        }
        if let Ok(value) = value.extract::<i64>() {
            if value < 0 {
                return Err(ValidationError::new_err(
                    "UInt64 filter values must be non-negative",
                ));
            }
            return u64::try_from(value)
                .map(FilterLiteral::UInt64)
                .map_err(|_| {
                    ValidationError::new_err("UInt64 filter values must be non-negative")
                });
        }
    }
    filter_literal(value)
}

fn filter_literal(value: &Bound<'_, PyAny>) -> PyResult<FilterLiteral> {
    if let Ok(value) = value.extract::<bool>() {
        return Ok(FilterLiteral::Boolean(value));
    }
    if let Ok(value) = value.extract::<i64>() {
        return Ok(FilterLiteral::Int64(value));
    }
    if let Ok(value) = value.extract::<u64>() {
        return Ok(FilterLiteral::UInt64(value));
    }
    if let Ok(value) = value.extract::<f64>() {
        return Ok(FilterLiteral::Float64(value));
    }
    if let Ok(value) = value.extract::<String>() {
        return Ok(FilterLiteral::Utf8(value));
    }
    Err(ValidationError::new_err(
        "filter value must be bool, int, float, or str",
    ))
}

fn scan_result_to_ipc(snapshot: &Snapshot, result: &ScanResult) -> Result<Vec<u8>, String> {
    scan_result_to_ipc_with_schema(snapshot.schema().as_ref(), result)
}

fn scan_result_to_ipc_with_schema(schema: &Schema, result: &ScanResult) -> Result<Vec<u8>, String> {
    projected_rows_to_ipc(schema, &result.projection, &result.rows)
}

fn projected_rows_to_ipc(
    schema: &Schema,
    projection: &[String],
    rows: &[ProjectedRow],
) -> Result<Vec<u8>, String> {
    let fields = projection
        .iter()
        .map(|name| {
            schema
                .field_with_name(name)
                .cloned()
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let values = rows
        .iter()
        .map(|row| {
            row.fields
                .iter()
                .map(|field| &field.value)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    values_to_ipc(fields, &values)
}

fn group_by_result_to_ipc(snapshot: &Snapshot, result: &GroupByResult) -> Result<Vec<u8>, String> {
    let schema = snapshot.schema();
    let mut fields = result
        .group_by()
        .iter()
        .map(|name| {
            schema
                .field_with_name(name)
                .cloned()
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    for aggregate in result.aggregates() {
        fields.push(Field::new(
            aggregate.alias(),
            logical_type_to_arrow(aggregate.data_type())?,
            *aggregate.data_type() != LogicalType::UInt64,
        ));
    }
    let values = result
        .rows()
        .iter()
        .map(|row| {
            row.keys
                .iter()
                .chain(row.aggregates.iter())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    values_to_ipc(fields, &values)
}

fn logical_type_to_arrow(data_type: &LogicalType) -> Result<DataType, String> {
    match data_type {
        LogicalType::Boolean => Ok(DataType::Boolean),
        LogicalType::Int64 => Ok(DataType::Int64),
        LogicalType::UInt64 => Ok(DataType::UInt64),
        LogicalType::Float64 => Ok(DataType::Float64),
        LogicalType::Utf8 => Ok(DataType::Utf8),
        LogicalType::Vector { dimensions } => {
            let dimensions = i32::try_from(*dimensions)
                .map_err(|_| "vector output dimensions exceed Arrow limits".to_owned())?;
            Ok(DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                dimensions,
            ))
        }
    }
}

fn values_to_ipc(fields: Vec<Field>, rows: &[Vec<&ResultValue>]) -> Result<Vec<u8>, String> {
    let arrays = fields
        .iter()
        .enumerate()
        .map(|(column_index, field)| {
            let values = rows
                .iter()
                .map(|row| {
                    row.get(column_index)
                        .copied()
                        .ok_or_else(|| "query result row does not match its schema".to_owned())
                })
                .collect::<Result<Vec<_>, _>>()?;
            values_to_array(field, &values)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    let batch = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)
        .map_err(|error| error.to_string())?;
    record_batch_to_ipc(&batch)
}

fn values_to_array(field: &Field, values: &[&ResultValue]) -> Result<ArrayRef, String> {
    match field.data_type() {
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(collect_values(
            values,
            |value| {
                if let ResultValue::Boolean(value) = value {
                    Some(*value)
                } else {
                    None
                }
            },
        )?))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(collect_values(
            values,
            |value| {
                if let ResultValue::Int64(value) = value {
                    Some(*value)
                } else {
                    None
                }
            },
        )?))),
        DataType::UInt64 => Ok(Arc::new(UInt64Array::from(collect_values(
            values,
            |value| {
                if let ResultValue::UInt64(value) = value {
                    Some(*value)
                } else {
                    None
                }
            },
        )?))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(collect_values(
            values,
            |value| {
                if let ResultValue::Float64(value) = value {
                    Some(*value)
                } else {
                    None
                }
            },
        )?))),
        DataType::Utf8 => {
            let strings = values
                .iter()
                .map(|value| match value {
                    ResultValue::Null => Ok(None),
                    ResultValue::Utf8(value) => Ok(Some(value.clone())),
                    _ => Err("query value does not match the output schema".to_owned()),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(StringArray::from(strings)))
        }
        DataType::FixedSizeList(item, length) if item.data_type() == &DataType::Float32 => {
            vector_values_to_array(item.as_ref().clone(), *length, values)
        }
        data_type => Err(format!(
            "Arrow IPC conversion does not support query output type {data_type:?}"
        )),
    }
}

fn collect_values<T>(
    values: &[&ResultValue],
    extract: impl Fn(&ResultValue) -> Option<T>,
) -> Result<Vec<Option<T>>, String> {
    values
        .iter()
        .map(|value| match value {
            ResultValue::Null => Ok(None),
            value => extract(value)
                .map(Some)
                .ok_or_else(|| "query value does not match the output schema".to_owned()),
        })
        .collect()
}

fn vector_values_to_array(
    item: Field,
    length: i32,
    values: &[&ResultValue],
) -> Result<ArrayRef, String> {
    let dimensions = usize::try_from(length)
        .map_err(|_| "vector output has an invalid negative dimension".to_owned())?;
    let mut flattened = Vec::with_capacity(values.len().saturating_mul(dimensions));
    let mut validity = Vec::with_capacity(values.len());
    for value in values {
        match value {
            ResultValue::Null => {
                flattened.extend(std::iter::repeat_n(0.0, dimensions));
                validity.push(false);
            }
            ResultValue::Vector(vector) if vector.len() == dimensions => {
                flattened.extend(vector);
                validity.push(true);
            }
            _ => return Err("query vector value does not match the output schema".to_owned()),
        }
    }
    let nulls = (!validity.iter().all(|valid| *valid)).then(|| NullBuffer::from(validity));
    Ok(Arc::new(FixedSizeListArray::new(
        Arc::new(item),
        length,
        Arc::new(Float32Array::from(flattened)),
        nulls,
    )))
}

fn record_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, batch.schema().as_ref())
        .map_err(|error| error.to_string())?;
    writer.write(batch).map_err(|error| error.to_string())?;
    writer.finish().map_err(|error| error.to_string())?;
    Ok(bytes)
}

fn vector_result_to_python(
    py: Python<'_>,
    result: &VectorSearchResult,
) -> PyResult<Vec<Py<PyDict>>> {
    result
        .hits()
        .iter()
        .map(|hit| {
            let output = PyDict::new(py);
            output.set_item("row_id", hit.row_id.0)?;
            output.set_item("squared_l2_distance", hit.squared_l2_distance)?;
            match &hit.hydration {
                VectorHydrationState::NotRequested => {
                    output.set_item("row", py.None())?;
                    output.set_item("hydration_error", py.None())?;
                }
                VectorHydrationState::Hydrated(row) => {
                    output.set_item("row", projected_row_to_python(py, row)?)?;
                    output.set_item("hydration_error", py.None())?;
                }
                VectorHydrationState::Unresolved(error) => {
                    output.set_item("row", py.None())?;
                    output.set_item("hydration_error", hydration_error_to_python(py, error)?)?;
                }
            }
            Ok(output.unbind())
        })
        .collect()
}

fn migration_result_to_python(
    py: Python<'_>,
    result: &strata_txn::SchemaMigrationResult,
) -> PyResult<Py<PyDict>> {
    let output = PyDict::new(py);
    output.set_item("name", result.name)?;
    output.set_item("source_schema_version", result.source_schema_version)?;
    output.set_item("target_schema_version", result.target_schema_version)?;
    output.set_item("manifest_version", result.manifest_version)?;
    Ok(output.unbind())
}

fn plan_to_python(py: Python<'_>, plan: &PhysicalPlan) -> PyResult<Py<PyDict>> {
    let output = PyDict::new(py);
    output.set_item(
        "logical_operators",
        plan.logical_operators
            .iter()
            .map(logical_operator_name)
            .collect::<Vec<_>>(),
    )?;
    output.set_item(
        "physical_operators",
        plan.physical_operators
            .iter()
            .map(physical_operator_name)
            .collect::<Vec<_>>(),
    )?;
    let observations = PyDict::new(py);
    observations.set_item("data_files_total", plan.observations.data_files_total)?;
    observations.set_item("data_files_scanned", plan.observations.data_files_scanned)?;
    observations.set_item("data_files_pruned", plan.observations.data_files_pruned)?;
    observations.set_item(
        "index_segments_total",
        plan.observations.index_segments_total,
    )?;
    observations.set_item(
        "index_segments_scanned",
        plan.observations.index_segments_scanned,
    )?;
    observations.set_item(
        "index_segments_pruned",
        plan.observations.index_segments_pruned,
    )?;
    observations.set_item("transaction_overlay", plan.observations.transaction_overlay)?;
    output.set_item("observations", observations)?;
    Ok(output.unbind())
}

fn logical_operator_name(operator: &LogicalOperator) -> &'static str {
    match operator {
        LogicalOperator::Source => "source",
        LogicalOperator::Predicate { .. } => "predicate",
        LogicalOperator::Projection { .. } => "projection",
        LogicalOperator::Grouping { .. } => "grouping",
        LogicalOperator::VectorSearch { .. } => "vector_search",
        LogicalOperator::Materialize => "materialize",
    }
}

fn physical_operator_name(operator: &PhysicalOperator) -> &'static str {
    match operator {
        PhysicalOperator::ManifestSnapshotSource => "manifest_snapshot_source",
        PhysicalOperator::ZoneMapPruning => "zone_map_pruning",
        PhysicalOperator::TombstoneFilter => "tombstone_filter",
        PhysicalOperator::RowFilter => "row_filter",
        PhysicalOperator::ColumnProjection => "column_projection",
        PhysicalOperator::HashGroupBy => "hash_group_by",
        PhysicalOperator::FilterLiveSet => "filter_live_set",
        PhysicalOperator::ImmutableSegmentVectorSearch => "immutable_segment_vector_search",
        PhysicalOperator::HydrationLookup => "hydration_lookup",
        PhysicalOperator::TransactionOverlay => "transaction_overlay",
        PhysicalOperator::Materialize => "materialize",
    }
}

fn hydration_error_to_python<'py>(
    py: Python<'py>,
    error: &HydrationError,
) -> PyResult<Bound<'py, PyDict>> {
    let output = PyDict::new(py);
    let (category, message) = match error {
        HydrationError::NotFound => ("not_found", None),
        HydrationError::Tombstoned => ("tombstoned", None),
        HydrationError::VectorUnavailable => ("vector_unavailable", None),
        HydrationError::IntegrityFailure { message } => ("integrity_failure", Some(message)),
    };
    output.set_item("category", category)?;
    if let Some(message) = message {
        output.set_item("message", message)?;
    }
    Ok(output)
}

fn projected_row_to_python(py: Python<'_>, row: &ProjectedRow) -> PyResult<Py<PyDict>> {
    let output = PyDict::new(py);
    for field in &row.fields {
        match &field.value {
            ResultValue::Null => output.set_item(&field.name, py.None())?,
            ResultValue::Boolean(value) => output.set_item(&field.name, *value)?,
            ResultValue::Int64(value) => output.set_item(&field.name, *value)?,
            ResultValue::UInt64(value) => output.set_item(&field.name, *value)?,
            ResultValue::Float64(value) => output.set_item(&field.name, *value)?,
            ResultValue::Utf8(value) => output.set_item(&field.name, value)?,
            ResultValue::Vector(value) => output.set_item(&field.name, value)?,
        }
    }
    Ok(output.unbind())
}

enum BindingFailure {
    Query(QueryError),
    Execution(String),
}

fn map_binding_failure(error: BindingFailure) -> PyErr {
    match error {
        BindingFailure::Query(error) => map_query_error(error),
        BindingFailure::Execution(message) => ExecutionError::new_err(message),
    }
}

fn map_query_error(error: QueryError) -> PyErr {
    match error {
        QueryError::Validation(error) => InvalidQueryError::new_err(error.to_string()),
        QueryError::Execution(QueryExecutionError::UnsupportedTransactionRead { operation }) => {
            UnsupportedTransactionReadError::new_err(format!(
                "transaction read operation '{operation}' cannot merge staged writes safely"
            ))
        }
        QueryError::Execution(QueryExecutionError::Engine(error)) => map_txn_error(error.as_ref()),
        QueryError::Execution(error) => ExecutionError::new_err(error.to_string()),
    }
}

fn map_txn_error(error: &TxnError) -> PyErr {
    match error {
        TxnError::Conflict { contested_row_ids } => conflict_error(contested_row_ids),
        TxnError::InsufficientHistory { .. } => {
            InsufficientHistoryError::new_err(error.to_string())
        }
        TxnError::BatchSchemaMismatch { .. }
        | TxnError::SchemaMismatch { .. }
        | TxnError::ReservedColumnName(_)
        | TxnError::InvalidUpdateShape { .. } => SchemaMigrationError::new_err(error.to_string()),
        TxnError::Storage(error) => match error {
            StorageError::UnknownSchemaVersion { .. }
            | StorageError::MigrationSourceVersion { .. }
            | StorageError::MigrationUnsupportedDirection { .. }
            | StorageError::MigrationUnsupported { .. }
            | StorageError::MigrationIncompatibleType { .. }
            | StorageError::MigrationLossyConversion { .. }
            | StorageError::SchemaVersionChanged { .. }
            | StorageError::LegacyFormatNeedsMigration(_) => {
                SchemaMigrationError::new_err(error.to_string())
            }
            StorageError::CorruptManifest(_, _)
            | StorageError::CorruptDataFile(_, _)
            | StorageError::EmptyDataFile(_)
            | StorageError::MissingRowIdHighWater(_) => CorruptionError::new_err(error.to_string()),
            StorageError::Io(_)
            | StorageError::Arrow(_)
            | StorageError::Serde(_)
            | StorageError::DurabilityUnsupported(_)
            | StorageError::AlreadyExists(_) => StorageDurabilityError::new_err(error.to_string()),
        },
        TxnError::Io(_)
        | TxnError::Arrow(_)
        | TxnError::Index(_)
        | TxnError::AlreadyExists(_)
        | TxnError::NotFound(_)
        | TxnError::ManifestOverflow(_)
        | TxnError::RowIdReservationDurability { .. }
        | TxnError::Clock(_) => StorageDurabilityError::new_err(error.to_string()),
        TxnError::CorruptSegment(_)
        | TxnError::UnsafeManifestPath(_)
        | TxnError::UnreasonableCapacity(_, _)
        | TxnError::RowIdRangeMismatch { .. } => CorruptionError::new_err(error.to_string()),
        _ => ExecutionError::new_err(error.to_string()),
    }
}

fn conflict_error(contested_row_ids: &[u64]) -> PyErr {
    Python::attach(|py| {
        let error = ConflictError::new_err(format!(
            "conflict: {contested_row_ids:?} were modified by another transaction"
        ));
        if let Err(attribute_error) = error
            .value(py)
            .setattr("contested_row_ids", contested_row_ids)
        {
            return attribute_error;
        }
        error
    })
}

#[pymodule]
mod strata_ext {
    #[pymodule_export]
    use super::{
        ConflictError, CorruptionError, ExecutionError, InsufficientHistoryError,
        InvalidQueryError, PyDataset, PySnapshot, PyTransaction, SchemaMigrationError,
        StorageDurabilityError, StrataError, UnsupportedTransactionReadError, ValidationError,
    };
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::sync::Arc;

    use pyo3::prelude::*;
    use pyo3::types::{PyBytes, PyBytesMethods, PyDict, PyList};
    use strata_txn::arrow::array::{
        FixedSizeListArray, Float32Array, Int64Array, StringArray, UInt64Array,
    };
    use strata_txn::arrow::datatypes::{DataType, Field, Schema};
    use strata_txn::arrow::ipc::reader::StreamReader;
    use strata_txn::arrow::record_batch::RecordBatch;

    use super::{
        CorruptionError, HydrationError, InvalidQueryError, PyDataset, SchemaMigrationError,
        StorageDurabilityError, StrataError, TxnError, UnsupportedTransactionReadError,
        ValidationError, hydration_error_to_python, map_query_error, map_txn_error,
        record_batch_to_ipc,
    };

    #[test]
    fn stable_api_exposes_versioned_dataset_snapshot_and_transaction_lifecycle()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let dataset = PyDataset::create(
                py,
                directory.path().to_path_buf(),
                vec![("name".to_owned(), "utf8".to_owned(), false)],
            )?;
            assert_eq!(dataset.api_version(), "1.0");
            assert_eq!(dataset.version(), 0);
            assert_eq!(dataset.snapshot(py).version(), 0);

            let mut transaction = dataset.begin();
            assert_eq!(transaction.state(), "active");
            transaction.abort();
            assert_eq!(transaction.state(), "aborted");
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn transaction_wrapper_reads_its_arrow_ipc_write_and_abort_keeps_it_private()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["staged"]))],
        )?;
        let input = record_batch_to_ipc(&batch)?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let dataset = PyDataset::create(
                py,
                directory.path().to_path_buf(),
                vec![("name".to_owned(), "utf8".to_owned(), false)],
            )?;
            let mut transaction = dataset.begin();
            transaction.insert(&PyBytes::new(py, &input))?;

            let staged = transaction.scan(py, Some(vec!["name".to_owned()]), None)?;
            assert_eq!(first_ipc_batch(staged.as_bytes())?.num_rows(), 1);
            assert_eq!(
                first_ipc_batch(dataset.snapshot(py).scan(py, None, None)?.as_bytes())?.num_rows(),
                0
            );

            transaction.abort();
            assert_eq!(transaction.state(), "aborted");
            assert_eq!(
                first_ipc_batch(dataset.snapshot(py).scan(py, None, None)?.as_bytes())?.num_rows(),
                0
            );
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn transaction_schema_accessor_stays_bound_to_its_base_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let dataset = strata_txn::Dataset::create(directory.path(), Arc::clone(&schema))?;
        let transaction = dataset.begin();

        dataset.migrate_schema(&strata_txn::SchemaMigration::add_nullable_column(
            1,
            2,
            Field::new("note", DataType::Utf8, true),
        ))?;

        assert_eq!(transaction.schema(), schema);
        assert_eq!(transaction.schema().fields().len(), 1);
        Ok(())
    }

    #[test]
    fn transaction_vector_search_and_error_mapping_use_stable_categories()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = populated_dataset()?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let dataset = PyDataset::open(py, directory.path().to_path_buf())?;
            let mut transaction = dataset.begin();
            assert_eq!(
                transaction
                    .vector_search(py, "vector".to_owned(), vec![0.0, 0.0], 1, None, None)?
                    .len(),
                1
            );
            transaction
                .active_mut()?
                .delete(0)
                .map_err(|error| map_txn_error(&error))?;
            let Err(unsupported) =
                transaction.vector_search(py, "vector".to_owned(), vec![0.0, 0.0], 1, None, None)
            else {
                return Err(ValidationError::new_err(
                    "staged vector search was accepted",
                ));
            };
            assert!(unsupported.is_instance_of::<UnsupportedTransactionReadError>(py));

            let conflict = map_txn_error(&TxnError::Conflict {
                contested_row_ids: vec![7, 9],
            });
            assert!(conflict.is_instance_of::<super::ConflictError>(py));
            assert_eq!(
                conflict
                    .value(py)
                    .getattr("contested_row_ids")?
                    .extract::<Vec<u64>>()?,
                vec![7, 9]
            );

            let schema = map_txn_error(&TxnError::Storage(
                strata_txn::StorageError::MigrationSourceVersion {
                    expected: 2,
                    actual: 1,
                },
            ));
            assert!(schema.is_instance_of::<SchemaMigrationError>(py));

            let query = map_query_error(strata_txn::QueryError::Validation(
                strata_txn::QueryValidationError::InvalidVectorK,
            ));
            assert!(query.is_instance_of::<InvalidQueryError>(py));

            let durability = map_txn_error(&TxnError::RowIdReservationDurability {
                end: 7,
                source: strata_txn::StorageError::DurabilityUnsupported(PathBuf::from("dataset")),
            });
            assert!(durability.is_instance_of::<StorageDurabilityError>(py));

            let corruption = map_txn_error(&TxnError::Storage(
                strata_txn::StorageError::CorruptManifest(
                    PathBuf::from("dataset/current"),
                    "checksum mismatch".to_owned(),
                ),
            ));
            assert!(corruption.is_instance_of::<CorruptionError>(py));
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn stable_python_contract_commits_migrates_and_explains_without_rust_layouts()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["durable"]))],
        )?;
        let input = record_batch_to_ipc(&batch)?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let dataset = PyDataset::create(
                py,
                directory.path().to_path_buf(),
                vec![("name".to_owned(), "utf8".to_owned(), false)],
            )?;
            let mut transaction = dataset.begin();
            transaction.insert(&PyBytes::new(py, &input))?;
            transaction.commit(py)?;
            assert_eq!(transaction.state(), "committed");
            assert_eq!(dataset.version(), 1);

            let migration = dataset.migrate_add_nullable_column(py, "note".to_owned(), "utf8")?;
            let migration = migration.bind(py);
            assert_eq!(
                migration
                    .get_item("name")?
                    .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing name"))?
                    .extract::<String>()?,
                "add_nullable_column"
            );
            assert_eq!(
                migration
                    .get_item("target_schema_version")?
                    .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err(
                        "missing target version"
                    ))?
                    .extract::<u32>()?,
                2
            );

            let plan =
                dataset
                    .snapshot(py)
                    .explain_scan(py, Some(vec!["name".to_owned()]), None)?;
            let plan = plan.bind(py);
            assert!(
                plan.get_item("logical_operators")?
                    .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err(
                        "missing logical operators"
                    ))?
                    .is_instance_of::<PyList>()
            );
            assert!(
                plan.get_item("observations")?
                    .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err(
                        "missing observations"
                    ))?
                    .is_instance_of::<PyDict>()
            );
            assert_eq!(
                PyDataset::open(py, directory.path().to_path_buf())?.schema_version(),
                2
            );
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn python_explain_serializes_scalar_zero_segment_counts()
    -> Result<(), Box<dyn std::error::Error>> {
        // Break caught: the Python explain DTO reports immutable-segment scans
        // for a scalar snapshot plan that does not select a vector operator.
        let directory = populated_dataset()?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let plan = PyDataset::open(py, directory.path().to_path_buf())?
                .snapshot(py)
                .explain_scan(py, Some(vec!["name".to_owned()]), None)?;
            let plan = plan.bind(py);
            let observations_value = plan
                .get_item("observations")?
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing observations"))?;
            let observations = observations_value.cast::<PyDict>()?;
            assert_eq!(
                observations
                    .get_item("index_segments_total")?
                    .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err(
                        "missing segment total"
                    ))?
                    .extract::<usize>()?,
                1
            );
            assert_eq!(
                observations
                    .get_item("index_segments_scanned")?
                    .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err(
                        "missing scanned segments"
                    ))?
                    .extract::<usize>()?,
                0
            );
            assert_eq!(
                observations
                    .get_item("index_segments_pruned")?
                    .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err(
                        "missing pruned segments"
                    ))?
                    .extract::<usize>()?,
                0
            );
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn scan_returns_arrow_ipc_and_invalid_projection_is_a_typed_validation_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = populated_dataset()?;
        let path = directory.path().to_path_buf();

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let snapshot = PyDataset::open(py, path)?.snapshot(py);
            let stream = snapshot.scan(py, Some(vec!["name".to_owned()]), None)?;
            let mut reader = StreamReader::try_new(Cursor::new(stream.as_bytes()), None)
                .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
            let batch = reader
                .next()
                .transpose()
                .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing IPC batch"))?;

            assert_eq!(batch.num_rows(), 2);
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("expected Utf8 column"))?;
            assert_eq!(names.value(0), "near");
            assert_eq!(names.value(1), "far");

            let Err(error) = snapshot.scan(py, Some(vec!["absent".to_owned()]), None) else {
                return Err(ValidationError::new_err("unknown projection was accepted"));
            };
            assert!(error.is_instance_of::<ValidationError>(py));
            assert!(error.is_instance_of::<StrataError>(py));
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn empty_projection_preserves_matching_row_count_in_arrow_ipc()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = populated_dataset()?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let snapshot = PyDataset::open(py, directory.path().to_path_buf())?.snapshot(py);
            let stream = snapshot.scan(py, Some(Vec::new()), None)?;
            let batch = first_ipc_batch(stream.as_bytes())?;

            assert_eq!(batch.num_columns(), 0);
            assert_eq!(batch.num_rows(), 2);
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn lookup_returns_a_live_row_as_arrow_ipc() -> Result<(), Box<dyn std::error::Error>> {
        let directory = populated_dataset()?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let snapshot = PyDataset::open(py, directory.path().to_path_buf())?.snapshot(py);
            let stream = snapshot
                .lookup(py, 0, Some(vec!["name".to_owned()]))?
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("live row was absent"))?;
            let batch = first_ipc_batch(stream.as_bytes())?;
            assert_eq!(batch.num_rows(), 1);
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("expected Utf8 column"))?;
            assert_eq!(names.value(0), "near");
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn scan_comparison_filter_is_applied_before_ipc_conversion()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = populated_dataset()?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let filter_value = 2_i64.into_pyobject(py)?.unbind().into_any();
            let snapshot = PyDataset::open(py, directory.path().to_path_buf())?.snapshot(py);
            let stream = snapshot.scan(
                py,
                Some(vec!["name".to_owned()]),
                Some(("rank".to_owned(), ">=".to_owned(), filter_value)),
            )?;
            let batch = first_ipc_batch(stream.as_bytes())?;
            assert_eq!(batch.num_rows(), 1);
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("expected Utf8 column"))?;
            assert_eq!(names.value(0), "far");
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn scan_accepts_unsigned_python_int_for_uint64_filter() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = uint64_dataset()?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let filter_value = u64::MAX.into_pyobject(py)?.unbind().into_any();
            let snapshot = PyDataset::open(py, directory.path().to_path_buf())?.snapshot(py);
            let stream = snapshot.scan(
                py,
                Some(vec!["name".to_owned()]),
                Some(("rank".to_owned(), "==".to_owned(), filter_value)),
            )?;
            let batch = first_ipc_batch(stream.as_bytes())?;
            assert_eq!(batch.num_rows(), 1);
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("expected Utf8 column"))?;
            assert_eq!(names.value(0), "max");
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn scan_converts_ordinary_python_int_to_uint64_using_persisted_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = uint64_dataset()?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let filter_value = 1_i64.into_pyobject(py)?.unbind().into_any();
            let snapshot = PyDataset::open(py, directory.path().to_path_buf())?.snapshot(py);
            let stream = snapshot.scan(
                py,
                Some(vec!["name".to_owned()]),
                Some(("rank".to_owned(), "==".to_owned(), filter_value)),
            )?;
            let batch = first_ipc_batch(stream.as_bytes())?;
            assert_eq!(batch.num_rows(), 1);
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("expected Utf8 column"))?;
            assert_eq!(names.value(0), "small");
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn scan_rejects_negative_python_int_for_uint64_filter() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = uint64_dataset()?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let filter_value = (-1_i64).into_pyobject(py)?.unbind().into_any();
            let snapshot = PyDataset::open(py, directory.path().to_path_buf())?.snapshot(py);
            let Err(error) = snapshot.scan(
                py,
                Some(vec!["name".to_owned()]),
                Some(("rank".to_owned(), "==".to_owned(), filter_value)),
            ) else {
                return Err(ValidationError::new_err(
                    "negative UInt64 filter value was accepted",
                ));
            };
            assert!(error.is_instance_of::<ValidationError>(py));
            assert!(error.value(py).to_string().contains("non-negative"));
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn group_by_returns_arrow_ipc_with_aggregate_output() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = populated_dataset()?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let snapshot = PyDataset::open(py, directory.path().to_path_buf())?.snapshot(py);
            let stream = snapshot.group_by(
                py,
                vec!["name".to_owned()],
                vec![("rank".to_owned(), "sum".to_owned(), "total".to_owned())],
                None,
            )?;
            let batch = first_ipc_batch(stream.as_bytes())?;
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.schema().field(1).name(), "total");
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn group_by_arrow_schema_preserves_count_nullability() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = populated_dataset()?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let snapshot = PyDataset::open(py, directory.path().to_path_buf())?.snapshot(py);
            let stream = snapshot.group_by(
                py,
                vec!["name".to_owned()],
                vec![
                    ("rank".to_owned(), "count".to_owned(), "count".to_owned()),
                    ("rank".to_owned(), "sum".to_owned(), "total".to_owned()),
                ],
                None,
            )?;
            let batch = first_ipc_batch(stream.as_bytes())?;
            assert!(!batch.schema().field(1).is_nullable());
            assert!(batch.schema().field(2).is_nullable());
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn insufficient_history_maps_to_distinct_python_exception()
    -> Result<(), Box<dyn std::error::Error>> {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let error = super::map_txn_error(&TxnError::InsufficientHistory {
                base_version: 1,
                oldest_retained_version: 2,
                latest_version: 3,
            });
            assert!(error.is_instance_of::<super::InsufficientHistoryError>(py));
            assert!(!error.is_instance_of::<super::ConflictError>(py));

            let conflict = super::map_txn_error(&TxnError::Conflict {
                contested_row_ids: vec![7],
            });
            assert!(conflict.is_instance_of::<super::ConflictError>(py));
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn vector_hydration_errors_preserve_typed_categories() -> Result<(), Box<dyn std::error::Error>>
    {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let error = hydration_error_to_python(
                py,
                &HydrationError::IntegrityFailure {
                    message: "checksum mismatch".to_owned(),
                },
            )?;
            assert_eq!(
                error
                    .get_item("category")?
                    .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing category"))?
                    .extract::<String>()?,
                "integrity_failure"
            );
            assert_eq!(
                error
                    .get_item("message")?
                    .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing message"))?
                    .extract::<String>()?,
                "checksum mismatch"
            );
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn vector_search_returns_hydrated_python_dictionaries() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = populated_dataset()?;

        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let snapshot = PyDataset::open(py, directory.path().to_path_buf())?.snapshot(py);
            let hits = snapshot.vector_search(
                py,
                "vector".to_owned(),
                vec![0.0, 0.0],
                2,
                None,
                Some(vec!["name".to_owned()]),
            )?;
            let first = hits
                .first()
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing vector hit"))?
                .bind(py);
            let row_id = first
                .get_item("row_id")?
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing row_id"))?
                .extract::<u64>()?;
            let distance = first
                .get_item("squared_l2_distance")?
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing distance"))?
                .extract::<f32>()?;
            assert_eq!(row_id, 0);
            assert!((distance - 0.0).abs() < f32::EPSILON);
            assert!(first.get_item("row")?.is_some());
            Ok(())
        })?;
        Ok(())
    }

    fn first_ipc_batch(bytes: &[u8]) -> PyResult<RecordBatch> {
        let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
            .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
        reader
            .next()
            .transpose()
            .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing IPC batch"))
    }

    fn populated_dataset() -> Result<tempfile::TempDir, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("rank", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2),
                false,
            ),
        ]));
        let dataset = strata_txn::Dataset::create(directory.path(), Arc::clone(&schema))?;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["near", "far"])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(FixedSizeListArray::new(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    2,
                    Arc::new(Float32Array::from(vec![0.0, 0.0, 10.0, 0.0])),
                    None,
                )),
            ],
        )?;
        let mut transaction = dataset.begin();
        transaction.insert(batch)?;
        transaction.commit()?;
        Ok(directory)
    }

    fn uint64_dataset() -> Result<tempfile::TempDir, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("rank", DataType::UInt64, false),
        ]));
        let dataset = strata_txn::Dataset::create(directory.path(), Arc::clone(&schema))?;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["small", "max"])),
                Arc::new(UInt64Array::from(vec![1, u64::MAX])),
            ],
        )?;
        let mut transaction = dataset.begin();
        transaction.insert(batch)?;
        transaction.commit()?;
        Ok(directory)
    }
}
