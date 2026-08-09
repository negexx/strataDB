//! A thin Python facade over Strata's immutable snapshot query API.
//!
//! The extension remains embedded and supports one process sharing one
//! [`strata_txn::Dataset`] handle. It does not provide a read/write transaction
//! API, cross-process coordination, or stronger isolation.

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
use strata_txn::arrow::ipc::writer::StreamWriter;
use strata_txn::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use strata_txn::{
    Aggregate, AggregateFunction, Comparison, ComparisonOperator, FilterExpression, FilterLiteral,
    GroupByRequest, GroupByResult, HydrationError, LogicalType, ProjectedRow, Projection,
    QueryError, QueryExecutionError, ResultValue, RowId, RowLookupOutcome, RowLookupRequest,
    ScanRequest, ScanResult, Snapshot, TxnError, VectorHydration, VectorHydrationState,
    VectorSearchRequest, VectorSearchResult,
};

create_exception!(strata_ext, StrataError, pyo3::exceptions::PyException);
create_exception!(strata_ext, ValidationError, StrataError);
create_exception!(strata_ext, ExecutionError, StrataError);
create_exception!(strata_ext, ConflictError, StrataError);
create_exception!(strata_ext, InsufficientHistoryError, StrataError);

/// A Python handle to one embedded Strata dataset.
#[pyclass(name = "Dataset", module = "strata_ext")]
struct PyDataset {
    inner: strata_txn::Dataset,
}

#[pymethods]
impl PyDataset {
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
}

/// An immutable snapshot that executes typed read queries.
#[pyclass(name = "Snapshot", module = "strata_ext")]
struct PySnapshot {
    inner: Arc<Snapshot>,
}

#[pymethods]
impl PySnapshot {
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
                RowLookupOutcome::Live(row) => {
                    projected_rows_to_ipc(&snapshot, &result.projection, std::slice::from_ref(&row))
                        .map(Some)
                        .map_err(BindingFailure::Execution)
                }
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
    projected_rows_to_ipc(snapshot, &result.projection, &result.rows)
}

fn projected_rows_to_ipc(
    snapshot: &Snapshot,
    projection: &[String],
    rows: &[ProjectedRow],
) -> Result<Vec<u8>, String> {
    let schema = snapshot.schema();
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
        QueryError::Validation(error) => ValidationError::new_err(error.to_string()),
        QueryError::Execution(QueryExecutionError::Engine(error)) => map_txn_error(error.as_ref()),
        QueryError::Execution(error) => ExecutionError::new_err(error.to_string()),
    }
}

fn map_txn_error(error: &TxnError) -> PyErr {
    match error {
        TxnError::Conflict { .. } => ConflictError::new_err(error.to_string()),
        TxnError::InsufficientHistory { .. } => {
            InsufficientHistoryError::new_err(error.to_string())
        }
        _ => ExecutionError::new_err(error.to_string()),
    }
}

#[pymodule]
mod strata_ext {
    #[pymodule_export]
    use super::{
        ConflictError, ExecutionError, InsufficientHistoryError, PyDataset, PySnapshot,
        StrataError, ValidationError,
    };
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use pyo3::prelude::*;
    use pyo3::types::PyBytesMethods;
    use strata_txn::arrow::array::{
        FixedSizeListArray, Float32Array, Int64Array, StringArray, UInt64Array,
    };
    use strata_txn::arrow::datatypes::{DataType, Field, Schema};
    use strata_txn::arrow::ipc::reader::StreamReader;
    use strata_txn::arrow::record_batch::RecordBatch;

    use super::{
        HydrationError, PyDataset, StrataError, TxnError, ValidationError,
        hydration_error_to_python,
    };

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
