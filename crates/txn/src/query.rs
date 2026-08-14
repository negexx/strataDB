//! Public, snapshot-bound query contract types.
//!
//! This module contains the typed requests, results, validation, and errors
//! used by the implemented snapshot-bound query facade.

use std::collections::HashSet;

use thiserror::Error;

/// A scalar literal accepted by a filter comparison.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterLiteral {
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(String),
}

impl FilterLiteral {
    #[must_use]
    pub fn logical_type(&self) -> LogicalType {
        match self {
            Self::Boolean(_) => LogicalType::Boolean,
            Self::Int64(_) => LogicalType::Int64,
            Self::UInt64(_) => LogicalType::UInt64,
            Self::Float64(_) => LogicalType::Float64,
            Self::Utf8(_) => LogicalType::Utf8,
        }
    }
}

/// A type supported by the public logical query contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalType {
    Boolean,
    Int64,
    UInt64,
    Float64,
    Utf8,
    Vector { dimensions: usize },
}

#[allow(dead_code)]
impl LogicalType {
    fn is_scalar(&self) -> bool {
        !matches!(self, Self::Vector { .. })
    }

    fn is_numeric(&self) -> bool {
        matches!(self, Self::Int64 | Self::Float64)
    }
}

/// One logical column owned by a persisted dataset.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalColumn {
    pub(crate) name: String,
    pub(crate) data_type: LogicalType,
    pub(crate) nullable: bool,
}

#[allow(dead_code)]
impl LogicalColumn {
    #[must_use]
    pub(crate) fn new(name: impl Into<String>, data_type: LogicalType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

/// Internal logical schema owned by a persisted dataset, excluding physical columns.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DatasetSchema {
    columns: Vec<LogicalColumn>,
}

#[allow(dead_code)]
impl DatasetSchema {
    /// Builds a schema while rejecting duplicate and reserved column names.
    ///
    /// # Errors
    ///
    /// Returns [`QueryValidationError::ReservedColumn`] or
    /// [`QueryValidationError::DuplicateSchemaColumn`] for invalid names.
    pub(crate) fn new(columns: Vec<LogicalColumn>) -> QueryResult<Self> {
        let mut names = HashSet::with_capacity(columns.len());
        for column in &columns {
            validate_user_column_name(&column.name)?;
            if matches!(column.data_type, LogicalType::Vector { dimensions: 0 }) {
                return Err(QueryValidationError::ZeroVectorDimensions {
                    name: column.name.clone(),
                }
                .into());
            }
            if !names.insert(column.name.as_str()) {
                return Err(QueryValidationError::DuplicateSchemaColumn {
                    name: column.name.clone(),
                }
                .into());
            }
        }
        Ok(Self { columns })
    }

    #[must_use]
    pub(crate) fn columns(&self) -> &[LogicalColumn] {
        &self.columns
    }

    /// Validates a scan and returns its output columns in requested order.
    ///
    /// # Errors
    ///
    /// Returns an error when the projection or filter is incompatible with
    /// this dataset-owned schema.
    pub(crate) fn validate_scan(&self, request: &ScanRequest) -> QueryResult<Vec<String>> {
        let projection = self.validate_projection(&request.projection)?;
        if let Some(filter) = &request.filter {
            self.validate_filter(filter)?;
        }
        Ok(projection)
    }

    /// Validates a physical row lookup and resolves its requested projection.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown, reserved, or duplicate projected names.
    pub(crate) fn validate_row_lookup(
        &self,
        request: &RowLookupRequest,
    ) -> QueryResult<Vec<String>> {
        self.validate_projection(&request.projection)
    }

    /// Resolves a projection without changing its order.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown, reserved, or duplicate projected names.
    pub(crate) fn validate_projection(&self, projection: &Projection) -> QueryResult<Vec<String>> {
        match projection {
            Projection::All => Ok(self
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect()),
            Projection::Columns(columns) => {
                let mut seen = HashSet::with_capacity(columns.len());
                for column in columns {
                    self.column(column)?;
                    if !seen.insert(column.as_str()) {
                        return Err(QueryValidationError::DuplicateProjection {
                            name: column.clone(),
                        }
                        .into());
                    }
                }
                Ok(columns.clone())
            }
        }
    }

    /// Validates that a filter reads scalar columns with matching scalar values.
    ///
    /// # Errors
    ///
    /// Returns an error if a filter references an unknown or reserved column,
    /// compares a vector, uses the wrong scalar type, or uses an invalid
    /// comparison operator.
    pub(crate) fn validate_filter(&self, filter: &FilterExpression) -> QueryResult<()> {
        match filter {
            FilterExpression::Compare(comparison) => self.validate_comparison(comparison),
            FilterExpression::And(left, right) | FilterExpression::Or(left, right) => {
                self.validate_filter(left)?;
                self.validate_filter(right)
            }
            FilterExpression::Not(expression) => self.validate_filter(expression),
        }
    }

    /// Validates group keys, filters, aggregate aliases, and output types.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid group keys or filters, duplicate output
    /// names, or an aggregate that cannot consume its input type.
    pub(crate) fn validate_group_by(
        &self,
        request: &GroupByRequest,
    ) -> QueryResult<Vec<AggregateOutput>> {
        if request.group_by.is_empty() {
            return Err(QueryValidationError::EmptyGroupBy.into());
        }
        let mut output_names =
            HashSet::with_capacity(request.group_by.len() + request.aggregates.len());
        for column in &request.group_by {
            let field = self.column(column)?;
            if !field.data_type.is_scalar() {
                return Err(QueryValidationError::NonScalarGroupColumn {
                    name: column.clone(),
                }
                .into());
            }
            if !output_names.insert(column.as_str()) {
                return Err(QueryValidationError::DuplicateGroupColumn {
                    name: column.clone(),
                }
                .into());
            }
        }
        if let Some(filter) = &request.filter {
            self.validate_filter(filter)?;
        }

        let mut outputs = Vec::with_capacity(request.aggregates.len());
        for aggregate in &request.aggregates {
            validate_user_column_name(&aggregate.alias)?;
            if !output_names.insert(aggregate.alias.as_str()) {
                return Err(QueryValidationError::DuplicateAggregateAlias {
                    alias: aggregate.alias.clone(),
                }
                .into());
            }
            let input = self.column(&aggregate.column)?;
            let data_type = aggregate
                .function
                .output_type(&input.data_type)
                .ok_or_else(|| QueryValidationError::InvalidAggregateType {
                    column: aggregate.column.clone(),
                    function: aggregate.function,
                    actual: input.data_type.clone(),
                })?;
            outputs.push(AggregateOutput::new(aggregate.alias.clone(), data_type));
        }
        Ok(outputs)
    }

    /// Validates a vector query against a dataset-owned vector column.
    ///
    /// # Errors
    ///
    /// Returns an error for zero `k`, a non-vector or invalid vector column,
    /// non-finite query components, dimension mismatch, or an invalid filter.
    pub(crate) fn validate_vector_search(
        &self,
        request: &VectorSearchRequest,
    ) -> QueryResult<Option<Vec<String>>> {
        if request.k == 0 {
            return Err(QueryValidationError::InvalidVectorK.into());
        }
        if request.vector_column != "vector" {
            return Err(QueryValidationError::UnsupportedVectorColumn {
                name: request.vector_column.clone(),
            }
            .into());
        }
        let field = self.column(&request.vector_column)?;
        let LogicalType::Vector { dimensions } = field.data_type else {
            return Err(QueryValidationError::NotVectorColumn {
                name: request.vector_column.clone(),
                actual: field.data_type.clone(),
            }
            .into());
        };
        for (index, component) in request.query.iter().enumerate() {
            if !component.is_finite() {
                return Err(QueryValidationError::NonFiniteVectorComponent { index }.into());
            }
        }
        if request.query.len() != dimensions {
            return Err(QueryValidationError::VectorDimensionMismatch {
                expected: dimensions,
                actual: request.query.len(),
            }
            .into());
        }
        if let Some(filter) = &request.filter {
            self.validate_filter(filter)?;
        }
        match &request.hydration {
            VectorHydration::NotRequested => Ok(None),
            VectorHydration::Projection(projection) => {
                self.validate_projection(projection).map(Some)
            }
        }
    }

    fn validate_comparison(&self, comparison: &Comparison) -> QueryResult<()> {
        let field = self.column(&comparison.column)?;
        if !field.data_type.is_scalar() {
            return Err(QueryValidationError::NonScalarFilterColumn {
                name: comparison.column.clone(),
            }
            .into());
        }
        let actual = comparison.value.logical_type();
        if field.data_type != actual {
            return Err(QueryValidationError::FilterTypeMismatch {
                column: comparison.column.clone(),
                expected: field.data_type.clone(),
                actual,
            }
            .into());
        }
        if matches!(field.data_type, LogicalType::Boolean)
            && !matches!(
                comparison.operator,
                ComparisonOperator::Equal | ComparisonOperator::NotEqual
            )
        {
            return Err(QueryValidationError::InvalidComparisonOperator {
                column: comparison.column.clone(),
                operator: comparison.operator,
                data_type: field.data_type.clone(),
            }
            .into());
        }
        Ok(())
    }

    fn column(&self, name: &str) -> ValidationResult<&LogicalColumn> {
        validate_user_column_name(name)?;
        self.columns
            .iter()
            .find(|column| column.name == name)
            .ok_or_else(|| QueryValidationError::UnknownColumn {
                name: name.to_owned(),
            })
    }
}

/// The supported output projection for scans and point lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    All,
    Columns(Vec<String>),
}

/// A comparison operand in a filter expression.
#[derive(Debug, Clone, PartialEq)]
pub struct Comparison {
    pub column: String,
    pub operator: ComparisonOperator,
    pub value: FilterLiteral,
}

/// The comparison relation applied to a scalar column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// A typed boolean filter expression.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterExpression {
    Compare(Comparison),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
}

/// A scan request to be executed against an immutable snapshot facade.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanRequest {
    pub projection: Projection,
    pub filter: Option<FilterExpression>,
}

/// A named projected value. Rows preserve this vector's order.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedField {
    pub name: String,
    pub value: ResultValue,
}

impl ProjectedField {
    #[must_use]
    pub fn new(name: impl Into<String>, value: ResultValue) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

/// A value returned from a scan, lookup, aggregation, or vector hydration.
#[derive(Debug, Clone, PartialEq)]
pub enum ResultValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(String),
    Vector(Vec<f32>),
}

impl ResultValue {
    fn logical_type(&self) -> Option<LogicalType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(LogicalType::Boolean),
            Self::Int64(_) => Some(LogicalType::Int64),
            Self::UInt64(_) => Some(LogicalType::UInt64),
            Self::Float64(_) => Some(LogicalType::Float64),
            Self::Utf8(_) => Some(LogicalType::Utf8),
            Self::Vector(values) => Some(LogicalType::Vector {
                dimensions: values.len(),
            }),
        }
    }
}

/// One self-describing projected result row.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedRow {
    pub fields: Vec<ProjectedField>,
}

/// The contract result for a scan.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanResult {
    pub projection: Vec<String>,
    pub rows: Vec<ProjectedRow>,
}

/// A dataset-global physical row identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct RowId(pub u64);

/// A point lookup request, bound to immutable snapshot execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowLookupRequest {
    pub row_id: RowId,
    pub projection: Projection,
}

/// The physical visibility outcome for one row lookup.
#[derive(Debug, Clone, PartialEq)]
pub enum RowLookupOutcome {
    Live(ProjectedRow),
    Tombstoned,
    /// The row ID was never allocated in this dataset snapshot.
    NotFound,
}

/// The contract result for a physical row lookup.
#[derive(Debug, Clone, PartialEq)]
pub struct RowLookupResult {
    pub row_id: RowId,
    pub projection: Vec<String>,
    pub outcome: RowLookupOutcome,
}

/// One aggregate requested for every group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregate {
    pub column: String,
    pub function: AggregateFunction,
    pub alias: String,
}

impl Aggregate {
    #[must_use]
    pub fn new(
        column: impl Into<String>,
        function: AggregateFunction,
        alias: impl Into<String>,
    ) -> Self {
        Self {
            column: column.into(),
            function,
            alias: alias.into(),
        }
    }
}

/// A grouped aggregate function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Minimum,
    Maximum,
    Average,
}

#[allow(dead_code)]
impl AggregateFunction {
    fn output_type(self, input: &LogicalType) -> Option<LogicalType> {
        match self {
            Self::Count if input.is_scalar() => Some(LogicalType::UInt64),
            Self::Sum | Self::Minimum | Self::Maximum if input.is_numeric() => Some(input.clone()),
            Self::Average if input.is_numeric() => Some(LogicalType::Float64),
            _ => None,
        }
    }
}

/// A group-by request. Result group order is unspecified.
///
/// `Count` produces [`LogicalType::UInt64`] and ignores null input values.
/// `Sum`, `Minimum`, `Maximum`, and `Average` ignore null inputs; their
/// all-null result is [`ResultValue::Null`]. `Sum` over `Int64` must use
/// checked arithmetic and return [`QueryExecutionError::Int64SumOverflow`]
/// rather than wrap. Float inputs and averages use `Float64` arithmetic.
/// Empty input produces zero rows.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupByRequest {
    pub group_by: Vec<String>,
    pub aggregates: Vec<Aggregate>,
    pub filter: Option<FilterExpression>,
}

/// A validated aggregate output field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateOutput {
    alias: String,
    data_type: LogicalType,
}

impl AggregateOutput {
    #[must_use]
    pub(crate) fn new(alias: impl Into<String>, data_type: LogicalType) -> Self {
        Self {
            alias: alias.into(),
            data_type,
        }
    }

    #[must_use]
    pub fn alias(&self) -> &str {
        &self.alias
    }

    #[must_use]
    pub fn data_type(&self) -> &LogicalType {
        &self.data_type
    }
}

/// One grouped output row. Key and aggregate values follow their request order.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupedRow {
    pub keys: Vec<ResultValue>,
    pub aggregates: Vec<ResultValue>,
}

/// The contract result for group-by execution. Row order is unspecified.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupByResult {
    group_by: Vec<String>,
    aggregates: Vec<AggregateOutput>,
    rows: Vec<GroupedRow>,
}

impl GroupByResult {
    /// Builds a grouped result after enforcing its public shape invariants.
    ///
    /// # Errors
    ///
    /// Returns a typed execution error for row shape mismatch, vector group
    /// keys, null count outputs, or aggregate value type mismatch.
    pub fn new(
        group_by: Vec<String>,
        aggregates: Vec<AggregateOutput>,
        rows: Vec<GroupedRow>,
    ) -> QueryResult<Self> {
        let mut output_names = HashSet::with_capacity(group_by.len() + aggregates.len());
        for name in &group_by {
            validate_user_column_name(name)?;
            if !output_names.insert(name.as_str()) {
                return Err(
                    QueryValidationError::DuplicateGroupColumn { name: name.clone() }.into(),
                );
            }
        }
        for output in &aggregates {
            validate_user_column_name(&output.alias)?;
            if !output_names.insert(output.alias.as_str()) {
                return Err(QueryValidationError::DuplicateAggregateAlias {
                    alias: output.alias.clone(),
                }
                .into());
            }
            if !matches!(
                output.data_type,
                LogicalType::Int64 | LogicalType::UInt64 | LogicalType::Float64
            ) {
                return Err(QueryValidationError::InvalidAggregateOutputType {
                    alias: output.alias.clone(),
                    actual: output.data_type.clone(),
                }
                .into());
            }
        }
        for (row_index, row) in rows.iter().enumerate() {
            if row.keys.len() != group_by.len() || row.aggregates.len() != aggregates.len() {
                return Err(QueryExecutionError::GroupResultShape {
                    row: row_index,
                    expected_keys: group_by.len(),
                    actual_keys: row.keys.len(),
                    expected_aggregates: aggregates.len(),
                    actual_aggregates: row.aggregates.len(),
                }
                .into());
            }
            if row
                .keys
                .iter()
                .any(|value| matches!(value, ResultValue::Vector(_)))
            {
                return Err(QueryExecutionError::NonScalarGroupResult { row: row_index }.into());
            }
            for (output, value) in aggregates.iter().zip(&row.aggregates) {
                if output.data_type == LogicalType::UInt64 && matches!(value, ResultValue::Null) {
                    return Err(QueryExecutionError::NullCountAggregate {
                        alias: output.alias.clone(),
                    }
                    .into());
                }
                if let Some(actual) = value.logical_type()
                    && actual != output.data_type
                {
                    return Err(QueryExecutionError::AggregateResultTypeMismatch {
                        alias: output.alias.clone(),
                        expected: output.data_type.clone(),
                        actual,
                    }
                    .into());
                }
            }
        }
        Ok(Self {
            group_by,
            aggregates,
            rows,
        })
    }

    #[must_use]
    pub fn group_by(&self) -> &[String] {
        &self.group_by
    }

    #[must_use]
    pub fn aggregates(&self) -> &[AggregateOutput] {
        &self.aggregates
    }

    #[must_use]
    pub fn rows(&self) -> &[GroupedRow] {
        &self.rows
    }
}

/// A vector nearest-neighbor request.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchRequest {
    pub vector_column: String,
    pub query: Vec<f32>,
    pub k: usize,
    pub filter: Option<FilterExpression>,
    pub hydration: VectorHydration,
}

/// Whether vector hits should include projected row data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorHydration {
    NotRequested,
    Projection(Projection),
}

/// A typed reason why a requested vector-hit row could not be hydrated.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HydrationError {
    #[error("the matching row was not found")]
    NotFound,
    #[error("the matching row is tombstoned")]
    Tombstoned,
    #[error("the matching row has no available row data")]
    VectorUnavailable,
    #[error("row hydration integrity failure: {message}")]
    IntegrityFailure { message: String },
}

/// The row-hydration state for one vector hit.
#[derive(Debug, Clone, PartialEq)]
pub enum VectorHydrationState {
    NotRequested,
    Hydrated(ProjectedRow),
    Unresolved(HydrationError),
}

/// One vector result. Distance is squared L2, matching the current index metric.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorHit {
    pub row_id: RowId,
    pub squared_l2_distance: f32,
    pub hydration: VectorHydrationState,
}

/// The contract result for vector search.
///
/// `hits` are ordered by ascending squared-L2 distance, then ascending row ID,
/// and contain at most `requested_k` entries. Fewer entries are permitted when
/// the index cannot return enough live matches.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchResult {
    requested_k: usize,
    hydration_projection: Option<Vec<String>>,
    hits: Vec<VectorHit>,
}

impl VectorSearchResult {
    /// Builds a vector result that cannot exceed its requested result count.
    ///
    /// # Errors
    ///
    /// Returns [`QueryExecutionError::TooManyVectorHits`] when `hits` exceeds
    /// `requested_k`.
    pub fn new(
        requested_k: usize,
        hydration_projection: Option<Vec<String>>,
        hits: Vec<VectorHit>,
    ) -> QueryResult<Self> {
        if hits.len() > requested_k {
            return Err(QueryExecutionError::TooManyVectorHits {
                requested_k,
                actual: hits.len(),
            }
            .into());
        }
        let mut previous = None;
        for hit in &hits {
            if !hit.squared_l2_distance.is_finite() {
                return Err(
                    QueryExecutionError::NonFiniteVectorDistance { row_id: hit.row_id }.into(),
                );
            }
            if hit.squared_l2_distance < 0.0 {
                return Err(
                    QueryExecutionError::NegativeVectorDistance { row_id: hit.row_id }.into(),
                );
            }
            if let Some((previous_row_id, previous_distance)) = previous
                && hit
                    .squared_l2_distance
                    .total_cmp(&previous_distance)
                    .then_with(|| hit.row_id.cmp(&previous_row_id))
                    .is_lt()
            {
                return Err(QueryExecutionError::NonMonotonicVectorDistances {
                    previous_row_id,
                    row_id: hit.row_id,
                }
                .into());
            }
            match (&hydration_projection, &hit.hydration) {
                (None, VectorHydrationState::NotRequested)
                | (Some(_), VectorHydrationState::Unresolved(_)) => {}
                (None, _) => {
                    return Err(
                        QueryExecutionError::UnexpectedHydration { row_id: hit.row_id }.into(),
                    );
                }
                (Some(_), VectorHydrationState::NotRequested) => {
                    return Err(QueryExecutionError::MissingHydration { row_id: hit.row_id }.into());
                }
                (Some(projection), VectorHydrationState::Hydrated(row)) => {
                    let actual = row
                        .fields
                        .iter()
                        .map(|field| field.name.clone())
                        .collect::<Vec<_>>();
                    if actual != *projection {
                        return Err(QueryExecutionError::HydrationProjectionMismatch {
                            row_id: hit.row_id,
                            expected: projection.clone(),
                            actual,
                        }
                        .into());
                    }
                }
            }
            previous = Some((hit.row_id, hit.squared_l2_distance));
        }
        Ok(Self {
            requested_k,
            hydration_projection,
            hits,
        })
    }

    #[must_use]
    pub fn requested_k(&self) -> usize {
        self.requested_k
    }

    #[must_use]
    pub fn hydration_projection(&self) -> Option<&[String]> {
        self.hydration_projection.as_deref()
    }

    #[must_use]
    pub fn hits(&self) -> &[VectorHit] {
        &self.hits
    }
}

/// An execution failure described by this contract.
#[derive(Debug, Error)]
pub enum QueryExecutionError {
    #[error("transaction read operation '{operation}' cannot merge staged writes safely")]
    UnsupportedTransactionRead { operation: &'static str },
    #[error("engine query execution failed: {0}")]
    Engine(#[source] Box<crate::TxnError>),
    #[error("checked Int64 sum overflowed for aggregate '{alias}'")]
    Int64SumOverflow { alias: String },
    #[error("vector result returned {actual} hits for requested k={requested_k}")]
    TooManyVectorHits { requested_k: usize, actual: usize },
    #[error("vector hit row {row_id:?} has a negative squared-L2 distance")]
    NegativeVectorDistance { row_id: RowId },
    #[error("vector hit row {row_id:?} has a non-finite squared-L2 distance")]
    NonFiniteVectorDistance { row_id: RowId },
    #[error("vector hit distances are not monotonic")]
    NonMonotonicVectorDistances {
        previous_row_id: RowId,
        row_id: RowId,
    },
    #[error("vector hit row {row_id:?} was hydrated without a requested projection")]
    UnexpectedHydration { row_id: RowId },
    #[error("vector hit row {row_id:?} was not hydrated despite a requested projection")]
    MissingHydration { row_id: RowId },
    #[error("vector hit row {row_id:?} fields do not match the requested hydration projection")]
    HydrationProjectionMismatch {
        row_id: RowId,
        expected: Vec<String>,
        actual: Vec<String>,
    },
    #[error(
        "group result row {row} has {actual_keys} keys and {actual_aggregates} aggregates; expected {expected_keys} and {expected_aggregates}"
    )]
    GroupResultShape {
        row: usize,
        expected_keys: usize,
        actual_keys: usize,
        expected_aggregates: usize,
        actual_aggregates: usize,
    },
    #[error("group result row {row} contains a vector key")]
    NonScalarGroupResult { row: usize },
    #[error("count aggregate '{alias}' cannot be null")]
    NullCountAggregate { alias: String },
    #[error("aggregate '{alias}' has type {actual:?}, expected {expected:?}")]
    AggregateResultTypeMismatch {
        alias: String,
        expected: LogicalType,
        actual: LogicalType,
    },
}

/// A query-contract validation failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QueryValidationError {
    #[error("column name '{name}' is reserved for internal use")]
    ReservedColumn { name: String },
    #[error("dataset schema contains duplicate column '{name}'")]
    DuplicateSchemaColumn { name: String },
    #[error("vector column '{name}' must have at least one dimension")]
    ZeroVectorDimensions { name: String },
    #[error("unknown dataset column '{name}'")]
    UnknownColumn { name: String },
    #[error("projection contains duplicate column '{name}'")]
    DuplicateProjection { name: String },
    #[error("filter for '{column}' has type {actual:?}, expected {expected:?}")]
    FilterTypeMismatch {
        column: String,
        expected: LogicalType,
        actual: LogicalType,
    },
    #[error("filter cannot compare non-scalar column '{name}'")]
    NonScalarFilterColumn { name: String },
    #[error("operator {operator:?} is invalid for '{column}' with type {data_type:?}")]
    InvalidComparisonOperator {
        column: String,
        operator: ComparisonOperator,
        data_type: LogicalType,
    },
    #[error("group-by cannot use non-scalar column '{name}'")]
    NonScalarGroupColumn { name: String },
    #[error("group-by contains duplicate column '{name}'")]
    DuplicateGroupColumn { name: String },
    #[error("group-by requires at least one group column")]
    EmptyGroupBy,
    #[error("aggregate aliases must be unique; duplicate alias '{alias}'")]
    DuplicateAggregateAlias { alias: String },
    #[error("aggregate output '{alias}' has invalid type {actual:?}")]
    InvalidAggregateOutputType { alias: String, actual: LogicalType },
    #[error("{function:?} cannot aggregate '{column}' with type {actual:?}")]
    InvalidAggregateType {
        column: String,
        function: AggregateFunction,
        actual: LogicalType,
    },
    #[error("vector search k must be greater than zero")]
    InvalidVectorK,
    #[error("vector search supports only the logical column 'vector'; '{name}' is unsupported")]
    UnsupportedVectorColumn { name: String },
    #[error("vector search requires a vector column; '{name}' has type {actual:?}")]
    NotVectorColumn { name: String, actual: LogicalType },
    #[error("vector query component {index} is not finite")]
    NonFiniteVectorComponent { index: usize },
    #[error("vector query dimension mismatch: expected {expected}, found {actual}")]
    VectorDimensionMismatch { expected: usize, actual: usize },
}

/// A public query-contract failure.
#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    Validation(#[from] QueryValidationError),
    #[error(transparent)]
    Execution(#[from] QueryExecutionError),
}

impl From<crate::TxnError> for QueryExecutionError {
    fn from(error: crate::TxnError) -> Self {
        Self::Engine(Box::new(error))
    }
}

impl From<crate::TxnError> for QueryError {
    fn from(error: crate::TxnError) -> Self {
        Self::Execution(error.into())
    }
}

/// The result type for public query-contract operations.
pub type QueryResult<T> = std::result::Result<T, QueryError>;
#[allow(dead_code)]
type ValidationResult<T> = std::result::Result<T, QueryValidationError>;

#[allow(dead_code)]
fn validate_user_column_name(name: &str) -> ValidationResult<()> {
    if name == crate::ROW_ID_COLUMN || name == crate::TIMESTAMP_COLUMN {
        return Err(QueryValidationError::ReservedColumn {
            name: name.to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn query_execution_engine_error_preserves_typed_txn_source_and_display() {
        let error = QueryError::from(crate::TxnError::NotFound(std::path::PathBuf::from(
            "missing-dataset",
        )));

        match error {
            QueryError::Execution(QueryExecutionError::Engine(source)) => {
                assert!(matches!(source.as_ref(), crate::TxnError::NotFound(_)));
                assert_eq!(
                    source.to_string(),
                    "no dataset found at missing-dataset — call Dataset::create first"
                );
            }
            other => panic!("expected a typed engine error, got {other:?}"),
        }
    }

    fn schema() -> DatasetSchema {
        DatasetSchema::new(vec![
            LogicalColumn::new("title", LogicalType::Utf8, false),
            LogicalColumn::new("score", LogicalType::Int64, true),
            LogicalColumn::new("active", LogicalType::Boolean, false),
            LogicalColumn::new("embedding", LogicalType::Vector { dimensions: 3 }, false),
        ])
        .unwrap()
    }

    fn vector_schema() -> DatasetSchema {
        DatasetSchema::new(vec![
            LogicalColumn::new("title", LogicalType::Utf8, false),
            LogicalColumn::new("score", LogicalType::Int64, true),
            LogicalColumn::new("active", LogicalType::Boolean, false),
            LogicalColumn::new("vector", LogicalType::Vector { dimensions: 3 }, false),
        ])
        .unwrap()
    }

    #[test]
    fn query_contract_preserves_projection_order_and_rejects_invalid_projection_names() {
        let schema = schema();
        let request = ScanRequest {
            projection: Projection::Columns(vec!["score".into(), "title".into()]),
            filter: None,
        };

        assert_eq!(
            schema.validate_scan(&request).unwrap(),
            vec!["score", "title"]
        );
        assert_eq!(
            schema
                .validate_projection(&Projection::Columns(vec![
                    "score".into(),
                    "embedding".into(),
                ]))
                .unwrap(),
            vec!["score", "embedding"]
        );
        assert!(matches!(
            schema.validate_projection(&Projection::Columns(vec!["title".into(), "title".into()])),
            Err(QueryError::Validation(
                QueryValidationError::DuplicateProjection { .. }
            ))
        ));
        assert!(matches!(
            schema.validate_projection(&Projection::Columns(vec!["_row_id".into()])),
            Err(QueryError::Validation(
                QueryValidationError::ReservedColumn { .. }
            ))
        ));
    }

    #[test]
    fn query_contract_rejects_filters_with_the_wrong_scalar_type() {
        let schema = schema();
        let request = ScanRequest {
            projection: Projection::All,
            filter: Some(FilterExpression::Compare(Comparison {
                column: "score".into(),
                operator: ComparisonOperator::GreaterThan,
                value: FilterLiteral::Utf8("high".into()),
            })),
        };

        assert!(matches!(
            schema.validate_scan(&request),
            Err(QueryError::Validation(
                QueryValidationError::FilterTypeMismatch { .. }
            ))
        ));
    }

    #[test]
    fn query_contract_validates_aggregate_aliases_and_output_types() {
        let schema = schema();
        let request = GroupByRequest {
            group_by: vec!["title".into()],
            aggregates: vec![
                Aggregate::new("score", AggregateFunction::Sum, "total_score"),
                Aggregate::new("score", AggregateFunction::Average, "average_score"),
            ],
            filter: None,
        };

        assert_eq!(
            schema.validate_group_by(&request).unwrap(),
            vec![
                AggregateOutput::new("total_score", LogicalType::Int64),
                AggregateOutput::new("average_score", LogicalType::Float64),
            ]
        );
        assert!(matches!(
            schema.validate_group_by(&GroupByRequest {
                group_by: vec!["title".into()],
                aggregates: vec![
                    Aggregate::new("score", AggregateFunction::Count, "rows"),
                    Aggregate::new("score", AggregateFunction::Maximum, "rows"),
                ],
                filter: None,
            }),
            Err(QueryError::Validation(
                QueryValidationError::DuplicateAggregateAlias { .. }
            ))
        ));
    }

    #[test]
    fn query_contract_rejects_group_by_without_group_keys() {
        let schema = schema();

        assert!(matches!(
            schema.validate_group_by(&GroupByRequest {
                group_by: Vec::new(),
                aggregates: vec![Aggregate::new("score", AggregateFunction::Count, "count")],
                filter: None,
            }),
            Err(QueryError::Validation(_))
        ));
    }

    #[test]
    fn query_contract_rejects_invalid_vector_requests() {
        let schema = vector_schema();
        let request = VectorSearchRequest {
            vector_column: "vector".into(),
            query: vec![0.0, f32::NAN, 2.0],
            k: 0,
            filter: None,
            hydration: VectorHydration::NotRequested,
        };

        assert!(matches!(
            schema.validate_vector_search(&request),
            Err(QueryError::Validation(QueryValidationError::InvalidVectorK))
        ));

        let request = VectorSearchRequest {
            vector_column: "vector".into(),
            query: vec![0.0, f32::NAN, 2.0],
            k: 3,
            filter: None,
            hydration: VectorHydration::NotRequested,
        };
        assert!(matches!(
            schema.validate_vector_search(&request),
            Err(QueryError::Validation(
                QueryValidationError::NonFiniteVectorComponent { index: 1 }
            ))
        ));

        let request = VectorSearchRequest {
            vector_column: "vector".into(),
            query: vec![0.0, 1.0],
            k: 3,
            filter: None,
            hydration: VectorHydration::NotRequested,
        };
        assert!(matches!(
            schema.validate_vector_search(&request),
            Err(QueryError::Validation(
                QueryValidationError::VectorDimensionMismatch {
                    expected: 3,
                    actual: 2
                }
            ))
        ));
    }

    #[test]
    fn query_contract_rejects_same_dimension_alternate_vector_column() {
        let schema = schema();
        let request = VectorSearchRequest {
            vector_column: "embedding".into(),
            query: vec![0.0, 1.0, 2.0],
            k: 1,
            filter: None,
            hydration: VectorHydration::NotRequested,
        };

        assert!(matches!(
            schema.validate_vector_search(&request),
            Err(QueryError::Validation(
                QueryValidationError::UnsupportedVectorColumn { name }
            )) if name == "embedding"
        ));
    }

    #[test]
    fn query_contract_represents_nullable_and_vector_projected_values_in_order() {
        let row = ProjectedRow {
            fields: vec![
                ProjectedField::new("score", ResultValue::Null),
                ProjectedField::new("embedding", ResultValue::Vector(vec![0.0, 1.0, 2.0])),
            ],
        };

        assert_eq!(row.fields[0].name, "score");
        assert_eq!(row.fields[0].value, ResultValue::Null);
        assert_eq!(
            row.fields[1],
            ProjectedField::new("embedding", ResultValue::Vector(vec![0.0, 1.0, 2.0]))
        );
    }

    #[test]
    fn query_contract_has_distinct_lookup_request_and_physical_outcomes() {
        let schema = schema();
        let request = RowLookupRequest {
            row_id: RowId(41),
            projection: Projection::Columns(vec!["title".into()]),
        };
        assert_eq!(schema.validate_row_lookup(&request).unwrap(), vec!["title"]);

        let live = RowLookupResult {
            row_id: RowId(41),
            projection: vec!["title".into()],
            outcome: RowLookupOutcome::Live(ProjectedRow {
                fields: vec![ProjectedField::new(
                    "title",
                    ResultValue::Utf8("kept".into()),
                )],
            }),
        };
        assert!(matches!(live.outcome, RowLookupOutcome::Live(_)));
        assert!(matches!(
            RowLookupOutcome::Tombstoned,
            RowLookupOutcome::Tombstoned
        ));
        assert!(matches!(
            RowLookupOutcome::NotFound,
            RowLookupOutcome::NotFound
        ));
        assert!(matches!(
            schema.validate_row_lookup(&RowLookupRequest {
                row_id: RowId(42),
                projection: Projection::Columns(vec!["unknown".into()]),
            }),
            Err(QueryError::Validation(
                QueryValidationError::UnknownColumn { .. }
            ))
        ));
    }

    #[test]
    fn query_contract_makes_vector_hydration_and_underfill_unambiguous() {
        let schema = vector_schema();
        let request = VectorSearchRequest {
            vector_column: "vector".into(),
            query: vec![0.0, 1.0, 2.0],
            k: 5,
            filter: None,
            hydration: VectorHydration::Projection(Projection::Columns(vec!["title".into()])),
        };
        assert_eq!(
            schema.validate_vector_search(&request).unwrap(),
            Some(vec!["title".into()])
        );

        let result = VectorSearchResult::new(
            5,
            Some(vec!["title".into()]),
            vec![
                VectorHit {
                    row_id: RowId(2),
                    squared_l2_distance: 0.25,
                    hydration: VectorHydrationState::Hydrated(ProjectedRow {
                        fields: vec![ProjectedField::new(
                            "title",
                            ResultValue::Utf8("near".into()),
                        )],
                    }),
                },
                VectorHit {
                    row_id: RowId(7),
                    squared_l2_distance: 1.0,
                    hydration: VectorHydrationState::Unresolved(HydrationError::VectorUnavailable),
                },
            ],
        )
        .unwrap();
        assert!(result.hits().len() <= result.requested_k());
        assert!(matches!(
            result.hits()[1].hydration,
            VectorHydrationState::Unresolved(HydrationError::VectorUnavailable)
        ));
        assert!(matches!(
            VectorSearchResult::new(
                1,
                None,
                vec![
                    VectorHit {
                        row_id: RowId(2),
                        squared_l2_distance: 0.25,
                        hydration: VectorHydrationState::NotRequested,
                    },
                    VectorHit {
                        row_id: RowId(7),
                        squared_l2_distance: 1.0,
                        hydration: VectorHydrationState::NotRequested,
                    },
                ],
            ),
            Err(QueryError::Execution(
                QueryExecutionError::TooManyVectorHits { .. }
            ))
        ));
        for error in [
            HydrationError::NotFound,
            HydrationError::Tombstoned,
            HydrationError::VectorUnavailable,
            HydrationError::IntegrityFailure {
                message: "checksum mismatch".into(),
            },
        ] {
            assert!(matches!(
                VectorHydrationState::Unresolved(error),
                VectorHydrationState::Unresolved(_)
            ));
        }
    }

    #[test]
    fn query_contract_encodes_group_nulls_count_type_and_empty_input_semantics() {
        let schema = schema();
        let request = GroupByRequest {
            group_by: vec!["title".into()],
            aggregates: vec![
                Aggregate::new("score", AggregateFunction::Count, "count"),
                Aggregate::new("score", AggregateFunction::Average, "average"),
            ],
            filter: None,
        };
        assert_eq!(
            schema.validate_group_by(&request).unwrap(),
            vec![
                AggregateOutput::new("count", LogicalType::UInt64),
                AggregateOutput::new("average", LogicalType::Float64),
            ]
        );

        let result = GroupByResult::new(
            vec!["title".into()],
            vec![
                AggregateOutput::new("count", LogicalType::UInt64),
                AggregateOutput::new("average", LogicalType::Float64),
            ],
            vec![GroupedRow {
                keys: vec![ResultValue::Utf8("all-null".into())],
                aggregates: vec![ResultValue::UInt64(0), ResultValue::Null],
            }],
        )
        .unwrap();
        assert_eq!(result.rows()[0].aggregates[1], ResultValue::Null);
        assert!(
            GroupByResult::new(vec!["title".into()], Vec::new(), Vec::new())
                .unwrap()
                .rows()
                .is_empty()
        );
        assert!(matches!(
            GroupByResult::new(
                vec!["title".into()],
                vec![AggregateOutput::new("count", LogicalType::UInt64)],
                vec![GroupedRow {
                    keys: vec![ResultValue::Utf8("all-null".into())],
                    aggregates: vec![ResultValue::Null],
                }],
            ),
            Err(QueryError::Execution(
                QueryExecutionError::NullCountAggregate { .. }
            ))
        ));
        assert!(matches!(
            GroupByResult::new(
                vec!["title".into()],
                Vec::new(),
                vec![GroupedRow {
                    keys: vec![ResultValue::Vector(vec![1.0])],
                    aggregates: Vec::new(),
                }],
            ),
            Err(QueryError::Execution(
                QueryExecutionError::NonScalarGroupResult { .. }
            ))
        ));
    }

    #[test]
    fn query_contract_rejects_invalid_names_inputs_boolean_ordering_and_zero_vectors() {
        assert!(matches!(
            DatasetSchema::new(vec![LogicalColumn::new(
                "_timestamp",
                LogicalType::Utf8,
                false,
            )]),
            Err(QueryError::Validation(
                QueryValidationError::ReservedColumn { .. }
            ))
        ));
        assert!(matches!(
            DatasetSchema::new(vec![LogicalColumn::new(
                "empty",
                LogicalType::Vector { dimensions: 0 },
                false,
            )]),
            Err(QueryError::Validation(
                QueryValidationError::ZeroVectorDimensions { .. }
            ))
        ));

        let schema = schema();
        assert!(matches!(
            schema.validate_group_by(&GroupByRequest {
                group_by: vec!["embedding".into()],
                aggregates: Vec::new(),
                filter: None,
            }),
            Err(QueryError::Validation(
                QueryValidationError::NonScalarGroupColumn { .. }
            ))
        ));
        assert!(matches!(
            schema.validate_group_by(&GroupByRequest {
                group_by: vec!["title".into()],
                aggregates: vec![Aggregate::new("title", AggregateFunction::Sum, "sum")],
                filter: None,
            }),
            Err(QueryError::Validation(
                QueryValidationError::InvalidAggregateType { .. }
            ))
        ));
        assert!(
            schema
                .validate_filter(&FilterExpression::Compare(Comparison {
                    column: "active".into(),
                    operator: ComparisonOperator::Equal,
                    value: FilterLiteral::Boolean(true),
                }))
                .is_ok()
        );
        assert!(matches!(
            schema.validate_filter(&FilterExpression::Compare(Comparison {
                column: "active".into(),
                operator: ComparisonOperator::GreaterThan,
                value: FilterLiteral::Boolean(true),
            })),
            Err(QueryError::Validation(
                QueryValidationError::InvalidComparisonOperator { .. }
            ))
        ));
        assert!(
            schema
                .validate_filter(&FilterExpression::And(
                    Box::new(FilterExpression::Compare(Comparison {
                        column: "active".into(),
                        operator: ComparisonOperator::Equal,
                        value: FilterLiteral::Boolean(true),
                    })),
                    Box::new(FilterExpression::Not(Box::new(FilterExpression::Or(
                        Box::new(FilterExpression::Compare(Comparison {
                            column: "title".into(),
                            operator: ComparisonOperator::Equal,
                            value: FilterLiteral::Utf8("archived".into()),
                        })),
                        Box::new(FilterExpression::Compare(Comparison {
                            column: "score".into(),
                            operator: ComparisonOperator::GreaterThan,
                            value: FilterLiteral::Int64(10),
                        })),
                    )))),
                ))
                .is_ok()
        );
    }

    #[test]
    fn query_contract_rejects_invalid_vector_distances_and_ordering() {
        let hit = |row_id, distance| VectorHit {
            row_id: RowId(row_id),
            squared_l2_distance: distance,
            hydration: VectorHydrationState::NotRequested,
        };

        assert!(matches!(
            VectorSearchResult::new(1, None, vec![hit(1, -0.1)]),
            Err(QueryError::Execution(
                QueryExecutionError::NegativeVectorDistance { .. }
            ))
        ));
        assert!(matches!(
            VectorSearchResult::new(1, None, vec![hit(1, f32::NAN)]),
            Err(QueryError::Execution(
                QueryExecutionError::NonFiniteVectorDistance { .. }
            ))
        ));
        assert!(matches!(
            VectorSearchResult::new(1, None, vec![hit(1, f32::INFINITY)]),
            Err(QueryError::Execution(
                QueryExecutionError::NonFiniteVectorDistance { .. }
            ))
        ));
        assert!(matches!(
            VectorSearchResult::new(2, None, vec![hit(1, 2.0), hit(2, 1.0)]),
            Err(QueryError::Execution(
                QueryExecutionError::NonMonotonicVectorDistances { .. }
            ))
        ));
    }

    #[test]
    fn query_contract_rejects_equal_distance_hits_with_descending_row_ids() {
        let hit = |row_id| VectorHit {
            row_id: RowId(row_id),
            squared_l2_distance: 1.0,
            hydration: VectorHydrationState::NotRequested,
        };

        assert!(matches!(
            VectorSearchResult::new(2, None, vec![hit(2), hit(1)]),
            Err(QueryError::Execution(
                QueryExecutionError::NonMonotonicVectorDistances {
                    previous_row_id: RowId(2),
                    row_id: RowId(1),
                }
            ))
        ));
    }

    #[test]
    fn query_contract_rejects_inconsistent_vector_hydration() {
        let hydrated = |row_id, fields| VectorHit {
            row_id: RowId(row_id),
            squared_l2_distance: 0.0,
            hydration: VectorHydrationState::Hydrated(ProjectedRow { fields }),
        };
        let not_requested = |row_id| VectorHit {
            row_id: RowId(row_id),
            squared_l2_distance: 0.0,
            hydration: VectorHydrationState::NotRequested,
        };

        assert!(matches!(
            VectorSearchResult::new(
                1,
                None,
                vec![hydrated(
                    1,
                    vec![ProjectedField::new("title", ResultValue::Utf8("x".into()))]
                )],
            ),
            Err(QueryError::Execution(
                QueryExecutionError::UnexpectedHydration { .. }
            ))
        ));
        assert!(matches!(
            VectorSearchResult::new(1, Some(vec!["title".into()]), vec![not_requested(1)]),
            Err(QueryError::Execution(
                QueryExecutionError::MissingHydration { .. }
            ))
        ));
        assert!(matches!(
            VectorSearchResult::new(
                1,
                Some(vec!["title".into()]),
                vec![hydrated(
                    1,
                    vec![ProjectedField::new("score", ResultValue::Int64(4))]
                )],
            ),
            Err(QueryError::Execution(
                QueryExecutionError::HydrationProjectionMismatch { .. }
            ))
        ));
    }

    #[test]
    fn query_contract_rejects_invalid_aggregate_output_definitions() {
        let row = GroupedRow {
            keys: Vec::new(),
            aggregates: vec![ResultValue::Int64(1)],
        };
        assert!(matches!(
            GroupByResult::new(
                Vec::new(),
                vec![AggregateOutput::new("_row_id", LogicalType::Int64)],
                vec![row.clone()],
            ),
            Err(QueryError::Validation(
                QueryValidationError::ReservedColumn { .. }
            ))
        ));
        assert!(matches!(
            GroupByResult::new(
                Vec::new(),
                vec![
                    AggregateOutput::new("total", LogicalType::Int64),
                    AggregateOutput::new("total", LogicalType::Float64),
                ],
                vec![row.clone()],
            ),
            Err(QueryError::Validation(
                QueryValidationError::DuplicateAggregateAlias { .. }
            ))
        ));
        assert!(matches!(
            GroupByResult::new(
                Vec::new(),
                vec![AggregateOutput::new(
                    "vectors_are_not_aggregates",
                    LogicalType::Vector { dimensions: 2 },
                )],
                vec![GroupedRow {
                    keys: Vec::new(),
                    aggregates: vec![ResultValue::Vector(vec![1.0, 2.0])],
                }],
            ),
            Err(QueryError::Validation(
                QueryValidationError::InvalidAggregateOutputType { .. }
            ))
        ));
    }

    #[test]
    fn query_contract_rejects_invalid_group_result_names() {
        assert!(matches!(
            GroupByResult::new(vec!["_row_id".into()], Vec::new(), Vec::new()),
            Err(QueryError::Validation(
                QueryValidationError::ReservedColumn { .. }
            ))
        ));
        assert!(matches!(
            GroupByResult::new(vec!["title".into(), "title".into()], Vec::new(), Vec::new(),),
            Err(QueryError::Validation(
                QueryValidationError::DuplicateGroupColumn { .. }
            ))
        ));
        assert!(matches!(
            GroupByResult::new(
                vec!["title".into()],
                vec![AggregateOutput::new("title", LogicalType::Int64)],
                Vec::new(),
            ),
            Err(QueryError::Validation(
                QueryValidationError::DuplicateAggregateAlias { .. }
            ))
        ));
    }
}
