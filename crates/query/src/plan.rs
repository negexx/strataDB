//! Small logical/physical planning DTOs for Strata's supported query primitives.
//!
//! The planner deliberately has no cost model. Its observations describe the
//! captured snapshot facts that were available when the plan was built; they
//! are evidence for explain output, not cardinality or latency guarantees.

use std::error::Error;
use std::fmt::{Display, Formatter};

/// One supported logical operation in a snapshot-bound query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalOperator {
    /// Reads one immutable manifest snapshot.
    Source,
    /// Applies a validated scalar predicate, optionally with zone-map pruning.
    Predicate { zone_map_eligible: bool },
    /// Emits fields in the supplied request order.
    Projection { columns: Vec<String> },
    /// Computes supported grouped aggregates.
    Grouping {
        keys: Vec<String>,
        aggregate_count: usize,
    },
    /// Searches the immutable vector segments listed by the manifest.
    VectorSearch {
        vector_column: String,
        k: usize,
        has_filter: bool,
        hydration: bool,
    },
    /// Produces the request's typed result value.
    Materialize,
}

/// A validated logical query pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalPlan {
    operators: Vec<LogicalOperator>,
}

impl LogicalPlan {
    /// Builds a supported logical pipeline.
    ///
    /// # Errors
    ///
    /// Returns [`PlanError::InvalidLogicalShape`] when the source or result
    /// materialization boundary is absent, and
    /// [`PlanError::ConflictingTerminalOperators`] when the result stream
    /// tries to combine incompatible primitive families.
    pub fn new(operators: Vec<LogicalOperator>) -> Result<Self, PlanError> {
        let has_source = matches!(operators.first(), Some(LogicalOperator::Source));
        let has_materialize = matches!(operators.last(), Some(LogicalOperator::Materialize));
        if !has_source || !has_materialize {
            return Err(PlanError::InvalidLogicalShape);
        }

        let result_operators = operators
            .iter()
            .filter(|operator| {
                matches!(
                    operator,
                    LogicalOperator::Projection { .. }
                        | LogicalOperator::Grouping { .. }
                        | LogicalOperator::VectorSearch { .. }
                )
            })
            .count();
        if result_operators != 1 {
            return Err(PlanError::ConflictingTerminalOperators);
        }

        let supported_order = matches!(
            operators.as_slice(),
            [
                LogicalOperator::Source,
                LogicalOperator::Projection { .. }
                    | LogicalOperator::Grouping { .. }
                    | LogicalOperator::VectorSearch { .. },
                LogicalOperator::Materialize,
            ] | [
                LogicalOperator::Source,
                LogicalOperator::Predicate { .. },
                LogicalOperator::Projection { .. }
                    | LogicalOperator::Grouping { .. }
                    | LogicalOperator::VectorSearch { .. },
                LogicalOperator::Materialize,
            ]
        );
        if !supported_order {
            return Err(PlanError::InvalidLogicalShape);
        }

        Ok(Self { operators })
    }

    /// Returns the logical operations in execution order.
    #[must_use]
    pub fn operators(&self) -> &[LogicalOperator] {
        &self.operators
    }
}

/// A physical operation selected from Strata's existing snapshot operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalOperator {
    /// Reads rows through the immutable manifest snapshot.
    ManifestSnapshotSource,
    /// Skips manifest-listed files or segments whose zone maps cannot match.
    ZoneMapPruning,
    /// Hides tombstoned physical rows before the result is materialized.
    TombstoneFilter,
    /// Applies the validated scalar predicate to surviving rows.
    RowFilter,
    /// Decodes only columns necessary for filtering and output.
    ColumnProjection,
    /// Executes the existing grouped aggregate operator.
    HashGroupBy,
    /// Resolves filter-matching row IDs for vector search.
    FilterLiveSet,
    /// Searches immutable manifest-listed vector segments.
    ImmutableSegmentVectorSearch,
    /// Hydrates vector hits through the same captured snapshot.
    HydrationLookup,
    /// Merges staged transaction rows into the fully decoded base read view.
    TransactionOverlay,
    /// Produces the request's typed result value.
    Materialize,
}

/// Snapshot facts available to the planner at explain time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanObservations {
    /// Number of manifest-listed row data files.
    pub data_files_total: usize,
    /// Number of row data files selected by the current statically-known physical path.
    ///
    /// Per-hit vector hydration remains dynamic at explain time, so its
    /// possible point lookups are represented by [`PhysicalOperator::HydrationLookup`]
    /// rather than an overclaimed file count here.
    pub data_files_scanned: usize,
    /// Number of row data files the current statically-known physical path proved skippable.
    pub data_files_pruned: usize,
    /// Number of manifest-listed immutable vector segments.
    pub index_segments_total: usize,
    /// Number of vector segments selected by the current physical path.
    pub index_segments_scanned: usize,
    /// Number of vector segments the current physical path proved skippable.
    pub index_segments_pruned: usize,
    /// Whether the query's caller supplied a transaction-local overlay.
    pub transaction_overlay: bool,
}

/// A stable explain representation for a logical query and its selected path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalPlan {
    /// The request's validated logical pipeline.
    pub logical_operators: Vec<LogicalOperator>,
    /// Existing snapshot operators selected to execute that pipeline.
    pub physical_operators: Vec<PhysicalOperator>,
    /// Captured pruning and cardinality observations, not cost estimates.
    pub observations: PlanObservations,
}

/// Logical-plan construction or selection failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    /// The pipeline did not start at an immutable source and end at materialization.
    InvalidLogicalShape,
    /// The request mixed result operators that have no supported combined path.
    ConflictingTerminalOperators,
}

impl Display for PlanError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLogicalShape => formatter
                .write_str("logical plans must start at a source and end at materialization"),
            Self::ConflictingTerminalOperators => formatter.write_str(
                "logical plans must contain exactly one of projection, grouping, or vector search",
            ),
        }
    }
}

impl Error for PlanError {}

/// Selects a physical path over Strata's existing snapshot operators.
pub struct Planner;

impl Planner {
    /// Produces the physical operator list and retains available observations.
    ///
    /// # Errors
    ///
    /// Returns the logical-plan validation error when a caller constructs an
    /// unsupported primitive combination.
    pub fn plan(
        logical: LogicalPlan,
        observations: PlanObservations,
    ) -> Result<PhysicalPlan, PlanError> {
        let observations = if observations.transaction_overlay {
            PlanObservations {
                data_files_total: observations.data_files_total,
                data_files_scanned: observations.data_files_total,
                data_files_pruned: 0,
                index_segments_total: observations.index_segments_total,
                index_segments_scanned: 0,
                index_segments_pruned: 0,
                transaction_overlay: true,
            }
        } else {
            observations
        };
        let has_predicate = logical
            .operators()
            .iter()
            .any(|operator| matches!(operator, LogicalOperator::Predicate { .. }));
        let zone_map_eligible = logical.operators().iter().any(|operator| {
            matches!(
                operator,
                LogicalOperator::Predicate {
                    zone_map_eligible: true
                }
            )
        });
        let mut physical_operators = Vec::new();

        match logical.operators().iter().find(|operator| {
            matches!(
                operator,
                LogicalOperator::Projection { .. }
                    | LogicalOperator::Grouping { .. }
                    | LogicalOperator::VectorSearch { .. }
            )
        }) {
            Some(LogicalOperator::Projection { .. }) => {
                physical_operators.push(PhysicalOperator::ManifestSnapshotSource);
                if observations.transaction_overlay {
                    physical_operators.push(PhysicalOperator::TombstoneFilter);
                    physical_operators.push(PhysicalOperator::TransactionOverlay);
                } else {
                    if zone_map_eligible {
                        physical_operators.push(PhysicalOperator::ZoneMapPruning);
                    }
                    physical_operators.push(PhysicalOperator::TombstoneFilter);
                }
                if has_predicate {
                    physical_operators.push(PhysicalOperator::RowFilter);
                }
                if !observations.transaction_overlay {
                    physical_operators.push(PhysicalOperator::ColumnProjection);
                }
            }
            Some(LogicalOperator::Grouping { .. }) => {
                physical_operators.push(PhysicalOperator::ManifestSnapshotSource);
                if observations.transaction_overlay {
                    physical_operators.push(PhysicalOperator::TombstoneFilter);
                    physical_operators.push(PhysicalOperator::TransactionOverlay);
                } else {
                    if zone_map_eligible {
                        physical_operators.push(PhysicalOperator::ZoneMapPruning);
                    }
                    physical_operators.push(PhysicalOperator::TombstoneFilter);
                }
                if has_predicate {
                    physical_operators.push(PhysicalOperator::RowFilter);
                }
                physical_operators.push(PhysicalOperator::HashGroupBy);
            }
            Some(LogicalOperator::VectorSearch {
                has_filter,
                hydration,
                ..
            }) => {
                if *has_filter {
                    if zone_map_eligible {
                        physical_operators.push(PhysicalOperator::ZoneMapPruning);
                    }
                    physical_operators.push(PhysicalOperator::FilterLiveSet);
                }
                physical_operators.push(PhysicalOperator::TombstoneFilter);
                physical_operators.push(PhysicalOperator::ImmutableSegmentVectorSearch);
                if *hydration {
                    physical_operators.push(PhysicalOperator::HydrationLookup);
                }
            }
            Some(
                LogicalOperator::Source
                | LogicalOperator::Predicate { .. }
                | LogicalOperator::Materialize,
            )
            | None => return Err(PlanError::InvalidLogicalShape),
        }

        physical_operators.push(PhysicalOperator::Materialize);
        Ok(PhysicalPlan {
            logical_operators: logical.operators,
            physical_operators,
            observations,
        })
    }
}
