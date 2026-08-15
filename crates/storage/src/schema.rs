//! Versioned dataset schema catalog and the deliberately narrow migration
//! descriptors the durable manifest can publish.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::error::{Result, StorageError};

/// Schema catalog version assigned to every newly created dataset.
pub const INITIAL_SCHEMA_VERSION: u32 = 1;
/// The only currently supported forward catalog transition.
pub const ADD_NULLABLE_COLUMN_SCHEMA_VERSION: u32 = 2;

/// An explicitly requested deterministic dataset schema transformation.
#[derive(Clone, Debug)]
pub enum SchemaMigration {
    /// Adds one nullable logical column and initializes every existing row to
    /// null. This is the smallest evolution that never needs a value cast.
    AddNullableColumn {
        source_version: u32,
        target_version: u32,
        column: Field,
    },
    /// A requested type change. The catalog deliberately rejects this rather
    /// than choosing an implicit conversion that could lose values.
    ChangeColumnType {
        source_version: u32,
        target_version: u32,
        column: String,
        target_type: DataType,
    },
}

/// Stable result details for a successfully published schema migration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaMigrationResult {
    pub name: &'static str,
    pub source_schema_version: u32,
    pub target_schema_version: u32,
    pub manifest_version: u64,
}

impl SchemaMigration {
    #[must_use]
    pub fn add_nullable_column(source_version: u32, target_version: u32, column: Field) -> Self {
        Self::AddNullableColumn {
            source_version,
            target_version,
            column,
        }
    }

    #[must_use]
    pub fn change_column_type(
        source_version: u32,
        target_version: u32,
        column: impl Into<String>,
        target_type: DataType,
    ) -> Self {
        Self::ChangeColumnType {
            source_version,
            target_version,
            column: column.into(),
            target_type,
        }
    }

    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::AddNullableColumn { .. } => "add_nullable_column",
            Self::ChangeColumnType { .. } => "change_column_type",
        }
    }

    #[must_use]
    pub fn source_version(&self) -> u32 {
        match self {
            Self::AddNullableColumn { source_version, .. }
            | Self::ChangeColumnType { source_version, .. } => *source_version,
        }
    }

    #[must_use]
    pub fn target_version(&self) -> u32 {
        match self {
            Self::AddNullableColumn { target_version, .. }
            | Self::ChangeColumnType { target_version, .. } => *target_version,
        }
    }

    #[must_use]
    pub fn added_nullable_column(&self) -> Option<&Field> {
        match self {
            Self::AddNullableColumn { column, .. } => Some(column),
            Self::ChangeColumnType { .. } => None,
        }
    }

    /// Validates this request against `current_schema` and returns its target
    /// schema without writing any durable objects.
    ///
    /// # Errors
    ///
    /// Returns a typed error for an unknown, stale, reverse, unsupported,
    /// incompatible, or lossy migration request.
    pub fn target_schema(
        &self,
        current_version: u32,
        current_schema: &SchemaRef,
    ) -> Result<SchemaRef> {
        validate_schema_version(current_version, None)?;
        if self.source_version() != current_version {
            return Err(StorageError::MigrationSourceVersion {
                expected: current_version,
                actual: self.source_version(),
            });
        }
        if self.target_version() <= self.source_version() {
            return Err(StorageError::MigrationUnsupportedDirection {
                from_version: self.source_version(),
                target: self.target_version(),
            });
        }

        match self {
            Self::AddNullableColumn {
                source_version,
                target_version,
                column,
            } => {
                if *source_version != INITIAL_SCHEMA_VERSION
                    || *target_version != ADD_NULLABLE_COLUMN_SCHEMA_VERSION
                {
                    return Err(StorageError::MigrationUnsupported {
                        name: self.name(),
                        from_version: *source_version,
                        target: *target_version,
                    });
                }
                if !column.is_nullable() {
                    return Err(StorageError::MigrationIncompatibleType {
                        detail: format!(
                            "new column {:?} is required, but existing rows have no value",
                            column.name()
                        ),
                    });
                }
                if current_schema
                    .fields()
                    .iter()
                    .any(|field| field.name() == column.name())
                {
                    return Err(StorageError::MigrationIncompatibleType {
                        detail: format!("column {:?} already exists", column.name()),
                    });
                }
                let mut fields = current_schema.fields().to_vec();
                fields.push(Arc::new(column.clone()));
                Ok(Arc::new(Schema::new_with_metadata(
                    fields,
                    current_schema.metadata().clone(),
                )))
            }
            Self::ChangeColumnType {
                column,
                target_type,
                ..
            } => Err(StorageError::MigrationLossyConversion {
                detail: format!(
                    "implicit conversion of column {column:?} to {target_type:?} is not supported"
                ),
            }),
        }
    }

    #[must_use]
    pub fn result(&self, manifest_version: u64) -> SchemaMigrationResult {
        SchemaMigrationResult {
            name: self.name(),
            source_schema_version: self.source_version(),
            target_schema_version: self.target_version(),
            manifest_version,
        }
    }
}

/// Rejects catalog versions that this binary cannot interpret.
///
/// # Errors
///
/// Returns [`StorageError::UnknownSchemaVersion`] when `version` is not a
/// catalog version supported by this binary.
pub fn validate_schema_version(version: u32, path: Option<&std::path::Path>) -> Result<()> {
    if matches!(
        version,
        INITIAL_SCHEMA_VERSION | ADD_NULLABLE_COLUMN_SCHEMA_VERSION
    ) {
        return Ok(());
    }
    Err(StorageError::UnknownSchemaVersion {
        version,
        path: path.map_or_else(std::path::PathBuf::new, std::path::Path::to_path_buf),
    })
}
