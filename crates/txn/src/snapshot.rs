//! A point-in-time, immutable view of a [`Dataset`](crate::Dataset) — see
//! `docs/superpowers/specs/2026-07-17-phase-5-mvcc-snapshot-isolation-design.md`.
//! Every field is either `Copy` or `Arc`-wrapped, so cloning a `Snapshot` is
//! cheap and never touches the data it points to.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, UInt64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use strata_index::HnswIndex;
use strata_query::{Predicate, filter, should_scan_file};
use strata_storage::{DataFileEntry, Manifest, read_batch};

use crate::dataset::{ROW_ID_COLUMN, cast_batch_to_schema, data_subdir, safe_join};
use crate::error::{Result, TxnError};

pub struct Snapshot {
    pub(crate) dir: PathBuf,
    pub(crate) version: u64,
    pub(crate) manifest: Arc<Manifest>,
    pub(crate) graph: Arc<HnswIndex>,
    pub(crate) watermark: u64,
    pub(crate) tombstones: Arc<imbl::HashSet<u64>>,
}

/// The outcome of [`Snapshot::explain`] — which files a predicate would
/// touch, without actually reading any of their bodies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainResult {
    pub total_files: usize,
    pub scanned: Vec<String>,
    pub skipped: Vec<String>,
}

// HNSW search-widening parameters — see `widen_ef`'s doc comment.
const EF_SEARCH_DEFAULT: usize = 32;
const MIN_SELECTIVITY_FLOOR: f64 = 0.01;
const MAX_EF_SCALE: f64 = 20.0;

/// Widens `base_ef` using `Snapshot::explain`'s scanned/total file ratio as
/// a coarse, file-granularity *upper bound* on selectivity — see
/// `.claude/docs/design/phase-4-vector-index-spec.md` §4. Erring toward a
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
    /// Whether `row_id` is visible under this snapshot: committed at or
    /// before this snapshot's version, and not tombstoned as of this
    /// snapshot's version. No delta-log schema change is needed for this
    /// to be correct — the version boundary comes from *when* a `Snapshot`
    /// was built (immediately after the commit that produced it), not from
    /// a stored version per tombstone entry. See the design doc's
    /// "Tombstone mechanism" section.
    pub(crate) fn is_visible(&self, row_id: u64) -> bool {
        row_id <= self.watermark && !self.tombstones.contains(&row_id)
    }

    /// Data file entries (name + per-column stats) belonging to this
    /// snapshot's version. Exposed for tests that need to inspect the raw
    /// on-disk representation directly.
    #[must_use]
    pub fn data_files(&self) -> &[DataFileEntry] {
        &self.manifest.data_files
    }

    /// Iterates `self.manifest.data_files`, keeping only entries
    /// `should_scan_file` says could match `predicate` (or every entry, if
    /// `predicate` is `None`), reads and joins each surviving file's path
    /// via [`safe_join`], and applies `process` to each raw batch. Shared
    /// by [`Snapshot::scan`], [`Snapshot::scan_with_predicate`], and
    /// [`Snapshot::row_ids_matching`].
    fn read_surviving_files<T>(
        &self,
        predicate: Option<&Predicate>,
        mut process: impl FnMut(RecordBatch) -> Result<T>,
    ) -> Result<Vec<T>> {
        let data_dir = data_subdir(&self.dir);
        self.manifest
            .data_files
            .iter()
            .filter(|entry| predicate.is_none_or(|p| should_scan_file(&entry.stats, p)))
            .map(|entry| {
                let batch = read_batch(&safe_join(&data_dir, &entry.name)?)?;
                process(batch)
            })
            .collect()
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
        let batches =
            self.read_surviving_files(None, |batch| cast_batch_to_schema(&batch, schema))?;
        Ok(concat_batches(schema, &batches)?)
    }

    /// Reports which of this snapshot's files `predicate` would require
    /// scanning, without opening any file body — pure introspection over
    /// stats already loaded in the manifest. See
    /// `.claude/docs/design/phase-3-query-refinement-spec.md` §3.
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
        ExplainResult {
            total_files: self.manifest.data_files.len(),
            scanned,
            skipped,
        }
    }

    /// Like [`Snapshot::scan`], but skips any file `predicate` provably
    /// can't match (per [`Snapshot::explain`]'s decision) and row-filters
    /// the rest.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`Snapshot::scan`],
    /// plus if `predicate`'s column doesn't exist or its value's type
    /// doesn't match the column's Arrow type.
    pub fn scan_with_predicate(
        &self,
        schema: &SchemaRef,
        predicate: &Predicate,
    ) -> Result<RecordBatch> {
        let batches = self.read_surviving_files(Some(predicate), |batch| {
            let cast = cast_batch_to_schema(&batch, schema)?;
            Ok(filter(&cast, predicate)?)
        })?;
        Ok(concat_batches(schema, &batches)?)
    }

    /// Approximate nearest-neighbor search over the vector column, as of
    /// this snapshot's version, optionally narrowed to rows matching
    /// `predicate`. Visibility (both the snapshot watermark and the
    /// tombstone set) is enforced by passing `Self::is_visible` into
    /// [`HnswIndex::search`]/[`HnswIndex::search_filtered`] — see
    /// `.claude/docs/design/phase-4-vector-index-spec.md` §3-4 and the
    /// Phase 5 design doc.
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
    /// let dataset = Dataset::create(&dir)?;
    ///
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int64, false),
    ///     Field::new(
    ///         "vector",
    ///         DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
    ///         false,
    ///     ),
    /// ]));
    /// let ids = Arc::new(Int64Array::from(vec![1, 2]));
    /// let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    /// let values = Arc::new(Float32Array::from(vec![0.0, 0.0, 0.0, 9.0, 9.0, 9.0]));
    /// let vectors = Arc::new(arrow::array::FixedSizeListArray::new(item_field, 3, values, None));
    /// let batch = RecordBatch::try_new(schema, vec![ids, vectors])?;
    ///
    /// let mut txn = dataset.begin();
    /// txn.insert(batch);
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
    /// Returns an error if `predicate` is supplied and its column doesn't
    /// exist or its value's type doesn't match the column's Arrow type, or
    /// if `query`'s dimensionality doesn't match the indexed vectors'.
    pub fn vector_search(
        &self,
        query: &[f32],
        k: usize,
        predicate: Option<&Predicate>,
    ) -> Result<Vec<strata_index::VectorMatch>> {
        let Some(predicate) = predicate else {
            return Ok(self
                .graph
                .search(query, k, EF_SEARCH_DEFAULT, |id| self.is_visible(id))?);
        };

        let mut live_ids = self.row_ids_matching(predicate)?;
        live_ids.sort_unstable();
        let ef = widen_ef(EF_SEARCH_DEFAULT, self, predicate);
        Ok(self
            .graph
            .search_filtered(query, k, ef, &live_ids, |id| self.is_visible(id))?)
    }

    /// Resolves the row-ids of every row matching `predicate`, reading each
    /// surviving (per `should_scan_file`) file's raw on-disk batch
    /// directly — not through the public `scan_with_predicate`.
    fn row_ids_matching(&self, predicate: &Predicate) -> Result<Vec<usize>> {
        let per_file_ids = self.read_surviving_files(Some(predicate), |batch| {
            let matched = filter(&batch, predicate)?;
            let row_id_idx = matched.schema_ref().index_of(ROW_ID_COLUMN)?;
            let row_ids = matched
                .column(row_id_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    TxnError::Arrow(arrow::error::ArrowError::CastError(format!(
                        "{ROW_ID_COLUMN} column must be UInt64"
                    )))
                })?;
            #[allow(clippy::cast_possible_truncation)]
            let ids: Vec<usize> = (0..row_ids.len())
                .map(|i| row_ids.value(i) as usize)
                .collect();
            Ok(ids)
        })?;
        Ok(per_file_ids.into_iter().flatten().collect())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use strata_index::{EfConstruction, MaxConnections, MaxElements, MaxLayers};

    use super::*;

    fn test_snapshot(watermark: u64, tombstoned: &[u64]) -> Snapshot {
        Snapshot {
            dir: PathBuf::from("unused-in-these-tests"),
            version: 1,
            manifest: Arc::new(Manifest::empty()),
            graph: Arc::new(
                HnswIndex::new(
                    MaxConnections(16),
                    MaxElements(100),
                    MaxLayers(16),
                    EfConstruction(200),
                )
                .unwrap(),
            ),
            watermark,
            tombstones: Arc::new(tombstoned.iter().copied().collect()),
        }
    }

    #[test]
    fn row_at_or_below_watermark_and_not_tombstoned_is_visible() {
        let snapshot = test_snapshot(10, &[]);
        assert!(snapshot.is_visible(0));
        assert!(snapshot.is_visible(10));
    }

    #[test]
    fn row_above_watermark_is_not_visible() {
        let snapshot = test_snapshot(10, &[]);
        assert!(!snapshot.is_visible(11));
    }

    #[test]
    fn tombstoned_row_at_or_below_watermark_is_not_visible() {
        let snapshot = test_snapshot(10, &[5]);
        assert!(!snapshot.is_visible(5));
        assert!(snapshot.is_visible(6));
    }
}
