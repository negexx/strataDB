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

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, RecordBatch, UInt64Array};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::{Field, SchemaRef};
use strata_index::LiveSet;
use strata_query::{Predicate, PredicateKey, filter, mask, should_scan_file};
use strata_storage::{DataFileEntry, Manifest, read_batch, read_batch_columns};

use crate::dataset::{ROW_ID_COLUMN, cast_batch_to_schema, data_subdir, safe_join};
use crate::error::{Result, TxnError};

use crate::live_set_cache::LiveSetCache;

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

pub struct Snapshot {
    pub(crate) dir: PathBuf,
    pub(crate) version: u64,
    pub(crate) schema: SchemaRef,
    pub(crate) manifest: Arc<Manifest>,
    pub(crate) index: strata_index::SegmentSet,
    pub(crate) tombstones: Arc<imbl::HashSet<u64>>,
    /// Per-predicate resolved-row-id cache — see
    /// `docs/analysis/2026-07-25-filtered-vector-search-memory-audit.md`
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
    /// transaction's row at all. Proven, not assumed:
    /// `dataset::loom_tests::a_reader_never_sees_one_in_flight_commits_row_while_observing_an_unrelated_commits_row_id_counter`.
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
        let data_dir = data_subdir(&self.dir);
        self.manifest
            .data_files
            .iter()
            .filter(|entry| predicate.is_none_or(|p| should_scan_file(&entry.stats, p)))
            .map(|entry| {
                let path = safe_join(&data_dir, &entry.name)?;
                let batch = match columns {
                    Some(cols) => read_batch_columns(&path, cols)?,
                    None => read_batch(&path)?,
                };
                let batch = self.filter_tombstoned_rows(batch)?;
                process(batch)
            })
            .collect()
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
        // `docs/analysis/2026-07-25-filtered-vector-search-memory-audit.md`.
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
        // `docs/analysis/2026-07-25-filtered-vector-search-memory-audit.md`.
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn test_snapshot(tombstoned: &[u64]) -> Snapshot {
        Snapshot {
            dir: PathBuf::from("unused-in-these-tests"),
            version: 1,
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
}
