//! A point-in-time, immutable view of a [`Dataset`](crate::Dataset) — see
//! `docs/superpowers/specs/2026-07-17-phase-5-mvcc-snapshot-isolation-design.md`.
//! Every field is `Copy`, `Arc`-wrapped, or (for `index: SegmentSet`) itself
//! just an `Arc<[_]>` internally, so cloning a `Snapshot` is cheap and never
//! touches the data it points to.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, UInt64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use strata_query::{Predicate, filter, mask, should_scan_file};
use strata_storage::{DataFileEntry, Manifest, read_batch, read_batch_columns};

use crate::dataset::{ROW_ID_COLUMN, cast_batch_to_schema, data_subdir, safe_join};
use crate::error::{Result, TxnError};
use crate::row_id::RowIdRange;

/// Downcasts a `SegmentSet` part's opaque zone-map payload back to the
/// concrete type `crates/txn` is the only crate that knows it really is
/// (`HashMap<String, ColumnStats>`), and applies the existing
/// `should_scan_file` evaluator — S1 W4b. `crates/index` never sees a
/// `Predicate` or a `ColumnStats`; see
/// `docs/superpowers/specs/2026-07-26-s1-w4-zone-map-design-amendment.md` §2.
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
    pub(crate) manifest: Arc<Manifest>,
    pub(crate) index: strata_index::SegmentSet,
    pub(crate) watermark: u64,
    /// Row-id ranges claimed by transactions that had not yet committed
    /// when this snapshot was published — subtracted from `watermark`'s
    /// coverage by [`Snapshot::is_visible`]. See [`crate::row_id`] for why a
    /// scalar watermark alone cannot express "committed".
    pub(crate) in_flight: Arc<[RowIdRange]>,
    pub(crate) tombstones: Arc<imbl::HashSet<u64>>,
}

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
    /// snapshot's version. No per-tombstone version needs to be stored for
    /// this to be correct — the version boundary comes from *when* a
    /// `Snapshot` was built (immediately after the commit that produced
    /// it), and tombstones are just row-ids in `Manifest.tombstones` as of
    /// that version. See the design doc's "Tombstone mechanism" section.
    ///
    /// "Committed" is `watermark` *minus* `in_flight`, not `watermark`
    /// alone. The watermark comes from the global row-id counter, which
    /// advances when a transaction *claims* its ids — before the commit
    /// lock, and so before it commits — so on its own it also covers
    /// row-ids belonging to transactions still in flight. Those are exactly
    /// the ones spec §2 says must stay invisible "until commit succeeds",
    /// and `in_flight` is what subtracts them. See [`crate::row_id`].
    ///
    /// This runs once per candidate during HNSW graph traversal, so the
    /// order of the three checks is the order of their cost: an integer
    /// compare, then a scan of a set that is empty unless another
    /// transaction was mid-commit when this snapshot was published (and
    /// holds one entry per such transaction when it is not), then the hash
    /// lookup.
    pub(crate) fn is_visible(&self, row_id: u64) -> bool {
        row_id <= self.watermark
            && !self.in_flight.iter().any(|range| range.contains(row_id))
            && !self.tombstones.contains(&row_id)
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
    /// `columns`, when `Some`, restricts the Arrow IPC read to those columns
    /// so the rest are never decoded. Callers that need whole rows pass
    /// `None`; callers that only need a couple of scalar columns out of a
    /// table carrying wide embeddings should not.
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
            self.read_surviving_files(None, None, |batch| cast_batch_to_schema(&batch, schema))?;
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
        let batches = self.read_surviving_files(Some(predicate), None, |batch| {
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

        // Not sorted: `search_filtered` builds an order-insensitive bitset
        // from these, so sorting them was pure work with no consumer.
        //
        // Measured phase split on a 100k-row, 512-dim dataset with a 1-in-10
        // predicate: `row_ids_matching` 133-157ms, `widen_ef` 9us,
        // `search_filtered` 1.3-1.8ms. This path is ~99% the cost of
        // resolving `live_ids`, and that cost is dominated by re-reading the
        // whole data file per query — see `row_ids_matching`'s own comment.
        // Zone-map pruning (S1 W4b) shrinks the fan-out/search side of that
        // split only — it does nothing for `row_ids_matching` — so it is not
        // expected to move this end-to-end split; its proof of "working" is
        // `Snapshot::explain` reporting fewer segments scanned, not a
        // wall-clock win (design amendment §6).
        let live_ids = self.row_ids_matching(predicate)?;
        let ef = widen_ef(EF_SEARCH_DEFAULT, self, predicate);
        Ok(self.index.search_filtered_pruned(
            query,
            k,
            ef,
            &live_ids,
            |id| self.is_visible(id),
            |zone_map| zone_map_permits_scan(zone_map, predicate),
        )?)
    }

    /// Resolves the row-ids of every row matching `predicate`, reading each
    /// surviving (per `should_scan_file`) file's raw on-disk batch
    /// directly — not through the public `scan_with_predicate`.
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
        // query, at ~1.5GB/s.
        //
        // Eliminating that needs one of: a per-snapshot cache of the
        // resolved row-ids (snapshots are immutable, so this is sound but is
        // a memory/latency tradeoff worth deciding deliberately), or a
        // genuinely column-chunked file format so a single column can be
        // read without its neighbours — the format change `datafile.rs`'s
        // module doc already defers. Neither is a drive-by change.
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

    fn test_snapshot(watermark: u64, tombstoned: &[u64]) -> Snapshot {
        test_snapshot_with_in_flight(watermark, tombstoned, &[])
    }

    fn test_snapshot_with_in_flight(
        watermark: u64,
        tombstoned: &[u64],
        in_flight: &[RowIdRange],
    ) -> Snapshot {
        Snapshot {
            dir: PathBuf::from("unused-in-these-tests"),
            version: 1,
            manifest: Arc::new(Manifest::empty()),
            // These tests exercise `is_visible` only — the watermark,
            // in-flight and tombstone arithmetic — and never search, so an
            // empty segment set is exactly right and avoids building an
            // index nothing queries.
            index: strata_index::SegmentSet::empty(),
            watermark,
            in_flight: in_flight.into(),
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

    #[test]
    fn in_flight_row_at_or_below_watermark_is_not_visible() {
        // The watermark comes from the global row-id counter, so it covers
        // ids claimed by transactions that have not committed. `in_flight`
        // is what keeps spec §2's "not visible until commit succeeds" true
        // for them.
        let snapshot = test_snapshot_with_in_flight(10, &[], &[RowIdRange { base: 4, len: 3 }]);
        assert!(snapshot.is_visible(3), "below the claimed range");
        assert!(!snapshot.is_visible(4), "first id of the claimed range");
        assert!(!snapshot.is_visible(6), "last id of the claimed range");
        assert!(
            snapshot.is_visible(7),
            "the range is half-open — 7 is past its end"
        );
    }

    #[test]
    fn several_concurrent_claims_are_all_excluded() {
        let snapshot = test_snapshot_with_in_flight(
            10,
            &[8],
            &[
                RowIdRange { base: 2, len: 1 },
                RowIdRange { base: 5, len: 2 },
            ],
        );
        assert!(snapshot.is_visible(0));
        assert!(!snapshot.is_visible(2));
        assert!(snapshot.is_visible(4), "between two claimed ranges");
        assert!(!snapshot.is_visible(5));
        assert!(!snapshot.is_visible(6));
        assert!(snapshot.is_visible(7));
        assert!(!snapshot.is_visible(8), "tombstones still apply");
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
        let dataset = Dataset::create(&dir).unwrap();

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
        );
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
        );
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
        // both data files must still be considered scan-worthy, since
        // `category` also appears in the row files' own per-file stats and
        // segment 0's row file also only ever holds "a".
        assert_eq!(explain.total_files, 2);
        assert_eq!(explain.scanned.len() + explain.skipped.len(), 2);

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
        let dataset = Dataset::create(&dir).unwrap();

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
        );
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
        );
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
}
