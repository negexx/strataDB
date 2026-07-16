//! Single-writer transaction path for Phase 1's vertical slice. Implements
//! the commit protocol from
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §3, minus
//! §3.2's conflict check — Phase 1 has exactly one writer, so there is
//! nothing to conflict with yet. See `.claude/rules/concurrency-txn-layer.md`
//! before adding real conflict detection here; this API is shaped so Phase 6
//! can slot it in without a rewrite, but it is not implemented yet.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use strata_index::{DeltaEntry, HnswIndex, read_delta_log, write_delta_log};
use strata_query::{Predicate, filter, should_scan_file};
use strata_storage::{
    DataFileEntry, Manifest, commit_manifest, compute_stats, read_batch, read_current, write_batch,
};

use crate::error::{Result, TxnError};

/// The hidden internal row-id column every committed batch carries
/// alongside its logical columns. Exported so callers that need it back
/// (e.g. the CLI's `search` subcommand, Task 6) can request it through the
/// existing `scan`/`scan_with_predicate` API by including it in their own
/// schema, rather than needing a bespoke lookup method. See
/// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8.
pub const ROW_ID_COLUMN: &str = "_row_id";

pub struct Dataset {
    dir: PathBuf,
    manifest: Manifest,
    index: HnswIndex,
}

/// The outcome of [`Dataset::explain`] — which files a predicate would
/// touch, without actually reading any of their bodies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainResult {
    pub total_files: usize,
    pub scanned: Vec<String>,
    pub skipped: Vec<String>,
}

impl Dataset {
    /// Creates a brand-new, empty dataset at `dir`. Errors if one already
    /// exists there.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::AlreadyExists`] if a dataset already exists at
    /// `dir`, or an I/O/storage error if the directory or initial manifest
    /// can't be created.
    pub fn create(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        if read_current(&dir)?.is_some() {
            return Err(TxnError::AlreadyExists(dir));
        }
        std::fs::create_dir_all(dir.join("data"))?;
        let manifest = Manifest::empty();
        commit_manifest(&dir, &manifest)?;
        let index = new_hnsw_index(0)?;
        Ok(Self {
            dir,
            manifest,
            index,
        })
    }

    /// Opens an existing dataset, recovering to the last successfully
    /// committed version. This is the crash-recovery path: `read_current`
    /// can only ever see a fully-renamed manifest (see
    /// `strata_storage::manifest`), so a process killed mid-commit leaves
    /// this returning the *previous* version, never a torn one — the Phase 1
    /// MVP checklist's kill-9 test exercises exactly this.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::NotFound`] if no dataset exists at `dir`, or a
    /// storage error if the current manifest exists but fails to read.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        let manifest = read_current(&dir)?.ok_or_else(|| TxnError::NotFound(dir.clone()))?;
        let index = replay_index(&dir, &manifest)?;
        Ok(Self {
            dir,
            manifest,
            index,
        })
    }

    #[must_use]
    pub fn current_version(&self) -> u64 {
        self.manifest.version
    }

    #[must_use]
    pub fn data_dir(&self) -> PathBuf {
        self.dir.join("data")
    }

    /// Data file entries (name + per-column stats) belonging to the current
    /// version. Exposed for tests that need to inspect the raw on-disk
    /// representation directly.
    #[must_use]
    pub fn data_files(&self) -> &[DataFileEntry] {
        &self.manifest.data_files
    }

    /// Reads every committed row as a single `RecordBatch`, cast back to
    /// `schema` — the caller's logical schema, not necessarily the physical
    /// on-disk representation. Phase 1 has no per-fragment scan pushdown —
    /// see `crates/query` and Phase 2/3 of the roadmap for real vectorized
    /// scan.
    ///
    /// Each committed file's columns are cast to `schema`'s types before
    /// concatenation. This is required, not optional: `encode_batch`
    /// (`crates/storage::encoding`) dictionary-encodes low-cardinality
    /// columns independently per commit, based on that commit's own data —
    /// so two files belonging to the same logical column can legitimately
    /// have different physical types (e.g. one `Utf8`, another
    /// `Dictionary(Int32, Utf8)`), and `concat_batches` requires every
    /// batch to match a single schema exactly. Casting on read is what lets
    /// `scan`'s logical contract stay stable regardless of any file's
    /// physical encoding.
    ///
    /// # Errors
    ///
    /// Returns an error if any committed data file fails to read, if a
    /// column can't be cast to `schema`'s corresponding field type, or if
    /// the cast batches can't be concatenated against `schema`.
    pub fn scan(&self, schema: &SchemaRef) -> Result<RecordBatch> {
        let data_dir = self.data_dir();
        let batches = self
            .manifest
            .data_files
            .iter()
            .map(|entry| {
                let batch = read_batch(&data_dir.join(&entry.name))?;
                cast_batch_to_schema(&batch, schema)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(concat_batches(schema, &batches)?)
    }

    /// Reports which committed files `predicate` would require scanning,
    /// without opening any file body — pure introspection over stats
    /// already loaded in the manifest. See
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

    /// Like [`Dataset::scan`], but skips any file `predicate` provably
    /// can't match (per [`Dataset::explain`]'s decision) and row-filters
    /// the rest. This is the real performance path; `explain` is its
    /// introspection twin — both call the exact same
    /// `strata_query::should_scan_file`, so they can never disagree about
    /// what would be skipped.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`Dataset::scan`],
    /// plus if `predicate`'s column doesn't exist or its value's type
    /// doesn't match the column's Arrow type.
    pub fn scan_with_predicate(
        &self,
        schema: &SchemaRef,
        predicate: &Predicate,
    ) -> Result<RecordBatch> {
        let data_dir = self.data_dir();
        let batches = self
            .manifest
            .data_files
            .iter()
            .filter(|entry| should_scan_file(&entry.stats, predicate))
            .map(|entry| {
                let batch = read_batch(&data_dir.join(&entry.name))?;
                cast_batch_to_schema(&batch, schema)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let scanned = concat_batches(schema, &batches)?;
        Ok(filter(&scanned, predicate)?)
    }

    /// Approximate nearest-neighbor search over the vector column, optionally
    /// narrowed to rows matching `predicate`. See
    /// `.claude/docs/design/phase-4-vector-index-spec.md` §3-4.
    ///
    /// # Errors
    ///
    /// Returns an error if `predicate` is supplied and its column doesn't
    /// exist or its value's type doesn't match the column's Arrow type
    /// (surfaced by the same row-id resolution path `filter`/
    /// `scan_with_predicate` already use), or if `query`'s dimensionality
    /// doesn't match the indexed vectors'.
    pub fn vector_search(
        &self,
        query: &[f32],
        k: usize,
        predicate: Option<&Predicate>,
    ) -> Result<Vec<strata_index::VectorMatch>> {
        let Some(predicate) = predicate else {
            return Ok(self.index.search(query, k, EF_SEARCH_DEFAULT)?);
        };

        let mut live_ids = self.row_ids_matching(predicate)?;
        live_ids.sort_unstable();
        let ef = widen_ef(EF_SEARCH_DEFAULT, self, predicate);
        Ok(self.index.search_filtered(query, k, ef, &live_ids)?)
    }

    /// Resolves the row-ids of every row matching `predicate`, reading each
    /// surviving (per `should_scan_file`) file's raw on-disk batch directly —
    /// not through the public `scan_with_predicate`, whose caller-supplied
    /// logical schema never includes `ROW_ID_COLUMN` and would drop it (see
    /// Task 1's note on `cast_batch_to_schema`'s positional zip).
    fn row_ids_matching(&self, predicate: &Predicate) -> Result<Vec<usize>> {
        let data_dir = self.data_dir();
        let mut ids = Vec::new();
        for entry in &self.manifest.data_files {
            if !should_scan_file(&entry.stats, predicate) {
                continue;
            }
            let batch = read_batch(&data_dir.join(&entry.name))?;
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
            ids.extend((0..row_ids.len()).map(|i| row_ids.value(i) as usize));
        }
        Ok(ids)
    }

    #[must_use]
    pub fn begin(&self) -> Transaction {
        Transaction {
            dir: self.dir.clone(),
            base_manifest: self.manifest.clone(),
            pending: Vec::new(),
        }
    }
}

pub struct Transaction {
    dir: PathBuf,
    base_manifest: Manifest,
    pending: Vec<RecordBatch>,
}

impl Transaction {
    /// Buffers a batch of rows for this transaction. Nothing is visible to
    /// any other reader — including a fresh `Dataset::open` in another
    /// process — until [`Transaction::commit`] succeeds. See spec §2.
    pub fn insert(&mut self, batch: RecordBatch) {
        self.pending.push(batch);
    }

    /// Commits per spec §3, steps 3-5 (step 2's conflict check is a
    /// deliberate no-op in Phase 1 — see the module doc comment).
    ///
    /// # Errors
    ///
    /// Returns an error if any pending batch fails to dictionary-encode
    /// (see `strata_storage::encode_batch`) or write durably, or if the
    /// manifest commit's atomic rename fails.
    pub fn commit(self) -> Result<Dataset> {
        let mut manifest = self.base_manifest;
        let new_version = manifest.version + 1;
        let data_dir = self.dir.join("data");
        std::fs::create_dir_all(&data_dir)?;

        for (i, batch) in self.pending.iter().enumerate() {
            // Stats computed on the original, pre-encoding, pre-row-id batch — see
            // .claude/docs/design/phase-3-query-refinement-spec.md §1 for why
            // (logical values, no dictionary-decode step needed later; _row_id is
            // an internal column, not a user column subject to file-pruning stats).
            let stats = compute_stats(batch);

            let num_rows = u64::try_from(batch.num_rows())?;
            let row_id_base = manifest.next_row_id;
            manifest.next_row_id += num_rows;
            let with_row_id = append_row_id_column(batch, row_id_base, num_rows)?;

            let encoded = strata_storage::encode_batch(&with_row_id)?;
            let file_name = format!("{new_version:020}-{i}.arrow");
            write_batch(&data_dir.join(&file_name), &encoded)?;

            let deltas = build_delta_entries(batch, row_id_base)?;
            let delta_file_name = format!("{new_version:020}-{i}.deltalog");
            write_delta_log(&data_dir.join(&delta_file_name), &deltas)?;

            manifest.data_files.push(DataFileEntry {
                name: file_name,
                stats,
                delta_log: delta_file_name,
            });
        }
        manifest.version = new_version;

        // Fsyncing each data file's *content* (already done inside
        // write_batch) is not sufficient — the new directory entries
        // themselves must also be fsynced, or a real power-loss crash can
        // leave a file's bytes durable while the file itself is absent.
        // Must happen before the manifest commit below, which is what makes
        // these files visible to a future reader.
        strata_storage::sync_dir(&data_dir)?;

        commit_manifest(&self.dir, &manifest)?;

        // Rebuilt from the just-committed manifest's full delta-log history
        // (the same path `Dataset::open` uses), not carried over from the
        // base `Dataset` the transaction started from — this is what
        // guarantees the returned `Dataset`'s index can never diverge from
        // what a fresh `open()` of the same directory would produce.
        let index = replay_index(&self.dir, &manifest)?;

        Ok(Dataset {
            dir: self.dir,
            manifest,
            index,
        })
    }
}

// HNSW parameter defaults — small, correctness-only values for now.
// Task 7's benchmark is what tunes the real production defaults; see
// .claude/rules/vector-index.md ("tuned via benchmarks, not guessed").
const HNSW_MAX_NB_CONNECTION: usize = 16;
const HNSW_MAX_LAYER: usize = 16;
const HNSW_EF_CONSTRUCTION: usize = 200;
const EF_SEARCH_DEFAULT: usize = 64;
const MIN_SELECTIVITY_FLOOR: f64 = 0.01;
const MAX_EF_SCALE: f64 = 20.0;

/// Widens `base_ef` using `Dataset::explain`'s scanned/total file ratio as
/// a coarse, file-granularity *upper bound* on selectivity — see
/// `.claude/docs/design/phase-4-vector-index-spec.md` §4. Erring toward a
/// wider `ef` costs search time, never correctness, so an overestimate of
/// how many rows survive is the safe direction.
fn widen_ef(base_ef: usize, dataset: &Dataset, predicate: &Predicate) -> usize {
    let explain = dataset.explain(predicate);
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

fn new_hnsw_index(capacity: usize) -> Result<HnswIndex> {
    Ok(HnswIndex::new(
        HNSW_MAX_NB_CONNECTION,
        capacity.max(1),
        HNSW_MAX_LAYER,
        HNSW_EF_CONSTRUCTION,
    )?)
}

/// Rebuilds a fresh `HnswIndex` by replaying every delta-log entry across
/// every committed data file in `manifest`, in order. Used by both
/// `Dataset::open` (crash recovery) and `Transaction::commit` (so a
/// newly-committed `Dataset`'s index can never diverge from what a fresh
/// `open()` of the same directory would produce) — see
/// `.claude/rules/vector-index.md` ("index lives inside the same
/// transaction boundary as row data").
///
/// # Errors
///
/// Returns an error if any delta-log file listed in `manifest` fails to
/// read or parse.
fn replay_index(dir: &Path, manifest: &Manifest) -> Result<HnswIndex> {
    let capacity = usize::try_from(manifest.next_row_id).unwrap_or(usize::MAX);
    let mut index = new_hnsw_index(capacity)?;
    let data_dir = dir.join("data");
    for entry in &manifest.data_files {
        for delta in read_delta_log(&data_dir.join(&entry.delta_log))? {
            match delta {
                DeltaEntry::Insert { row_id, vector } => index.insert(row_id, &vector),
                DeltaEntry::Tombstone { row_id } => index.tombstone(row_id),
            }
        }
    }
    Ok(index)
}

/// Builds one `Insert` delta-log entry per row in `batch` with a non-null
/// vector, keyed by the row-ids assigned starting at `row_id_base` — see
/// `.claude/docs/design/phase-4-vector-index-spec.md` §2. A `batch` with no
/// `"vector"` column at all (a table with no vector column defined) simply
/// produces no entries — that's not an error, unlike a `"vector"` column
/// present with the wrong type, which is.
///
/// # Errors
///
/// Returns an error if `batch` has a `"vector"` column that isn't a
/// `FixedSizeList<Float32>`.
fn build_delta_entries(batch: &RecordBatch, row_id_base: u64) -> Result<Vec<DeltaEntry>> {
    let Ok(vec_idx) = batch.schema_ref().index_of("vector") else {
        return Ok(Vec::new());
    };
    let vectors = batch
        .column(vec_idx)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeListArray>()
        .ok_or_else(|| {
            TxnError::Arrow(arrow::error::ArrowError::CastError(
                "vector column must be FixedSizeList".to_string(),
            ))
        })?;

    let mut entries = Vec::with_capacity(vectors.len());
    for i in 0..vectors.len() {
        if vectors.is_null(i) {
            continue;
        }
        let row = vectors.value(i);
        let row: &arrow::array::Float32Array = row.as_any().downcast_ref().ok_or_else(|| {
            TxnError::Arrow(arrow::error::ArrowError::CastError(
                "vector column's inner type must be Float32".to_string(),
            ))
        })?;
        entries.push(DeltaEntry::Insert {
            row_id: row_id_base + u64::try_from(i)?,
            vector: row.values().to_vec(),
        });
    }
    Ok(entries)
}

/// Casts every column of `batch` to the corresponding field type in
/// `schema`, leaving already-matching columns untouched (a cheap `Arc`
/// clone, not a copy). See [`Dataset::scan`]'s doc comment for why this is
/// necessary rather than a defensive nicety.
fn cast_batch_to_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns: std::result::Result<Vec<ArrayRef>, arrow::error::ArrowError> = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column.as_ref(), field.data_type())
            }
        })
        .collect();
    Ok(RecordBatch::try_new(Arc::clone(schema), columns?)?)
}

/// Appends a `_row_id: UInt64` column to `batch`, assigning
/// `row_id_base..row_id_base + num_rows` in row order. This is what makes
/// every committed row addressable by a stable, global identity — see
/// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8.
fn append_row_id_column(
    batch: &RecordBatch,
    row_id_base: u64,
    num_rows: u64,
) -> Result<RecordBatch> {
    let row_ids: Vec<u64> = (0..num_rows).map(|i| row_id_base + i).collect();
    let row_id_array: ArrayRef = Arc::new(UInt64Array::from(row_ids));

    let mut fields: Vec<Field> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(ROW_ID_COLUMN, DataType::UInt64, false));

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(row_id_array);

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("strata-txn-test-{label}-{}", std::process::id()))
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    #[test]
    fn scan_succeeds_on_a_dictionary_encoded_low_cardinality_column() {
        // Regression test: found by the Phase 2 whole-branch review.
        // encode_batch dictionary-encodes low-cardinality columns
        // (crates/storage::encoding) before write_batch, but scan() used to
        // pass the caller's original logical schema straight into
        // concat_batches — which rejects any batch whose physical column
        // type doesn't exactly match. A 100-row, 2-distinct-value batch
        // (well under the 0.4 encoding threshold) reproduced this
        // deterministically: scan() returned
        // Err(InvalidArgumentError("expected Utf8 but found
        // Dictionary(Int32, Utf8)")) for every realistic low-cardinality
        // dataset. Fixed by cast_batch_to_schema casting each file's
        // columns back to the logical schema before concatenation.
        use arrow::array::StringArray;
        let dir = temp_dir("scan-dict-encoded");
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let names: Vec<&str> = (0..100)
            .map(|i| if i % 2 == 0 { "alice" } else { "bob" })
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(names.clone()))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();

        // Confirm the file really was dictionary-encoded, so this test
        // can't silently stop testing the regression it exists to catch.
        let on_disk = read_batch(&ds.data_dir().join(&ds.data_files()[0].name)).unwrap();
        assert!(
            matches!(
                on_disk.schema_ref().field(0).data_type(),
                DataType::Dictionary(_, _)
            ),
            "test data must actually trigger dictionary encoding to be a valid regression test"
        );

        let scanned = ds.scan(&schema).unwrap();
        assert_eq!(scanned.schema_ref().field(0).data_type(), &DataType::Utf8);
        let scanned_names = scanned
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..scanned.num_rows())
            .map(|i| scanned_names.value(i))
            .collect();
        assert_eq!(got, names);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn create_then_open_recovers_same_version() {
        let dir = temp_dir("create-open");
        let ds = Dataset::create(&dir).unwrap();
        assert_eq!(ds.current_version(), 0);

        let reopened = Dataset::open(&dir).unwrap();
        assert_eq!(reopened.current_version(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn insert_then_commit_then_scan_round_trips() {
        let dir = temp_dir("insert-scan");
        let schema = test_schema();
        let ds = Dataset::create(&dir).unwrap();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch.clone());
        let ds = txn.commit().unwrap();

        assert_eq!(ds.current_version(), 1);
        let scanned = ds.scan(&schema).unwrap();
        assert_eq!(scanned, batch);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn create_twice_errors() {
        let dir = temp_dir("create-twice");
        let _ds = Dataset::create(&dir).unwrap();
        let result = Dataset::create(&dir);
        assert!(matches!(result, Err(TxnError::AlreadyExists(_))));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_computes_and_stores_column_stats() {
        let dir = temp_dir("commit-stats");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![30, 10, 20]))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();

        let entry = &ds.data_files()[0];
        let id_stats = entry.stats.get("id").unwrap();
        assert_eq!(id_stats.min, strata_storage::Value::Int64(10));
        assert_eq!(id_stats.max, strata_storage::Value::Int64(30));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn low_cardinality_column_is_dictionary_encoded_on_commit() {
        use arrow::array::StringArray;
        use arrow::datatypes::DataType;

        let dir = temp_dir("encode-on-commit");
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let names: Vec<&str> = vec!["x"; 20]; // single distinct value, well under threshold
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();

        // Read the raw written file back directly (bypassing Dataset::scan's
        // concat_batches, which would already show us the encoded type, but
        // reading the file directly proves the *durable* representation is
        // encoded, not just an in-memory artifact).
        let data_dir = ds.data_dir();
        let file_name = &ds.data_files()[0].name;
        let on_disk = strata_storage::read_batch(&data_dir.join(file_name)).unwrap();
        assert!(matches!(
            on_disk.schema_ref().field(0).data_type(),
            DataType::Dictionary(_, _)
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn explain_reports_skipped_files_by_range() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("explain-skip");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir).unwrap();

        // Two commits, disjoint id ranges -> two files with non-overlapping stats.
        let low = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(low);
        let ds = txn.commit().unwrap();

        let high = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![100, 101, 102]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(high);
        let ds = txn.commit().unwrap();

        let predicate = Predicate::Eq("id".to_string(), Value::Int64(2));
        let result = ds.explain(&predicate);

        assert_eq!(result.total_files, 2);
        assert_eq!(
            result.scanned.len(),
            1,
            "only the [1,3] file could match id=2"
        );
        assert_eq!(
            result.skipped.len(),
            1,
            "the [100,102] file must be skipped"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn row_ids_are_assigned_sequentially_and_monotonically_across_commits() {
        let dir = temp_dir("row-id-sequential");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let first = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(first);
        let ds = txn.commit().unwrap();

        let second =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![40, 50]))]).unwrap();
        let mut txn = ds.begin();
        txn.insert(second);
        let ds = txn.commit().unwrap();

        let data_dir = ds.data_dir();
        let first_on_disk = read_batch(&data_dir.join(&ds.data_files()[0].name)).unwrap();
        let second_on_disk = read_batch(&data_dir.join(&ds.data_files()[1].name)).unwrap();

        let row_id_col = |batch: &RecordBatch| -> Vec<u64> {
            let idx = batch.schema_ref().index_of(ROW_ID_COLUMN).unwrap();
            let arr = batch
                .column(idx)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            (0..arr.len()).map(|i| arr.value(i)).collect()
        };

        assert_eq!(row_id_col(&first_on_disk), vec![0, 1, 2]);
        assert_eq!(row_id_col(&second_on_disk), vec![3, 4]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn row_id_column_never_leaks_into_scan_output() {
        let dir = temp_dir("row-id-hidden");
        let schema = test_schema(); // just "id", no _row_id
        let ds = Dataset::create(&dir).unwrap();

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();

        let scanned = ds.scan(&schema).unwrap();
        assert_eq!(
            scanned.schema_ref().fields().len(),
            1,
            "_row_id must not appear in scan() output when the caller's schema doesn't ask for it"
        );
        assert!(scanned.schema_ref().index_of(ROW_ID_COLUMN).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_with_predicate_returns_only_matching_rows_from_unskipped_files() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("scan-with-predicate");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let low = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(low);
        let ds = txn.commit().unwrap();

        let high = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![100, 101, 102]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(high);
        let ds = txn.commit().unwrap();

        let predicate = Predicate::Eq("id".to_string(), Value::Int64(2));
        let result = ds.scan_with_predicate(&schema, &predicate).unwrap();

        assert_eq!(result.num_rows(), 1);
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    fn vector_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]))
    }

    #[allow(clippy::needless_pass_by_value)]
    fn vector_batch(ids: Vec<i64>, vectors: Vec<[f32; 3]>) -> RecordBatch {
        let id_arr = Arc::new(Int64Array::from(ids));
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let flat: Vec<f32> = vectors.iter().flatten().copied().collect();
        let values = Arc::new(arrow::array::Float32Array::from(flat));
        let vec_arr = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field, 3, values, None,
        ));
        RecordBatch::try_new(vector_test_schema(), vec![id_arr, vec_arr]).unwrap()
    }

    #[test]
    fn vector_search_without_predicate_finds_the_true_nearest_neighbor() {
        let dir = temp_dir("vector-search-unfiltered");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(
            vec![1, 2, 3],
            vec![[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [10.0, 10.0, 10.0]],
        );
        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();

        let results = ds.vector_search(&[0.0, 0.0, 0.0], 1, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_id, 0); // row-id 0 is the first committed row (id=1)

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn vector_search_with_predicate_only_returns_matching_rows() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("vector-search-filtered");
        let ds = Dataset::create(&dir).unwrap();

        // Two vectors close to the query; only id=2's row should survive the
        // predicate `id eq 2`, even though id=1's vector is the true nearest
        // neighbor of the query.
        let batch = vector_batch(vec![1, 2], vec![[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]]);
        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();

        let predicate = Predicate::Eq("id".to_string(), Value::Int64(2));
        let results = ds
            .vector_search(&[0.0, 0.0, 0.0], 5, Some(&predicate))
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_id, 1); // row-id 1 is the second committed row (id=2)

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reopening_a_dataset_rebuilds_the_vector_index_from_the_delta_log() {
        let dir = temp_dir("delta-log-replay");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));
        let ds = Dataset::create(&dir).unwrap();

        let ids = Arc::new(Int64Array::from(vec![1, 2]));
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let values = Arc::new(arrow::array::Float32Array::from(vec![
            0.0, 0.0, 0.0, // row 0's vector
            9.0, 9.0, 9.0, // row 1's vector
        ]));
        let vectors = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field, 3, values, None,
        ));
        let batch = RecordBatch::try_new(schema, vec![ids, vectors]).unwrap();

        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();
        drop(ds);

        // Force a real replay from disk, not an in-memory shortcut — this is
        // the crash-recovery-equivalent test for the index (a fresh Dataset
        // struct, same process, but the index cache is definitely rebuilt from
        // the delta-log file, not carried over).
        let reopened = Dataset::open(&dir).unwrap();
        let results = reopened.vector_search(&[0.0, 0.0, 0.0], 1, None).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].row_id, 0,
            "row 0's vector [0,0,0] is the true nearest match"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
