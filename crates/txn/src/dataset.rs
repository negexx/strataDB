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

/// The single source of truth for "where does this dataset's data live,
/// relative to its root directory" — used by `Dataset::data_dir`,
/// `Transaction::commit`, and `replay_index`, which each need it from a
/// different type/context (a `&Dataset`, a `Transaction`, and a bare
/// `&Path` respectively) and previously each hardcoded `dir.join("data")`
/// independently.
fn data_subdir(dir: &Path) -> PathBuf {
    dir.join("data")
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
        data_subdir(&self.dir)
    }

    /// Data file entries (name + per-column stats) belonging to the current
    /// version. Exposed for tests that need to inspect the raw on-disk
    /// representation directly.
    #[must_use]
    pub fn data_files(&self) -> &[DataFileEntry] {
        &self.manifest.data_files
    }

    /// Iterates `self.manifest.data_files`, keeping only entries
    /// `should_scan_file` says could match `predicate` (or every entry, if
    /// `predicate` is `None`), reads and joins each surviving file's path
    /// via [`safe_join`], and applies `process` to each raw batch. Shared
    /// by [`Dataset::scan`], [`Dataset::scan_with_predicate`], and
    /// [`Dataset::row_ids_matching`] — previously each re-implemented this
    /// same "iterate -> prune -> read -> process" skeleton independently.
    fn read_surviving_files<T>(
        &self,
        predicate: Option<&Predicate>,
        mut process: impl FnMut(RecordBatch) -> Result<T>,
    ) -> Result<Vec<T>> {
        let data_dir = self.data_dir();
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
        let batches =
            self.read_surviving_files(None, |batch| cast_batch_to_schema(&batch, schema))?;
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
        let batches = self.read_surviving_files(Some(predicate), |batch| {
            let cast = cast_batch_to_schema(&batch, schema)?;
            Ok(filter(&cast, predicate)?)
        })?;
        Ok(concat_batches(schema, &batches)?)
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
    /// Returns [`TxnError::NonFiniteVectorComponent`] if any pending batch's
    /// vector column contains a `NaN`/`Infinity` component — checked, and
    /// rejected, before any file for that batch is written to disk. Also
    /// returns an error if any pending batch fails to dictionary-encode (see
    /// `strata_storage::encode_batch`) or write durably, if rebuilding the
    /// vector index from the (already-written) delta-log history fails, or
    /// if the manifest commit's atomic rename fails. The index rebuild runs
    /// before the manifest commit, so any of these failures leaves the
    /// manifest unadvanced — the new data/delta-log files are orphaned on
    /// disk but never made visible, the same as an interrupted crash.
    pub fn commit(self) -> Result<Dataset> {
        let mut manifest = self.base_manifest;
        let new_version = manifest.version.checked_add(1).ok_or_else(|| {
            TxnError::ManifestOverflow(format!("version {} + 1", manifest.version))
        })?;
        let data_dir = data_subdir(&self.dir);
        std::fs::create_dir_all(&data_dir)?;

        Self::write_pending_batches(&self.pending, &data_dir, new_version, &mut manifest)?;
        manifest.version = new_version;

        // Fsyncing each data file's *content* (already done inside
        // write_batch) is not sufficient — the new directory entries
        // themselves must also be fsynced, or a real power-loss crash can
        // leave a file's bytes durable while the file itself is absent.
        // Must happen before the index rebuild/manifest commit below.
        strata_storage::sync_dir(&data_dir)?;

        // Rebuilt from `manifest`'s full delta-log history (the same path
        // `Dataset::open` uses) — see `replay_index`'s doc comment for why
        // this must run before `commit_manifest`.
        let index = replay_index(&self.dir, &manifest)?;

        commit_manifest(&self.dir, &manifest)?;

        Ok(Dataset {
            dir: self.dir,
            manifest,
            index,
        })
    }

    /// Writes every pending batch's data file and delta-log file to
    /// `data_dir`, assigning row-ids and advancing `manifest.next_row_id`/
    /// `manifest.data_files` in place. Extracted from [`Transaction::commit`]
    /// so the per-batch write step and the outer commit-protocol steps
    /// (sync, index rebuild, manifest CAS) read as named phases rather than
    /// one long function — see `.claude/rules/concurrency-txn-layer.md`'s
    /// note on the commit protocol's load-bearing ordering.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`Transaction::commit`]'s
    /// own doc comment (dictionary-encoding failure, non-finite vector
    /// component, I/O failure writing a data/delta-log file, or a
    /// [`TxnError::ManifestOverflow`] if row-id assignment would overflow).
    fn write_pending_batches(
        pending: &[RecordBatch],
        data_dir: &Path,
        new_version: u64,
        manifest: &mut Manifest,
    ) -> Result<()> {
        for (i, batch) in pending.iter().enumerate() {
            // Stats computed on the original, pre-encoding, pre-row-id batch — see
            // .claude/docs/design/phase-3-query-refinement-spec.md §1 for why
            // (logical values, no dictionary-decode step needed later; _row_id is
            // an internal column, not a user column subject to file-pruning stats).
            let stats = compute_stats(batch);

            let num_rows = u64::try_from(batch.num_rows())?;
            let row_id_base = manifest.next_row_id;

            // Extracts (and validates — rejects non-finite vector
            // components) this batch's delta-log entries before anything is
            // written to disk for it. A batch that fails validation here
            // must leave no trace: no data file, no delta-log file, and
            // manifest.next_row_id must not have been advanced yet either.
            let deltas = build_delta_entries(batch, row_id_base)?;
            manifest.next_row_id = manifest.next_row_id.checked_add(num_rows).ok_or_else(|| {
                TxnError::ManifestOverflow(format!(
                    "next_row_id {} + {num_rows}",
                    manifest.next_row_id
                ))
            })?;
            let with_row_id = append_row_id_column(batch, row_id_base, num_rows)?;

            let encoded = strata_storage::encode_batch(&with_row_id)?;
            let file_name = format!("{new_version:020}-{i}.arrow");
            write_batch(&data_dir.join(&file_name), &encoded)?;

            let delta_file_name = format!("{new_version:020}-{i}.deltalog");
            write_delta_log(&data_dir.join(&delta_file_name), &deltas)?;

            manifest.data_files.push(DataFileEntry {
                name: file_name,
                stats,
                delta_log: delta_file_name,
            });
        }
        Ok(())
    }
}

// HNSW parameter defaults — small, correctness-only values for now.
// Task 7's benchmark is what tunes the real production defaults; see
// .claude/rules/vector-index.md ("tuned via benchmarks, not guessed").
const HNSW_MAX_NB_CONNECTION: usize = 16;
const HNSW_MAX_LAYER: usize = 16;
const HNSW_EF_CONSTRUCTION: usize = 200;
const EF_SEARCH_DEFAULT: usize = 32;
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

/// Sane ceiling for a manifest's `next_row_id` before it's used to size an
/// eager HNSW allocation. `hnsw_rs::Hnsw::new`'s `max_elements` parameter
/// drives a `Vec::with_capacity` sized proportionally to it (verified
/// against the installed `hnsw_rs-0.3.4` source) — an unvalidated,
/// manifest-controlled value near `u64::MAX` would attempt an
/// unreasonably large allocation on open of a corrupted/hostile dataset
/// instead of returning a typed error. One billion rows is far beyond any
/// realistic embedded dataset today; revisit if a real workload needs more.
const MAX_REASONABLE_ROW_ID_CAPACITY: u64 = 1_000_000_000;

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
/// read or parse, or if `manifest.next_row_id` exceeds
/// [`MAX_REASONABLE_ROW_ID_CAPACITY`].
fn replay_index(dir: &Path, manifest: &Manifest) -> Result<HnswIndex> {
    if manifest.next_row_id > MAX_REASONABLE_ROW_ID_CAPACITY {
        return Err(TxnError::UnreasonableCapacity(
            manifest.next_row_id,
            MAX_REASONABLE_ROW_ID_CAPACITY,
        ));
    }
    let capacity = usize::try_from(manifest.next_row_id).unwrap_or(usize::MAX);
    let mut index = new_hnsw_index(capacity)?;
    let data_dir = data_subdir(dir);
    for entry in &manifest.data_files {
        for delta in read_delta_log(&safe_join(&data_dir, &entry.delta_log)?)? {
            match delta {
                DeltaEntry::Insert { row_id, vector } => index.insert(row_id, &vector)?,
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
/// Also rejects any row whose vector contains a non-finite (`NaN`/`Infinity`)
/// component: the delta log is serialized as JSON (`serde_json`), which
/// silently encodes non-finite `f32`s as `null` and then fails to parse them
/// back — letting one through here would durably commit a row that
/// permanently breaks every future `replay_index` (including the very one
/// `Transaction::commit` runs on its own return path). Must run before any
/// file for this batch is written to disk — see the call site in
/// `Transaction::commit`.
///
/// # Errors
///
/// Returns an error if `batch` has a `"vector"` column that isn't a
/// `FixedSizeList<Float32>`, or if any row's vector contains a non-finite
/// component.
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
        let row_id = row_id_base.checked_add(u64::try_from(i)?).ok_or_else(|| {
            TxnError::ManifestOverflow(format!("row_id_base {row_id_base} + {i}"))
        })?;
        if row.values().iter().any(|component| !component.is_finite()) {
            return Err(TxnError::NonFiniteVectorComponent { row_id });
        }
        entries.push(DeltaEntry::Insert {
            row_id,
            vector: row.values().to_vec(),
        });
    }
    Ok(entries)
}

/// Joins `name` onto `data_dir`, rejecting any `name` whose path
/// components aren't all bare filename segments (`Component::Normal`) — a
/// `name` containing `..` or an absolute path (which `Path::join` would
/// otherwise resolve/replace unchecked) must never let a corrupted/hostile
/// manifest read a file outside the dataset's own `data/` directory.
/// `DataFileEntry.name`/`.delta_log` are documented as "relative to the
/// dataset's data/ directory" (`crates/storage/src/manifest.rs`) — this is
/// what actually enforces that contract instead of merely documenting it.
fn safe_join(data_dir: &Path, name: &str) -> Result<PathBuf> {
    let candidate = Path::new(name);
    let all_normal = candidate
        .components()
        .all(|c| matches!(c, std::path::Component::Normal(_)));
    if !all_normal {
        return Err(TxnError::UnsafeManifestPath(name.to_string()));
    }
    Ok(data_dir.join(candidate))
}

/// Casts every column of `batch` to the corresponding field type in
/// `schema`, leaving already-matching columns untouched (a cheap `Arc`
/// clone, not a copy). See [`Dataset::scan`]'s doc comment for why this is
/// necessary rather than a defensive nicety.
///
/// Every committed file physically carries a trailing hidden
/// [`ROW_ID_COLUMN`] (see `append_row_id_column`). It only counts toward
/// the batch's *logical* width when the caller's `schema` explicitly
/// requests it (as the CLI's `search` subcommand does) — otherwise the
/// positional zip below deliberately drops it. Any other width
/// disagreement is an error: without the up-front check, `Iterator::zip`
/// would silently truncate to the shorter side — dropping real columns, or
/// worse, pairing the hidden row-id column with a caller field and casting
/// row-ids into the caller's data.
///
/// # Errors
///
/// Returns [`TxnError::SchemaMismatch`] if `schema`'s field count doesn't
/// match `batch`'s logical column count, or an Arrow error if a column
/// can't be cast to its corresponding field's type.
fn cast_batch_to_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let physical = batch.num_columns();
    let hidden_row_id = batch.schema_ref().index_of(ROW_ID_COLUMN).is_ok()
        && schema.index_of(ROW_ID_COLUMN).is_err();
    let logical = if hidden_row_id {
        physical.saturating_sub(1)
    } else {
        physical
    };
    if logical != schema.fields().len() {
        return Err(TxnError::SchemaMismatch {
            expected: schema.fields().len(),
            actual: logical,
        });
    }
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

    // NOTE (Batch 1, Task 2): the plan also specified a sibling test,
    // `commit_errors_instead_of_overflowing_when_next_row_id_would_wrap`,
    // crafting a hostile manifest with `next_row_id: u64::MAX - 1`. Task 2
    // deferred it because `Dataset::open` -> `replay_index` panicked
    // ("capacity overflow") on such a manifest before `commit` ever ran.
    // Resolved by Batch 1, Task 4: `replay_index` now rejects any manifest
    // whose `next_row_id` exceeds `MAX_REASONABLE_ROW_ID_CAPACITY` with a
    // typed `TxnError::UnreasonableCapacity` at open — covered by
    // `open_errors_instead_of_attempting_a_huge_allocation_on_an_unreasonable_next_row_id`
    // below. The capacity ceiling makes a near-`u64::MAX` `next_row_id`
    // unreachable through `open`, so the originally-specified commit-time
    // wrap test is intentionally subsumed by the open-time guard test.

    #[test]
    fn open_errors_instead_of_attempting_a_huge_allocation_on_an_unreasonable_next_row_id() {
        let dir = temp_dir("unreasonable-capacity");
        let hostile = Manifest {
            version: 0,
            data_files: Vec::new(),
            next_row_id: u64::MAX,
        };
        strata_storage::commit_manifest(&dir, &hostile).unwrap();

        let result = Dataset::open(&dir);
        // `Dataset` doesn't implement `Debug` (its HNSW index can't), so
        // only the `Err` side is printable on failure.
        assert!(
            matches!(result, Err(TxnError::UnreasonableCapacity(_, _))),
            "expected UnreasonableCapacity, got {:?}",
            result.err()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_errors_instead_of_overflowing_when_version_would_wrap() {
        let dir = temp_dir("version-overflow");
        // Craft a manifest whose version sits at u64::MAX, bypassing the
        // normal create/commit path (which could never reach this value in
        // practice) to simulate a hostile/corrupted manifest.
        let hostile = Manifest {
            version: u64::MAX,
            data_files: Vec::new(),
            next_row_id: 0,
        };
        strata_storage::commit_manifest(&dir, &hostile).unwrap();
        let ds = Dataset::open(&dir).unwrap();

        let schema = test_schema();
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        let result = txn.commit();

        // `Dataset` doesn't implement `Debug` (its HNSW index can't), so
        // only the `Err` side is printable on failure.
        assert!(
            matches!(&result, Err(TxnError::ManifestOverflow(_))),
            "expected ManifestOverflow, got {:?}",
            result.err()
        );
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
        let low_file_name = ds.data_files()[0].name.clone();
        let high_file_name = ds.data_files()[1].name.clone();
        assert_eq!(
            result.scanned,
            vec![low_file_name],
            "the [1,3] file must be the one actually named in scanned, not just counted"
        );
        assert_eq!(
            result.skipped,
            vec![high_file_name],
            "the [100,102] file must be the one actually named in skipped, not just counted"
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

    /// Generates `count` points scattered within a small cube of side
    /// `spacing` around `center`. Mirrors `crates/index/src/hnsw.rs`'s own
    /// `insert_cluster` test helper (see commit `733579f`): `hnsw_rs`'s
    /// `StdRng::from_os_rng()` layer-assignment RNG has no exposed seed, so
    /// tiny (2-3 point) fixtures occasionally produce a graph shape where
    /// greedy search misses the true nearest neighbor. Many points spread
    /// across well-separated clusters makes "which cluster is nearest"
    /// unambiguous regardless of layer-assignment luck. Offsets come from
    /// an irrational-multiplier equidistribution sequence rather than a
    /// line/grid, since collinear near-duplicate points let `hnsw_rs`'s
    /// neighbor-diversification heuristic prune almost all direct links
    /// between them.
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    fn cluster_vectors(count: usize, center: [f32; 3], spacing: f32) -> Vec<[f32; 3]> {
        const PHI: f64 = 0.618_033_988_749_895; // fractional part of the golden ratio
        const SQRT2: f64 = 0.414_213_562_373_095; // fractional part of sqrt(2)
        const SQRT3: f64 = 0.732_050_807_568_877; // fractional part of sqrt(3)
        (0..count)
            .map(|i| {
                let n = i as f64;
                let frac = |mult: f64| (n * mult).fract();
                let dx = (frac(PHI) as f32) * spacing;
                let dy = (frac(SQRT2) as f32) * spacing;
                let dz = (frac(SQRT3) as f32) * spacing;
                [center[0] + dx, center[1] + dy, center[2] + dz]
            })
            .collect()
    }

    #[test]
    fn vector_search_with_predicate_only_returns_matching_rows() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("vector-search-filtered");
        let ds = Dataset::create(&dir).unwrap();

        // Two well-separated 15-point clusters, mirroring
        // crates/index/src/hnsw.rs's own flaky-test fix (commit 733579f):
        // a 2-point fixture is fragile against hnsw_rs's unseeded internal
        // RNG on tiny graphs. id=1's cluster sits at the origin (where the
        // query point also sits, so the *unfiltered* nearest neighbors are
        // unambiguously from this cluster); id=2's cluster sits 1000 units
        // away. `Predicate::Eq("id", 2)` must narrow results to only the
        // far cluster, even though every one of its points is vastly
        // farther from the query than every point in the near cluster.
        let near_cluster = cluster_vectors(15, [0.0, 0.0, 0.0], 0.01);
        let far_cluster = cluster_vectors(15, [1000.0, 0.0, 0.0], 0.01);
        let mut ids = vec![1i64; 15];
        ids.extend(vec![2i64; 15]);
        let mut vectors = near_cluster;
        vectors.extend(far_cluster);
        let batch = vector_batch(ids, vectors);
        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();

        // Sanity check: without the predicate, the true nearest neighbors
        // really do come from the near (non-matching) cluster — otherwise
        // this test wouldn't prove the predicate is doing any narrowing.
        let unfiltered = ds.vector_search(&[0.0, 0.0, 0.0], 3, None).unwrap();
        assert_eq!(unfiltered.len(), 3);
        assert!(
            unfiltered.iter().all(|r| r.row_id < 15),
            "unfiltered nearest neighbors must come from the near cluster: {unfiltered:?}"
        );

        let predicate = Predicate::Eq("id".to_string(), Value::Int64(2));
        let results = ds
            .vector_search(&[0.0, 0.0, 0.0], 3, Some(&predicate))
            .unwrap();

        assert_eq!(results.len(), 3, "unexpected results: {results:?}");
        assert!(
            results.iter().all(|r| r.row_id >= 15),
            "predicate must narrow results to only the far (id=2) cluster: {results:?}"
        );

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

    #[test]
    fn committing_a_batch_with_a_non_finite_vector_component_is_rejected_cleanly() {
        // Regression test for the Phase 4 final-review finding: a
        // non-finite (NaN/Infinity) vector component used to durably
        // commit — serde_json silently encodes it as JSON `null` — and
        // then permanently brick the dataset, since every future
        // replay_index (including Dataset::open) would fail to parse that
        // `null` back into an f32. Must now be rejected upfront, before any
        // file for the offending batch is written to disk, leaving no
        // trace: no manifest advance, no orphaned-but-referenced files.
        let dir = temp_dir("non-finite-vector-rejected");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(vec![1, 2], vec![[0.0, 0.0, 0.0], [f32::NAN, 1.0, 1.0]]);
        let mut txn = ds.begin();
        txn.insert(batch);
        let result = txn.commit();

        match result {
            Err(TxnError::NonFiniteVectorComponent { row_id }) => {
                assert_eq!(row_id, 1, "row-id 1 (the second row) carries the NaN");
            }
            Err(other) => {
                panic!("expected NonFiniteVectorComponent, got a different error: {other}")
            }
            Ok(_) => panic!("commit of a NaN vector component must not succeed"),
        }

        // The rejected commit must have left no trace: the manifest never
        // advanced, and the dataset still opens and scans cleanly
        // afterward — not a permanently bricked dataset.
        let reopened = Dataset::open(&dir).unwrap();
        assert_eq!(reopened.current_version(), 0);
        assert!(reopened.data_files().is_empty());

        let scanned = reopened.scan(&vector_test_schema()).unwrap();
        assert_eq!(scanned.num_rows(), 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn row_ids_stay_disjoint_across_multiple_pending_batches_in_one_transaction() {
        let dir = temp_dir("row-id-multi-batch-txn");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let first = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .unwrap();
        let second =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![30, 40, 50]))])
                .unwrap();

        let mut txn = ds.begin();
        txn.insert(first);
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

        assert_eq!(row_id_col(&first_on_disk), vec![0, 1]);
        assert_eq!(row_id_col(&second_on_disk), vec![2, 3, 4]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_errors_instead_of_traversing_outside_data_dir_on_an_unsafe_manifest_entry() {
        let dir = temp_dir("path-traversal");
        Dataset::create(&dir).unwrap();

        // Simulate a hostile manifest: hand-craft a DataFileEntry whose name
        // tries to escape data/ via a parent-directory component. No real
        // commit can ever produce this - file names are always generated
        // internally - so this is only reachable via a corrupted/hand-edited
        // manifest, which is exactly the threat model this guards against.
        let hostile = Manifest {
            version: 1,
            data_files: vec![DataFileEntry {
                name: "../../etc/passwd".to_string(),
                stats: std::collections::HashMap::new(),
                delta_log: "d.deltalog".to_string(),
            }],
            next_row_id: 0,
        };
        strata_storage::commit_manifest(&dir, &hostile).unwrap();
        // The delta log must exist (empty is fine — it replays to zero
        // entries) or Dataset::open's replay_index fails on a plain
        // missing-file I/O error before scan ever sees the hostile name.
        std::fs::write(dir.join("data").join("d.deltalog"), "").unwrap();
        let ds = Dataset::open(&dir).unwrap();

        let result = ds.scan(&test_schema());
        assert!(
            matches!(result, Err(TxnError::UnsafeManifestPath(_))),
            "expected UnsafeManifestPath, got {result:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_errors_on_column_count_mismatch_between_physical_file_and_caller_schema() {
        let dir = temp_dir("schema-mismatch");
        let write_schema = test_schema(); // single "id" column
        let ds = Dataset::create(&dir).unwrap();

        let batch = RecordBatch::try_new(
            write_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();

        // Caller asks to scan with a schema declaring 2 columns, but the
        // committed file only has 1 logical column ("id" — the trailing
        // hidden _row_id doesn't count unless the caller requests it) -
        // must error, not silently zip/truncate or, worse, cast the hidden
        // row-id column into the caller's "extra" field.
        let mismatched_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("extra", DataType::Utf8, false),
        ]));
        let result = ds.scan(&mismatched_schema);
        assert!(
            matches!(
                result,
                Err(TxnError::SchemaMismatch {
                    expected: 2,
                    actual: 1
                })
            ),
            "expected SchemaMismatch, got {result:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn vector_search_with_predicate_skips_pruned_files() {
        use strata_query::Predicate;
        use strata_storage::Value;

        // Mirrors explain_reports_skipped_files_by_range's fixture shape
        // (two commits with disjoint id ranges, so should_scan_file prunes
        // one file entirely for an id=2 predicate), but with a vector
        // column so this also exercises row_ids_matching's file-pruning
        // branch on the vector_search path, not just explain().
        let dir = temp_dir("vector-search-file-pruning");
        let ds = Dataset::create(&dir).unwrap();

        let low = vector_batch(vec![1, 1], vec![[0.0, 0.0, 0.0], [0.01, 0.01, 0.01]]);
        let mut txn = ds.begin();
        txn.insert(low);
        let ds = txn.commit().unwrap();

        let high = vector_batch(
            vec![2, 2],
            vec![[1000.0, 1000.0, 1000.0], [1000.01, 1000.01, 1000.01]],
        );
        let mut txn = ds.begin();
        txn.insert(high);
        let ds = txn.commit().unwrap();

        // Sanity: the id=1 file's stats don't overlap id=2's, so explain()
        // must confirm one file is prunable for this predicate — otherwise
        // this test wouldn't actually exercise the pruning branch.
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(2));
        let explain = ds.explain(&predicate);
        assert_eq!(explain.scanned.len(), 1);
        assert_eq!(explain.skipped.len(), 1);

        let results = ds
            .vector_search(&[1000.0, 1000.0, 1000.0], 2, Some(&predicate))
            .unwrap();

        assert_eq!(results.len(), 2, "unexpected results: {results:?}");
        assert!(
            results.iter().all(|r| r.row_id >= 2),
            "only the surviving (id=2) file's rows may be considered: {results:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn open_errors_with_not_found_for_a_nonexistent_dataset() {
        let dir = temp_dir("open-missing");
        let result = Dataset::open(&dir);
        // `Dataset` doesn't implement `Debug` (its HNSW index can't), so
        // only the `Err` side is printable on failure.
        assert!(
            matches!(result, Err(TxnError::NotFound(_))),
            "expected NotFound, got {:?}",
            result.err()
        );
    }

    #[test]
    fn committing_a_transaction_with_zero_pending_batches_still_advances_the_version() {
        let dir = temp_dir("empty-commit");
        let ds = Dataset::create(&dir).unwrap();
        let txn = ds.begin();
        let ds = txn.commit().unwrap();

        assert_eq!(
            ds.current_version(),
            1,
            "an empty commit still advances the manifest version"
        );
        assert!(
            ds.data_files().is_empty(),
            "an empty commit adds no data files"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_errors_cleanly_when_a_manifest_listed_file_is_missing_from_disk() {
        let dir = temp_dir("scan-missing-file");
        let schema = test_schema();
        let ds = Dataset::create(&dir).unwrap();

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();

        let data_dir = ds.data_dir();
        std::fs::remove_file(data_dir.join(&ds.data_files()[0].name)).unwrap();

        let result = ds.scan(&schema);
        assert!(
            result.is_err(),
            "scan must error cleanly, not panic, when a manifest-listed file is missing"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_concatenates_two_files_with_genuinely_different_physical_encodings() {
        use arrow::array::StringArray;
        let dir = temp_dir("mixed-encoding-scan");
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let ds = Dataset::create(&dir).unwrap();

        // First commit: high-cardinality (all-distinct) -> stays plain Utf8.
        let owned: Vec<String> = (0..20).map(|i| format!("name-{i}")).collect();
        let high_card: Vec<&str> = owned.iter().map(String::as_str).collect();
        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(high_card))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch1);
        let ds = txn.commit().unwrap();

        // Second commit: low-cardinality (2 distinct values over 20 rows) ->
        // gets dictionary-encoded.
        let low_card: Vec<&str> = (0..20)
            .map(|i| if i % 2 == 0 { "alice" } else { "bob" })
            .collect();
        let batch2 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(low_card))])
                .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch2);
        let ds = txn.commit().unwrap();

        // Confirm the two files really do have different physical
        // encodings, so this test can't silently stop testing the scenario
        // it exists for.
        let data_dir = ds.data_dir();
        let file0 = read_batch(&data_dir.join(&ds.data_files()[0].name)).unwrap();
        let file1 = read_batch(&data_dir.join(&ds.data_files()[1].name)).unwrap();
        assert_eq!(file0.schema_ref().field(0).data_type(), &DataType::Utf8);
        assert!(matches!(
            file1.schema_ref().field(0).data_type(),
            DataType::Dictionary(_, _)
        ));

        let scanned = ds.scan(&schema).unwrap();
        assert_eq!(scanned.num_rows(), 40);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn build_delta_entries_skips_null_vector_rows_without_erroring() {
        let ids = Arc::new(Int64Array::from(vec![1, 2]));
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let values = Arc::new(arrow::array::Float32Array::from(vec![
            1.0, 2.0, 3.0, 0.0, 0.0, 0.0,
        ]));
        let null_buffer = arrow::buffer::NullBuffer::from(vec![true, false]);
        let vectors = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field,
            3,
            values,
            Some(null_buffer),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(schema, vec![ids, vectors]).unwrap();

        let deltas = build_delta_entries(&batch, 0).unwrap();
        assert_eq!(
            deltas.len(),
            1,
            "the null-vector row must be skipped, not errored on"
        );
        match &deltas[0] {
            DeltaEntry::Insert { row_id, .. } => assert_eq!(*row_id, 0),
            DeltaEntry::Tombstone { .. } => panic!("expected an Insert entry"),
        }
    }

    #[test]
    fn build_delta_entries_errors_when_vector_column_is_not_a_fixed_size_list() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("vector", DataType::Int64, false), // wrong type
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap();

        let result = build_delta_entries(&batch, 0);
        assert!(result.is_err(), "expected an error, got {result:?}");
    }

    #[test]
    fn build_delta_entries_errors_when_vector_inner_type_is_not_float32() {
        let item_field = Arc::new(Field::new("item", DataType::Int32, false));
        let values = Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]));
        let vectors = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field, 3, values, None,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 3),
                false,
            ),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1])), vectors])
                .unwrap();

        let result = build_delta_entries(&batch, 0);
        assert!(result.is_err(), "expected an error, got {result:?}");
    }

    #[test]
    fn replay_index_applies_tombstone_entries_from_the_delta_log() {
        let dir = temp_dir("tombstone-replay");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(vec![1, 2], vec![[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]]);
        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();

        // Hand-append a Tombstone entry to the just-written delta-log file,
        // simulating what a future real DELETE path (Phase 5/6) will
        // produce - build_delta_entries itself never emits Tombstone
        // entries today.
        let data_dir = ds.data_dir();
        let delta_log_path = data_dir.join(&ds.data_files()[0].delta_log);
        let mut entries = strata_index::read_delta_log(&delta_log_path).unwrap();
        entries.push(DeltaEntry::Tombstone { row_id: 0 });
        strata_index::write_delta_log(&delta_log_path, &entries).unwrap();

        drop(ds);
        let reopened = Dataset::open(&dir).unwrap();
        let results = reopened.vector_search(&[0.0, 0.0, 0.0], 2, None).unwrap();

        assert!(
            results.iter().all(|r| r.row_id != 0),
            "the hand-tombstoned row must be excluded after replay: {results:?}"
        );
        assert_eq!(
            results.len(),
            1,
            "only row 1 should remain live: {results:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_computes_stats_for_multiple_columns_including_utf8() {
        let dir = temp_dir("multi-column-stats");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ds = Dataset::create(&dir).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![30, 10, 20])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "banana", "apple", "cherry",
                ])),
            ],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();

        let entry = &ds.data_files()[0];
        let id_stats = entry.stats.get("id").unwrap();
        assert_eq!(id_stats.min, strata_storage::Value::Int64(10));
        assert_eq!(id_stats.max, strata_storage::Value::Int64(30));

        let name_stats = entry.stats.get("name").unwrap();
        assert_eq!(
            name_stats.min,
            strata_storage::Value::Utf8("apple".to_string())
        );
        assert_eq!(
            name_stats.max,
            strata_storage::Value::Utf8("cherry".to_string())
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn explain_on_a_dataset_with_no_data_files_reports_zero_scanned_and_skipped() {
        let dir = temp_dir("explain-empty-dataset");
        let ds = Dataset::create(&dir).unwrap();

        let predicate =
            strata_query::Predicate::Eq("id".to_string(), strata_storage::Value::Int64(1));
        let result = ds.explain(&predicate);

        assert_eq!(result.total_files, 0);
        assert!(result.scanned.is_empty());
        assert!(result.skipped.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_with_predicate_on_a_dataset_with_no_data_files_returns_an_empty_batch() {
        let dir = temp_dir("scan-with-predicate-empty-dataset");
        let schema = test_schema();
        let ds = Dataset::create(&dir).unwrap();

        let predicate =
            strata_query::Predicate::Eq("id".to_string(), strata_storage::Value::Int64(1));
        let result = ds.scan_with_predicate(&schema, &predicate).unwrap();

        assert_eq!(result.num_rows(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn explain_reports_every_file_skipped_when_the_predicate_matches_none() {
        let dir = temp_dir("explain-all-pruned");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ds = Dataset::create(&dir).unwrap();

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        let mut txn = ds.begin();
        txn.insert(batch);
        let ds = txn.commit().unwrap();

        let predicate =
            strata_query::Predicate::Eq("id".to_string(), strata_storage::Value::Int64(999));
        let result = ds.explain(&predicate);

        assert_eq!(result.total_files, 1);
        assert!(result.scanned.is_empty());
        assert_eq!(result.skipped.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }
}
