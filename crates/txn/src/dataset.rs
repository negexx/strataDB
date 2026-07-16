//! Single-writer transaction path for Phase 1's vertical slice. Implements
//! the commit protocol from
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §3, minus
//! §3.2's conflict check — Phase 1 has exactly one writer, so there is
//! nothing to conflict with yet. See `.claude/rules/concurrency-txn-layer.md`
//! before adding real conflict detection here; this API is shaped so Phase 6
//! can slot it in without a rewrite, but it is not implemented yet.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::SchemaRef;
use strata_storage::{Manifest, commit_manifest, read_batch, read_current, write_batch};

use crate::error::{Result, TxnError};

pub struct Dataset {
    dir: PathBuf,
    manifest: Manifest,
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
        Ok(Self { dir, manifest })
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
        Ok(Self { dir, manifest })
    }

    #[must_use]
    pub fn current_version(&self) -> u64 {
        self.manifest.version
    }

    #[must_use]
    pub fn data_dir(&self) -> PathBuf {
        self.dir.join("data")
    }

    /// Data file names (relative to `data_dir()`) belonging to the current
    /// version. Exposed for tests that need to inspect the raw on-disk
    /// representation directly.
    #[must_use]
    pub fn data_files(&self) -> &[String] {
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
            .map(|name| {
                let batch = read_batch(&data_dir.join(name))?;
                cast_batch_to_schema(&batch, schema)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(concat_batches(schema, &batches)?)
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
            let encoded = strata_storage::encode_batch(batch)?;
            let file_name = format!("{new_version:020}-{i}.arrow");
            write_batch(&data_dir.join(&file_name), &encoded)?;
            manifest.data_files.push(file_name);
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

        Ok(Dataset {
            dir: self.dir,
            manifest,
        })
    }
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
        let on_disk = read_batch(&ds.data_dir().join(&ds.data_files()[0])).unwrap();
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
        let file_name = &ds.data_files()[0];
        let on_disk = strata_storage::read_batch(&data_dir.join(file_name)).unwrap();
        assert!(matches!(
            on_disk.schema_ref().field(0).data_type(),
            DataType::Dictionary(_, _)
        ));
        std::fs::remove_dir_all(&dir).ok();
    }
}
