//! Manifest-only retention execution.
//!
//! This first executor slice deletes only policy-eligible historical manifest
//! objects. Row files, vector segments, temporary objects, and arbitrary
//! orphans remain outside its authority.

use strata_storage::{Backend, LocalFs};

use crate::dataset::Dataset;
use crate::error::Result;
use crate::lifecycle::checked_add;
use crate::retention::{
    AgeRetentionPolicy, ManifestPruneCandidate, RetentionPolicy,
    build_age_manifest_prune_authority, build_manifest_prune_authority,
};

#[cfg(test)]
use std::sync::{Mutex, mpsc};

/// Outcome of one manifest-only retention execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestPruneReport {
    /// Durable manifest version observed while execution held lifecycle and
    /// publication exclusivity.
    pub observed_version: u64,
    /// Historical manifest versions whose deletes completed successfully.
    pub deleted_manifest_versions: Vec<u64>,
    /// Total bytes of the successfully deleted listed manifest objects.
    pub deleted_manifest_bytes: u64,
}

pub(crate) fn prune(dataset: &Dataset, policy: RetentionPolicy) -> Result<ManifestPruneReport> {
    let authority = build_manifest_prune_authority(dataset, policy)?;
    let backend = LocalFs::new(dataset.retention_dir());
    let (deleted_manifest_versions, deleted_manifest_bytes) =
        delete_authorized_manifests(&authority.candidates, |candidate| {
            Ok(backend.delete(&candidate.key)?)
        })?;

    Ok(ManifestPruneReport {
        observed_version: authority.observed_version,
        deleted_manifest_versions,
        deleted_manifest_bytes,
    })
}

pub(crate) fn prune_by_age(
    dataset: &Dataset,
    policy: AgeRetentionPolicy,
) -> Result<ManifestPruneReport> {
    let authority = build_age_manifest_prune_authority(dataset, policy)?;
    let backend = LocalFs::new(dataset.retention_dir());
    let (deleted_manifest_versions, deleted_manifest_bytes) =
        delete_authorized_manifests(&authority.candidates, |candidate| {
            Ok(backend.delete(&candidate.key)?)
        })?;
    Ok(ManifestPruneReport {
        observed_version: authority.observed_version,
        deleted_manifest_versions,
        deleted_manifest_bytes,
    })
}

fn delete_authorized_manifests<F>(
    candidates: &[ManifestPruneCandidate],
    mut delete: F,
) -> Result<(Vec<u64>, u64)>
where
    F: FnMut(&ManifestPruneCandidate) -> Result<()>,
{
    let deleted_manifest_bytes = candidates.iter().try_fold(0, |total, candidate| {
        checked_add("deleted_manifest_bytes", total, candidate.bytes)
    })?;
    let mut deleted_manifest_versions = Vec::with_capacity(candidates.len());

    for candidate in candidates {
        delete(candidate)?;
        deleted_manifest_versions.push(candidate.version);
    }

    Ok((deleted_manifest_versions, deleted_manifest_bytes))
}

#[cfg(test)]
struct PruneCheckpoint {
    reached: mpsc::SyncSender<()>,
    resume: mpsc::Receiver<()>,
}

#[cfg(test)]
pub(crate) struct PruneCheckpointControl {
    reached: mpsc::Receiver<()>,
    resume: mpsc::SyncSender<()>,
}

#[cfg(test)]
static AFTER_LIFECYCLE_EXCLUSIVE: Mutex<Option<PruneCheckpoint>> = Mutex::new(None);

#[cfg(test)]
pub(crate) fn install_after_lifecycle_exclusive_checkpoint() -> PruneCheckpointControl {
    let (reached, reached_control) = mpsc::sync_channel(1);
    let (resume_control, resume) = mpsc::sync_channel(1);
    let mut checkpoint = AFTER_LIFECYCLE_EXCLUSIVE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        checkpoint.is_none(),
        "only one prune checkpoint may be installed"
    );
    *checkpoint = Some(PruneCheckpoint { reached, resume });
    PruneCheckpointControl {
        reached: reached_control,
        resume: resume_control,
    }
}

#[cfg(test)]
pub(crate) fn pause_after_lifecycle_exclusive() {
    let checkpoint = AFTER_LIFECYCLE_EXCLUSIVE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(checkpoint) = checkpoint {
        let _ = checkpoint.reached.send(());
        let _ = checkpoint.resume.recv();
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
impl PruneCheckpointControl {
    fn is_reached_within(&self, timeout: std::time::Duration) -> bool {
        self.reached.recv_timeout(timeout).is_ok()
    }

    fn release(&self) {
        self.resume
            .send(())
            .expect("pruning thread dropped before its lifecycle checkpoint released");
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use super::*;
    use crate::TxnError;
    use crate::dataset::checkpoint_pair;
    use crate::retention::ManifestPruneCandidate;

    const WAIT: Duration = Duration::from_millis(100);
    const DEADLINE: Duration = Duration::from_secs(2);
    static PRUNE_CHECKPOINT_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn pending_transaction(dataset: &Dataset) -> crate::Transaction {
        let batch = RecordBatch::try_new(
            dataset.schema(),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let mut transaction = dataset.begin();
        transaction.insert(batch).unwrap();
        transaction
    }

    #[test]
    fn byte_total_overflow_prevents_every_delete_attempt() {
        // Break caught: calculating the report total after an unlink can
        // return ManifestOverflow only after an irreversible deletion.
        let candidates = [
            ManifestPruneCandidate {
                version: 0,
                key: "_versions/0.manifest".to_string(),
                bytes: u64::MAX,
            },
            ManifestPruneCandidate {
                version: 1,
                key: "_versions/1.manifest".to_string(),
                bytes: 1,
            },
        ];
        let mut delete_attempts = 0;

        let result = delete_authorized_manifests(&candidates, |_| {
            delete_attempts += 1;
            Ok(())
        });

        assert!(matches!(
            result,
            Err(TxnError::ManifestOverflow(total)) if total == "deleted_manifest_bytes"
        ));
        assert_eq!(delete_attempts, 0);
    }

    #[test]
    fn pruning_waits_for_an_inflight_preparation_before_building_authority() {
        // Break caught: pruning while a commit is still preparing can retain
        // authority that races its later manifest publication.
        let _serial = PRUNE_CHECKPOINT_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = tempfile::tempdir().unwrap();
        let dataset = Dataset::create(root.path().join("waits-for-preparation"), schema()).unwrap();
        let (checkpoint, control) = checkpoint_pair();
        let mut transaction = pending_transaction(&dataset);
        transaction.pause_after_row_id_claim(checkpoint);
        let commit = std::thread::spawn(move || transaction.commit());
        control.wait();

        let prune_checkpoint = install_after_lifecycle_exclusive_checkpoint();
        let pruning_dataset = dataset.clone();
        let (pruned, pruned_result) = mpsc::channel();
        let pruning = std::thread::spawn(move || {
            pruned.send(pruning_dataset.prune_manifests(RetentionPolicy {
                keep_latest_versions: 1,
            }))
        });

        assert!(
            !prune_checkpoint.is_reached_within(WAIT),
            "pruning must not acquire lifecycle exclusivity while preparation is in flight"
        );
        control.release();
        assert!(
            prune_checkpoint.is_reached_within(DEADLINE),
            "pruning must acquire lifecycle exclusivity after preparation finishes"
        );
        prune_checkpoint.release();
        assert!(commit.join().unwrap().is_ok());
        assert!(pruned_result.recv_timeout(DEADLINE).unwrap().is_ok());
        pruning.join().unwrap().unwrap();
    }

    #[test]
    fn queued_pruning_blocks_a_later_preparation_before_it_creates_a_file() {
        // Break caught: allowing a later commit to start preparation after an
        // executor queues lets it create a file outside pruning's authority.
        let _serial = PRUNE_CHECKPOINT_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = tempfile::tempdir().unwrap();
        let dataset =
            Dataset::create(root.path().join("blocks-later-preparation"), schema()).unwrap();
        let (first_checkpoint, first_control) = checkpoint_pair();
        let mut first = pending_transaction(&dataset);
        first.pause_after_row_id_claim(first_checkpoint);
        let first_commit = std::thread::spawn(move || first.commit());
        first_control.wait();

        let prune_checkpoint = install_after_lifecycle_exclusive_checkpoint();
        let pruning_dataset = dataset.clone();
        let (pruned, pruned_result) = mpsc::channel();
        let pruning = std::thread::spawn(move || {
            pruned.send(pruning_dataset.prune_manifests(RetentionPolicy {
                keep_latest_versions: 1,
            }))
        });
        dataset.wait_for_executor_to_queue();

        let (later_checkpoint, later_control) = checkpoint_pair();
        let mut later = pending_transaction(&dataset);
        later.pause_after_row_id_claim(later_checkpoint);
        let later_commit = std::thread::spawn(move || later.commit());

        first_control.release();
        assert!(
            prune_checkpoint.is_reached_within(DEADLINE),
            "queued pruning must win lifecycle admission after the first preparation"
        );
        assert!(
            !later_control.is_reached_within(WAIT),
            "later preparation must block before its file-creation checkpoint"
        );
        prune_checkpoint.release();
        assert!(first_commit.join().unwrap().is_ok());
        assert!(pruned_result.recv_timeout(DEADLINE).unwrap().is_ok());
        pruning.join().unwrap().unwrap();
        later_control.wait();
        later_control.release();
        assert!(later_commit.join().unwrap().is_ok());
    }
}
