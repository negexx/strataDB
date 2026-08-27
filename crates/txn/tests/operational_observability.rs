#![allow(clippy::expect_used, clippy::unwrap_used)]

use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};
use strata_txn::{
    CompactionPolicy, Dataset, OperationalEventFilter, OperationalEventKind,
    OperationalEventOutcome,
};

#[test]
fn transaction_outcomes_are_redacted_ordered_and_shared_by_clones() {
    let temp = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(temp.path().join("dataset"), mvp_schema()).unwrap();
    let clone = dataset.clone();

    let mut seed = dataset.begin();
    seed.insert(mvp_batch(&[(1, "seed", [1.0, 0.0, 0.0])]).unwrap())
        .unwrap();
    seed.commit().unwrap();

    let mut first = dataset.begin();
    let mut second = clone.begin();
    first.delete(0).unwrap();
    second.delete(0).unwrap();
    first.commit().unwrap();
    assert!(second.commit().is_err());

    let events = dataset.operational_events(OperationalEventFilter::default());
    assert_eq!(
        events.first().unwrap().kind,
        OperationalEventKind::DatasetCreated
    );
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );
    assert!(events.iter().any(|event| {
        event.kind == OperationalEventKind::TransactionCommitted
            && event.outcome == OperationalEventOutcome::Succeeded
    }));
    assert!(events.iter().any(|event| {
        event.kind == OperationalEventKind::TransactionConflict
            && event.outcome == OperationalEventOutcome::Conflict
    }));

    let conflicts = dataset.operational_events(OperationalEventFilter {
        kind: Some(OperationalEventKind::TransactionConflict),
        outcome: None,
    });
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].sequence, events.last().unwrap().sequence);

    assert!(
        dataset
            .compact(CompactionPolicy {
                retain_snapshots: false,
            })
            .is_err()
    );
    assert_eq!(
        dataset
            .operational_events(OperationalEventFilter {
                kind: Some(OperationalEventKind::LifecycleFailed),
                outcome: Some(OperationalEventOutcome::Failed),
            })
            .len(),
        1
    );
}

#[test]
fn filtered_drain_preserves_unselected_events() {
    let temp = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(temp.path().join("dataset"), mvp_schema()).unwrap();
    let drained = dataset.drain_operational_events(OperationalEventFilter {
        kind: Some(OperationalEventKind::DatasetCreated),
        outcome: None,
    });

    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].outcome, OperationalEventOutcome::Succeeded);
    assert!(
        dataset
            .operational_events(OperationalEventFilter::default())
            .is_empty()
    );
}
