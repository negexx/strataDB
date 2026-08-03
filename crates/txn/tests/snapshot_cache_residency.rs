use std::sync::Arc;

use strata_query::Predicate;
use strata_storage::Value;
use strata_txn::Dataset;
use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};

#[test]
#[allow(clippy::unwrap_used, clippy::expect_used)]
fn a_warmed_historical_snapshots_cache_is_released_after_its_last_handle_drops() {
    let root = tempfile::Builder::new()
        .prefix("strata-snapshot-cache-residency-")
        .tempdir()
        .unwrap();
    let dir = root.path().join("dataset");
    let dataset = Dataset::create(&dir, mvp_schema()).unwrap();

    let mut first = dataset.begin();
    first
        .insert(mvp_batch(&[(1, "alice", [1.0, 1.0, 1.0])]).unwrap())
        .unwrap();
    first.commit().unwrap();

    let historical = dataset.snapshot();
    let predicate = Predicate::Eq("name".to_owned(), Value::Utf8("alice".to_owned()));
    let hits = historical
        .vector_search(&[1.0, 1.0, 1.0], 1, Some(&predicate))
        .unwrap();
    assert_eq!(
        hits.len(),
        1,
        "the warmed snapshot must search its own data"
    );

    let cache = historical.live_set_cache_accounting();
    assert_eq!(
        cache.entry_count, 1,
        "the predicate must create one cache entry"
    );
    assert!(
        cache.charged_bytes > 0,
        "a cached live set must report a non-zero charged footprint"
    );
    assert!(
        cache.charged_bytes <= cache.byte_budget,
        "one small cache entry must remain within the configured budget"
    );

    let historical_weak = Arc::downgrade(&historical);
    let mut second = dataset.begin();
    second
        .insert(mvp_batch(&[(2, "bob", [9.0, 9.0, 9.0])]).unwrap())
        .unwrap();
    second.commit().unwrap();

    let repeated_hits = historical
        .vector_search(&[1.0, 1.0, 1.0], 1, Some(&predicate))
        .unwrap();
    assert_eq!(
        repeated_hits.len(),
        1,
        "warming a historical cache must not let a later commit alter its immutable view"
    );

    drop(historical);
    assert!(
        historical_weak.upgrade().is_none(),
        "after publication moves the Dataset to a new snapshot, dropping the final historical \
         handle must release the warmed snapshot and its per-snapshot cache"
    );
}

#[test]
#[allow(clippy::unwrap_used, clippy::expect_used)]
fn retained_pin_matrix_creates_releases_and_preserves_each_immutable_view() {
    // Break caught: a requested retained snapshot must remain independently
    // queryable and cache-warmed until release, even after a later commit.
    for pin_count in [0_usize, 1, 4, 16, 64] {
        let root = tempfile::Builder::new()
            .prefix("strata-snapshot-cache-pin-matrix-")
            .tempdir()
            .unwrap();
        let dir = root.path().join("dataset");
        let dataset = Dataset::create(&dir, mvp_schema()).unwrap();
        let predicate = Predicate::Eq("name".to_owned(), Value::Utf8("alice".to_owned()));
        let mut pinned = Vec::new();

        for row_id in 0_u16..64 {
            let mut txn = dataset.begin();
            txn.insert(
                mvp_batch(&[(i64::from(row_id), "alice", [f32::from(row_id), 1.0, 1.0])]).unwrap(),
            )
            .unwrap();
            txn.commit().unwrap();
            if pinned.len() < pin_count {
                pinned.push((row_id, dataset.snapshot()));
            }
        }

        assert_eq!(
            pinned.len(),
            pin_count,
            "pin count {pin_count}: exactly the requested snapshot handles must be retained"
        );
        for (row_id, snapshot) in &pinned {
            let hits = snapshot
                .vector_search(&[f32::from(*row_id), 1.0, 1.0], 1, Some(&predicate))
                .unwrap();
            assert_eq!(
                hits.first().map(|hit| hit.row_id),
                Some(u64::from(*row_id)),
                "pin count {pin_count}: historical snapshot {row_id} must return its immutable vector view"
            );
            assert_eq!(snapshot.live_set_cache_accounting().entry_count, 1);
        }
        assert_eq!(
            pinned
                .iter()
                .map(|(_, snapshot)| snapshot.live_set_cache_accounting().entry_count)
                .sum::<usize>(),
            pin_count,
            "pin count {pin_count}: exactly one cache entry must be created per pinned snapshot"
        );

        let weak: Vec<_> = pinned
            .iter()
            .map(|(_, snapshot)| Arc::downgrade(snapshot))
            .collect();
        let mut later = dataset.begin();
        later
            .insert(mvp_batch(&[(64, "bob", [9.0, 9.0, 9.0])]).unwrap())
            .unwrap();
        later.commit().unwrap();
        for (row_id, snapshot) in &pinned {
            assert_eq!(
                snapshot
                    .vector_search(&[f32::from(*row_id), 1.0, 1.0], 1, Some(&predicate))
                    .unwrap()
                    .first()
                    .map(|hit| hit.row_id),
                Some(u64::from(*row_id)),
                "pin count {pin_count}: later publication must not mutate historical snapshot {row_id}"
            );
        }
        drop(pinned);
        assert!(
            weak.iter().all(|snapshot| snapshot.upgrade().is_none()),
            "pin count {pin_count}: every released historical snapshot and cache must become unreachable"
        );
    }
}
