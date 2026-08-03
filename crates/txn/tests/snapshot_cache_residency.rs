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
