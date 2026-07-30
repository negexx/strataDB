//! `Predicate` — the shared vocabulary for row-level filtering (`filter`)
//! and file-level pruning (`should_scan_file`). See
//! `docs/design/phase-3-query-refinement-spec.md` §2.

use std::collections::HashMap;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::boolean::{and_kleene, or_kleene};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq};
use arrow::error::ArrowError;
use strata_storage::{ColumnStats, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Eq(String, Value),
    Lt(String, Value),
    LtEq(String, Value),
    Gt(String, Value),
    GtEq(String, Value),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
}

impl Predicate {
    /// Every column this predicate reads from, in tree order. A leaf
    /// contributes one column; `And`/`Or` contribute their children's
    /// columns in order. May contain duplicates (e.g. `a = 1 OR a = 2`) —
    /// callers building a projection list should dedup the result.
    #[must_use]
    pub fn columns(&self) -> Vec<&str> {
        match self {
            Predicate::Eq(c, _)
            | Predicate::Lt(c, _)
            | Predicate::LtEq(c, _)
            | Predicate::Gt(c, _)
            | Predicate::GtEq(c, _) => vec![c.as_str()],
            Predicate::And(l, r) | Predicate::Or(l, r) => {
                let mut cols = l.columns();
                cols.extend(r.columns());
                cols
            }
        }
    }
}

/// Filters `batch` to rows matching `predicate`.
///
/// # Errors
///
/// Returns an [`ArrowError`] if any of `predicate`'s columns doesn't exist,
/// or if its value's type doesn't match the column's actual Arrow type (the
/// underlying comparison kernel enforces this).
pub fn filter(batch: &RecordBatch, predicate: &Predicate) -> Result<RecordBatch, ArrowError> {
    let selection = mask(batch, predicate)?;
    filter_record_batch(batch, &selection)
}

/// Computes the boolean selection mask that [`filter`] would apply, without
/// materialising the filtered batch.
///
/// Exists for callers that need only a subset of the columns — applying this
/// mask to one column with `arrow::compute::filter` avoids copying the rest.
/// [`filter`] copies *every* column, which is the right default but is
/// enormously wasteful when the batch carries a wide embedding column and the
/// caller only wants row-ids out of it.
///
/// # Errors
///
/// Same as [`filter`]: any of `predicate`'s columns must exist and its
/// value's type must match the column's Arrow type.
pub fn mask(batch: &RecordBatch, predicate: &Predicate) -> Result<BooleanArray, ArrowError> {
    // Kleene (three-valued) composition: `or_kleene` is load-bearing for
    // nullable columns (plain `or` would drop a row that matches only via
    // a non-null disjunct - see `mask_or_with_kleene_semantics_...` test
    // below). `and_kleene` is used for consistency, not necessity - for
    // `And`, Kleene and non-Kleene always produce the same row-selection
    // outcome once `filter`/`filter_record_batch` treat both false and
    // null as "don't take."
    match predicate {
        Predicate::And(l, r) => Ok(and_kleene(&mask(batch, l)?, &mask(batch, r)?)?),
        Predicate::Or(l, r) => Ok(or_kleene(&mask(batch, l)?, &mask(batch, r)?)?),
        Predicate::Eq(c, _)
        | Predicate::Lt(c, _)
        | Predicate::LtEq(c, _)
        | Predicate::Gt(c, _)
        | Predicate::GtEq(c, _) => {
            let idx = batch.schema_ref().index_of(c)?;
            let array = batch.column(idx);
            compare(array, predicate)
        }
    }
}

fn compare(array: &ArrayRef, predicate: &Predicate) -> Result<BooleanArray, ArrowError> {
    let cmp_fn: fn(
        &dyn arrow::array::Datum,
        &dyn arrow::array::Datum,
    ) -> Result<BooleanArray, ArrowError> = match predicate {
        Predicate::Eq(..) => eq,
        Predicate::Lt(..) => lt,
        Predicate::LtEq(..) => lt_eq,
        Predicate::Gt(..) => gt,
        Predicate::GtEq(..) => gt_eq,
        Predicate::And(..) | Predicate::Or(..) => {
            unreachable!("compare() is only reachable from mask()'s leaf arm")
        }
    };
    let value = match predicate {
        Predicate::Eq(_, v)
        | Predicate::Lt(_, v)
        | Predicate::LtEq(_, v)
        | Predicate::Gt(_, v)
        | Predicate::GtEq(_, v) => v,
        Predicate::And(..) | Predicate::Or(..) => {
            unreachable!("compare() is only reachable from mask()'s leaf arm")
        }
    };
    match value {
        Value::Int64(v) => {
            let scalar = Int64Array::new_scalar(*v);
            cmp_fn(array, &scalar)
        }
        Value::Float64(v) => {
            let scalar = Float64Array::new_scalar(*v);
            cmp_fn(array, &scalar)
        }
        Value::Utf8(v) => {
            let scalar = StringArray::new_scalar(v.as_str());
            cmp_fn(array, &scalar)
        }
    }
}

/// Decides whether a file whose column stats are `stats` could possibly
/// contain a row matching `predicate`. Fails open (returns `true`)
/// whenever it can't prove otherwise — see
/// `docs/design/phase-3-query-refinement-spec.md` §2. Pure
/// function, zero I/O.
///
/// For a compound predicate: an `And` can prune the file if *either* side
/// alone proves no match is possible (both conjuncts must be satisfiable
/// for the AND to be satisfiable). An `Or` can only prune if *both* sides
/// prove no match is possible (either disjunct being satisfiable is enough
/// for the OR to be satisfiable). This is the same fail-open direction as
/// the leaf case: `should_scan_file` never says "skip" unless it can prove
/// the skip is safe.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn should_scan_file(stats: &HashMap<String, ColumnStats>, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::And(l, r) => should_scan_file(stats, l) && should_scan_file(stats, r),
        Predicate::Or(l, r) => should_scan_file(stats, l) || should_scan_file(stats, r),
        Predicate::Eq(c, v)
        | Predicate::Lt(c, v)
        | Predicate::LtEq(c, v)
        | Predicate::Gt(c, v)
        | Predicate::GtEq(c, v) => {
            let Some(col_stats) = stats.get(c) else {
                return true; // no stats for this column - fail open, must scan
            };
            // A mismatched Value variant (e.g. a Utf8 predicate value against an
            // Int64 column's stats) can't be proven to miss - fail open rather
            // than trust derived PartialOrd's cross-variant ordering, which
            // compares by declaration order, not value semantics.
            if std::mem::discriminant(v) != std::mem::discriminant(&col_stats.min) {
                return true;
            }
            match predicate {
                Predicate::Eq(..) => *v >= col_stats.min && *v <= col_stats.max,
                Predicate::Lt(..) => *v > col_stats.min,
                Predicate::LtEq(..) => *v >= col_stats.min,
                Predicate::Gt(..) => *v < col_stats.max,
                Predicate::GtEq(..) => *v <= col_stats.max,
                Predicate::And(..) | Predicate::Or(..) => {
                    unreachable!("handled by outer match")
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use proptest::prelude::*;

    use super::*;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn filter_eq_on_int64_column() {
        let result = filter(
            &sample_batch(),
            &Predicate::Eq("id".to_string(), Value::Int64(20)),
        )
        .unwrap();
        assert_eq!(result.num_rows(), 1);
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 20);
    }

    #[test]
    fn filter_lt_on_int64_column() {
        let result = filter(
            &sample_batch(),
            &Predicate::Lt("id".to_string(), Value::Int64(25)),
        )
        .unwrap();
        assert_eq!(result.num_rows(), 2); // 10, 20
    }

    #[test]
    fn filter_gt_eq_on_int64_column() {
        let result = filter(
            &sample_batch(),
            &Predicate::GtEq("id".to_string(), Value::Int64(20)),
        )
        .unwrap();
        assert_eq!(result.num_rows(), 2); // 20, 30
    }

    #[test]
    fn filter_eq_on_utf8_column() {
        let result = filter(
            &sample_batch(),
            &Predicate::Eq("name".to_string(), Value::Utf8("b".to_string())),
        )
        .unwrap();
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn mask_and_combines_two_leaf_conditions() {
        let batch = sample_batch(); // id: [10, 20, 30], name: ["a", "b", "c"]
        let predicate = Predicate::And(
            Box::new(Predicate::GtEq("id".to_string(), Value::Int64(20))),
            Box::new(Predicate::Eq(
                "name".to_string(),
                Value::Utf8("b".to_string()),
            )),
        );
        let result = filter(&batch, &predicate).unwrap();
        assert_eq!(result.num_rows(), 1);
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 20);
    }

    #[test]
    fn mask_or_combines_two_leaf_conditions() {
        let batch = sample_batch(); // id: [10, 20, 30], name: ["a", "b", "c"]
        let predicate = Predicate::Or(
            Box::new(Predicate::Eq("id".to_string(), Value::Int64(10))),
            Box::new(Predicate::Eq("id".to_string(), Value::Int64(30))),
        );
        let result = filter(&batch, &predicate).unwrap();
        assert_eq!(result.num_rows(), 2);
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut got: Vec<i64> = (0..result.num_rows()).map(|i| ids.value(i)).collect();
        got.sort_unstable();
        assert_eq!(got, vec![10, 30]);
    }

    #[test]
    fn mask_or_with_kleene_semantics_keeps_a_row_matched_only_via_a_null_leaf_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true), // nullable
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![None, Some(1)])),
                Arc::new(Int64Array::from(vec![2, 2])),
            ],
        )
        .unwrap();
        // Row 0: a=NULL, b=2. Row 1: a=1, b=2.
        // Or(Eq(a,1), Eq(b,2)): row 0 doesn't match on "a" (unknown, not false)
        // but DOES match on "b=2" - Kleene OR must keep it. Row 1 matches both.
        let predicate = Predicate::Or(
            Box::new(Predicate::Eq("a".to_string(), Value::Int64(1))),
            Box::new(Predicate::Eq("b".to_string(), Value::Int64(2))),
        );
        let result = filter(&batch, &predicate).unwrap();
        assert_eq!(
            result.num_rows(),
            2,
            "both rows match via the b=2 leaf, even though a is NULL in row 0 - \
             a non-Kleene OR would incorrectly drop row 0"
        );
    }

    fn naive_eval(predicate: &Predicate, id: i64, name: &str) -> bool {
        let actual_for = |column: &str| -> Value {
            match column {
                "id" => Value::Int64(id),
                "name" => Value::Utf8(name.to_string()),
                other => panic!("naive_eval: unknown column {other}"),
            }
        };
        match predicate {
            Predicate::Eq(c, v) => actual_for(c) == *v,
            Predicate::Lt(c, v) => actual_for(c) < *v,
            Predicate::LtEq(c, v) => actual_for(c) <= *v,
            Predicate::Gt(c, v) => actual_for(c) > *v,
            Predicate::GtEq(c, v) => actual_for(c) >= *v,
            Predicate::And(l, r) => naive_eval(l, id, name) && naive_eval(r, id, name),
            Predicate::Or(l, r) => naive_eval(l, id, name) || naive_eval(r, id, name),
        }
    }

    #[test]
    fn mask_matches_naive_reference_over_compound_predicates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = vec![1i64, 2, 3, 4, 5, 6];
        let names = vec!["a", "b", "a", "c", "b", "a"];
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(StringArray::from(names.clone())),
            ],
        )
        .unwrap();

        let leaves = [
            Predicate::GtEq("id".to_string(), Value::Int64(3)),
            Predicate::Lt("id".to_string(), Value::Int64(5)),
            Predicate::Eq("name".to_string(), Value::Utf8("a".to_string())),
        ];

        let mut compounds = Vec::new();
        for i in 0..leaves.len() {
            for j in 0..leaves.len() {
                if i == j {
                    continue;
                }
                compounds.push(Predicate::And(
                    Box::new(leaves[i].clone()),
                    Box::new(leaves[j].clone()),
                ));
                compounds.push(Predicate::Or(
                    Box::new(leaves[i].clone()),
                    Box::new(leaves[j].clone()),
                ));
            }
        }

        for predicate in &compounds {
            let selection = mask(&batch, predicate).unwrap();
            for row in 0..ids.len() {
                let expected = naive_eval(predicate, ids[row], names[row]);
                assert_eq!(
                    selection.value(row),
                    expected,
                    "predicate {predicate:?} row {row} (id={}, name={}): mask()={} naive={}",
                    ids[row],
                    names[row],
                    selection.value(row),
                    expected
                );
            }
        }
    }

    #[test]
    fn columns_collects_every_leaf_column_in_a_compound_predicate() {
        let predicate = Predicate::And(
            Box::new(Predicate::GtEq("timestamp".to_string(), Value::Int64(100))),
            Box::new(Predicate::Eq(
                "category".to_string(),
                Value::Utf8("x".to_string()),
            )),
        );
        assert_eq!(predicate.columns(), vec!["timestamp", "category"]);
    }

    #[test]
    fn columns_on_a_leaf_predicate_returns_one_column() {
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(1));
        assert_eq!(predicate.columns(), vec!["id"]);
    }

    #[test]
    fn should_scan_file_prunes_when_range_cannot_overlap() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(200),
            },
        );
        // Eq(id, 50) can't match a file whose id range is [100, 200].
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(50));
        assert!(!should_scan_file(&stats, &predicate));
    }

    #[test]
    fn should_scan_file_scans_when_range_could_overlap() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(200),
            },
        );
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(150));
        assert!(should_scan_file(&stats, &predicate));
    }

    #[test]
    fn should_scan_file_fails_open_when_column_has_no_stats() {
        let stats: HashMap<String, ColumnStats> = HashMap::new();
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(50));
        assert!(
            should_scan_file(&stats, &predicate),
            "a column with no stats must never be pruned - always scan"
        );
    }

    #[test]
    fn should_scan_file_fails_open_on_range_predicates() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(200),
            },
        );
        // Lt(id, 100): no value in [100, 200] is < 100 -> should prune.
        assert!(!should_scan_file(
            &stats,
            &Predicate::Lt("id".to_string(), Value::Int64(100))
        ));
        // Gt(id, 200): no value in [100, 200] is > 200 -> should prune.
        assert!(!should_scan_file(
            &stats,
            &Predicate::Gt("id".to_string(), Value::Int64(200))
        ));
        // GtEq(id, 200): 200 itself is in range -> must scan.
        assert!(should_scan_file(
            &stats,
            &Predicate::GtEq("id".to_string(), Value::Int64(200))
        ));
    }

    #[test]
    fn should_scan_file_lteq_prunes_when_value_below_range() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(200),
            },
        );
        // LtEq(id, 50): no value in [100, 200] is <= 50 -> should prune.
        assert!(!should_scan_file(
            &stats,
            &Predicate::LtEq("id".to_string(), Value::Int64(50))
        ));
    }

    #[test]
    fn should_scan_file_lteq_scans_at_boundary() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(200),
            },
        );
        // LtEq(id, 100): 100 <= 100 -> the boundary value qualifies, must scan.
        assert!(should_scan_file(
            &stats,
            &Predicate::LtEq("id".to_string(), Value::Int64(100))
        ));
    }

    #[test]
    fn should_scan_file_lt_scans_when_value_could_match() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(200),
            },
        );
        // Lt(id, 150): 100 < 150 -> some value in range qualifies, must scan.
        assert!(should_scan_file(
            &stats,
            &Predicate::Lt("id".to_string(), Value::Int64(150))
        ));
    }

    #[test]
    fn should_scan_file_gt_scans_when_value_could_match() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(200),
            },
        );
        // Gt(id, 150): 200 > 150 -> some value in range qualifies, must scan.
        assert!(should_scan_file(
            &stats,
            &Predicate::Gt("id".to_string(), Value::Int64(150))
        ));
    }

    #[test]
    fn should_scan_file_gteq_prunes_when_value_above_range() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(200),
            },
        );
        // GtEq(id, 250): no value in [100, 200] is >= 250 -> should prune.
        assert!(!should_scan_file(
            &stats,
            &Predicate::GtEq("id".to_string(), Value::Int64(250))
        ));
    }

    #[test]
    fn should_scan_file_fails_open_on_value_variant_mismatch() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(200),
            },
        );
        // Eq(id, "x"): a Utf8 predicate value against Int64-typed stats -
        // the discriminant guard must fail open rather than trust derived
        // PartialOrd's cross-variant, declaration-order comparison.
        assert!(should_scan_file(
            &stats,
            &Predicate::Eq("id".to_string(), Value::Utf8("x".to_string()))
        ));
    }

    #[test]
    fn filter_eq_on_float64_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Float64Array::from(vec![
                9.99, 19.99, 29.99,
            ]))],
        )
        .unwrap();

        let result = filter(
            &batch,
            &Predicate::Eq("price".to_string(), Value::Float64(19.99)),
        )
        .unwrap();
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn should_scan_file_prunes_on_utf8_range() {
        let mut stats = HashMap::new();
        stats.insert(
            "name".to_string(),
            ColumnStats {
                min: Value::Utf8("apple".to_string()),
                max: Value::Utf8("mango".to_string()),
            },
        );
        // "zebra" can't be in ["apple", "mango"] lexicographically.
        assert!(!should_scan_file(
            &stats,
            &Predicate::Eq("name".to_string(), Value::Utf8("zebra".to_string()))
        ));
        assert!(should_scan_file(
            &stats,
            &Predicate::Eq("name".to_string(), Value::Utf8("banana".to_string()))
        ));
    }

    #[test]
    fn should_scan_file_prunes_on_float64_range() {
        let mut stats = HashMap::new();
        stats.insert(
            "price".to_string(),
            ColumnStats {
                min: Value::Float64(10.0),
                max: Value::Float64(20.0),
            },
        );
        assert!(!should_scan_file(
            &stats,
            &Predicate::Lt("price".to_string(), Value::Float64(10.0))
        ));
        assert!(should_scan_file(
            &stats,
            &Predicate::Lt("price".to_string(), Value::Float64(15.0))
        ));
    }

    #[test]
    fn should_scan_file_and_prunes_using_either_operand_even_if_the_other_could_match() {
        let mut stats = HashMap::new();
        stats.insert(
            "a".to_string(),
            ColumnStats {
                min: Value::Int64(0),
                max: Value::Int64(10),
            },
        );
        stats.insert(
            "b".to_string(),
            ColumnStats {
                min: Value::Int64(20),
                max: Value::Int64(30),
            },
        );

        // a=5 is in [0,10] - a single leaf on "a" alone cannot prune.
        let leaf_a = Predicate::Eq("a".to_string(), Value::Int64(5));
        // b=999 is outside [20,30] - this leaf alone already prunes.
        let leaf_b = Predicate::Eq("b".to_string(), Value::Int64(999));
        let compound = Predicate::And(Box::new(leaf_a.clone()), Box::new(leaf_b));

        assert!(
            should_scan_file(&stats, &leaf_a),
            "a single leaf (a=5) can't prove this file has no match"
        );
        assert!(
            !should_scan_file(&stats, &compound),
            "the AND must prune using leaf_b's information, which a single leaf (a=5) alone couldn't"
        );
    }

    #[test]
    fn should_scan_file_or_requires_both_operands_to_prune() {
        let mut stats = HashMap::new();
        stats.insert(
            "a".to_string(),
            ColumnStats {
                min: Value::Int64(0),
                max: Value::Int64(10),
            },
        );
        stats.insert(
            "b".to_string(),
            ColumnStats {
                min: Value::Int64(20),
                max: Value::Int64(30),
            },
        );

        let out_of_range_a = Predicate::Eq("a".to_string(), Value::Int64(999));
        let out_of_range_b = Predicate::Eq("b".to_string(), Value::Int64(999));
        let in_range_b = Predicate::Eq("b".to_string(), Value::Int64(25));

        assert!(
            !should_scan_file(
                &stats,
                &Predicate::Or(Box::new(out_of_range_a.clone()), Box::new(out_of_range_b))
            ),
            "OR of two out-of-range leaves must prune - neither side could match"
        );
        assert!(
            should_scan_file(
                &stats,
                &Predicate::Or(Box::new(out_of_range_a), Box::new(in_range_b))
            ),
            "OR must scan if either side could still match"
        );
    }

    proptest! {
        #[test]
        fn mask_matches_a_naive_per_row_reference(
            (values, threshold) in prop::collection::vec(any::<i64>(), 0..30)
                .prop_flat_map(|values| {
                    // threshold is NOT independently random -- see this
                    // task's own "Design note" above. Predicate::Eq's
                    // naive arm needs threshold to actually equal one of
                    // values' entries some of the time, or that variant's
                    // true branch is never meaningfully exercised.
                    let mut candidates = values.clone();
                    candidates.push(0); // always non-empty, even if values is empty
                    (Just(values), prop_oneof![any::<i64>(), prop::sample::select(candidates)])
                }),
            variant in 0..5u8,
        ) {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(values.clone()))],
            ).unwrap();

            let predicate = match variant {
                0 => Predicate::Eq("id".to_string(), Value::Int64(threshold)),
                1 => Predicate::Lt("id".to_string(), Value::Int64(threshold)),
                2 => Predicate::LtEq("id".to_string(), Value::Int64(threshold)),
                3 => Predicate::Gt("id".to_string(), Value::Int64(threshold)),
                _ => Predicate::GtEq("id".to_string(), Value::Int64(threshold)),
            };

            let actual = mask(&batch, &predicate).unwrap();

            let naive: Vec<bool> = values
                .iter()
                .map(|&v| match variant {
                    0 => v == threshold,
                    1 => v < threshold,
                    2 => v <= threshold,
                    3 => v > threshold,
                    _ => v >= threshold,
                })
                .collect();

            prop_assert_eq!(actual.len(), naive.len());
            for (i, expected) in naive.iter().enumerate() {
                prop_assert_eq!(actual.value(i), *expected, "row {}", i);
            }
        }
    }
}
