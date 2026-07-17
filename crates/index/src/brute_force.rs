//! Brute-force nearest-neighbor search. No HNSW yet — that's Phase 4, see
//! `.claude/rules/vector-index.md`. This exists so Phase 1's MVP checklist
//! ("run a brute-force nearest-neighbor search on the vector column,
//! correctly") has a real, correct implementation to build the rest of the
//! vertical slice against.

use arrow::array::{Array, FixedSizeListArray, Float32Array};
use arrow::error::ArrowError;

/// A single search result: which row, and its squared L2 distance to the
/// query vector. Sorted closest-first.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Neighbor {
    pub row_index: usize,
    pub squared_distance: f32,
}

/// Linear-scans every row in `vectors`, returning the `k` closest to `query`
/// by squared L2 distance.
///
/// # Errors
///
/// Returns an [`ArrowError`] if `vectors`' child array isn't `Float32`, or if
/// `query`'s length doesn't match `vectors`' fixed list size — a silent
/// truncation to the shorter length would compute a wrong-but-plausible
/// distance instead of failing loudly.
pub fn brute_force_search(
    vectors: &FixedSizeListArray,
    query: &[f32],
    k: usize,
) -> Result<Vec<Neighbor>, ArrowError> {
    let expected_len = usize::try_from(vectors.value_length()).unwrap_or(0);
    if query.len() != expected_len {
        return Err(ArrowError::InvalidArgumentError(format!(
            "query vector has {} dimensions, but the indexed vectors have {expected_len}",
            query.len()
        )));
    }

    let mut scored = Vec::with_capacity(vectors.len());
    for i in 0..vectors.len() {
        let row = vectors.value(i);
        let row: &Float32Array = row
            .as_any()
            .downcast_ref()
            .ok_or_else(|| ArrowError::CastError("vector column must be Float32".to_string()))?;
        scored.push(Neighbor {
            row_index: i,
            squared_distance: squared_l2(row.values(), query),
        });
    }
    // Partial selection (O(n)) instead of a full sort (O(n log n)): only the
    // k closest rows need to end up in order, not the whole scored Vec.
    let k = k.min(scored.len());
    if k < scored.len() {
        scored.select_nth_unstable_by(k, |a, b| a.squared_distance.total_cmp(&b.squared_distance));
    }
    scored.truncate(k);
    scored.sort_by(|a, b| a.squared_distance.total_cmp(&b.squared_distance));
    Ok(scored)
}

fn squared_l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Float32Array as F32Arr;
    use arrow::datatypes::{DataType, Field};

    use super::*;

    fn make_vectors(rows: &[[f32; 3]]) -> FixedSizeListArray {
        let flat: Vec<f32> = rows.iter().flatten().copied().collect();
        let values = Arc::new(F32Arr::from(flat));
        let field = Arc::new(Field::new("item", DataType::Float32, false));
        FixedSizeListArray::new(field, 3, values, None)
    }

    #[test]
    fn brute_force_search_finds_exact_and_nearest_match() {
        let vectors = make_vectors(&[[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [10.0, 10.0, 10.0]]);
        let results = brute_force_search(&vectors, &[0.0, 0.0, 0.0], 2).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].row_index, 0);
        // Exact zero, not approximate: identical points sum to exactly 0.0
        // with no rounding involved, so a strict comparison is correct here.
        #[allow(clippy::float_cmp)]
        {
            assert_eq!(results[0].squared_distance, 0.0);
        }
        assert_eq!(results[1].row_index, 1);
    }

    #[test]
    fn brute_force_search_errors_on_dimension_mismatch() {
        let vectors = make_vectors(&[[0.0, 0.0, 0.0]]);
        let result = brute_force_search(&vectors, &[0.0, 0.0], 1);
        assert!(
            matches!(result, Err(ArrowError::InvalidArgumentError(_))),
            "expected a dimension-mismatch error, got {result:?}"
        );
    }

    #[test]
    fn brute_force_search_returns_everything_when_k_exceeds_row_count() {
        let vectors = make_vectors(&[[0.0, 0.0, 0.0], [1.0, 0.0, 0.0]]);
        let results = brute_force_search(&vectors, &[0.0, 0.0, 0.0], 10).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].row_index, 0);
        assert_eq!(results[1].row_index, 1);
    }

    #[test]
    fn brute_force_search_returns_nothing_when_k_is_zero() {
        let vectors = make_vectors(&[[0.0, 0.0, 0.0], [1.0, 0.0, 0.0]]);
        let results = brute_force_search(&vectors, &[0.0, 0.0, 0.0], 0).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn brute_force_search_errors_when_vector_child_is_not_float32() {
        let item_field = Arc::new(Field::new("item", DataType::Int32, false));
        let values = Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]));
        let vectors = FixedSizeListArray::new(item_field, 3, values, None);

        let result = brute_force_search(&vectors, &[0.0, 0.0, 0.0], 1);
        assert!(
            matches!(result, Err(ArrowError::CastError(_))),
            "expected a CastError, got {result:?}"
        );
    }

    #[test]
    fn brute_force_search_on_an_empty_vectors_array_returns_no_results() {
        let vectors = make_vectors(&[]);
        let results = brute_force_search(&vectors, &[0.0, 0.0, 0.0], 3).unwrap();
        assert!(results.is_empty());
    }
}
