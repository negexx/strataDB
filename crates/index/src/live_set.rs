//! A dense bitset of live row-ids, hoisted out of
//! [`crate::hnsw::HnswIndex::search_filtered`] so it can be built once and
//! reused (cached per `(Snapshot, Predicate)` by `crates/txn`) instead of
//! rebuilt from a `&[usize]` on every filtered search call.
//!
//! Row-ids are dense and monotonic (see `NodeTable`'s own contract), so
//! membership is one indexed load plus a shift instead of a `SipHash` probe,
//! and no hashing or rehash growth is paid while building it. Sizing note:
//! the bitset is `max_row_id / 8` bytes — proportional to the largest live
//! row-id, not to the number of live ids. See `search_filtered`'s own doc
//! comment for the tradeoff this implies against a `HashSet`.

/// A dense, order-insensitive bitset over row-ids.
#[derive(Debug, Clone)]
pub struct LiveSet {
    bits: Box<[u64]>,
}

impl LiveSet {
    /// Builds a bitset covering every id in `row_ids`. `row_ids` need not be
    /// sorted or deduplicated.
    #[must_use]
    pub fn from_row_ids(row_ids: &[usize]) -> Self {
        let max_id = row_ids.iter().copied().max().unwrap_or(0);
        let mut bits = vec![0_u64; max_id / 64 + 1];
        for &id in row_ids {
            bits[id / 64] |= 1_u64 << (id % 64);
        }
        LiveSet {
            bits: bits.into_boxed_slice(),
        }
    }

    /// Whether `id` is a member of this set. An id past the end of the
    /// bitset simply isn't a member, which is also what makes the
    /// empty-`row_ids` case behave correctly.
    #[must_use]
    pub fn contains(&self, id: u64) -> bool {
        let word = usize::try_from(id / 64).unwrap_or(usize::MAX);
        word < self.bits.len() && (self.bits[word] >> (id % 64)) & 1 == 1
    }

    /// The bitset's resident size, in bytes — what a per-snapshot cache
    /// should charge against its byte budget for one entry.
    #[must_use]
    pub fn byte_size(&self) -> usize {
        std::mem::size_of_val(&*self.bits)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn contains_every_id_passed_in() {
        let set = LiveSet::from_row_ids(&[3, 10, 17]);
        assert!(set.contains(3));
        assert!(set.contains(10));
        assert!(set.contains(17));
    }

    #[test]
    fn does_not_contain_ids_not_passed_in() {
        let set = LiveSet::from_row_ids(&[3, 10, 17]);
        assert!(!set.contains(0));
        assert!(!set.contains(4));
        assert!(!set.contains(16));
    }

    #[test]
    fn empty_row_ids_contains_nothing() {
        let set = LiveSet::from_row_ids(&[]);
        assert!(!set.contains(0));
        assert!(!set.contains(1000));
    }

    #[test]
    fn unsorted_input_behaves_identically_to_sorted_input() {
        let sorted = LiveSet::from_row_ids(&[1, 5, 9]);
        let unsorted = LiveSet::from_row_ids(&[9, 1, 5]);
        for id in 0..12 {
            assert_eq!(
                sorted.contains(id),
                unsorted.contains(id),
                "mismatch at id {id}"
            );
        }
    }

    #[test]
    fn an_id_far_past_the_max_live_id_is_not_a_member() {
        let set = LiveSet::from_row_ids(&[5]);
        assert!(!set.contains(1_000_000));
    }

    #[test]
    fn byte_size_is_proportional_to_the_max_row_id_not_the_count() {
        let few_high_ids = LiveSet::from_row_ids(&[999_999]);
        let many_low_ids: Vec<usize> = (0..100).collect();
        let many = LiveSet::from_row_ids(&many_low_ids);
        assert!(
            few_high_ids.byte_size() > many.byte_size(),
            "one high id (999_999) should need a bigger bitset than 100 low ids"
        );
    }
}
