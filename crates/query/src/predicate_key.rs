//! A hashable, structurally-exact identity key for [`Predicate`](crate::Predicate).
//!
//! `Predicate` derives `Debug`/`Clone`/`PartialEq` but not `Eq`/`Hash`, because
//! `f64` (inside `Value::Float64`) has no `Hash` impl at all.
//!
//! **This key deliberately does NOT implement "logical" float equality (the
//! `-0.0 == 0.0`, "any NaN is like any other NaN" rules `f64`'s own
//! `PartialEq` gives you) — it uses the raw bit pattern, full stop.** That
//! matches what actually decides which rows a predicate matches:
//! `strata_query::mask`'s comparison kernel goes through arrow-rs, which
//! compares `f64`s by **total order** (bitwise), not IEEE-754 `==`. `arrow`'s
//! `is_eq` for `f64` is literally `self.to_bits() == rhs.to_bits()`, so
//! `Eq(col, -0.0)` and `Eq(col, 0.0)` select different rows from a column
//! holding both — and a cache key that folded those two predicates together
//! would return one predicate's cached live set for the other, a silently
//! wrong answer, not just a missed cache hit. A key built from anything other
//! than the raw bits (canonicalizing `-0.0`/`+0.0`, collapsing NaN payloads)
//! reintroduces exactly that bug. Distinct bit patterns simply never sharing
//! a key is the safe direction: worst case a redundant recompute, never a
//! wrong answer.
//!
//! `PredicateKey` exists so callers that need a `Predicate` as a `HashMap` key
//! (a per-snapshot resolved-row-id cache, see `crates/txn/src/snapshot.rs`)
//! don't reach for `format!("{predicate:?}")` either — `Debug` output is for
//! humans reading error messages, not a contracted-stable identity: a future
//! `Predicate` field addition or float-rendering change would silently change
//! which predicates collide.

use strata_storage::Value;

use crate::predicate::Predicate;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum KeyValue {
    Int(i64),
    /// The `f64`'s raw bit pattern — see the module doc for why this must
    /// NOT canonicalize `-0.0`/`+0.0` or fold NaN payloads together.
    Float(u64),
    Str(String),
}

impl From<&Value> for KeyValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Int64(v) => KeyValue::Int(*v),
            Value::Float64(v) => KeyValue::Float(v.to_bits()),
            Value::Utf8(v) => KeyValue::Str(v.clone()),
        }
    }
}

/// A hashable identity key for a [`Predicate`]: two `PredicateKey`s are equal
/// if and only if the `Predicate`s they were built from are the same
/// comparison variant over the same column against a value with the exact
/// same bit pattern (see the module doc for why bit-identity, not logical
/// float equality).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PredicateKey {
    discriminant: u8,
    column: String,
    value: KeyValue,
}

impl PredicateKey {
    /// Discriminants are assigned explicitly (not derived from enum
    /// declaration order) so `Predicate`'s own variant order can change
    /// without silently renumbering existing keys.
    fn discriminant(predicate: &Predicate) -> u8 {
        match predicate {
            Predicate::Eq(..) => 0,
            Predicate::Lt(..) => 1,
            Predicate::LtEq(..) => 2,
            Predicate::Gt(..) => 3,
            Predicate::GtEq(..) => 4,
        }
    }

    /// Approximate heap bytes owned by this key's variable-length fields —
    /// the column name, plus the value's own bytes if it's a UTF-8 string.
    /// `Int`/`Float` values contribute nothing extra (they're stored
    /// inline, not on the heap). For a cache charging a fixed per-entry
    /// overhead against a byte budget (see
    /// `crates/txn/src/live_set_cache.rs`'s `ENTRY_OVERHEAD_BYTES`), a fixed
    /// charge alone underestimates a predicate carrying a long column name
    /// or a long string value — this is the piece that closes that gap.
    #[must_use]
    pub fn variable_byte_size(&self) -> usize {
        self.column.len()
            + match &self.value {
                KeyValue::Str(s) => s.len(),
                KeyValue::Int(_) | KeyValue::Float(_) => 0,
            }
    }
}

impl From<&Predicate> for PredicateKey {
    fn from(predicate: &Predicate) -> Self {
        PredicateKey {
            discriminant: Self::discriminant(predicate),
            column: predicate.column().to_string(),
            value: KeyValue::from(predicate.value()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use super::*;

    fn hash_of(key: &PredicateKey) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn identical_predicates_produce_equal_keys() {
        let a = Predicate::Eq("category".to_string(), Value::Int64(3));
        let b = Predicate::Eq("category".to_string(), Value::Int64(3));
        assert_eq!(PredicateKey::from(&a), PredicateKey::from(&b));
        assert_eq!(
            hash_of(&PredicateKey::from(&a)),
            hash_of(&PredicateKey::from(&b))
        );
    }

    #[test]
    fn different_columns_produce_different_keys() {
        let a = Predicate::Eq("category".to_string(), Value::Int64(3));
        let b = Predicate::Eq("other".to_string(), Value::Int64(3));
        assert_ne!(PredicateKey::from(&a), PredicateKey::from(&b));
    }

    #[test]
    fn different_values_produce_different_keys() {
        let a = Predicate::Eq("category".to_string(), Value::Int64(3));
        let b = Predicate::Eq("category".to_string(), Value::Int64(4));
        assert_ne!(PredicateKey::from(&a), PredicateKey::from(&b));
    }

    #[test]
    fn different_comparison_variants_over_the_same_column_and_value_are_distinct() {
        let eq = Predicate::Eq("category".to_string(), Value::Int64(3));
        let lt = Predicate::Lt("category".to_string(), Value::Int64(3));
        let lteq = Predicate::LtEq("category".to_string(), Value::Int64(3));
        let gt = Predicate::Gt("category".to_string(), Value::Int64(3));
        let gteq = Predicate::GtEq("category".to_string(), Value::Int64(3));
        let keys = [
            PredicateKey::from(&eq),
            PredicateKey::from(&lt),
            PredicateKey::from(&lteq),
            PredicateKey::from(&gt),
            PredicateKey::from(&gteq),
        ];
        for i in 0..keys.len() {
            for j in (i + 1)..keys.len() {
                assert_ne!(
                    keys[i], keys[j],
                    "variant {i} and variant {j} must not collide"
                );
            }
        }
    }

    #[test]
    fn negative_zero_and_positive_zero_produce_different_keys() {
        // strata_query::mask's comparison kernel (arrow-rs) treats -0.0 and
        // +0.0 as bitwise-distinct for Eq — Eq("x", -0.0) and Eq("x", 0.0)
        // select different rows on a column holding both. A cache key that
        // folded them together would return one predicate's cached live set
        // for the other: a silently wrong answer, not just a missed reuse.
        let a = Predicate::Eq("amount".to_string(), Value::Float64(0.0));
        let b = Predicate::Eq("amount".to_string(), Value::Float64(-0.0));
        assert_ne!(PredicateKey::from(&a), PredicateKey::from(&b));
    }

    #[test]
    fn different_nan_payloads_produce_different_keys() {
        // Two NaN bit patterns are, in general, distinguishable to arrow's
        // bitwise comparison kernel (total-order semantics), so this key
        // must not collapse them — that direction (recompute unnecessarily)
        // is safe; folding them together is not.
        let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
        assert!(other_nan.is_nan());
        assert_ne!(
            f64::NAN.to_bits(),
            other_nan.to_bits(),
            "test assumption: these NaNs differ in bit pattern"
        );
        let a = Predicate::Eq("amount".to_string(), Value::Float64(f64::NAN));
        let b = Predicate::Eq("amount".to_string(), Value::Float64(other_nan));
        assert_ne!(PredicateKey::from(&a), PredicateKey::from(&b));
    }

    #[test]
    fn the_same_bit_pattern_always_produces_equal_keys() {
        let a = Predicate::Eq("amount".to_string(), Value::Float64(f64::NAN));
        let b = Predicate::Eq("amount".to_string(), Value::Float64(f64::NAN));
        assert_eq!(PredicateKey::from(&a), PredicateKey::from(&b));
    }

    #[test]
    fn variable_byte_size_grows_with_the_column_name_and_a_string_value() {
        let short = PredicateKey::from(&Predicate::Eq(
            "x".to_string(),
            Value::Utf8("y".to_string()),
        ));
        let long = PredicateKey::from(&Predicate::Eq(
            "a_much_longer_column_name".to_string(),
            Value::Utf8("z".repeat(1000)),
        ));
        assert!(
            long.variable_byte_size() > short.variable_byte_size() + 900,
            "a long column name and a 1000-byte string value must be reflected \
             in variable_byte_size, not just a fixed per-entry charge: short={}, long={}",
            short.variable_byte_size(),
            long.variable_byte_size()
        );
    }

    #[test]
    fn variable_byte_size_is_zero_extra_for_numeric_values() {
        let int_key = PredicateKey::from(&Predicate::Eq("x".to_string(), Value::Int64(3)));
        assert_eq!(int_key.variable_byte_size(), "x".len());
        let float_key = PredicateKey::from(&Predicate::Eq("x".to_string(), Value::Float64(3.0)));
        assert_eq!(float_key.variable_byte_size(), "x".len());
    }

    #[test]
    fn utf8_values_are_compared_by_content() {
        let a = Predicate::Eq("name".to_string(), Value::Utf8("a".to_string()));
        let b = Predicate::Eq("name".to_string(), Value::Utf8("a".to_string()));
        let c = Predicate::Eq("name".to_string(), Value::Utf8("b".to_string()));
        assert_eq!(PredicateKey::from(&a), PredicateKey::from(&b));
        assert_ne!(PredicateKey::from(&a), PredicateKey::from(&c));
    }

    #[test]
    fn different_value_variants_are_distinct_even_with_similar_rendering() {
        let int_key = Predicate::Eq("x".to_string(), Value::Int64(3));
        let float_key = Predicate::Eq("x".to_string(), Value::Float64(3.0));
        let str_key = Predicate::Eq("x".to_string(), Value::Utf8("3".to_string()));
        assert_ne!(PredicateKey::from(&int_key), PredicateKey::from(&float_key));
        assert_ne!(PredicateKey::from(&int_key), PredicateKey::from(&str_key));
        assert_ne!(PredicateKey::from(&float_key), PredicateKey::from(&str_key));
    }
}
