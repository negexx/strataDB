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
//! `And`/`Or` predicates key on their full tree shape: `And(a, b)` and
//! `And(b, a)` deliberately produce DIFFERENT keys, even though
//! `and_kleene`/`or_kleene` are commutative and would compute the same live
//! set for either. Do not "fix" this by sorting operands or flattening
//! nested `And`/`Or` — that would turn this key's safety property from a
//! structural guarantee (two distinct trees can never collide) into a
//! semantic claim (this particular combinator happens to be commutative
//! today), which is exactly the kind of reasoning the `-0.0`/NaN handling
//! above exists to forbid. The cost of not normalizing is at most a
//! redundant recompute for a logically-equivalent-but-differently-shaped
//! predicate; the cost of normalizing incorrectly is a silently wrong
//! answer the day a non-commutative or order-sensitive combinator is added.
//!
//! `PredicateKey` exists so callers that need a `Predicate` as a `HashMap` key
//! (a per-snapshot resolved-row-id cache, see `crates/txn/src/snapshot.rs`)
//! don't reach for `format!("{predicate:?}")` either — `Debug` output is for
//! humans reading error messages, not a contracted-stable identity: a future
//! `Predicate` field addition or float-rendering change would silently change
//! which predicates collide, and `f64`'s `Debug` renders every NaN as `"NaN"`
//! regardless of payload, silently reintroducing the exact NaN-collision bug
//! the bit-pattern handling above exists to prevent.

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

/// Fixed per-interior-node charge `variable_byte_size` adds for every `And`/
/// `Or` node, on top of its children's own sizes. Approximates the two
/// `Box<Node>` children's heap allocations (`size_of::<Node>()` each, since
/// `Leaf`'s `String` + `KeyValue` + discriminant dominates the enum's size)
/// plus the enum's own tag overhead — deliberately not exact, just non-zero,
/// so a deeply-nested compound predicate (e.g. `Or(Or(Or(...)))` over many
/// distinct leaves) can't slip past `crates/txn/src/live_set_cache.rs`'s
/// byte budget uncounted the way a `0`-cost interior node would.
const NODE_OVERHEAD_BYTES: usize = 128;

/// A hashable identity key for a [`Predicate`]: two `PredicateKey`s are equal
/// if and only if the `Predicate`s they were built from have the exact same
/// tree shape, down to the same comparison variant over the same column
/// against a value with the exact same bit pattern at every leaf (see the
/// module doc for why bit-identity, not logical float equality, and why
/// `And`/`Or` are never normalized).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PredicateKey(Node);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Node {
    Leaf {
        discriminant: u8,
        column: String,
        value: KeyValue,
    },
    And(Box<Node>, Box<Node>),
    Or(Box<Node>, Box<Node>),
}

impl Node {
    /// Discriminants are assigned explicitly (not derived from enum
    /// declaration order) so `Predicate`'s own variant order can change
    /// without silently renumbering existing keys. Leaf-only: `And`/`Or`
    /// are distinguished by `Node`'s own variant, not by a discriminant
    /// field, so this is never called for a compound predicate.
    fn leaf_discriminant(predicate: &Predicate) -> u8 {
        match predicate {
            Predicate::Eq(..) => 0,
            Predicate::Lt(..) => 1,
            Predicate::LtEq(..) => 2,
            Predicate::Gt(..) => 3,
            Predicate::GtEq(..) => 4,
            Predicate::And(..) | Predicate::Or(..) => {
                unreachable!("leaf_discriminant is only reachable from Node::from's leaf arm")
            }
        }
    }

    fn variable_byte_size(&self) -> usize {
        match self {
            Node::Leaf { column, value, .. } => {
                column.len()
                    + match value {
                        KeyValue::Str(s) => s.len(),
                        KeyValue::Int(_) | KeyValue::Float(_) => 0,
                    }
            }
            Node::And(l, r) | Node::Or(l, r) => {
                NODE_OVERHEAD_BYTES + l.variable_byte_size() + r.variable_byte_size()
            }
        }
    }
}

impl From<&Predicate> for Node {
    fn from(predicate: &Predicate) -> Self {
        match predicate {
            Predicate::Eq(c, v)
            | Predicate::Lt(c, v)
            | Predicate::LtEq(c, v)
            | Predicate::Gt(c, v)
            | Predicate::GtEq(c, v) => Node::Leaf {
                discriminant: Node::leaf_discriminant(predicate),
                column: c.clone(),
                value: KeyValue::from(v),
            },
            Predicate::And(l, r) => {
                Node::And(Box::new(Node::from(&**l)), Box::new(Node::from(&**r)))
            }
            Predicate::Or(l, r) => Node::Or(Box::new(Node::from(&**l)), Box::new(Node::from(&**r))),
        }
    }
}

impl PredicateKey {
    /// Approximate heap bytes owned by this key: every leaf's column name
    /// plus its value's own bytes if it's a UTF-8 string, and a fixed
    /// per-node charge for every `And`/`Or` interior node (see
    /// `NODE_OVERHEAD_BYTES`). For a cache charging a fixed per-entry
    /// overhead against a byte budget (see
    /// `crates/txn/src/live_set_cache.rs`'s `ENTRY_OVERHEAD_BYTES`), a fixed
    /// charge alone underestimates a predicate carrying a long column name,
    /// a long string value, or -- for a compound predicate -- an arbitrarily
    /// deep tree of them; this is the piece that closes that gap.
    #[must_use]
    pub fn variable_byte_size(&self) -> usize {
        self.0.variable_byte_size()
    }
}

impl From<&Predicate> for PredicateKey {
    fn from(predicate: &Predicate) -> Self {
        PredicateKey(Node::from(predicate))
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

    #[test]
    fn and_and_or_over_the_same_operands_are_distinct() {
        let a = Predicate::Eq("x".to_string(), Value::Int64(1));
        let b = Predicate::Eq("y".to_string(), Value::Int64(2));
        let and = Predicate::And(Box::new(a.clone()), Box::new(b.clone()));
        let or = Predicate::Or(Box::new(a), Box::new(b));
        assert_ne!(PredicateKey::from(&and), PredicateKey::from(&or));
    }

    #[test]
    fn operand_order_is_significant() {
        // Deliberately not normalized -- see the module doc's explanation
        // of why And(a, b)/And(b, a) must stay distinct.
        let a = Predicate::Eq("x".to_string(), Value::Int64(1));
        let b = Predicate::Eq("y".to_string(), Value::Int64(2));
        let ab = Predicate::And(Box::new(a.clone()), Box::new(b.clone()));
        let ba = Predicate::And(Box::new(b), Box::new(a));
        assert_ne!(PredicateKey::from(&ab), PredicateKey::from(&ba));
    }

    #[test]
    fn a_compound_predicate_differing_only_in_a_deep_leaf_value_is_distinct() {
        // Regression test for the naive "just make it compile" fix this
        // type's redesign was written to rule out: reusing a single
        // column/value field with new discriminants for And/Or compiles
        // and passes every leaf-only test above, but silently collapses
        // every compound predicate sharing a first operand onto the same
        // key -- a live-set-cache hit returning the WRONG predicate's
        // cached rows, with no error.
        let a1 = Predicate::Eq("a".to_string(), Value::Int64(1));
        let b2 = Predicate::Eq("b".to_string(), Value::Int64(2));
        let b3 = Predicate::Eq("b".to_string(), Value::Int64(3));
        let left = Predicate::And(Box::new(a1.clone()), Box::new(b2));
        let right = Predicate::And(Box::new(a1), Box::new(b3));
        assert_ne!(PredicateKey::from(&left), PredicateKey::from(&right));
    }

    #[test]
    fn a_leaf_and_a_compound_wrapping_it_are_distinct() {
        let leaf = Predicate::Eq("a".to_string(), Value::Int64(1));
        let wrapped = Predicate::And(Box::new(leaf.clone()), Box::new(leaf.clone()));
        assert_ne!(PredicateKey::from(&leaf), PredicateKey::from(&wrapped));
    }

    #[test]
    fn nested_shape_is_significant() {
        let a = Predicate::Eq("a".to_string(), Value::Int64(1));
        let b = Predicate::Eq("b".to_string(), Value::Int64(2));
        let c = Predicate::Eq("c".to_string(), Value::Int64(3));
        let right_leaning = Predicate::And(
            Box::new(a.clone()),
            Box::new(Predicate::And(Box::new(b.clone()), Box::new(c.clone()))),
        );
        let left_leaning = Predicate::And(
            Box::new(Predicate::And(Box::new(a), Box::new(b))),
            Box::new(c),
        );
        assert_ne!(
            PredicateKey::from(&right_leaning),
            PredicateKey::from(&left_leaning)
        );
    }

    #[test]
    fn variable_byte_size_grows_with_tree_depth() {
        let leaf = Predicate::Eq("x".to_string(), Value::Int64(1));
        let shallow = Predicate::And(Box::new(leaf.clone()), Box::new(leaf.clone()));
        let deep = Predicate::And(
            Box::new(shallow.clone()),
            Box::new(Predicate::And(Box::new(leaf.clone()), Box::new(leaf))),
        );
        assert!(
            PredicateKey::from(&deep).variable_byte_size()
                > PredicateKey::from(&shallow).variable_byte_size()
        );
    }

    #[test]
    fn a_float_leaf_buried_inside_a_compound_predicate_still_keys_on_raw_bits() {
        let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
        let leaf = Predicate::Eq("x".to_string(), Value::Int64(1));
        let a = Predicate::And(
            Box::new(leaf.clone()),
            Box::new(Predicate::Eq(
                "amount".to_string(),
                Value::Float64(f64::NAN),
            )),
        );
        let b = Predicate::And(
            Box::new(leaf),
            Box::new(Predicate::Eq(
                "amount".to_string(),
                Value::Float64(other_nan),
            )),
        );
        assert_ne!(PredicateKey::from(&a), PredicateKey::from(&b));
    }
}
