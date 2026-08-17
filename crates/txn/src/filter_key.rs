//! A hashable, structurally exact identity key for [`FilterExpression`].
//!
//! Filter expressions contain `f64` literals and therefore cannot derive
//! `Eq`/`Hash` directly. This key preserves the full tree shape and raw float
//! bits: different expressions may miss reuse, but must never share a cached
//! live set.

use crate::{Comparison, ComparisonOperator, FilterExpression, FilterLiteral};

const NODE_OVERHEAD_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FilterKey(Node);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Node {
    Compare {
        operator: u8,
        column: String,
        value: Literal,
    },
    And(Box<Node>, Box<Node>),
    Or(Box<Node>, Box<Node>),
    Not(Box<Node>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Literal {
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(u64),
    Utf8(String),
}

impl From<&FilterLiteral> for Literal {
    fn from(literal: &FilterLiteral) -> Self {
        match literal {
            FilterLiteral::Boolean(value) => Self::Boolean(*value),
            FilterLiteral::Int64(value) => Self::Int64(*value),
            FilterLiteral::UInt64(value) => Self::UInt64(*value),
            FilterLiteral::Float64(value) => Self::Float64(value.to_bits()),
            FilterLiteral::Utf8(value) => Self::Utf8(value.clone()),
        }
    }
}

impl Node {
    fn comparison_operator(operator: ComparisonOperator) -> u8 {
        match operator {
            ComparisonOperator::Equal => 0,
            ComparisonOperator::NotEqual => 1,
            ComparisonOperator::LessThan => 2,
            ComparisonOperator::LessThanOrEqual => 3,
            ComparisonOperator::GreaterThan => 4,
            ComparisonOperator::GreaterThanOrEqual => 5,
        }
    }

    fn from_comparison(comparison: &Comparison) -> Self {
        Self::Compare {
            operator: Self::comparison_operator(comparison.operator),
            column: comparison.column.clone(),
            value: Literal::from(&comparison.value),
        }
    }

    fn variable_byte_size(&self) -> usize {
        match self {
            Self::Compare { column, value, .. } => {
                column.len()
                    + match value {
                        Literal::Utf8(value) => value.len(),
                        Literal::Boolean(_)
                        | Literal::Int64(_)
                        | Literal::UInt64(_)
                        | Literal::Float64(_) => 0,
                    }
            }
            Self::And(left, right) | Self::Or(left, right) => {
                NODE_OVERHEAD_BYTES + left.variable_byte_size() + right.variable_byte_size()
            }
            Self::Not(inner) => NODE_OVERHEAD_BYTES + inner.variable_byte_size(),
        }
    }
}

impl From<&FilterExpression> for Node {
    fn from(filter: &FilterExpression) -> Self {
        match filter {
            FilterExpression::Compare(comparison) => Self::from_comparison(comparison),
            FilterExpression::And(left, right) => Self::And(
                Box::new(Self::from(&**left)),
                Box::new(Self::from(&**right)),
            ),
            FilterExpression::Or(left, right) => Self::Or(
                Box::new(Self::from(&**left)),
                Box::new(Self::from(&**right)),
            ),
            FilterExpression::Not(inner) => Self::Not(Box::new(Self::from(&**inner))),
        }
    }
}

impl FilterKey {
    #[must_use]
    pub(crate) fn variable_byte_size(&self) -> usize {
        self.0.variable_byte_size()
    }
}

impl From<&FilterExpression> for FilterKey {
    fn from(filter: &FilterExpression) -> Self {
        Self(Node::from(filter))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::FilterKey;
    use crate::{Comparison, ComparisonOperator, FilterExpression, FilterLiteral};

    fn compare(column: &str, value: FilterLiteral) -> FilterExpression {
        FilterExpression::Compare(Comparison {
            column: column.into(),
            operator: ComparisonOperator::Equal,
            value,
        })
    }

    #[test]
    fn identical_filters_produce_equal_keys() {
        let left = FilterExpression::And(
            Box::new(compare("selected", FilterLiteral::Boolean(true))),
            Box::new(compare("category", FilterLiteral::Utf8("news".into()))),
        );
        let right = left.clone();

        assert_eq!(FilterKey::from(&left), FilterKey::from(&right));
    }

    #[test]
    fn filters_with_distinct_deep_leaves_do_not_collide() {
        let left = FilterExpression::And(
            Box::new(compare("selected", FilterLiteral::Boolean(true))),
            Box::new(FilterExpression::Not(Box::new(compare(
                "score",
                FilterLiteral::Int64(3),
            )))),
        );
        let right = FilterExpression::And(
            Box::new(compare("selected", FilterLiteral::Boolean(true))),
            Box::new(FilterExpression::Not(Box::new(compare(
                "score",
                FilterLiteral::Int64(4),
            )))),
        );

        assert_ne!(FilterKey::from(&left), FilterKey::from(&right));
    }

    #[test]
    fn float_literals_key_on_raw_bits() {
        let positive_zero = compare("score", FilterLiteral::Float64(0.0));
        let negative_zero = compare("score", FilterLiteral::Float64(-0.0));

        assert_ne!(
            FilterKey::from(&positive_zero),
            FilterKey::from(&negative_zero)
        );
    }

    #[test]
    fn comparison_operators_have_distinct_cache_identities() {
        let operators = [
            ComparisonOperator::Equal,
            ComparisonOperator::NotEqual,
            ComparisonOperator::LessThan,
            ComparisonOperator::LessThanOrEqual,
            ComparisonOperator::GreaterThan,
            ComparisonOperator::GreaterThanOrEqual,
        ];
        let keys: HashSet<_> = operators
            .into_iter()
            .map(|operator| {
                FilterKey::from(&FilterExpression::Compare(Comparison {
                    column: "score".into(),
                    operator,
                    value: FilterLiteral::Int64(7),
                }))
            })
            .collect();

        assert_eq!(
            keys.len(),
            6,
            "each comparison meaning needs its own cache key"
        );
    }

    #[test]
    fn variable_size_counts_string_bytes_and_each_tree_node() {
        let filter = FilterExpression::And(
            Box::new(compare("tag", FilterLiteral::Utf8("rust".into()))),
            Box::new(FilterExpression::Not(Box::new(compare(
                "active",
                FilterLiteral::Boolean(true),
            )))),
        );

        // `tag` + `rust` is 7 bytes; `active` is 6 bytes; the `Not` and
        // `And` nodes each add 128 bytes, for 269 bytes total.
        assert_eq!(FilterKey::from(&filter).variable_byte_size(), 269);
    }
}
