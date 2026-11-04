//! Storage-free condition matching and result order.

use super::predicates::field_predicate_matches;
use crate::db_operations::record_store::KeyedRecord;
use crate::schema::types::operations::{FieldPredicate, QueryOrderBy, SortOrder};
use crate::schema::types::KeyValuePageOrder;
use crate::schema::{Schema, SchemaError};
use lastdb_model::json_order::compare_json_values;

/// Current field conditions for one captured query.
#[derive(Default)]
pub struct FieldQueryOptions<'a> {
    pub predicates: Option<&'a [FieldPredicate]>,
    pub value_filters: Option<&'a [crate::schema::types::ValueFilter]>,
    pub order_by: Option<&'a QueryOrderBy>,
    pub predicate_limit: Option<usize>,
}

impl FieldQueryOptions<'_> {
    /// Empty condition lists keep the direct path. Ordering or any explicit
    /// condition limit still needs all candidates before the result window.
    pub(super) fn requires_condition_pass(&self) -> bool {
        self.predicates
            .is_some_and(|predicates| !predicates.is_empty())
            || self
                .value_filters
                .is_some_and(|filters| !filters.is_empty())
            || self.order_by.is_some()
            || self.predicate_limit.is_some()
    }
}

/// Refuse a condition or sort field the schema does not declare.
pub(super) fn reject_unknown_conditions(
    schema: &Schema,
    predicates: &[FieldPredicate],
    value_filters: &[crate::schema::types::ValueFilter],
    order_by: Option<&QueryOrderBy>,
) -> Result<(), SchemaError> {
    let mut names = Vec::new();
    for predicate in predicates {
        names.push(predicate.field_name());
    }
    for filter in value_filters {
        names.push(filter.field_name());
    }
    if let Some(order_by) = order_by {
        names.push(order_by.field.as_str());
    }
    names.sort_unstable();
    names.dedup();
    let unknown: Vec<&str> = names
        .into_iter()
        .filter(|name| !schema.runtime_fields.contains_key(*name))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    Err(SchemaError::InvalidField(format!(
        "query condition references unknown field '{}' on schema '{}'",
        unknown.join(", "),
        schema.name
    )))
}

/// Keep live records whose stored fields match every predicate and filter.
/// A missing field is absent, so an `absent` predicate matches it.
pub(super) fn matching_records(
    rows: Vec<KeyedRecord>,
    predicates: &[FieldPredicate],
    value_filters: &[crate::schema::types::ValueFilter],
) -> Vec<KeyedRecord> {
    rows.into_iter()
        .filter(|(_, record)| {
            let matches_predicates = predicates.iter().all(|predicate| {
                let value = record.fields.get(predicate.field_name());
                field_predicate_matches(predicate, value)
            });
            matches_predicates
                && value_filters.iter().all(|filter| {
                    record
                        .fields
                        .get(filter.field_name())
                        .is_none_or(|value| filter.matches(value))
                })
        })
        .collect()
}

/// Preserve key order unless a value order is requested, then limit.
pub(super) fn order_records(
    mut rows: Vec<KeyedRecord>,
    order_by: Option<&QueryOrderBy>,
    limit: Option<usize>,
) -> Vec<KeyedRecord> {
    if let Some(order_by) = order_by {
        rows.sort_by(|left, right| {
            let av = left.1.fields.get(&order_by.field);
            let bv = right.1.fields.get(&order_by.field);
            let primary = compare_json_values(av, bv);
            let ordered = match order_by.order.as_ref() {
                Some(SortOrder::Desc) => primary.reverse(),
                _ => primary,
            };
            ordered.then_with(|| left.0.cmp_page_order(&right.0))
        });
    } else {
        rows.sort_by(|left, right| left.0.cmp_page_order(&right.0));
    }
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    rows
}
