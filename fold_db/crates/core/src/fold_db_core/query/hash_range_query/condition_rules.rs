//! Storage-free field selection, condition matching, and result order.

use super::predicates::field_predicate_matches;
use super::CokeyRows;
use crate::json_order::compare_json_values;
use crate::schema::types::field::FieldValue;
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::operations::{FieldPredicate, QueryOrderBy, SortOrder};
use crate::schema::{Schema, SchemaError};
use std::collections::HashMap;

pub(super) fn condition_fields(
    schema: &Schema,
    predicates: &[FieldPredicate],
    value_filters: &[crate::schema::types::ValueFilter],
    order_by: Option<&QueryOrderBy>,
) -> Result<Vec<String>, SchemaError> {
    let mut fields = Vec::new();
    for predicate in predicates {
        fields.push(predicate.field_name().to_string());
    }
    for filter in value_filters {
        fields.push(filter.field_name().to_string());
    }
    if let Some(order_by) = order_by {
        fields.push(order_by.field.clone());
    }
    // Candidate discovery already prefers the declared key molecule. Every
    // hash is blinded, so pass A needs its value to recover the API hash.
    if let Some(key_field) = schema
        .key
        .as_ref()
        .and_then(|key| key.hash_field.as_ref().or(key.range_field.as_ref()))
    {
        if schema.runtime_fields.contains_key(key_field.as_str()) {
            fields.push(key_field.clone());
        }
    }
    fields.sort();
    fields.dedup();

    if fields.is_empty() {
        let Some(key_field) = schema
            .key
            .as_ref()
            .and_then(|k| k.hash_field.as_ref().or(k.range_field.as_ref()))
            .cloned()
            .or_else(|| schema.runtime_field_names().into_iter().next())
        else {
            return Ok(Vec::new());
        };
        fields.push(key_field);
    }

    for field in &fields {
        if !schema.runtime_fields.contains_key(field) {
            return Err(SchemaError::InvalidField(format!(
                "query condition references unknown field '{field}' on schema '{}'",
                schema.name
            )));
        }
    }
    Ok(fields)
}

pub(super) fn matching_field_predicate_keys(
    pass_a: &CokeyRows,
    predicates: &[FieldPredicate],
    value_filters: &[crate::schema::types::ValueFilter],
) -> Vec<KeyValue> {
    let mut keys: Vec<KeyValue> = pass_a
        .page_keys
        .iter()
        .filter(|key| {
            if !pass_a
                .fields
                .values()
                .any(|values| values.contains_key(*key))
            {
                return false;
            }
            let matches_predicates = predicates.iter().all(|predicate| {
                let value = pass_a
                    .fields
                    .get(predicate.field_name())
                    .and_then(|values| values.get(*key))
                    .map(|field| &field.value);
                field_predicate_matches(predicate, value)
            });
            // Numeric filters retain the established missing-value policy:
            // only a concrete value that fails its comparison drops a key.
            matches_predicates
                && value_filters.iter().all(|filter| {
                    pass_a
                        .fields
                        .get(filter.field_name())
                        .and_then(|values| values.get(*key))
                        .is_none_or(|field| filter.matches(&field.value))
                })
        })
        .cloned()
        .collect();
    keys.sort_by(KeyValue::cmp_page_order);
    keys
}

pub(super) fn sort_keys_by_field(
    keys: &mut [KeyValue],
    fields: &HashMap<String, HashMap<KeyValue, FieldValue>>,
    order_by: &QueryOrderBy,
) {
    let field_values = fields.get(&order_by.field);
    keys.sort_by(|a, b| {
        let av = field_values
            .and_then(|values| values.get(a))
            .map(|fv| &fv.value);
        let bv = field_values
            .and_then(|values| values.get(b))
            .map(|fv| &fv.value);
        let primary = compare_json_values(av, bv);
        let ordered = match order_by.order.as_ref() {
            Some(SortOrder::Desc) => primary.reverse(),
            _ => primary,
        };
        ordered.then_with(|| a.cmp_page_order(b))
    });
}

/// Pass B retains key and sort helpers without changing candidate discovery.
pub(super) fn exact_key_selected_fields(
    schema: &Schema,
    fields: &[String],
    order_by: Option<&QueryOrderBy>,
) -> Vec<String> {
    let mut selected = super::field_rules::selected_fields(schema, fields);
    // Pass B formats records from the field maps it hydrates. Carry the key
    // field so a kept row with none of the projected values still has an
    // identity map entry and cannot disappear after satisfying `Absent`.
    if let Some(key_field) = schema
        .key
        .as_ref()
        .and_then(|key| key.hash_field.as_ref().or(key.range_field.as_ref()))
    {
        if schema.runtime_fields.contains_key(key_field.as_str())
            && !selected.iter().any(|field| field == key_field)
        {
            selected.push(key_field.clone());
        }
    }
    // The host orders its formatted rows after the core returns its field
    // maps. Keep this captured value internally, then let its formatter
    // remove the helper from the public projection and metadata.
    if let Some(order_by) = order_by {
        if !selected.contains(&order_by.field) {
            selected.push(order_by.field.clone());
        }
    }
    selected
}
