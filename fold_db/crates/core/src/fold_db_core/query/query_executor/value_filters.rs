use crate::schema::types::field::FieldValue;
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::ValueFilter;
use std::collections::HashMap;

/// Drop entries from the result whose value does not satisfy every
/// `ValueFilter`. Filters are AND'd together at the **row** (`KeyValue`)
/// level, per the LLM prompt contract and the `Query::value_filters`
/// doc -- a row that fails any filter is removed from *all* fields, not
/// just the field the filter targets.
///
/// A filter whose target field is absent from the result is a no-op. A row
/// that doesn't have a value in a filter's target field is likewise left alone
/// -- only rows that have a concrete value that fails the predicate get
/// dropped.
pub(super) fn apply_value_filters(
    mut results: HashMap<String, HashMap<KeyValue, FieldValue>>,
    value_filters: Option<&[ValueFilter]>,
) -> HashMap<String, HashMap<KeyValue, FieldValue>> {
    let Some(filters) = value_filters else {
        return results;
    };
    if filters.is_empty() {
        return results;
    }
    let mut to_drop: std::collections::HashSet<KeyValue> = std::collections::HashSet::new();
    for filter in filters {
        let field = filter.field_name();
        let Some(entries) = results.get(field) else {
            continue;
        };
        for (kv, fv) in entries {
            if !filter.matches(&fv.value) {
                to_drop.insert(kv.clone());
            }
        }
    }
    if !to_drop.is_empty() {
        for entries in results.values_mut() {
            entries.retain(|kv, _| !to_drop.contains(kv));
        }
    }
    results
}
