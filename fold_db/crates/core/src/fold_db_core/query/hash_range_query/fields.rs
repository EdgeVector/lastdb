//! Field scans, predicate key matching, key ordering and exact-key queries.

use super::*;
use crate::json_order::compare_json_values;

impl HashRangeQueryProcessor {
    pub(super) fn scan_fields(
        &self,
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
        // Candidate discovery already prefers the declared key molecule. Only
        // blinded reads need its value in pass A to recover the API hash.
        // Adding it on plain homes would disable the empty-spine fallback for
        // sibling layouts whose shared fields establish their keys.
        if self.db_ops.atoms().key_codec().encoding() != crate::atom::HashKeyEncoding::Plain {
            if let Some(key_field) = schema
                .key
                .as_ref()
                .and_then(|key| key.hash_field.as_ref().or(key.range_field.as_ref()))
            {
                if schema.runtime_fields.contains_key(key_field.as_str()) {
                    fields.push(key_field.clone());
                }
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
        &self,
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
        &self,
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

    pub(super) async fn query_exact_keys(
        &self,
        schema: &mut Schema,
        fields: &[String],
        page_keys: &[KeyValue],
        key_sources: &HashMap<KeyValue, KeySource>,
        include_tombstones: bool,
        request_concurrency: Option<usize>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let selected = Self::exact_key_selected_fields(schema, fields);
        let mut result: HashMap<String, HashMap<KeyValue, FieldValue>> = selected
            .iter()
            .map(|field| (field.clone(), HashMap::new()))
            .collect();
        if selected.is_empty() || page_keys.is_empty() {
            return Ok(result);
        }

        let pending = self
            .collect_cokey_secondary_pending(
                schema,
                selected,
                page_keys,
                key_sources,
                include_tombstones,
                request_concurrency,
            )
            .await?;
        self.fill_cokey_secondaries(schema, pending, include_tombstones, &mut result)
            .await?;
        Ok(result)
    }

    /// Fields pass B hydrates: the projection, plus the key field for identity.
    fn exact_key_selected_fields(schema: &Schema, fields: &[String]) -> Vec<String> {
        let mut selected: Vec<String> = if fields.is_empty() {
            schema.runtime_field_names()
        } else {
            fields
                .iter()
                .filter(|f| schema.runtime_fields.contains_key(f.as_str()))
                .cloned()
                .collect()
        };
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
        selected
    }
}
