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
        captured: CokeyRows,
        options: &FieldQueryOptions<'_>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let selected = Self::exact_key_selected_fields(schema, fields, options.order_by);
        let mut result: HashMap<String, HashMap<KeyValue, FieldValue>> = selected
            .iter()
            .map(|field| (field.clone(), HashMap::new()))
            .collect();
        if selected.is_empty() || page_keys.is_empty() {
            return Ok(result);
        }

        let CokeyRows {
            fields: mut captured_fields,
            page_keys: captured_page_keys,
            key_sources: mut captured_sources,
            primary_field,
        } = captured;
        drop(captured_page_keys);
        let mut missing = Vec::new();
        for name in selected {
            // Only secondary values have the projection reader's metadata.
            if name == primary_field {
                missing.push(name);
                continue;
            }
            // A captured empty map also proves absence in this held read.
            if let Some(mut values) = captured_fields.remove(&name) {
                // Keep output capacity proportional to accepted keys as well.
                let values = page_keys
                    .iter()
                    .filter_map(|key| values.remove_entry(key))
                    .collect();
                result.insert(name, values);
            } else {
                missing.push(name);
            }
        }
        drop(captured_fields);
        if missing.is_empty() {
            return Ok(result);
        }
        let key_sources = page_keys
            .iter()
            .filter_map(|key| captured_sources.remove_entry(key))
            .collect();
        drop(captured_sources);

        let pending = self
            .collect_cokey_secondary_pending(
                schema,
                missing,
                page_keys,
                &key_sources,
                options.include_tombstones,
                options.secondary_concurrency,
            )
            .await?;
        self.fill_cokey_secondaries(schema, pending, options.include_tombstones, &mut result)
            .await?;
        Ok(result)
    }

    /// Pass B retains key and sort helpers without changing candidate discovery.
    fn exact_key_selected_fields(
        schema: &Schema,
        fields: &[String],
        order_by: Option<&QueryOrderBy>,
    ) -> Vec<String> {
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
}
