//! Field scans, predicate key matching, key ordering and exact-key queries.

use super::*;
use crate::json_order::compare_json_values;

impl HashRangeQueryProcessor {
    pub(super) fn scan_fields(
        &self,
        schema: &Schema,
        predicates: &[FieldPredicate],
        order_by: Option<&QueryOrderBy>,
    ) -> Result<Vec<String>, SchemaError> {
        let mut fields = Vec::new();
        for predicate in predicates {
            fields.push(predicate.field_name().to_string());
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
                .or_else(|| {
                    crate::record_molecule::declared_runtime_field_names(schema)
                        .into_iter()
                        .next()
                })
            else {
                return Ok(Vec::new());
            };
            fields.push(key_field);
        }

        for field in &fields {
            if !schema.runtime_fields.contains_key(field) {
                return Err(SchemaError::InvalidField(format!(
                    "field predicate references unknown field '{field}' on schema '{}'",
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
    ) -> Result<Vec<KeyValue>, SchemaError> {
        let records = records_from_field_map(&pass_a.fields);
        let mut keys: Vec<KeyValue> = pass_a
            .page_keys
            .iter()
            .filter(|key| {
                let Some(record) = records.get(*key) else {
                    return false;
                };
                predicates
                    .iter()
                    .all(|predicate| field_predicate_matches(predicate, &record.fields))
            })
            .cloned()
            .collect();
        keys.sort_by_key(KeyValue::to_storage_key);
        Ok(keys)
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
            ordered.then_with(|| a.to_storage_key().cmp(&b.to_storage_key()))
        });
    }

    pub(super) async fn query_exact_keys(
        &self,
        schema: &mut Schema,
        fields: &[String],
        page_keys: &[KeyValue],
        key_sources: &HashMap<KeyValue, KeySource>,
        include_tombstones: bool,
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
            .collect_exact_key_pending(
                schema,
                &selected,
                page_keys,
                key_sources,
                include_tombstones,
            )
            .await?;
        let values = self.resolve_pending_values(schema, &pending).await?;

        for (entry, value) in pending.into_iter().zip(values) {
            if let Some(value) = value {
                if include_tombstones || !crate::atom::is_tombstone_value(&value.value) {
                    result.entry(entry.0).or_default().insert(entry.1, value);
                }
            }
        }
        self.resolve_document_projection(
            schema,
            page_keys,
            key_sources,
            &mut result,
            include_tombstones,
        )
        .await?;
        Ok(result)
    }

    /// Fields pass B hydrates: the projection, plus the key field for identity.
    fn exact_key_selected_fields(schema: &Schema, fields: &[String]) -> Vec<String> {
        let mut selected: Vec<String> = if fields.is_empty() {
            crate::record_molecule::declared_runtime_field_names(schema)
        } else {
            fields
                .iter()
                .filter(|f| {
                    schema.runtime_fields.contains_key(f.as_str())
                        && !crate::record_molecule::is_record_molecule_field(f)
                })
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

    /// Find each selected field's current slot for every page key.
    async fn collect_exact_key_pending(
        &self,
        schema: &mut Schema,
        selected: &[String],
        page_keys: &[KeyValue],
        key_sources: &HashMap<KeyValue, KeySource>,
        include_tombstones: bool,
    ) -> Result<Vec<SecondaryPending>, SchemaError> {
        let mut pending: Vec<SecondaryPending> = Vec::new();
        for fname in selected {
            self.secondary_matches_current(
                schema,
                fname,
                page_keys,
                key_sources,
                include_tombstones,
                &mut pending,
            )
            .await?;
        }
        Ok(pending)
    }
}
