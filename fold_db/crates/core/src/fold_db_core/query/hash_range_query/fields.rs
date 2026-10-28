//! Project accepted keys through the captured read and its selected namespaces.

use super::*;

impl HashRangeQueryProcessor {
    pub(super) async fn query_exact_keys(
        &self,
        schema: &mut Schema,
        fields: &[String],
        page_keys: &[KeyValue],
        captured: CokeyRows,
        options: &FieldQueryOptions<'_>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let selected = exact_key_selected_fields(schema, fields, options.order_by);
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
}
