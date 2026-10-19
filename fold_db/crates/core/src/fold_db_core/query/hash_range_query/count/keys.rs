use super::*;

impl HashRangeQueryProcessor {
    /// Union the live keys of every counted field, in the node's own namespace
    /// and every received-from namespace. Keyed readers already return API keys;
    /// the atom store resolves competing physical spellings before this point.
    pub(super) async fn gather_count_keys(
        &self,
        schema: &mut Schema,
        count_field_names: &[String],
        filter: Option<&HashRangeFilter>,
        received_from_namespaces: &[String],
    ) -> Result<std::collections::HashSet<KeyValue>, SchemaError> {
        let mut keys: std::collections::HashSet<KeyValue> = std::collections::HashSet::new();
        for field_name in count_field_names {
            if !schema.runtime_fields.contains_key(field_name) {
                continue;
            }
            let binding = crate::storage::disk_pack::logical_paths::field::FieldScope::new(
                schema, field_name,
            )?;
            let field = schema
                .runtime_fields
                .get_mut(field_name)
                .expect("count field checked");
            // Cheap count: enumerate molecule keys + filter `KeyMetadata.tombstoned`
            // without fetching atom bodies. The tombstone gate is applied on the
            // returned metadata inside `live_field_keys_flag_only` — passing
            // `include_tombstones = false` down is not sufficient on its own.
            let own = binding
                .run(self.live_field_keys_flag_only(field, None, filter))
                .await?;
            keys.extend(own);

            for namespace in received_from_namespaces {
                let shared = binding
                    .run(self.live_field_keys_flag_only(field, Some(namespace.as_str()), filter))
                    .await?;
                keys.extend(shared);
            }
        }
        Ok(keys)
    }

    /// Fall back to the sibling key spine when the preferred one is empty.
    pub(super) async fn extend_count_keys_from_fallback(
        &self,
        schema: &Schema,
        fields: &[String],
        filter: Option<&HashRangeFilter>,
        received_from_namespaces: &[String],
        keys: &mut std::collections::HashSet<KeyValue>,
    ) -> Result<(), SchemaError> {
        // Mirror the read path's multi-key-expand fallback. A sibling layout
        // can have no atoms under its own key field while sharing projected
        // field hashes with the layout that received the writes. Only fall
        // back when the preferred key spine is empty; a non-empty key spine is
        // authoritative and must not be unioned with sparse projection keys.
        if let Some(fallback) = Self::count_fallback_field(schema, fields) {
            if let Some(field) = schema.runtime_fields.get(&fallback) {
                let binding = crate::storage::disk_pack::logical_paths::field::FieldScope::new(
                    schema, &fallback,
                )?;
                keys.extend(
                    binding
                        .run(self.live_field_keys_flag_only(field, None, filter))
                        .await?,
                );
                for namespace in received_from_namespaces {
                    keys.extend(
                        binding
                            .run(self.live_field_keys_flag_only(
                                field,
                                Some(namespace.as_str()),
                                filter,
                            ))
                            .await?,
                    );
                }
            }
        }
        Ok(())
    }
}
