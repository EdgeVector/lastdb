use super::*;

impl HashRangeQueryProcessor {
    /// Union the live keys of every counted field, in the node's own namespace
    /// and every received-from namespace. Also returns the molecule and storage
    /// prefix of the declared hash key field, captured while the field is in hand.
    pub(super) async fn gather_count_keys(
        &self,
        schema: &mut Schema,
        count_field_names: &[String],
        key_hash_field: Option<&str>,
        filter: Option<&HashRangeFilter>,
        received_from_namespaces: &[String],
    ) -> Result<
        (
            std::collections::HashSet<KeyValue>,
            Option<String>,
            Option<String>,
        ),
        SchemaError,
    > {
        let mut key_field_molecule: Option<String> = None;
        let mut key_field_storage_prefix: Option<String> = None;
        let mut keys: std::collections::HashSet<KeyValue> = std::collections::HashSet::new();
        for field_name in count_field_names {
            let Some(field) = schema.runtime_fields.get_mut(field_name) else {
                continue;
            };
            if key_hash_field == Some(field_name.as_str()) {
                key_field_molecule = field.common().molecule_uuid().cloned();
                key_field_storage_prefix = field.common().storage_prefix().map(ToString::to_string);
            }

            // Cheap count: enumerate molecule keys + filter `KeyMetadata.tombstoned`
            // without fetching atom bodies. The tombstone gate is applied on the
            // returned metadata inside `live_field_keys_flag_only` — passing
            // `include_tombstones = false` down is not sufficient on its own.
            let own = self.live_field_keys_flag_only(field, None, filter).await?;
            keys.extend(own);

            for namespace in received_from_namespaces {
                let shared = self
                    .live_field_keys_flag_only(field, Some(namespace.as_str()), filter)
                    .await?;
                keys.extend(shared);
            }
        }
        Ok((keys, key_field_molecule, key_field_storage_prefix))
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
                keys.extend(self.live_field_keys_flag_only(field, None, filter).await?);
                for namespace in received_from_namespaces {
                    keys.extend(
                        self.live_field_keys_flag_only(field, Some(namespace.as_str()), filter)
                            .await?,
                    );
                }
            }
        }
        Ok(())
    }

    /// Merge resident (not yet flushed) tips into the counted key set.
    pub(super) fn overlay_resident_count_keys(
        &self,
        keys: &mut std::collections::HashSet<KeyValue>,
        key_field_molecule: Option<&str>,
        key_field_storage_prefix: Option<&str>,
        filter: Option<&HashRangeFilter>,
    ) {
        // A resident write can publish its tip before the durable molecule
        // index contains the new key. The page path overlays that tip before
        // it hydrates rows, but this count runs first and supplies the page
        // window. Merge the resident key set here so a fresh partition cannot
        // be counted as empty and then windowed away.
        if let (Some(molecule_uuid), Some((hash, start, end))) = (
            key_field_molecule,
            Self::resident_partition_interval(filter),
        ) {
            let resident = self.db_ops.resident();
            if crate::db_operations::resident_read::tip_reads_resident_first(
                &self.db_ops,
                key_field_storage_prefix,
            ) {
                let start_key = crate::resident::ResidentMoleculeKey::new(&hash, &start);
                let end_key = end
                    .as_deref()
                    .map(|end| crate::resident::ResidentMoleculeKey::new(&hash, end));
                let snapshot = resident.resident_key_set_range(
                    molecule_uuid,
                    Some(&start_key),
                    end_key.as_ref(),
                );
                keys.extend(
                    snapshot
                        .keys
                        .into_iter()
                        .map(|key| KeyValue::new(Some(key.hash), Some(key.range))),
                );
                for key in snapshot.tombstones {
                    keys.remove(&KeyValue::new(Some(key.hash), Some(key.range)));
                }
            }
        }
    }
}
