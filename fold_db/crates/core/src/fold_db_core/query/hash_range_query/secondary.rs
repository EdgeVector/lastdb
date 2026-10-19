//! Secondary-field key resolution at the current head.

use super::*;

struct SecondaryFieldContext<'a> {
    field: &'a crate::schema::types::field::FieldVariant,
    name: &'a str,
    molecule: &'a str,
}

impl HashRangeQueryProcessor {
    /// Current-head secondary fan-out: batch-load mk: slots per storage prefix.
    pub(super) async fn secondary_matches_current(
        &self,
        schema: &Schema,
        fname: &str,
        page_keys: &[KeyValue],
        key_sources: &HashMap<KeyValue, KeySource>,
        include_tombstones: bool,
        pending: &mut Vec<SecondaryPending>,
    ) -> Result<(), SchemaError> {
        let field = schema.runtime_fields.get(fname).ok_or_else(|| {
            SchemaError::InvalidField(format!(
                "co-key secondary field '{fname}' missing from schema runtime"
            ))
        })?;
        let Some(mol_uuid) = field.common().molecule_uuid().cloned() else {
            return Ok(());
        };
        let binding =
            crate::storage::disk_pack::logical_paths::field::FieldScope::new(schema, fname)?;
        let context = SecondaryFieldContext {
            field,
            name: fname,
            molecule: &mol_uuid,
        };

        // Group page keys by the storage prefix that owns them.
        let mut by_prefix: HashMap<KeySource, Vec<KeyValue>> = HashMap::new();
        for kv in page_keys {
            let src = key_sources.get(kv).cloned().unwrap_or(None);
            by_prefix.entry(src).or_default().push(kv.clone());
        }

        for (prefix, keys) in by_prefix {
            binding
                .run(self.secondary_current_for_prefix(
                    &context,
                    prefix,
                    &keys,
                    include_tombstones,
                    pending,
                ))
                .await?;
        }
        Ok(())
    }

    /// Resolve one storage prefix's keys through the core model and disk root.
    async fn secondary_current_for_prefix(
        &self,
        context: &SecondaryFieldContext<'_>,
        prefix: KeySource,
        keys: &[KeyValue],
        include_tombstones: bool,
        pending: &mut Vec<SecondaryPending>,
    ) -> Result<(), SchemaError> {
        if self.secondary_warm_for_prefix(context, &prefix, keys, include_tombstones, pending)? {
            return Ok(());
        }
        let mut slots = Vec::with_capacity(keys.len());
        let mut kv_for_slots = Vec::with_capacity(keys.len());
        for kv in keys {
            if let Some(slot) = context.field.slot_coordinates(kv) {
                slots.push(slot);
                kv_for_slots.push(kv.clone());
            }
        }
        if slots.is_empty() {
            return Ok(());
        }
        self.secondary_current_load_slots(
            context,
            &prefix,
            include_tombstones,
            &slots,
            &kv_for_slots,
            pending,
        )
        .await
    }

    /// Batch-load selected records for keys not covered by the complete core result.
    async fn secondary_current_load_slots(
        &self,
        context: &SecondaryFieldContext<'_>,
        prefix: &KeySource,
        include_tombstones: bool,
        storage_slots: &[(String, String)],
        storage_kv_for_slots: &[KeyValue],
        pending: &mut Vec<SecondaryPending>,
    ) -> Result<(), SchemaError> {
        let records = self
            .db_ops
            .atoms()
            .load_per_key_records_for_slots(context.molecule, prefix.as_deref(), storage_slots)
            .await?;
        for ((kv, (h, r)), record) in storage_kv_for_slots.iter().zip(storage_slots).zip(records) {
            let Some((tip_key, rec)) = record else {
                let candidates = self
                    .db_ops
                    .atoms()
                    .key_codec_for_molecule(context.molecule)?
                    .api_hash_range_record_keys_for_read(context.molecule, h, r)
                    .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
                self.db_ops.atoms().note_missing_query_key(
                    prefix.as_deref(),
                    context.molecule,
                    kv,
                    &candidates,
                )?;
                continue;
            };
            self.db_ops.atoms().note_selected_query_tip(
                prefix.as_deref(),
                context.molecule,
                kv,
                &rec,
                &tip_key,
            )?;
            if !include_tombstones && rec.meta.as_ref().is_some_and(|m| m.tombstoned) {
                continue;
            }
            let writer = {
                let pk = rec.entry.writer_pubkey.clone();
                if pk.is_empty() {
                    None
                } else {
                    Some(pk)
                }
            };
            pending.push((
                context.name.to_string(),
                kv.clone(),
                rec.entry.atom_uuid.clone(),
                rec.meta.clone(),
                writer,
                prefix.clone(),
            ));
        }
        Ok(())
    }

    /// A fully known exact key batch uses the same core molecule as primary reads.
    fn secondary_warm_for_prefix(
        &self,
        context: &SecondaryFieldContext<'_>,
        prefix: &KeySource,
        keys: &[KeyValue],
        include_tombstones: bool,
        pending: &mut Vec<SecondaryPending>,
    ) -> Result<bool, SchemaError> {
        use crate::schema::types::field::FieldKind;
        let kind = context.field.kind;
        if !matches!(kind, FieldKind::Hash | FieldKind::HashRange) {
            return Ok(false);
        }
        let pairs: Vec<_> = keys
            .iter()
            .filter_map(|key| context.field.slot_coordinates(key))
            .collect();
        let filter = HashRangeFilter::HashRangeKeys(pairs);
        let Some(matches) = crate::storage::disk_pack::logical_paths::core_filter::matches(
            self.db_ops.namespaced_store().disk_pack_runtime(),
            prefix.as_deref(),
            Some(context.molecule),
            kind,
            &filter,
            include_tombstones,
        )?
        else {
            return Ok(false);
        };
        let matches: HashMap<_, _> = matches
            .into_iter()
            .map(|(key, atom, meta, writer)| (key, (atom, meta, writer)))
            .collect();
        for key in keys {
            let Some((hash, range)) = context.field.slot_coordinates(key) else {
                continue;
            };
            let selected =
                KeyValue::new(Some(hash), (kind == FieldKind::HashRange).then_some(range));
            if let Some((atom, meta, writer)) = matches.get(&selected) {
                pending.push((
                    context.name.to_string(),
                    key.clone(),
                    atom.clone(),
                    meta.clone(),
                    writer.clone(),
                    prefix.clone(),
                ));
            }
        }
        Ok(true)
    }
}
