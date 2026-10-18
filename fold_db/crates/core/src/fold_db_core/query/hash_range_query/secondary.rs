//! Secondary-field key resolution at the current head and `as_of`.

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
        let binding = crate::storage::disk_pack::logical_paths::field::FieldScope::for_runtime(
            self.db_ops.namespaced_store().disk_pack_runtime().is_some(),
            schema,
            fname,
        )?;
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
            if let Some(slot) = context.field.disk_slot_for_key(kv) {
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
                    .key_codec_for_molecule(context.molecule)
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
            // The body location belongs to the actual winning tip spelling,
            // including a fallback encoding or alternate molecule UUID.
            let partition = crate::atom::AtomPartition::from_record_key(&tip_key);
            pending.push((
                context.name.to_string(),
                kv.clone(),
                rec.entry.atom_uuid.clone(),
                rec.meta.clone(),
                writer,
                prefix.clone(),
                partition,
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
            .filter_map(|key| context.field.disk_slot_for_key(key))
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
            .map(|(key, atom, meta, writer, partition)| (key, (atom, meta, writer, partition)))
            .collect();
        for key in keys {
            let Some((hash, range)) = context.field.disk_slot_for_key(key) else {
                continue;
            };
            let selected =
                KeyValue::new(Some(hash), (kind == FieldKind::HashRange).then_some(range));
            if let Some((atom, meta, writer, partition)) = matches.get(&selected) {
                pending.push((
                    context.name.to_string(),
                    key.clone(),
                    atom.clone(),
                    meta.clone(),
                    writer.clone(),
                    prefix.clone(),
                    partition.clone(),
                ));
            }
        }
        Ok(true)
    }

    /// `as_of` secondary fan-out: hydrate + rewind per storage prefix, keep
    /// only `page_keys` (still one plan — keys from primary, values by key).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn secondary_matches_as_of(
        &self,
        schema: &mut Schema,
        fname: &str,
        page_keys: &[KeyValue],
        key_sources: &HashMap<KeyValue, KeySource>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
        pending: &mut Vec<SecondaryPending>,
    ) -> Result<(), SchemaError> {
        let field = schema.runtime_fields.get(fname).ok_or_else(|| {
            SchemaError::InvalidField(format!(
                "co-key secondary field '{fname}' missing from schema runtime"
            ))
        })?;

        let mut by_prefix: HashMap<KeySource, Vec<KeyValue>> = HashMap::new();
        for kv in page_keys {
            let src = key_sources.get(kv).cloned().unwrap_or(None);
            by_prefix.entry(src).or_default().push(kv.clone());
        }

        // `collect_matches` yields keys in THIS field's storage form (the range
        // is already decoded back to API form, the hash is not). The caller's
        // page keys are API-form, so encode them into this molecule's space to
        // match on, and keep the way back — the row must be filed under the key
        // the caller handed us, not under this field's blind token.
        let mol_uuid = field.common().molecule_uuid().cloned();
        let mut want: HashMap<KeyValue, KeyValue> = HashMap::with_capacity(page_keys.len());
        for kv in page_keys {
            let storage_kv = match (mol_uuid.as_deref(), kv.hash.as_deref()) {
                (Some(mol), Some(api_hash)) => KeyValue::new(
                    Some(self.db_ops.atoms().storage_hash(mol, api_hash)?),
                    kv.range.clone(),
                ),
                _ => kv.clone(),
            };
            want.insert(storage_kv, kv.clone());
        }

        for (prefix, _) in by_prefix {
            let mut cloned = field.cloned_without_molecule();
            if let Some(ref ns) = prefix {
                cloned.common_mut().set_storage_prefix(Some(ns.clone()));
            }
            // Full hydrate + rewind at as_of; filter to primary keys in memory.
            let matches = cloned
                .collect_matches(
                    &self.db_ops,
                    Some(full_span_page()),
                    as_of,
                    include_tombstones,
                )
                .await?;
            for (kv, uuid, meta, writer, partition) in matches {
                let Some(kv) = want.get(&kv).cloned() else {
                    continue;
                };
                // Only accept this key from the prefix that owns it.
                let owned = key_sources.get(&kv).cloned().unwrap_or(None);
                if owned != prefix {
                    continue;
                }
                pending.push((
                    fname.to_string(),
                    kv,
                    uuid,
                    meta,
                    writer,
                    prefix.clone(),
                    partition,
                ));
            }
        }
        Ok(())
    }
}
