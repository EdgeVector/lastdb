//! Secondary-field key resolution at the current head and `as_of`.

use super::*;

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
        // lint:fn-size-ok verbatim move from hash_range_query.rs; splitting this function is separate work
        let field = schema.runtime_fields.get(fname).ok_or_else(|| {
            SchemaError::InvalidField(format!(
                "co-key secondary field '{fname}' missing from schema runtime"
            ))
        })?;
        let Some(mol_uuid) = field.common().molecule_uuid().cloned() else {
            return Ok(());
        };

        // Group page keys by the storage prefix that owns them.
        let mut by_prefix: HashMap<KeySource, Vec<KeyValue>> = HashMap::new();
        for kv in page_keys {
            let src = key_sources.get(kv).cloned().unwrap_or(None);
            by_prefix.entry(src).or_default().push(kv.clone());
        }

        for (prefix, keys) in by_prefix {
            let mut slots = Vec::with_capacity(keys.len());
            let mut kv_for_slots = Vec::with_capacity(keys.len());
            for kv in &keys {
                if let Some(slot) = field.disk_slot_for_key(kv) {
                    slots.push(slot);
                    kv_for_slots.push(kv.clone());
                }
            }
            if slots.is_empty() {
                continue;
            }
            // Resident tips are keyed in API form. Keep that as the invariant:
            // `disk_slot_for_key` returns API-form `(hash, range)`, durable
            // storage encodes on lookup, and resident probes use the same API
            // slot. This avoids blinding an already-blinded co-key page key.
            let use_resident_tips =
                crate::db_operations::resident_read::tip_reads_resident_first(prefix.as_deref());
            let mut storage_slots = Vec::with_capacity(slots.len());
            let mut storage_kv_for_slots = Vec::with_capacity(kv_for_slots.len());
            for ((h, r), kv) in slots.iter().zip(kv_for_slots.iter()) {
                if use_resident_tips {
                    if let Some(tip) = self.db_ops.resident().resolve_tip(&mol_uuid, h, r) {
                        let tip = tip.value;
                        if !include_tombstones
                            && tip.key_metadata.as_ref().is_some_and(|m| m.tombstoned)
                        {
                            continue;
                        }
                        let tip_key = self
                            .db_ops
                            .atoms()
                            .key_codec_for_molecule(&mol_uuid)
                            .api_hash_range_record_key(&mol_uuid, h, r)
                            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                        let partition = crate::atom::AtomPartition::from_record_key(&tip_key);
                        pending.push((
                            fname.to_string(),
                            kv.clone(),
                            tip.atom_uuid,
                            tip.key_metadata,
                            (!tip.writer_pubkey.is_empty()).then_some(tip.writer_pubkey),
                            prefix.clone(),
                            partition,
                        ));
                        continue;
                    }
                }
                storage_slots.push((h.clone(), r.clone()));
                storage_kv_for_slots.push(kv.clone());
            }
            if storage_slots.is_empty() {
                continue;
            }
            let observed: HashMap<_, _> = storage_slots
                .iter()
                .map(|(h, r)| {
                    let control = self.db_ops.resident().observe_slot(&mol_uuid, h, r);
                    ((h.clone(), r.clone()), control)
                })
                .collect();
            let records = self
                .db_ops
                .atoms()
                .load_per_key_records_for_slots(&mol_uuid, prefix.as_deref(), &storage_slots)
                .await?;
            let mut by_slot = HashMap::with_capacity(records.len());
            for (h, r, rec) in records {
                by_slot.insert((h, r), rec);
            }
            for kv in &storage_kv_for_slots {
                let Some((h, r)) = field.disk_slot_for_key(kv) else {
                    continue;
                };
                let Some(rec) = by_slot.get(&(h.clone(), r.clone())) else {
                    continue;
                };
                if !include_tombstones && rec.meta.as_ref().is_some_and(|m| m.tombstoned) {
                    continue;
                }
                if use_resident_tips {
                    self.db_ops.resident().rehydrate_tip_at(
                        crate::resident::ResidentTip {
                            molecule_uuid: mol_uuid.clone(),
                            hash: h.clone(),
                            range: r.clone(),
                            atom_uuid: rec.entry.atom_uuid.clone(),
                            written_at: rec.entry.written_at,
                            logical_counter: rec.entry.logical_counter,
                            device_id: rec.entry.lww_device().to_string(),
                            mutation_uuid: rec.entry.mutation_uuid.clone(),
                            key_metadata: rec.meta.clone(),
                            writer_pubkey: rec.entry.writer_pubkey.clone(),
                        },
                        observed
                            .get(&(h.clone(), r.clone()))
                            .map(crate::resident::SlotRead::revisions),
                    );
                }
                let writer = {
                    let pk = rec.entry.writer_pubkey.clone();
                    if pk.is_empty() {
                        None
                    } else {
                        Some(pk)
                    }
                };
                // `(h, r)` is the API-form slot — the form `load_per_key_records_for_slots`
                // takes, because it encodes on the way in. The body partition
                // must be byte-identical to the one the TIP is filed under, so
                // encode here too: deriving it from the API hash on a blinded
                // home names a partition that does not exist.
                let tip_key = self
                    .db_ops
                    .atoms()
                    .key_codec_for_molecule(&mol_uuid)
                    .api_hash_range_record_key(&mol_uuid, &h, &r)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                let partition = crate::atom::AtomPartition::from_record_key(&tip_key);
                pending.push((
                    fname.to_string(),
                    kv.clone(),
                    rec.entry.atom_uuid.clone(),
                    rec.meta.clone(),
                    writer,
                    prefix.clone(),
                    partition,
                ));
            }
        }
        Ok(())
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
