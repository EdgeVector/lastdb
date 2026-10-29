//! Resolve each field's selected sources through the core model or disk slots.

use std::collections::HashMap;

use crate::schema::types::field::{FieldVariant, HashRangeFilter};
use crate::schema::types::key_value::KeyValue;
use crate::schema::{Schema, SchemaError};

use super::{secondary_rules, HashRangeQueryProcessor, KeySource, SecondaryPending};

struct SecondaryFieldContext<'a> {
    field: &'a FieldVariant,
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
    ) -> Result<Vec<SecondaryPending>, SchemaError> {
        let field = schema.runtime_fields.get(fname).ok_or_else(|| {
            SchemaError::InvalidField(format!(
                "co-key secondary field '{fname}' missing from schema runtime"
            ))
        })?;
        let Some(mol_uuid) = field.common().molecule_uuid().cloned() else {
            return Ok(Vec::new());
        };
        let binding =
            crate::storage::disk_pack::logical_paths::field::FieldScope::new(schema, fname)?;
        let context = SecondaryFieldContext {
            field,
            name: fname,
            molecule: &mol_uuid,
        };

        let mut pending = Vec::new();
        for (prefix, keys) in secondary_rules::keys_by_prefix(page_keys, key_sources) {
            binding
                .run(async {
                    // A complete core result also proves absence. Only an
                    // unavailable core result permits the disk-slot route.
                    if self.secondary_warm_for_prefix(
                        &context,
                        &prefix,
                        &keys,
                        include_tombstones,
                        &mut pending,
                    )? {
                        return Ok(());
                    }
                    let slots = secondary_rules::slot_batch(context.field, &keys);
                    if slots.storage_slots.is_empty() {
                        return Ok(());
                    }
                    self.secondary_current_load_slots(
                        &context,
                        &prefix,
                        include_tombstones,
                        &slots.storage_slots,
                        &slots.api_keys,
                        &mut pending,
                    )
                    .await
                })
                .await?;
        }
        Ok(pending)
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
                let base = self
                    .db_ops
                    .atoms()
                    .key_codec_for_molecule(context.molecule)?
                    .api_hash_range_record_key(context.molecule, h, r)
                    .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
                self.db_ops.atoms().note_missing_query_key(
                    prefix.as_deref(),
                    context.molecule,
                    kv,
                    &base,
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
                let pk = rec.entry.writer_pubkey().to_string();
                if pk.is_empty() {
                    None
                } else {
                    Some(pk)
                }
            };
            pending.push(SecondaryPending {
                field_name: context.name.to_string(),
                key: kv.clone(),
                atom_uuid: rec.entry.atom_uuid.clone(),
                metadata: rec.meta.clone(),
                writer_pubkey: writer,
                storage_prefix: prefix.clone(),
            });
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
                pending.push(SecondaryPending {
                    field_name: context.name.to_string(),
                    key: key.clone(),
                    atom_uuid: atom.clone(),
                    metadata: meta.clone(),
                    writer_pubkey: writer.clone(),
                    storage_prefix: prefix.clone(),
                });
            }
        }
        Ok(true)
    }
}
