use super::types::ArchiveMetadata;
use super::*;
use crate::atom::{molecule_key_codec, FieldKey, MutationEvent};
use crate::db_operations::atom_store::conflict_record::Recorded;
use crate::db_operations::atom_store::counter::RecordRefs;
use crate::db_operations::atom_store::{AtomStore, PerKeyRecord, RecordSlot};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use std::collections::{BTreeMap, HashMap};

impl AtomStore {
    pub(crate) async fn prepare_atom_history(
        &self,
        mode: HistoryMode,
        scope: Option<&str>,
        tips: &BTreeMap<String, Option<PerKeyRecord>>,
        old: &HashMap<String, PerKeyRecord>,
    ) -> Result<Recorded, SchemaError> {
        match mode {
            HistoryMode::Capture => Box::pin(self.capture_atom_history(scope, tips, old)).await,
            _ => Ok(Recorded::default()),
        }
    }
    pub(crate) async fn capture_atom_history(
        &self,
        scope: Option<&str>,
        tips: &BTreeMap<String, Option<PerKeyRecord>>,
        old: &HashMap<String, PerKeyRecord>,
    ) -> Result<Recorded, SchemaError> {
        let changed: Vec<_> = tips
            .iter()
            .filter_map(|(key, new)| {
                let previous = old.get(key)?;
                if new
                    .as_ref()
                    .is_some_and(|n| n.entry.atom_uuid == previous.entry.atom_uuid)
                {
                    return None;
                }
                let molecule = molecule_key_codec::molecule_uuid_from_storage_key(key)?;
                Some((key, new, previous, molecule))
            })
            .collect();
        let keys: Vec<_> = changed
            .iter()
            .map(|(_, _, _, m)| format!("ahf:v1:{m}"))
            .collect();
        let bindings: Vec<Option<HistoryBinding>> = self.main_store.get_items(&keys).await?;
        let policy_keys: Vec<_> = bindings
            .iter()
            .flatten()
            .map(|b| build_storage_key(scope, &format!("ahp:v1:{}", b.field)))
            .collect();
        let policies: Vec<Option<FieldHistoryPolicy>> =
            self.main_store.get_items(&policy_keys).await?;
        let mut policies = policies.into_iter();
        let mut recorded = Recorded::default();
        for ((key, new, previous, molecule), binding) in changed.into_iter().zip(bindings) {
            let Some(binding) = binding else { continue };
            let policy = policies.next().flatten().unwrap_or_default();
            if policy == FieldHistoryPolicy::Off {
                continue;
            }
            let (hash, range) =
                molecule_key_codec::decode_hash_range(key, molecule).ok_or_else(|| {
                    SchemaError::InvalidData("history tip has invalid coordinates".into())
                })?;
            let now = self.retention_state().clock_nanos();
            let expires_at = match policy {
                FieldHistoryPolicy::Ttl { seconds } => Some(
                    now.checked_add(
                        seconds.checked_mul(1_000_000_000).ok_or_else(|| {
                            SchemaError::InvalidData("history TTL overflow".into())
                        })?,
                    )
                    .ok_or_else(|| SchemaError::InvalidData("history expiry overflow".into()))?,
                ),
                _ => None,
            };
            let identity =
                serde_json::to_vec(&(key, &previous.entry, new.as_ref().map(|r| &r.entry)))
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
            let history_key = format!("{}{}", prefix(key)?, digest(&identity));
            let source = previous.clone();
            let event: MutationEvent = serde_json::from_value(serde_json::json!({
                "molecule_uuid": molecule, "timestamp": chrono::DateTime::from_timestamp_nanos(now as i64),
                "field_key": FieldKey::hash_range(&hash, &range),
                "old_atom_uuid": null, "new_atom_uuid": source.entry.atom_uuid,
            })).map_err(|e| SchemaError::InvalidData(e.to_string()))?;
            let (queue_key, mut rows) = self.archive_retention_rows(
                scope,
                &history_key,
                &RecordSlot {
                    molecule,
                    hash: &hash,
                    range: &range,
                },
                now,
                expires_at,
            )?;
            let record = ArchiveRecord {
                event,
                archive: ArchiveMetadata {
                    version: 1,
                    binding,
                    source,
                    archived_at: now,
                    expires_at,
                    reason: if new.is_some() { "replace" } else { "delete" }.into(),
                    queue_key,
                },
            };
            rows.push((
                history_key.clone(),
                serde_json::to_value(&record)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?,
            ));
            recorded.refs.push(RecordRefs {
                key: history_key,
                atoms: vec![record.event.new_atom_uuid],
                files: super::super::blob_counter::tip_blobs(previous),
                new_rows: rows,
                molecule: molecule.to_string(),
            });
        }
        Ok(recorded)
    }
}
