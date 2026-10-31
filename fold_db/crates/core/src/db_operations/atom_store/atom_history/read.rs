use super::*;

use crate::schema::types::field::build_storage_key;
use crate::schema::{types::key_value::KeyValue, SchemaError};

impl<S: HistoryPorts> AtomHistory<'_, S> {
    pub async fn atom_history_slot(
        &self,
        molecule: &str,
        key: &KeyValue,
        scope: Option<&str>,
    ) -> Result<String, SchemaError> {
        let codec = self.store.history_key_codec(molecule).await?;
        let bare = codec
            .api_hash_range_record_key(
                molecule,
                key.hash.as_deref().unwrap_or(""),
                key.range.as_deref().unwrap_or(""),
            )
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        Ok(build_storage_key(scope, &bare))
    }

    pub async fn atom_history_page(
        &self,
        slot: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<AtomHistoryPage, SchemaError> {
        self.store.atom_read_scope(async {
            let mut walk = history_walk(slot, cursor, limit)?;
            let step = self.store.walk_step(&mut walk).await?;
            let now = self.store.retention_state().clock_nanos();
            let scope = crate::storage::current_keys::exact_scope(slot)?;
            let base = slot.strip_prefix(&build_storage_key(scope, ""))
                .ok_or_else(|| history_error("history slot is outside its exact scope"))?
                .to_owned();
            let current = self.store.load_visible_per_key_records(scope, &[base]).await?
                .into_iter().next().flatten()
                .filter(|r| !r.meta.as_ref().is_some_and(|m| m.tombstoned))
                .map(|r| serde_json::json!({"atom": r.entry.atom_uuid, "mutation": r.entry.mutation_uuid}));
            let mut entries = Vec::new();
            for (key, bytes) in step.rows {
                let key = String::from_utf8(key).map_err(history_error)?;
                let record: ArchiveRecord = serde_json::from_slice(&bytes).map_err(history_error)?;
                if !record.expired(now) { entries.push(record.entry(&key)); }
            }
            Ok(AtomHistoryPage { current, entries, cursor: next_cursor(&walk) })
        }).await
    }

    pub async fn atom_history_value(
        &self,
        slot: &str,
        id: &str,
    ) -> Result<Option<serde_json::Value>, SchemaError> {
        validate_id(id)?;
        self.store
            .atom_read_scope(async {
                let key = format!("{}{id}", prefix(slot)?);
                let Some(record) = self.store.rows().get_item::<ArchiveRecord>(&key).await? else {
                    return Ok(None);
                };
                if record.expired(self.store.retention_state().clock_nanos()) {
                    return Ok(None);
                }
                let scope = crate::storage::current_keys::exact_scope(slot)?;
                let atoms = self
                    .store
                    .get_atoms_by_storage_keys(&[build_storage_key(
                        scope,
                        &crate::atom::atom_key_codec::body_key(&record.event.new_atom_uuid),
                    )])
                    .await?;
                let atom = atoms.into_iter().next().flatten().ok_or_else(|| {
                    SchemaError::InvalidData("history atom body is missing".into())
                })?;
                Ok(Some(serde_json::json!({
                    "entry": record.entry(&key), "value": atom.content(),
                    "metadata": record.archive.source.meta,
                })))
            })
            .await
    }
}

fn validate_id(id: &str) -> Result<(), SchemaError> {
    if id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(());
    }
    Err(SchemaError::InvalidData(
        "invalid history entry identity".into(),
    ))
}

fn history_error(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("atom history: {error}"))
}
