use super::*;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::{types::key_value::KeyValue, SchemaError};

impl AtomStore {
    pub async fn atom_history_slot(
        &self,
        molecule: &str,
        key: &KeyValue,
        scope: Option<&str>,
    ) -> Result<String, SchemaError> {
        let codec = self.history_key_codec(molecule).await?;
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
        self.atom_read_scope(async {
            let mut walk = history_walk(slot, cursor, limit)?;
            let step = self.walk_step(&mut walk).await?;
            let now = self.retention_state().clock_nanos();
            let scope = crate::storage::current_keys::exact_scope(slot)?;
            let base = crate::storage::current_keys::source_key(slot)?.to_owned();
            let current = self.load_visible_per_key_records(scope, &[base]).await?
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
        self.atom_read_scope(async {
            let key = format!("{}{id}", prefix(slot)?);
            let Some(record) = self.main_store.get_item::<ArchiveRecord>(&key).await? else {
                return Ok(None);
            };
            if record.expired(self.retention_state().clock_nanos()) {
                return Ok(None);
            }
            let scope = crate::storage::current_keys::exact_scope(slot)?;
            let atoms = self
                .get_atoms_by_storage_keys(&[build_storage_key(
                    scope,
                    &crate::atom::atom_key_codec::body_key(&record.event.new_atom_uuid),
                )])
                .await?;
            let atom =
                atoms.into_iter().next().flatten().ok_or_else(|| {
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
