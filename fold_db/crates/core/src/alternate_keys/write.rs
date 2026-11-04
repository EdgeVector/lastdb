//! Derive pointer changes from explicit old/new addresses, never payload fields.
use super::*;
use crate::db_operations::record_store::RecordStore;
use crate::storage::format::records::AlternateChange;
use lastdb_model::record_space::record_space_uuid;

impl RecordStore {
    pub(crate) fn alternate_entry_changes(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
        before: &[AlternateValues],
        after: &[AlternateValues],
    ) -> Result<Vec<AlternateChange>, SchemaError> {
        let definitions = schema.require_record_keys()?;
        if definitions.alternates.is_empty() || keys.is_empty() {
            return Ok(Vec::new());
        }
        if before.len() != keys.len() || after.len() != keys.len() {
            return Err(invalid("alternate change batch is incomplete"));
        }
        let space = record_space_uuid(&schema.name);
        let primary_codec = self.key_codec_for_field_key(&space)?;
        let mut changes = Vec::new();
        for ((primary, before), after) in keys.iter().zip(before).zip(after) {
            for name in definitions.alternates.keys() {
                let old = before.get(name).and_then(Option::as_ref);
                let new = after.get(name).and_then(Option::as_ref);
                if old == new {
                    continue;
                }
                // The full pointer key includes its primary identity. Another
                // record sharing the same address cannot own this slot.
                if let Some(old) = old {
                    changes.push(AlternateChange::delete(storage_address(
                        self, schema, name, old, primary,
                    )?));
                }
                if let Some(new) = new {
                    let id = field_key(schema, name);
                    changes.push(AlternateChange::put(
                        &primary_codec,
                        &space,
                        primary.clone(),
                        &self.key_codec_for_field_key(&id)?,
                        &id,
                        new.clone(),
                    )?);
                }
            }
        }
        if changes.len() > MAX_ALTERNATE_ENTRIES {
            return Err(invalid("alternate change batch exceeds 4096 entries"));
        }
        Ok(changes)
    }
}
