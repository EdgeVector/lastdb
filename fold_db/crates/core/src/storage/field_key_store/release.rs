//! Delete retired payload-field bundles by the exact ids the old writer stored.
//!
//! Each id is two rows in the field-key namespace: `fkb:{id}` and `fkw:{id}:…`.
//! The id is the value in the schema's old `field_key_ids` map when that field
//! is present, and otherwise the digest of `lastdb:field-key:v1`, the schema
//! name, and the field name. This deletes both, for every current field,
//! transform field, and map entry. Record-space and alternate ids are kept.
//! A field removed from the schema after its map entry was already dropped has
//! no exact id left. This path does not scan the namespace to find it.
//! Absent rows are not written. A second open probes the same ids and stops.

use super::{
    metadata_key, schema_field_key_ids, wrap_key_for, FieldKeyStore, StorageError, StorageResult,
    StoredPayloadFieldKeys,
};
use crate::schema::types::Schema;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};

/// Two rows per id. The read cap is 10000 keys and write admission is 4096.
const RELEASE_KEYS_PER_BATCH: usize = 2048;

impl FieldKeyStore {
    /// Remove payload-field bundles for the schemas just loaded from the catalog.
    ///
    /// Runs once per open, before the catalog is marked ready. A failure fails
    /// the open. The record-space bundle and every alternate bundle stay.
    pub(crate) async fn release_retired_payload_bundles(
        &self,
        schemas: &HashMap<String, Schema>,
        stored: &StoredPayloadFieldKeys,
    ) -> StorageResult<()> {
        let ids = retired_payload_bundle_ids(schemas, stored);
        if ids.is_empty() {
            return Ok(());
        }
        let mut pending = Vec::new();
        let mut released = 0usize;
        for id in &ids {
            if pending.len() + 2 > RELEASE_KEYS_PER_BATCH {
                released += self.delete_present(&mut pending).await?;
            }
            pending.push(metadata_key(id));
            pending.push(wrap_key_for(id));
        }
        released += self.delete_present(&mut pending).await?;
        if released > 0 {
            tracing::info!(rows = released, "deleted retired payload field key bundles");
        }
        Ok(())
    }

    async fn delete_present(&self, keys: &mut Vec<String>) -> StorageResult<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let batch = std::mem::take(keys);
        let found = self
            .entries
            .inner()
            .get_many(
                batch
                    .iter()
                    .map(|key| key.as_bytes().to_vec())
                    .collect::<Vec<_>>(),
            )
            .await?;
        if found.len() != batch.len() {
            return Err(StorageError::BackendError(
                "field key batch read returned an incomplete result".into(),
            ));
        }
        let present: Vec<String> = batch
            .into_iter()
            .zip(found)
            .filter_map(|(key, value)| value.map(|_| key))
            .collect();
        let count = present.len();
        if count > 0 {
            self.format.release_rows(present).await?;
        }
        Ok(count)
    }
}

fn retired_payload_bundle_ids(
    schemas: &HashMap<String, Schema>,
    stored: &StoredPayloadFieldKeys,
) -> BTreeSet<String> {
    let mut keep = BTreeSet::new();
    for schema in schemas.values() {
        keep.extend(schema_field_key_ids(schema));
    }
    let mut drop_ids = BTreeSet::new();
    for schema in schemas.values() {
        let mut names = BTreeSet::new();
        if let Some(fields) = &schema.fields {
            names.extend(fields.iter().cloned());
        }
        if let Some(transforms) = &schema.transform_fields {
            names.extend(transforms.keys().cloned());
        }
        if let Some(saved) = stored.get(&schema.name) {
            names.extend(saved.keys().cloned());
            for id in saved.values() {
                note_retired(&mut drop_ids, &keep, id);
            }
        }
        for name in &names {
            note_retired(
                &mut drop_ids,
                &keep,
                &retired_payload_field_key_id(&schema.name, name),
            );
        }
    }
    drop_ids
}

fn note_retired(drop_ids: &mut BTreeSet<String>, keep: &BTreeSet<String>, id: &str) {
    if id.is_empty() || keep.contains(id) {
        return;
    }
    drop_ids.insert(id.to_owned());
}

/// The id an older build stored when `field_key_ids` had no entry for the field.
///
/// Nothing mints this id. It exists so open can name the row the old writer left.
fn retired_payload_field_key_id(schema_name: &str, field_name: &str) -> String {
    let mut hasher = Sha256::new();
    for part in [
        b"lastdb:field-key:v1".as_slice(),
        schema_name.as_bytes(),
        field_name.as_bytes(),
    ] {
        lastdb_model::canonical::push_field(&mut hasher, part);
    }
    URL_SAFE_NO_PAD.encode(hasher.finalize())
}
