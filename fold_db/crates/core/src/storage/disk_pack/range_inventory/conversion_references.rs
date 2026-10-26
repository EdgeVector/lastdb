//! Translate named storage references; preserve signed and opaque values.

use super::{conversion_plan::ConversionPlan, invalid, MetadataRecord, StorageResult};
use crate::atom::delete_barrier::{delete_barrier_key, DeleteBarrier};
use crate::atom::molecule_key_codec::{decode_hash_range_any, molecule_uuid_from_storage_key};
use crate::atom::MutationEvent;
use crate::db_operations::atom_store::{slot_dir_prefix, QueueValue};
use crate::storage::current_keys;
use laststore::disk_run::home::HomeMutation;
use sha2::{Digest, Sha256};

impl ConversionPlan {
    pub(super) fn translate_metadata(&mut self, records: &[MetadataRecord]) -> StorageResult<()> {
        let mut targets = std::collections::BTreeSet::new();
        for row in records {
            let next = self.translate(row)?;
            if !targets.insert((next.collection.clone(), next.key.clone())) {
                return Err(invalid("metadata translation collides with another record"));
            }
            self.metadata
                .insert((row.collection.clone(), row.key.clone()), next);
        }
        for extra in &self.extras {
            if let Some(row) = self
                .metadata
                .get_mut(&(extra.collection.clone(), extra.key.clone()))
            {
                *row = extra.clone();
            }
        }
        self.extras.retain(|extra| {
            !self
                .metadata
                .contains_key(&(extra.collection.clone(), extra.key.clone()))
        });
        Ok(())
    }

    fn translate(&self, row: &MetadataRecord) -> StorageResult<HomeMutation> {
        let mut next = HomeMutation {
            collection: row.collection.clone(),
            key: row.key.clone(),
            body: Some(row.value.clone()),
        };
        if row.key == crate::db_operations::AtomStore::CURRENT_KEY_FORMAT_KEY
            && row.collection
                == crate::storage::disk_pack::native_routing::collection_for_key(row.key.as_bytes())
        {
            next.body = Some(crate::atom::molecule_key_codec::TYPED_KEY_FORMAT.to_vec());
            return Ok(next);
        }
        if !matches!(
            row.collection.as_str(),
            "tips" | "indexes" | "proteins" | "molecule_ref_edges"
        ) {
            return Ok(next);
        }
        let bare = current_keys::source_key(&row.key)?;
        let scope = &row.key[..row.key.len() - bare.len()];
        let family = bare.split([':', '\0']).next().unwrap_or_default();
        match family {
            "mk" | "mgr" | "mgd" => {
                let (molecule, generation, suffix) = super::keys::slot_parts(bare, family)?;
                let original = format!("{scope}mk:{molecule}:{suffix}");
                let target = self.coordinate(&original)?;
                let suffix = target
                    .strip_prefix(&format!("{scope}mk:{molecule}:"))
                    .ok_or_else(|| invalid("a coordinate changed its molecule or scope"))?;
                next.key = match generation {
                    Some(generation) => format!("{scope}mgr:v1:{molecule}:{generation}:{suffix}"),
                    None if family == "mgd" => format!("{scope}mgd:v1:{molecule}:{suffix}"),
                    None => target.to_owned(),
                };
            }
            "rdel" => {
                let mut barrier: DeleteBarrier = super::logical::decode(&row.value)?;
                if delete_barrier_key(barrier.mk_key.as_bytes()) != row.key {
                    return Err(invalid("a barrier identity differs"));
                }
                barrier.mk_key = self.coordinate(&barrier.mk_key)?.to_owned();
                next.key = delete_barrier_key(barrier.mk_key.as_bytes());
                next.body = Some(encode(&barrier)?);
            }
            "history" => {
                let mut event: MutationEvent = super::logical::decode(&row.value)?;
                let profile = self
                    .profiles
                    .get(&event.molecule_uuid)
                    .ok_or_else(|| invalid("history has no molecule profile"))?;
                profile
                    .encode_value(event.field_key.range.as_deref().unwrap_or(""))
                    .map_err(|_| invalid("a historical range has no typed value"))?;
                if let Some(delete) = &mut event.suppressed_by_delete {
                    delete.mk_key = self.coordinate(&delete.mk_key)?.to_owned();
                    next.body = Some(encode(&event)?);
                }
            }
            "crq" | "crs" => {
                let (key, bytes) = self.retention(row, family)?;
                next.key = key;
                next.body = Some(bytes);
            }
            _ => {}
        }
        Ok(next)
    }

    fn retention(&self, row: &MetadataRecord, family: &str) -> StorageResult<(String, Vec<u8>)> {
        match QueueValue::decode(&row.value).map_err(|_| invalid("invalid retained reference"))? {
            QueueValue::Record(mut entry) => {
                if entry.version != 1
                    || (family == "crq" && entry.queue != row.key)
                    || (family == "crs" && entry.dir != row.key)
                {
                    return Err(invalid("retained reference identity differs"));
                }
                let old_prefix = directory_prefix(&entry.slot)?;
                let new_slot = self.coordinate(&entry.slot)?.to_owned();
                let suffix = entry
                    .dir
                    .strip_prefix(&old_prefix)
                    .ok_or_else(|| invalid("retained directory is outside its slot"))?;
                let digest = format!("{:x}", Sha256::digest(entry.history.as_bytes()));
                if suffix != digest {
                    return Err(invalid("retained directory has another history identity"));
                }
                entry.dir = format!("{}{suffix}", directory_prefix(&new_slot)?);
                entry.slot = new_slot;
                let key = if family == "crq" {
                    entry.queue.clone()
                } else {
                    entry.dir.clone()
                };
                Ok((key, encode(&entry)?))
            }
            QueueValue::Drain(mut marker) => {
                if family != "crq"
                    || marker.version != 1
                    || !marker.drain
                    || marker.queue != row.key
                    || marker.dir_prefix != directory_prefix(&marker.slot)?
                {
                    return Err(invalid("invalid retained drain identity"));
                }
                let old_id = format!(
                    "{:x}",
                    Sha256::digest(format!("drain:{}", marker.dir_prefix).as_bytes())
                );
                let (prefix, id) = marker
                    .queue
                    .rsplit_once(':')
                    .ok_or_else(|| invalid("invalid drain queue key"))?;
                if id != old_id {
                    return Err(invalid("drain key has another slot identity"));
                }
                marker.slot = self.coordinate(&marker.slot)?.to_owned();
                marker.dir_prefix = directory_prefix(&marker.slot)?;
                let new_id = format!(
                    "{:x}",
                    Sha256::digest(format!("drain:{}", marker.dir_prefix).as_bytes())
                );
                marker.queue = format!("{prefix}:{new_id}");
                Ok((marker.queue.clone(), encode(&marker)?))
            }
        }
    }
}

fn directory_prefix(slot: &str) -> StorageResult<String> {
    let bare = current_keys::source_key(slot)?;
    let molecule =
        molecule_uuid_from_storage_key(bare).ok_or_else(|| invalid("invalid retained molecule"))?;
    let (hash, range) =
        decode_hash_range_any(bare).ok_or_else(|| invalid("invalid retained slot"))?;
    Ok(slot_dir_prefix(
        current_keys::exact_scope(slot)?,
        molecule,
        &hash,
        &range,
    ))
}

fn encode(value: &impl serde::Serialize) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| invalid("a translated reference cannot be encoded"))
}
