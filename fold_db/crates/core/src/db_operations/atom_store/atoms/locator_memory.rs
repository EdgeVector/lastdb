//! UUID-only reads retain the actual typed locator at the same native boundary.

use super::*;
use crate::atom::AtomPartition;
use crate::storage::disk_pack::{core_memory_get, core_memory_insert, core_memory_sources};

static LOCATOR_MEMORY: u8 = 0;

impl AtomStore {
    pub(super) async fn load_atom_partitions(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<AtomPartition>>, SchemaError> {
        let mut held = self.atom_locators_from_memory(keys)?;
        let missing: Vec<_> = held
            .iter()
            .enumerate()
            .filter_map(|(slot, value)| value.is_none().then_some(slot))
            .collect();
        let disk_keys: Vec<_> = missing.iter().map(|&slot| keys[slot].clone()).collect();
        let disk = self.main_store.get_items::<Value>(&disk_keys).await?;
        for (slot, value) in missing.into_iter().zip(disk) {
            let partition = value.as_ref().and_then(atom_locator_codec::decode_value);
            if value.is_some() && partition.is_none() {
                tracing::warn!("unreadable atom locator value; using flat key");
            }
            held[slot] = Some(self.retain_atom_locator(&keys[slot], partition)?);
        }
        Ok(held.into_iter().flatten().collect())
    }

    /// Read final private values once, after every body and locator mutation.
    pub(crate) async fn stage_atom_locators(
        &self,
        runtime: &crate::storage::disk_pack::PackRuntime,
        scope: Option<&str>,
        uuids: &[String],
    ) -> Result<(), SchemaError> {
        let keys: Vec<_> = uuids
            .iter()
            .map(|uuid| build_storage_key(scope, &atom_locator_codec::locator_key(uuid)))
            .collect();
        let values = self.main_store.get_items::<Value>(&keys).await?;
        for (key, value) in keys.into_iter().zip(values) {
            let partition = value.as_ref().and_then(atom_locator_codec::decode_value);
            let bytes = locator_bytes(partition.as_ref());
            crate::storage::disk_pack::transaction::stage_core_memory(
                runtime,
                owner(),
                key.clone(),
                partition,
                core_memory_sources("main", key.as_bytes()),
                bytes,
            )?;
        }
        Ok(())
    }

    /// Return an unknown outer value only when a physical read is still needed.
    pub(super) fn atom_locators_from_memory(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<Option<AtomPartition>>>, SchemaError> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        keys.iter()
            .map(|key| {
                core_memory_get::<Option<AtomPartition>>(runtime.clone(), owner(), key)
                    .map(|value| value.map(|value| value.as_ref().as_ref().cloned()))
                    .map_err(SchemaError::from)
            })
            .collect()
    }

    pub(super) fn retain_atom_locator(
        &self,
        key: &str,
        partition: Option<AtomPartition>,
    ) -> Result<Option<AtomPartition>, SchemaError> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        if !crate::storage::disk_pack::read_snapshot_active(runtime.clone()) {
            return Ok(partition);
        }
        let bytes = locator_bytes(partition.as_ref());
        let value = core_memory_insert(
            runtime,
            owner(),
            key.to_owned(),
            partition,
            core_memory_sources("main", key.as_bytes()),
            bytes,
        )?;
        Ok(value.as_ref().as_ref().cloned())
    }
}

fn owner() -> usize {
    std::ptr::from_ref(&LOCATOR_MEMORY) as usize
}

fn locator_bytes(partition: Option<&AtomPartition>) -> usize {
    256usize.saturating_add(partition.map_or(0, |value| value.as_str().len().saturating_mul(4)))
}
