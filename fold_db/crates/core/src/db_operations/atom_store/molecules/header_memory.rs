//! Existing typed headers share the core publication and query lifetime.

use super::super::types::MoleculeHeader;
use super::super::AtomStore;
use crate::schema::SchemaError;
use crate::storage::disk_pack::{core_memory_get, core_memory_sources, core_record_insert};
use serde::{de::DeserializeOwned, Serialize};

static HEADER_MEMORY: u8 = 0;

impl AtomStore {
    pub(crate) async fn stage_current_molecule_selectors(
        &self,
        runtime: &crate::storage::disk_pack::PackRuntime,
        molecules: &[String],
        scope: Option<&str>,
    ) -> Result<(), SchemaError> {
        let mut keys: Vec<_> = molecules
            .iter()
            .map(|molecule| {
                crate::schema::types::field::build_storage_key(
                    scope,
                    &crate::atom::molecule_key_codec::molecule_generation_pointer_key(molecule),
                )
            })
            .collect();
        keys.sort();
        keys.dedup();
        let values = self
            .main_store
            .get_items::<super::super::MoleculeGenerationPointer>(&keys)
            .await?;
        for (key, value) in keys.into_iter().zip(values) {
            let bytes = serde_json::to_vec(&value)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?
                .len()
                .saturating_mul(8)
                .saturating_add(256);
            crate::storage::disk_pack::transaction::stage_core_memory(
                runtime,
                std::ptr::from_ref(&HEADER_MEMORY) as usize,
                key.clone(),
                value,
                core_memory_sources("main", key.as_bytes()),
                bytes,
            )?;
        }
        Ok(())
    }

    pub(crate) async fn stage_current_molecule_headers(
        &self,
        runtime: &crate::storage::disk_pack::PackRuntime,
        molecules: &[String],
        scope: Option<&str>,
    ) -> Result<(), SchemaError> {
        let keys: Vec<_> = molecules
            .iter()
            .map(|molecule| {
                crate::schema::types::field::build_storage_key(
                    scope,
                    &crate::atom::molecule_key_codec::header_key(molecule),
                )
            })
            .collect();
        let values = self.main_store.get_items::<MoleculeHeader>(&keys).await?;
        for (key, header) in keys.into_iter().zip(values) {
            let bytes = serde_json::to_vec(&header)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?
                .len()
                .saturating_mul(8)
                .saturating_add(256);
            crate::storage::disk_pack::transaction::stage_core_memory(
                runtime,
                std::ptr::from_ref(&HEADER_MEMORY) as usize,
                key.clone(),
                header,
                core_memory_sources("main", key.as_bytes()),
                bytes,
            )?;
        }
        Ok(())
    }

    pub(crate) fn stage_molecule_header(
        &self,
        runtime: &crate::storage::disk_pack::PackRuntime,
        molecule: &str,
        data: &super::super::MoleculeData,
        scope: Option<&str>,
    ) -> Result<(), SchemaError> {
        let key = crate::schema::types::field::build_storage_key(
            scope,
            &crate::atom::molecule_key_codec::header_key(molecule),
        );
        let header = Some(MoleculeHeader {
            version: data.version(),
            updated_at: data.updated_at(),
        });
        let bytes = serde_json::to_vec(&header)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?
            .len()
            .saturating_mul(8)
            .saturating_add(256);
        crate::storage::disk_pack::transaction::stage_core_memory(
            runtime,
            std::ptr::from_ref(&HEADER_MEMORY) as usize,
            key.clone(),
            header,
            core_memory_sources("main", key.as_bytes()),
            bytes,
        )?;
        Ok(())
    }

    pub(super) async fn header_from_memory(
        &self,
        key: &str,
    ) -> Result<Option<MoleculeHeader>, SchemaError> {
        self.molecule_control_from_memory(key).await
    }

    /// Headers and generation selectors remain typed core control objects.
    pub(super) async fn molecule_control_from_memory<T>(
        &self,
        key: &str,
    ) -> Result<Option<T>, SchemaError>
    where
        T: DeserializeOwned + Serialize + Clone + Send + Sync + 'static,
    {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        let owner = std::ptr::from_ref(&HEADER_MEMORY) as usize;
        if !crate::storage::disk_pack::read_snapshot_active(runtime.clone()) {
            return Ok(self.main_store.get_item::<T>(key).await?);
        }
        if let Some(value) = core_memory_get::<Option<T>>(runtime.clone(), owner, key)? {
            return Ok(value.as_ref().as_ref().cloned());
        }
        let control = self.main_store.get_item::<T>(key).await?;
        let bytes = serde_json::to_vec(&control)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?
            .len()
            .saturating_mul(8)
            .saturating_add(256);
        let sources = core_memory_sources("main", key.as_bytes());
        let held = core_record_insert(runtime, owner, key.to_owned(), control, sources, bytes)?;
        Ok(held.as_ref().as_ref().cloned())
    }
}
