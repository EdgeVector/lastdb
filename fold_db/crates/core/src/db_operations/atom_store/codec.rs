//! Atom body storage-key reads for [`AtomStore`]. The plain row codec is in
//! `crate::records::plain_atom`.

use super::*;

impl AtomStore {
    /// Warm an existing molecule key bundle without creating storage state.
    /// Schema publication establishes keys; physical tools can inspect unbound molecules.
    pub(crate) async fn load_molecule_key_bundle(
        &self,
        molecule_uuid: &str,
    ) -> Result<(), crate::schema::SchemaError> {
        if let Some(store) = self.molecule_keys.as_ref() {
            store.load(molecule_uuid).await.map_err(|error| {
                crate::schema::SchemaError::InvalidData(format!(
                    "load molecule key bundle for {molecule_uuid}: {error}"
                ))
            })?;
        }
        Ok(())
    }

    /// Batch-load atoms by full storage keys (`atom:{uuid}` or
    /// `{prefix}:atom:{uuid}`).
    ///
    /// **Must** be used instead of `raw().get_items::<Atom>(…)`: it uses the
    /// captured storage boundary, strictly decodes plain atom rows, and notes
    /// the logical path of each atom that it returns.
    pub(super) async fn get_atoms_by_storage_keys_captured(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<crate::atom::Atom>>, crate::schema::SchemaError> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        let lookup = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let mut raw = self.main_store.inner().get_many(lookup).await?.into_iter();
        let mut out = Vec::with_capacity(keys.len());
        for requested in keys {
            let atom = raw
                .next()
                .flatten()
                .map(|value| decode_plain_atom(&value))
                .transpose()?;
            if let Some(atom) = &atom {
                crate::storage::disk_pack::logical_paths::source::note_atom(
                    runtime.clone(),
                    requested,
                    atom,
                )?;
            }
            out.push(atom);
        }
        Ok(out)
    }
}
