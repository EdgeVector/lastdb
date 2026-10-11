//! v1 edge reindex entry points and manifest/backfill-status reads; the upgrade and v2 families live in the sibling `reindex_*` modules.

use super::*;

impl AtomStore {
    /// Point-read one molecule's cutover manifest.
    pub async fn atom_ref_molecule_manifest(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<AtomRefMoleculeManifest>, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::atom_ref_molecule_manifest_key(molecule_uuid),
        );
        self.main_store
            .get_item(&key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("load atom reverse-edge manifest: {e}")))
    }
}
