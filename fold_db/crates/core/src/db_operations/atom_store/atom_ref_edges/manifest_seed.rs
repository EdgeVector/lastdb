//! Tip-complete manifests for molecules that this build created.
//!
//! The reverse-edge backfill wrote one manifest for each molecule it found.
//! That backfill is gone, so a molecule made after it never got a manifest, and
//! the purge fell back to the proof over the whole schema. Every writer
//! maintains the compact edges from a molecule's first write, so the global
//! completion markers already prove the edges of such a molecule. This module
//! writes the manifest that the backfill would have written.

use super::*;

impl AtomStore {
    /// Read the manifests of `molecules` in one batch, in the order given.
    ///
    /// A molecule with no manifest gets a tip-complete one when its storage
    /// prefix has both global completion markers
    /// ([`Self::atom_ref_v2_reads_ready`]). The new manifests go out in one
    /// batch write. Without the markers the molecule stays `None`, and the
    /// caller keeps the proof over the whole schema.
    pub(crate) async fn atom_ref_molecule_manifests_seeding_current(
        &self,
        molecules: &[(&str, Option<&str>)],
    ) -> Result<Vec<Option<AtomRefMoleculeManifest>>, SchemaError> {
        let keys: Vec<String> = molecules
            .iter()
            .map(|(molecule_uuid, storage_prefix)| {
                build_storage_key(
                    *storage_prefix,
                    &molecule_key_codec::atom_ref_molecule_manifest_key(molecule_uuid),
                )
            })
            .collect();
        let mut manifests: Vec<Option<AtomRefMoleculeManifest>> =
            self.main_store.get_items(&keys).await.map_err(|e| {
                SchemaError::InvalidData(format!("load atom reverse-edge manifests: {e}"))
            })?;

        let seeded_at = unix_nanos();
        let mut ready_by_prefix: HashMap<Option<&str>, bool> = HashMap::new();
        let mut writes = Vec::new();
        for (index, (molecule_uuid, storage_prefix)) in molecules.iter().enumerate() {
            if manifests[index].is_some() {
                continue;
            }
            let ready = if let Some(ready) = ready_by_prefix.get(storage_prefix) {
                *ready
            } else {
                let ready = self.atom_ref_v2_reads_ready(*storage_prefix).await?;
                ready_by_prefix.insert(*storage_prefix, ready);
                ready
            };
            if !ready {
                continue;
            }
            push_manifest_item_with_version(
                &mut writes,
                molecule_uuid,
                seeded_at,
                true,
                ATOM_REF_MANIFEST_VERSION_TIPS,
                *storage_prefix,
            )?;
            manifests[index] = Some(AtomRefMoleculeManifest {
                version: ATOM_REF_MANIFEST_VERSION_TIPS,
                molecule_uuid: (*molecule_uuid).to_string(),
                mutation_watermark_nanos: seeded_at,
                replay_complete: true,
            });
        }
        if !writes.is_empty() {
            self.main_store.batch_put_items(writes).await.map_err(|e| {
                SchemaError::InvalidData(format!("seed atom reverse-edge manifests: {e}"))
            })?;
        }
        Ok(manifests)
    }
}
