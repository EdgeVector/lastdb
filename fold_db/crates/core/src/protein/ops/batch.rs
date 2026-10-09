//! Batched protein reads and loaded-protein sibling plan.

use super::{
    member_backref_key, protein_record_key, AtomEntry, AtomStore, ChangedKey, Ed25519KeyPair,
    HashMap, HashSet, MoleculeData, Protein, SchemaError,
};

impl AtomStore {
    /// Resolve distinct member backrefs with one storage batch.
    pub(crate) async fn protein_of_molecules(
        &self,
        molecule_uuids: &[String],
    ) -> Result<Vec<Option<String>>, SchemaError> {
        let keys: Vec<String> = molecule_uuids
            .iter()
            .map(|uuid| member_backref_key(uuid))
            .collect();
        self.raw()
            .get_items(&keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read molprot batch: {e}")))
    }

    /// Load distinct protein records with one storage batch.
    pub(crate) async fn protein_get_many(
        &self,
        protein_uuids: &[String],
    ) -> Result<Vec<Option<Protein>>, SchemaError> {
        let keys: Vec<String> = protein_uuids
            .iter()
            .map(|uuid| protein_record_key(uuid))
            .collect();
        self.raw()
            .get_items(&keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read protein batch: {e}")))
    }

    /// Variant used by the schema mutation path, where the entry key layout is
    /// known. A field-identity molecule can carry multiple key conformations;
    /// skip only the conformation written directly, not every member sharing
    /// the same molecule UUID.
    ///
    /// `author_clock` is `(written_at, logical_counter, device_id,
    /// mutation_uuid)` from the entry mutation. When set, sibling tips reuse
    /// that full clock and LWW-skip if a newer tip already won.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn protein_sibling_tip_updates_for_layout(
        &self,
        protein_uuid: &str,
        entry_molecule_uuid: &str,
        entry_layout: Option<(&str, Option<&str>)>,
        fields: &HashMap<String, String>,
        atom_uuid: &str,
        keypair: &Ed25519KeyPair,
        author_clock: Option<(u64, u64, String, String)>,
    ) -> Result<Vec<(String, MoleculeData, HashSet<ChangedKey>)>, SchemaError> {
        let protein = self.protein_get(protein_uuid).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("protein {protein_uuid} not found for sibling fold"))
        })?;
        self.protein_sibling_tip_updates_for_loaded_protein(
            &protein,
            entry_molecule_uuid,
            entry_layout,
            fields,
            atom_uuid,
            keypair,
            author_clock,
        )
        .await
    }

    /// Plan one field's sibling updates from a protein loaded for this write.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn protein_sibling_tip_updates_for_loaded_protein(
        &self,
        protein: &Protein,
        entry_molecule_uuid: &str,
        entry_layout: Option<(&str, Option<&str>)>,
        fields: &HashMap<String, String>,
        atom_uuid: &str,
        keypair: &Ed25519KeyPair,
        author_clock: Option<(u64, u64, String, String)>,
    ) -> Result<Vec<(String, MoleculeData, HashSet<ChangedKey>)>, SchemaError> {
        let honor_tip_lww = author_clock.is_some();
        let tip = match author_clock {
            Some((written_at, logical_counter, device_id, mutation_uuid)) => {
                AtomEntry::thin_with_author(
                    atom_uuid.to_string(),
                    written_at,
                    logical_counter,
                    device_id,
                    mutation_uuid,
                    String::new(),
                )
            }
            None => AtomEntry::thin(
                atom_uuid.to_string(),
                crate::clock::unix_nanos(),
                keypair.public_key_base64(),
            ),
        };
        let mut sibling_updates: Vec<(String, MoleculeData, HashSet<ChangedKey>)> = Vec::new();
        for member in &protein.members {
            let is_entry = member.molecule_uuid == entry_molecule_uuid
                && entry_layout.is_none_or(|(hash_field, range_field)| {
                    member.hash_field == hash_field && member.range_field.as_deref() == range_field
                });
            if is_entry {
                continue;
            }
            let Some((hash, range)) = member.tip_coords_from_fields(fields) else {
                continue;
            };
            sibling_updates.push(
                self.protein_member_tip_update(
                    &member.molecule_uuid,
                    &hash,
                    &range,
                    &tip,
                    keypair,
                    honor_tip_lww,
                )
                .await?,
            );
        }
        Ok(sibling_updates)
    }
}
