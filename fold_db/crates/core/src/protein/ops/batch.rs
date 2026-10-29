//! Batched protein reads and loaded-protein sibling plan.

use super::{
    member_backref_key, protein_record_key, AtomEntry, AtomStore, Ed25519KeyPair, HashMap,
    PreparedProteinTip, Protein, SchemaError,
};
use crate::atom::TipAuthor;

/// One loaded protein and the tip this write should copy onto its siblings.
pub(crate) struct LoadedProteinSiblingUpdate<'a> {
    pub(crate) protein: &'a Protein,
    pub(crate) entry_molecule_uuid: &'a str,
    pub(crate) entry_layout: Option<(&'a str, Option<&'a str>)>,
    pub(crate) fields: &'a HashMap<String, String>,
    pub(crate) atom_uuid: &'a str,
    pub(crate) keypair: &'a Ed25519KeyPair,
    pub(crate) author_clock: Option<TipAuthor>,
    pub(crate) storage_prefix: Option<&'a str>,
}

impl AtomStore {
    /// Resolve global molecule membership definitions with one storage batch.
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

    /// Load global protein definitions with one storage batch.
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

    /// Plan one field's sibling updates from a protein loaded for this write.
    pub(crate) async fn protein_sibling_tip_updates_for_loaded_protein(
        &self,
        update: LoadedProteinSiblingUpdate<'_>,
    ) -> Result<Vec<PreparedProteinTip>, SchemaError> {
        let LoadedProteinSiblingUpdate {
            protein,
            entry_molecule_uuid,
            entry_layout,
            fields,
            atom_uuid,
            keypair,
            author_clock,
            storage_prefix,
        } = update;
        let honor_tip_lww = author_clock.is_some();
        let tip = match author_clock {
            Some(author) => AtomEntry::thin_with_author(
                atom_uuid.to_string(),
                author.written_at,
                author.logical_counter,
                author.device_id,
                author.mutation_uuid,
                String::new(),
            )
            .with_origin_device(author.origin_device),
            None => AtomEntry::thin(
                atom_uuid.to_string(),
                crate::clock::unix_nanos(),
                keypair.public_key_base64(),
            ),
        };
        let mut sibling_updates = Vec::new();
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
                    super::ProteinTipScope {
                        honor_tip_lww,
                        storage_prefix,
                    },
                )
                .await?,
            );
        }
        Ok(sibling_updates)
    }
}
