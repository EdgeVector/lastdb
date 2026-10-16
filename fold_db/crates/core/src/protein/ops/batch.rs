//! Batched protein reads and loaded-protein sibling plan.

use super::{
    member_backref_key, protein_record_key, AtomEntry, AtomStore, Ed25519KeyPair, HashMap,
    PreparedProteinTip, Protein, SchemaError,
};
use crate::atom::TipAuthor;

impl AtomStore {
    /// Resolve distinct member backrefs with one storage batch.
    pub(crate) async fn protein_of_molecules_in_scope(
        &self,
        molecule_uuids: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Option<String>>, SchemaError> {
        let keys: Vec<String> = molecule_uuids
            .iter()
            .map(|uuid| {
                crate::schema::types::field::build_storage_key(
                    storage_prefix,
                    &member_backref_key(uuid),
                )
            })
            .collect();
        self.raw()
            .get_items(&keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read molprot batch: {e}")))
    }

    /// Load distinct protein records with one storage batch.
    pub(crate) async fn protein_get_many_in_scope(
        &self,
        protein_uuids: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Option<Protein>>, SchemaError> {
        let keys: Vec<String> = protein_uuids
            .iter()
            .map(|uuid| {
                crate::schema::types::field::build_storage_key(
                    storage_prefix,
                    &protein_record_key(uuid),
                )
            })
            .collect();
        self.raw()
            .get_items(&keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read protein batch: {e}")))
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
        author_clock: Option<TipAuthor>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<PreparedProteinTip>, SchemaError> {
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
