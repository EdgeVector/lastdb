//! Scoped protein member tip preparation.

use super::{
    AtomEntry, AtomStore, ChangedKey, Ed25519KeyPair, HashSet, MoleculeData, ProteinTipScope,
    SchemaError,
};
use crate::atom::{ImportedTip, MoleculeHashRange};
use chrono::Utc;

pub(crate) type PreparedProteinTip = (
    String,
    MoleculeData,
    HashSet<ChangedKey>,
    Option<(AtomEntry, Option<crate::atom::KeyMetadata>)>,
);

impl AtomStore {
    pub(super) async fn protein_member_tip_update(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        tip: &AtomEntry,
        keypair: &Ed25519KeyPair,
        scope: ProteinTipScope<'_>,
    ) -> Result<PreparedProteinTip, SchemaError> {
        let changed: HashSet<ChangedKey> =
            HashSet::from([ChangedKey::hash_range(hash.to_string(), range.to_string())]);

        let mut mol: MoleculeData = match self
            .load_molecule_for_write(molecule_uuid, scope.storage_prefix, &changed)
            .await?
        {
            Some(m) => m,
            None => {
                // Brand-new molecule identity for this member.
                MoleculeHashRange::from_write_records(
                    molecule_uuid.to_string(),
                    0,
                    Utc::now(),
                    Vec::new(),
                )
            }
        };
        let base = mol
            .get_atom_entry(hash, range)
            .cloned()
            .map(|entry| (entry, mol.get_key_metadata(hash, range).cloned()));

        // Local folds mint a fresh clock via set_atom_uuid_from_values.
        // Imported/replay folds must keep the envelope written_at/writer and
        // LWW-skip when a newer sibling tip already won.
        if scope.honor_tip_lww {
            let writer = if tip.device_id.is_empty() {
                tip.writer_pubkey().to_string()
            } else {
                tip.device_id.clone()
            };
            mol.set_atom_uuid_from_values_imported_with_author(
                hash.to_string(),
                range.to_string(),
                ImportedTip {
                    atom_uuid: tip.atom_uuid.clone(),
                    device_id: writer,
                    origin_device: tip.origin_device.clone(),
                    writer_pubkey: tip.writer_pubkey().to_string(),
                    written_at: Some(tip.written_at),
                    logical_counter: tip.logical_counter,
                    mutation_uuid: tip.mutation_uuid.clone(),
                    ..ImportedTip::default()
                },
            );
        } else {
            mol.set_atom_uuid_from_values(
                hash.to_string(),
                range.to_string(),
                tip.atom_uuid.clone(),
                keypair,
            );
        }

        Ok((molecule_uuid.to_string(), mol, changed, base))
    }
}
