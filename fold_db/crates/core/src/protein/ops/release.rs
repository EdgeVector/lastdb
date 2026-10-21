//! Release the protein membership of molecules that nothing else names.
//!
//! A protein member is a binding of the molecule (`MoleculeRefEdge::protein_member`,
//! counted in `bindings`). When a schema is dropped, its schema-field bindings
//! go. A molecule that a protein still names then has `bindings` above zero, and
//! it would never be dead. This call removes the membership of such a molecule,
//! in the batch of the schema drop, so `bindings` can reach zero and the molecule
//! drain can take it.
//!
//! The counter decides, not the list. A molecule is released only when its
//! `bindings` counter equals the number of protein member entries it has in its
//! protein. A counter that is higher means a schema, a catalog or another
//! protein still names the molecule. A counter that is `suspect`, or lower than
//! the member entries, is a fault that this call leaves alone.

use super::{member_backref_key, AtomStore, MoleculeRefEdge, Protein, ProteinMember, SchemaError};
use std::collections::{BTreeMap, BTreeSet};

impl AtomStore {
    /// Remove the protein membership of each molecule in `molecules` whose
    /// remaining bindings are all protein members. The caller holds
    /// `with_protein_membership`, so the reads and the writes are one batch.
    /// Returns the number of molecules released.
    pub(crate) async fn protein_release_members(
        &self,
        molecules: &[String],
    ) -> Result<usize, SchemaError> {
        if molecules.is_empty() {
            return Ok(0);
        }
        let owners = self.protein_of_molecules(molecules).await?;
        let counters = self.read_binding_counters(molecules, None).await?;
        let mut wanted: BTreeMap<String, Vec<(&String, u64)>> = BTreeMap::new();
        for ((molecule, owner), counter) in molecules.iter().zip(owners).zip(counters) {
            let (Some(owner), Some(counter)) = (owner, counter) else {
                continue;
            };
            if counter.bindings > 0 && !counter.suspect {
                wanted
                    .entry(owner)
                    .or_default()
                    .push((molecule, counter.bindings));
            }
        }
        let uuids: Vec<String> = wanted.keys().cloned().collect();
        let proteins = self.protein_get_many(&uuids).await?;
        let mut edges = Vec::new();
        let mut released: BTreeSet<&String> = BTreeSet::new();
        let mut changed: Vec<Protein> = Vec::new();
        for (uuid, protein) in uuids.iter().zip(proteins) {
            let Some(mut protein) = protein else { continue };
            let before = released.len();
            for (molecule, bindings) in &wanted[uuid] {
                let members: Vec<ProteinMember> = protein
                    .members
                    .iter()
                    .filter(|member| member.molecule_uuid == **molecule)
                    .cloned()
                    .collect();
                if members.is_empty() || *bindings != members.len() as u64 {
                    continue;
                }
                edges.extend(members.iter().map(|member| {
                    MoleculeRefEdge::protein_member(
                        uuid,
                        &member.molecule_uuid,
                        &member.hash_field,
                        member.range_field.as_deref(),
                    )
                }));
                protein
                    .members
                    .retain(|member| member.molecule_uuid != **molecule);
                released.insert(*molecule);
            }
            if released.len() > before {
                changed.push(protein);
            }
        }
        if released.is_empty() {
            return Ok(0);
        }
        self.remove_molecule_bindings(&edges, None).await?;
        for protein in &changed {
            self.put_protein(protein).await?;
        }
        for molecule in &released {
            self.raw()
                .delete_item(&member_backref_key(molecule))
                .await
                .map_err(|e| SchemaError::InvalidData(format!("delete molprot {molecule}: {e}")))?;
        }
        Ok(released.len())
    }
}
