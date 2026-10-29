//! One durable protein per field identity, independent of schema and app names.

use super::{
    member_backref_key, protein_record_key, AtomStore, MoleculeRefEdge, Protein, SchemaError,
};
use crate::db_operations::atom_store::counter::TipChangeSet;
use crate::protein::ProteinMember;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FieldProteinBinding {
    pub field_identity: String,
    pub member: ProteinMember,
}

impl FieldProteinBinding {
    fn protein_uuid(&self) -> String {
        format!("field:{}", self.field_identity)
    }
}

impl AtomStore {
    /// Keep membership, back-references and binding counters in one native batch.
    pub(crate) async fn bind_field_proteins(
        &self,
        bindings: &[FieldProteinBinding],
    ) -> Result<(), SchemaError> {
        self.with_protein_membership(async {
            let molecules: Vec<_> = bindings
                .iter()
                .map(|b| b.member.molecule_uuid.clone())
                .collect();
            let owners = self.protein_of_molecules(&molecules).await?;
            let ids: BTreeSet<_> = bindings
                .iter()
                .map(FieldProteinBinding::protein_uuid)
                .chain(owners.iter().flatten().cloned())
                .collect();
            let ids: Vec<_> = ids.into_iter().collect();
            let proteins = self.protein_get_many(&ids).await?;
            let stored: BTreeMap<_, _> = ids.into_iter().zip(proteins).collect();
            let plan = MembershipPlan::new(bindings, &owners, &stored)?;
            self.add_molecule_bindings(&plan.added, None).await?;
            self.remove_molecule_bindings(&plan.removed, None).await?;
            if !plan.rows.is_empty() || !plan.deletes.is_empty() {
                self.commit_tip_changes(TipChangeSet {
                    rows: plan.rows,
                    deletes: plan.deletes,
                    ..TipChangeSet::default()
                })
                .await?;
            }
            Ok(())
        })
        .await
    }
}

#[derive(Default)]
struct MembershipPlan {
    added: Vec<MoleculeRefEdge>,
    removed: Vec<MoleculeRefEdge>,
    rows: Vec<(String, serde_json::Value)>,
    deletes: Vec<Vec<u8>>,
}

impl MembershipPlan {
    /// Union old per-product proteins without changing a tip, atom or author clock.
    fn new(
        bindings: &[FieldProteinBinding],
        owners: &[Option<String>],
        stored: &BTreeMap<String, Option<Protein>>,
    ) -> Result<Self, SchemaError> {
        if bindings.len() != owners.len() {
            return Err(invalid("field protein membership batch is incomplete"));
        }
        let mut proteins = BTreeMap::new();
        let mut moved = BTreeMap::new();
        for (binding, owner) in bindings.iter().zip(owners) {
            let target = binding.protein_uuid();
            if binding.field_identity.is_empty()
                || binding.member.molecule_uuid.is_empty()
                || binding.member.hash_field.trim().is_empty()
            {
                return Err(invalid(
                    "field protein requires a field identity, molecule and hash field",
                ));
            }
            let protein = proteins.entry(target.clone()).or_insert_with(|| {
                stored
                    .get(&target)
                    .and_then(Clone::clone)
                    .unwrap_or_else(|| Protein::new(&target))
            });
            if let Some(owner) = owner.as_ref().filter(|owner| **owner != target) {
                if owner.starts_with("field:") || moved.get(owner).is_some_and(|id| id != &target) {
                    return Err(invalid(
                        "one molecule cannot name different canonical fields",
                    ));
                }
                let old = stored.get(owner).and_then(Option::as_ref).ok_or_else(|| {
                    invalid("field protein back-reference names an absent protein")
                })?;
                for member in &old.members {
                    if !protein.members.contains(member) {
                        protein.members.push(member.clone());
                    }
                }
                moved.insert(owner.clone(), target.clone());
            }
            if !protein.members.contains(&binding.member) {
                protein.members.push(binding.member.clone());
            }
        }
        Self::finish(proteins, moved, stored)
    }

    fn finish(
        proteins: BTreeMap<String, Protein>,
        moved: BTreeMap<String, String>,
        stored: &BTreeMap<String, Option<Protein>>,
    ) -> Result<Self, SchemaError> {
        let mut plan = Self::default();
        let mut assigned = BTreeMap::new();
        for (uuid, protein) in proteins {
            for member in &protein.members {
                if assigned
                    .insert(member.molecule_uuid.clone(), uuid.clone())
                    .is_some_and(|old| old != uuid)
                {
                    return Err(invalid(
                        "one molecule cannot name different canonical fields",
                    ));
                }
            }
            if stored.get(&uuid).and_then(Option::as_ref) == Some(&protein) {
                continue;
            }
            for member in &protein.members {
                plan.added.push(edge(&uuid, member));
                plan.rows.push((
                    member_backref_key(&member.molecule_uuid),
                    serde_json::json!(uuid),
                ));
            }
            plan.rows.push((
                protein_record_key(&uuid),
                serde_json::to_value(protein)
                    .map_err(|error| invalid(&format!("serialize field protein: {error}")))?,
            ));
        }
        for (old, target) in moved {
            for member in &stored[&old]
                .as_ref()
                .expect("validated old protein")
                .members
            {
                plan.removed.push(edge(&old, member));
            }
            plan.deletes.push(protein_record_key(&old).into_bytes());
            let identity = target
                .strip_prefix("field:")
                .expect("canonical protein prefix");
            plan.deletes
                .push(format!("fldprot:{identity}:{old}").into_bytes());
        }
        Ok(plan)
    }
}

fn edge(protein: &str, member: &ProteinMember) -> MoleculeRefEdge {
    MoleculeRefEdge::protein_member(
        protein,
        &member.molecule_uuid,
        &member.hash_field,
        member.range_field.as_deref(),
    )
}

fn invalid(message: &str) -> SchemaError {
    SchemaError::InvalidData(message.to_owned())
}
