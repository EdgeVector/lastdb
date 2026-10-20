//! Protein operations on [`AtomStore`]: create, bind, write, backfill.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::atom::AtomEntry;
use crate::db_operations::atom_store::{AtomStore, ChangedKey, MoleculeData, MoleculeRefEdge};
use crate::schema::SchemaError;
use crate::security::Ed25519KeyPair;

use super::keys::{member_backref_key, protein_record_key};
use super::types::{Protein, ProteinMember};

mod batch;
mod tip;
pub(crate) use tip::PreparedProteinTip;

struct ProteinTipScope<'a> {
    honor_tip_lww: bool,
    storage_prefix: Option<&'a str>,
}

impl AtomStore {
    /// Create a new empty protein (UUID identity). Membership is empty until
    /// [`Self::protein_add_member`].
    pub async fn protein_create(&self) -> Result<Protein, SchemaError> {
        let uuid = Uuid::new_v4().to_string();
        let protein = Protein::new(uuid);
        self.put_protein(&protein).await?;
        Ok(protein)
    }

    /// Load a protein by UUID.
    pub async fn protein_get(&self, protein_uuid: &str) -> Result<Option<Protein>, SchemaError> {
        self.raw()
            .get_item(&protein_record_key(protein_uuid))
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read protein {protein_uuid}: {e}")))
    }

    /// Protein UUID for a member molecule, if bound.
    pub async fn protein_of_molecule(
        &self,
        molecule_uuid: &str,
    ) -> Result<Option<String>, SchemaError> {
        let key = member_backref_key(molecule_uuid);
        let v: Option<String> =
            self.raw().get_item(&key).await.map_err(|e| {
                SchemaError::InvalidData(format!("read molprot {molecule_uuid}: {e}"))
            })?;
        Ok(v)
    }

    /// Bind a molecule as a protein member (bi-directional).
    ///
    /// Idempotent when the same molecule is already a member with the same
    /// key layout. Errors if the molecule is already bound to a *different*
    /// protein.
    pub async fn protein_add_member(
        &self,
        protein_uuid: &str,
        member: ProteinMember,
    ) -> Result<Protein, SchemaError> {
        self.with_protein_membership(self.protein_add_member_in_scope(protein_uuid, member))
            .await
    }

    /// The caller holds `with_protein_membership` across selection and both binds.
    pub(crate) async fn protein_add_member_in_scope(
        &self,
        protein_uuid: &str,
        member: ProteinMember,
    ) -> Result<Protein, SchemaError> {
        if member.molecule_uuid.is_empty() {
            return Err(SchemaError::InvalidData(
                "protein member molecule_uuid must be non-empty".into(),
            ));
        }
        if member.hash_field.trim().is_empty() {
            return Err(SchemaError::InvalidData(
                "protein member hash_field must be non-empty".into(),
            ));
        }

        if let Some(existing) = self.protein_of_molecule(&member.molecule_uuid).await? {
            if existing != protein_uuid {
                return Err(SchemaError::InvalidData(format!(
                    "molecule {} already bound to protein {existing}, not {protein_uuid}",
                    member.molecule_uuid
                )));
            }
        }

        let mut protein = self
            .protein_get(protein_uuid)
            .await?
            .ok_or_else(|| SchemaError::InvalidData(format!("protein {protein_uuid} not found")))?;

        // Same molecule + same key layout → no-op. Same molecule + different
        // layout → additional conformation (shared atom, multi-key tips).
        if protein
            .member_for_layout(
                &member.molecule_uuid,
                &member.hash_field,
                member.range_field.as_deref(),
            )
            .is_some()
        {
            return Ok(protein);
        }

        let molecule_ref = MoleculeRefEdge::protein_member(
            protein_uuid,
            &member.molecule_uuid,
            &member.hash_field,
            member.range_field.as_deref(),
        );
        // The binding record, its counter, the protein record and the back-ref
        // are one batch: the caller holds `with_protein_membership`, which is
        // one native write scope.
        self.add_molecule_bindings(std::slice::from_ref(&molecule_ref), None)
            .await?;
        protein.members.push(member.clone());
        self.put_protein(&protein).await?;
        self.raw()
            .put_item(&member_backref_key(&member.molecule_uuid), &protein_uuid)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("write molprot {}: {e}", member.molecule_uuid))
            })?;
        Ok(protein)
    }

    /// Read tip atom uuid at a member's key coordinates (None if absent).
    pub async fn protein_get_member_tip(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) -> Result<Option<AtomEntry>, SchemaError> {
        let storage_hash = self.storage_hash(molecule_uuid, hash)?;
        let storage_range = self.storage_range(molecule_uuid, range)?;
        let base = crate::atom::molecule_key_codec::hash_range_record_key(
            molecule_uuid,
            &storage_hash,
            &storage_range,
        );
        match self.get_per_key(&base, None).await? {
            Some((_, rec)) => Ok(Some(rec.entry)),
            None => Ok(None),
        }
    }

    // ── internals ──────────────────────────────────────────────────────────

    async fn put_protein(&self, protein: &Protein) -> Result<(), SchemaError> {
        self.raw()
            .put_item(&protein_record_key(&protein.uuid), protein)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("write protein {}: {e}", protein.uuid)))
    }
}
