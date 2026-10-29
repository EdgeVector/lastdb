//! Protein membership and tip operations on [`AtomStore`].

use std::collections::{HashMap, HashSet};

use crate::atom::AtomEntry;
use crate::db_operations::atom_store::{AtomStore, ChangedKey, MoleculeData, MoleculeRefEdge};
use crate::schema::SchemaError;
use crate::security::Ed25519KeyPair;

use super::keys::{member_backref_key, protein_record_key};
use super::types::{Protein, ProteinMember};

mod batch;
mod field_identity;
pub(crate) use field_identity::FieldProteinBinding;
mod release;
mod tip;
pub(crate) use tip::PreparedProteinTip;

struct ProteinTipScope<'a> {
    honor_tip_lww: bool,
    storage_prefix: Option<&'a str>,
}

impl AtomStore {
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
}
