//! The protein index: which molecules each protein binds.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use fold_db::protein::{member_backref_key, protein_record_key, Protein};
use fold_db::storage::traits::{NamespacedStore, PhysicalScanPage};

use super::keys::{mol_key, MolKey};
use super::walk::{pages_agree, walk_both};
use super::ReapError;

/// Members of every protein, and the back-reference rows.
#[derive(Debug, Default)]
pub(crate) struct ProteinIndex {
    /// Protein id to the molecules it binds. A molecule that a back-reference
    /// binds to a known protein is a member of that protein.
    pub members: BTreeMap<String, BTreeSet<MolKey>>,
    /// Molecule to the protein ids that its back-reference rows name.
    pub backrefs: BTreeMap<MolKey, BTreeSet<String>>,
    /// Count of `protein:` records read.
    pub records: u64,
    /// Count of `molprot:` rows read.
    pub backref_rows: u64,
}

impl ProteinIndex {
    /// Fold one row of the proteins plane into the index.
    pub(crate) fn absorb(&mut self, key: &[u8], value: &[u8]) -> Result<(), ReapError> {
        let Ok(key) = std::str::from_utf8(key) else {
            return Ok(());
        };
        if let Some(uuid) = key.strip_prefix(protein_record_key("").as_str()) {
            let protein = decode_protein(uuid, value)?;
            let members: BTreeSet<MolKey> = protein
                .members
                .iter()
                .map(|member| mol_key(&member.molecule_uuid))
                .collect();
            for id in [uuid, protein.uuid.as_str()] {
                self.members
                    .entry(id.to_string())
                    .or_default()
                    .extend(members.iter().copied());
            }
            self.records += 1;
        } else if let Some(molecule) = key.strip_prefix(member_backref_key("").as_str()) {
            let text = String::from_utf8_lossy(value);
            let uuid =
                serde_json::from_str::<String>(&text).unwrap_or_else(|_| text.trim().to_string());
            self.backrefs
                .entry(mol_key(molecule))
                .or_default()
                .insert(uuid);
            self.backref_rows += 1;
        }
        Ok(())
    }

    /// Make every back-referenced molecule a member of its known protein.
    pub(crate) fn merge_backrefs(&mut self) {
        for (molecule, uuids) in &self.backrefs {
            for uuid in uuids {
                if let Some(members) = self.members.get_mut(uuid) {
                    members.insert(*molecule);
                }
            }
        }
    }
}

fn decode_protein(uuid: &str, value: &[u8]) -> Result<Protein, ReapError> {
    serde_json::from_slice(value).map_err(|error| {
        ReapError::abort(
            "PROTEIN_UNDECODABLE",
            format!("protein record {uuid} does not decode: {error}"),
        )
    })
}

/// Fold one pair of pages (raw and through the seam) into the index.
///
/// A record that the seam hides would be a protein that the index misses, and
/// a missed protein could let a molecule with a live sibling look dead.
pub(crate) fn absorb_pair(
    index: &mut ProteinIndex,
    raw: &PhysicalScanPage,
    seam: &PhysicalScanPage,
) -> Result<(), ReapError> {
    pages_agree(raw, seam).map_err(|message| {
        ReapError::abort("PROTEIN_UNSEALED_ROW", format!("proteins plane: {message}"))
    })?;
    for (key, value) in &seam.rows {
        index.absorb(key, value)?;
    }
    Ok(())
}

/// Read the whole `proteins` plane with two readers in step.
pub(crate) async fn load(
    base: &Arc<dyn NamespacedStore>,
    store: &Arc<dyn NamespacedStore>,
) -> Result<ProteinIndex, ReapError> {
    let open = |store: &Arc<dyn NamespacedStore>| {
        let store = Arc::clone(store);
        async move {
            store
                .open_namespace("proteins")
                .await
                .map_err(|error| ReapError::Failed(format!("open proteins: {error}")))
        }
    };
    let (raw, seam) = (open(base).await?, open(store).await?);
    let mut index = ProteinIndex::default();
    walk_both(raw, seam, "PROTEIN_UNSEALED_ROW", |raw_page, seam_page| {
        absorb_pair(&mut index, raw_page, seam_page)
    })
    .await?;
    index.merge_backrefs();
    Ok(index)
}
