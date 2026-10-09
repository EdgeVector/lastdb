//! Protein record, membership, and fold-job types.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Marker stored on fold jobs / diagnostics so callers can assert protein path.
pub const PROTEIN_SCHEMA_MARKER: &str = "lastdb.protein.v1";

/// One conformation of a protein: a molecule under a particular key layout.
///
/// `hash_field` / `range_field` name the **field values** used to derive this
/// member's tip coordinates from a write's field map (not the molecule uuid).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProteinMember {
    /// Molecule that holds tips under this key layout.
    pub molecule_uuid: String,
    /// Field whose value is the hash/partition key for this member.
    pub hash_field: String,
    /// Optional range/sort field for HashRange members.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_field: Option<String>,
}

impl ProteinMember {
    #[must_use]
    pub fn new(
        molecule_uuid: impl Into<String>,
        hash_field: impl Into<String>,
        range_field: Option<String>,
    ) -> Self {
        Self {
            molecule_uuid: molecule_uuid.into(),
            hash_field: hash_field.into(),
            range_field,
        }
    }

    /// Resolve tip coordinates from a write's field map.
    ///
    /// Returns `None` when the hash field (or required range field) is missing
    /// or empty — that member is skipped for this write.
    #[must_use]
    pub fn tip_coords_from_fields(
        &self,
        fields: &HashMap<String, String>,
    ) -> Option<(String, String)> {
        let hash = fields
            .get(&self.hash_field)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;
        let range = if let Some(rf) = self.range_field.as_deref() {
            let rk = fields
                .get(rf)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())?;
            rk
        } else {
            String::new()
        };
        Some((hash, range))
    }
}

/// UUID-identified protein: member list is the authority on who must fold.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Protein {
    pub uuid: String,
    pub members: Vec<ProteinMember>,
}

impl Protein {
    #[must_use]
    pub fn new(uuid: impl Into<String>) -> Self {
        Self {
            uuid: uuid.into(),
            members: Vec::new(),
        }
    }

    /// Find the first member binding for a molecule uuid, if present.
    #[must_use]
    pub fn member_for_molecule(&self, molecule_uuid: &str) -> Option<&ProteinMember> {
        self.members
            .iter()
            .find(|m| m.molecule_uuid == molecule_uuid)
    }

    /// Find member by molecule + key layout (same molecule can have multiple
    /// conformations / key layouts).
    #[must_use]
    pub fn member_for_layout(
        &self,
        molecule_uuid: &str,
        hash_field: &str,
        range_field: Option<&str>,
    ) -> Option<&ProteinMember> {
        self.members.iter().find(|m| {
            m.molecule_uuid == molecule_uuid
                && m.hash_field == hash_field
                && m.range_field.as_deref() == range_field
        })
    }

    /// True when `molecule_uuid` is already a member (any layout).
    #[must_use]
    pub fn contains_molecule(&self, molecule_uuid: &str) -> bool {
        self.member_for_molecule(molecule_uuid).is_some()
    }
}

/// Enqueued fold: repoint sibling member tips to a shared atom tip.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProteinFoldJob {
    pub job_id: String,
    pub protein_uuid: String,
    /// Molecule that already holds the new tip (skip on fold).
    pub entry_molecule_uuid: String,
    /// Shared atom uuid all siblings must tip at.
    pub atom_uuid: String,
    pub written_at: u64,
    pub device_id: String,
    /// Full field map from the write — used to derive sibling tip keys.
    pub fields: HashMap<String, String>,
    /// Schema marker for diagnostics / evidence.
    #[serde(default = "default_schema_marker")]
    pub schema: String,
}

fn default_schema_marker() -> String {
    PROTEIN_SCHEMA_MARKER.to_string()
}

/// Result of a protein-aware write through the entry member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProteinWriteOutcome {
    pub protein_uuid: String,
    pub atom_uuid: String,
    pub entry_molecule_uuid: String,
    /// Fold jobs enqueued for sibling members (0 when sole member).
    pub fold_jobs_enqueued: usize,
    /// True when the entry tip was written on the protein path.
    pub used_protein_path: bool,
}
