//! Compatibility reports for clients that read the former skipped-body gauges.
//! Selected-tip reads now require every body and return an error on a miss.

use super::DbOperations;
use crate::schema::types::key_value::KeyValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnresolvedDamage {
    pub edges: u64,
    pub rows: u64,
    pub capped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct UnresolvedAtomIdentityReport {
    pub identities: Vec<UnresolvedAtomIdentity>,
    pub edges: u64,
    pub rows: u64,
    pub capped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct UnresolvedAtomIdentity {
    pub atom_uuid: String,
    pub key: KeyValue,
    pub molecule_uuid: Option<String>,
    pub schema: Option<String>,
    pub field: Option<String>,
    pub storage_namespace: Option<String>,
    pub tip_storage_key: Option<String>,
    pub atom_storage_key: Option<String>,
}

impl DbOperations {
    /// Native reads never skip a selected body, so this compatibility gauge is zero.
    #[must_use]
    pub fn unresolved_atom_skip_count(&self) -> u64 {
        0
    }

    /// Preserve the former damage-report shape without dormant identity storage.
    #[must_use]
    pub fn unresolved_atom_distinct(&self) -> UnresolvedDamage {
        UnresolvedDamage {
            edges: 0,
            rows: 0,
            capped: false,
        }
    }

    /// Missing selected bodies are read errors, never successful skipped records.
    pub fn unresolved_atom_identity_report(&self) -> Result<UnresolvedAtomIdentityReport, String> {
        Ok(UnresolvedAtomIdentityReport {
            identities: Vec::new(),
            edges: 0,
            rows: 0,
            capped: false,
        })
    }
}
