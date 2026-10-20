//! Binding records of a molecule: one record for each active source.
//!
//! A binding record is a set member that says "this source names this molecule":
//! a schema field, a protein member, or a database catalog. The record is the
//! state that makes a bind or an unbind idempotent. It is not an authority. The
//! counter `bindings` next to the records (see `molecule_bindings.rs`) is the
//! only test of whether a molecule is bound, and it changes in the same batch as
//! the record that flips.
//!
//! The key is addressable by the target molecule, so the schema-field sources of
//! one molecule are one range read under one hash. The memory-acknowledgement
//! path reads them to name every schema that shares a molecule.

use super::AtomStore;
use crate::hex::hex_lower;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MOLECULE_REF_PREFIX: &str = "mref:v1:e:";
const MOLECULE_BINDINGS_PREFIX: &str = "mref:v1:n:";
const MOLECULE_REF_DOMAIN: &[u8] = b"lastdb:molecule-ref-edge-source:v1\0";

/// One active source that binds a molecule.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MoleculeRefEdge {
    pub molecule_uuid: String,
    pub source: String,
    pub edge_type: String,
}

impl MoleculeRefEdge {
    /// A schema field source. The binding name and field make this identity
    /// stable across retries and distinct from another copied catalog.
    #[must_use]
    pub fn schema_field(schema: &str, field: &str, molecule_uuid: &str) -> Self {
        Self {
            molecule_uuid: molecule_uuid.to_string(),
            source: format!("schema:{schema}:field:{field}"),
            edge_type: "schema-field".to_string(),
        }
    }

    /// A protein member source. A JSON tuple preserves field-name boundaries
    /// and distinguishes no range field from an explicitly empty field name.
    /// One molecule can have several key layouts in one protein.
    #[must_use]
    pub fn protein_member(
        protein_uuid: &str,
        molecule_uuid: &str,
        hash_field: &str,
        range_field: Option<&str>,
    ) -> Self {
        Self {
            molecule_uuid: molecule_uuid.to_string(),
            source: format!(
                "protein:{}",
                serde_json::to_string(&(protein_uuid, molecule_uuid, hash_field, range_field))
                    .expect("protein member identity is serializable")
            ),
            edge_type: "protein-member".to_string(),
        }
    }

    /// One named database catalog's field path to an existing molecule.
    #[must_use]
    pub fn database_catalog_field(
        db_locator: &str,
        schema: &str,
        field: &str,
        molecule_uuid: &str,
    ) -> Self {
        Self {
            molecule_uuid: molecule_uuid.to_string(),
            source: format!("database:{db_locator}:schema:{schema}:field:{field}"),
            edge_type: "database-catalog-field".to_string(),
        }
    }

    pub(crate) fn storage_key(&self, storage_prefix: Option<&str>) -> String {
        let mut hasher = Sha256::new();
        hasher.update(MOLECULE_REF_DOMAIN);
        hasher.update(self.edge_type.as_bytes());
        hasher.update([0]);
        hasher.update(self.source.as_bytes());
        let source = hex_lower(hasher.finalize());
        build_storage_key(
            storage_prefix,
            &format!(
                "{MOLECULE_REF_PREFIX}{}\0{source}",
                molecule_target(&self.molecule_uuid)
            ),
        )
    }
}

/// The hex token of a molecule in the keys of its binding records.
fn molecule_target(molecule_uuid: &str) -> String {
    hex_lower(Sha256::digest(molecule_uuid.as_bytes()))
}

/// The key of the `bindings` counter of one molecule in one scope.
pub(super) fn molecule_bindings_key(molecule_uuid: &str, storage_prefix: Option<&str>) -> String {
    build_storage_key(
        storage_prefix,
        &format!(
            "{MOLECULE_BINDINGS_PREFIX}{}",
            molecule_target(molecule_uuid)
        ),
    )
}

/// The binding records of one molecule, and whether the counter agrees.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MoleculeRefLookup {
    /// The `bindings` counter equals the number of records read and is not
    /// `suspect`. A false value means the records and the counter disagree: the
    /// caller must not rely on the list.
    pub complete: bool,
    pub edges: Vec<MoleculeRefEdge>,
}

impl AtomStore {
    /// Read one molecule's binding records (one range read under one hash) and
    /// compare their number with the `bindings` counter.
    pub async fn molecule_ref_edges_for_molecule(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<MoleculeRefLookup, SchemaError> {
        self.atom_read_scope(
            self.molecule_ref_edges_for_molecule_captured(molecule_uuid, storage_prefix),
        )
        .await
    }

    async fn molecule_ref_edges_for_molecule_captured(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<MoleculeRefLookup, SchemaError> {
        let prefix = build_storage_key(
            storage_prefix,
            &format!("{MOLECULE_REF_PREFIX}{}\0", molecule_target(molecule_uuid)),
        );
        let rows = self
            .main_store
            .scan_items_with_prefix::<MoleculeRefEdge>(&prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("read molecule binding records: {error}"))
            })?;
        if rows.iter().any(|(key, edge)| {
            edge.molecule_uuid != molecule_uuid || edge.storage_key(storage_prefix) != *key
        }) {
            return Err(SchemaError::StorageRead {
                detail: "molecule binding record disagrees with its selected target key".into(),
            });
        }
        let counter = self
            .read_binding_counters(
                std::slice::from_ref(&molecule_uuid.to_string()),
                storage_prefix,
            )
            .await?
            .pop()
            .flatten()
            .unwrap_or_default();
        let edges: Vec<_> = rows.into_iter().map(|(_, edge)| edge).collect();
        Ok(MoleculeRefLookup {
            complete: !counter.suspect && counter.bindings == edges.len() as u64,
            edges,
        })
    }
}
