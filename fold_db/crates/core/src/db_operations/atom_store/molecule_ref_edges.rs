//! Durable active schema-field to molecule reverse edges.
//!
//! An edge is a set member, not a counter. The edge key is target-addressable,
//! so a reclaim check can stop after its first member. Catalog writers journal
//! the transition before they change membership. Startup recovery then makes
//! the membership, edge, and count state converge after a process stop.

use super::AtomStore;
use crate::hex::hex_lower;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

const MOLECULE_REF_PREFIX: &str = "mref:v1:e:";
const DATABASE_CATALOG_REF_PRESENT_KEY: &str = "mref:v1:m:database-catalog-present";
pub const MOLECULE_REF_COMPLETE_KEY: &str = "mref:v1:complete";
const MOLECULE_REF_DOMAIN: &[u8] = b"lastdb:molecule-ref-edge-source:v1\0";

/// One active source that keeps a molecule reachable.
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

    /// A protein member source. The key layout is part of the source identity
    /// because one molecule can have several conformations in one protein.
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
                "protein:{protein_uuid}:member:{molecule_uuid}:layout:{hash_field}:{}",
                range_field.unwrap_or("")
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
        let target = hex_lower(Sha256::digest(self.molecule_uuid.as_bytes()));
        let mut hasher = Sha256::new();
        hasher.update(MOLECULE_REF_DOMAIN);
        hasher.update(self.edge_type.as_bytes());
        hasher.update([0]);
        hasher.update(self.source.as_bytes());
        let source = hex_lower(hasher.finalize());
        build_storage_key(
            storage_prefix,
            &format!("{MOLECULE_REF_PREFIX}{target}\0{source}"),
        )
    }
}

/// Target-partition lookup result. A missing completeness marker never
/// authorizes reclaim, even when the target partition has no edge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MoleculeRefLookup {
    pub complete: bool,
    pub edges: Vec<MoleculeRefEdge>,
}

/// Durable proof that every canonical molecule source class was read.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MoleculeRefCompleteness {
    pub version: u8,
    pub schema_fields: u64,
    pub protein_members: u64,
}

impl AtomStore {
    pub(crate) fn database_catalog_ref_present_key(storage_prefix: Option<&str>) -> String {
        build_storage_key(storage_prefix, DATABASE_CATALOG_REF_PRESENT_KEY)
    }

    pub(crate) async fn database_catalog_refs_present(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        self.main_store
            .exists_item(&Self::database_catalog_ref_present_key(storage_prefix))
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "probe database-catalog reference marker: {error}"
                ))
            })
    }

    pub async fn molecule_ref_edges_complete(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let key = build_storage_key(storage_prefix, MOLECULE_REF_COMPLETE_KEY);
        self.main_store.exists_item(&key).await.map_err(|error| {
            SchemaError::InvalidData(format!("probe molecule reference completeness: {error}"))
        })
    }

    /// Retain one molecule source before its catalog row becomes visible.
    pub async fn put_molecule_ref_edge(
        &self,
        edge: &MoleculeRefEdge,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.main_store
            .put_item(&edge.storage_key(storage_prefix), edge)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("put molecule reference edge: {error}"))
            })
    }

    /// Remove one source edge after its authoritative catalog row is absent.
    pub async fn delete_molecule_ref_edge(
        &self,
        edge: &MoleculeRefEdge,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.main_store
            .delete_item(&edge.storage_key(storage_prefix))
            .await
            .map(|_| ())
            .map_err(|error| {
                SchemaError::InvalidData(format!("delete molecule reference edge: {error}"))
            })
    }

    /// Read one molecule target partition. This is the only liveness query a
    /// future molecule reclaimer may use; it is not a count.
    pub async fn molecule_ref_edges_for_molecule(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<MoleculeRefLookup, SchemaError> {
        let target = hex_lower(Sha256::digest(molecule_uuid.as_bytes()));
        let prefix = build_storage_key(storage_prefix, &format!("{MOLECULE_REF_PREFIX}{target}\0"));
        let edges = self
            .main_store
            .scan_items_with_prefix::<MoleculeRefEdge>(&prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("read molecule reference edges: {error}"))
            })?
            .into_iter()
            .map(|(_, edge)| edge)
            .collect();
        let complete = self.molecule_ref_edges_complete(storage_prefix).await?;
        Ok(MoleculeRefLookup { complete, edges })
    }

    /// Test the active edge set without materializing an operator explanation.
    pub async fn has_active_molecule_refs(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        if !self.molecule_ref_edges_complete(storage_prefix).await? {
            return Ok(true);
        }
        self.has_any_molecule_ref_edges(molecule_uuid, storage_prefix)
            .await
    }

    /// Probe whether a target has any durable active-source edge, without
    /// treating the global bootstrap marker as a liveness proof.
    ///
    /// Callers may use this only when another durable proof names the target
    /// and proves its source catalog binding was removed. A missing bootstrap
    /// marker otherwise remains a retain-all condition.
    pub(crate) async fn has_any_molecule_ref_edges(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let target = hex_lower(Sha256::digest(molecule_uuid.as_bytes()));
        let prefix = build_storage_key(storage_prefix, &format!("{MOLECULE_REF_PREFIX}{target}\0"));
        self.main_store
            .inner()
            .scan_prefix_paged(prefix.as_bytes(), 1)
            .await
            .map(|rows| !rows.is_empty())
            .map_err(|error| {
                SchemaError::InvalidData(format!("probe molecule reference edges: {error}"))
            })
    }

    /// Install a complete molecule edge set after a caller reads every
    /// canonical schema-field and protein-member source on an isolated copy.
    /// The marker is last. A crash before it leaves reclaim disabled.
    pub async fn bootstrap_molecule_ref_edges_on_isolated_copy(
        &self,
        edges: &[MoleculeRefEdge],
        schema_fields: u64,
        protein_members: u64,
        storage_prefix: Option<&str>,
    ) -> Result<MoleculeRefCompleteness, SchemaError> {
        let complete_key = build_storage_key(storage_prefix, MOLECULE_REF_COMPLETE_KEY);
        self.main_store
            .delete_item(&complete_key)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "clear molecule reference completeness before bootstrap: {error}"
                ))
            })?;
        let edge_prefix = build_storage_key(storage_prefix, MOLECULE_REF_PREFIX);
        let old_rows: Vec<(String, serde_json::Value)> = self
            .main_store
            .scan_items_with_prefix(&edge_prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "scan old molecule reference edges before bootstrap: {error}"
                ))
            })?;
        for (key, _) in old_rows {
            self.main_store.delete_item(&key).await.map_err(|error| {
                SchemaError::InvalidData(format!(
                    "clear old molecule reference edge before bootstrap: {error}"
                ))
            })?;
        }
        let mut expected = HashSet::new();
        for edge in edges {
            let key = edge.storage_key(storage_prefix);
            if expected.insert(key) {
                self.put_molecule_ref_edge(edge, storage_prefix).await?;
            }
        }
        let written: Vec<(String, MoleculeRefEdge)> = self
            .main_store
            .scan_items_with_prefix(&edge_prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("audit molecule reference bootstrap: {error}"))
            })?;
        let actual: HashSet<String> = written.into_iter().map(|(key, _)| key).collect();
        if actual != expected {
            return Err(SchemaError::InvalidData(format!(
                "molecule reference bootstrap audit failed: expected {} edge keys, found {}",
                expected.len(),
                actual.len()
            )));
        }
        let proof = MoleculeRefCompleteness {
            version: 1,
            schema_fields,
            protein_members,
        };
        self.main_store
            .put_item(&complete_key, &proof)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "persist molecule reference completeness: {error}"
                ))
            })?;
        Ok(proof)
    }
}
