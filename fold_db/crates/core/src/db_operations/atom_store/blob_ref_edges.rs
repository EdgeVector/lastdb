//! Durable active atom-to-blob reverse edges.
//!
//! The key partitions by blob reference. A writer places the edge before the
//! immutable atom body in one ordered batch. Atom reclaim removes the body
//! before it removes these derived edges. A crash can therefore retain a blob,
//! but cannot leave a live atom without a retaining edge.

use super::AtomStore;
use crate::hex::hex_lower;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

const BLOB_REF_PREFIX: &str = "bref:v1:e:";
pub const BLOB_REF_COMPLETE_KEY: &str = "bref:v1:complete";
const BLOB_REF_DOMAIN: &[u8] = b"lastdb:blob-ref-edge-source:v1\0";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobRefEdge {
    pub blob_ref: String,
    pub atom_uuid: String,
}

impl BlobRefEdge {
    #[must_use]
    pub fn atom(atom_uuid: &str, blob_ref: &str) -> Self {
        Self {
            blob_ref: blob_ref.to_string(),
            atom_uuid: atom_uuid.to_string(),
        }
    }

    pub(crate) fn storage_key(&self, storage_prefix: Option<&str>) -> String {
        let target = hex_lower(Sha256::digest(self.blob_ref.as_bytes()));
        let mut source = Sha256::new();
        source.update(BLOB_REF_DOMAIN);
        source.update(self.atom_uuid.as_bytes());
        build_storage_key(
            storage_prefix,
            &format!(
                "{BLOB_REF_PREFIX}{target}\0{}",
                hex_lower(source.finalize())
            ),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobRefLookup {
    pub complete: bool,
    pub edges: Vec<BlobRefEdge>,
}

/// Durable proof that the bootstrap read every canonical atom source.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobRefCompleteness {
    pub version: u8,
    pub atoms_read: u64,
    pub edges_written: u64,
}

impl AtomStore {
    pub async fn blob_ref_edges_complete(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let key = build_storage_key(storage_prefix, BLOB_REF_COMPLETE_KEY);
        self.main_store.exists_item(&key).await.map_err(|error| {
            SchemaError::InvalidData(format!("probe blob reference completeness: {error}"))
        })
    }

    pub async fn blob_ref_edges_for_blob(
        &self,
        blob_ref: &str,
        storage_prefix: Option<&str>,
    ) -> Result<BlobRefLookup, SchemaError> {
        let target = hex_lower(Sha256::digest(blob_ref.as_bytes()));
        let prefix = build_storage_key(storage_prefix, &format!("{BLOB_REF_PREFIX}{target}\0"));
        let edges = self
            .main_store
            .scan_items_with_prefix::<BlobRefEdge>(&prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("read blob reference edges: {error}"))
            })?
            .into_iter()
            .map(|(_, edge)| edge)
            .collect();
        let complete = self.blob_ref_edges_complete(storage_prefix).await?;
        Ok(BlobRefLookup { complete, edges })
    }

    pub async fn has_active_blob_refs(
        &self,
        blob_ref: &str,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        if !self.blob_ref_edges_complete(storage_prefix).await? {
            return Ok(true);
        }
        let target = hex_lower(Sha256::digest(blob_ref.as_bytes()));
        let prefix = build_storage_key(storage_prefix, &format!("{BLOB_REF_PREFIX}{target}\0"));
        self.main_store
            .inner()
            .scan_prefix_paged(prefix.as_bytes(), 1)
            .await
            .map(|rows| !rows.is_empty())
            .map_err(|error| {
                SchemaError::InvalidData(format!("probe blob reference edges: {error}"))
            })
    }

    /// Install all atom-to-blob edges after an isolated-copy atom walk.
    /// The marker is durable only after every edge write succeeds.
    pub async fn bootstrap_blob_ref_edges_on_isolated_copy(
        &self,
        edges: &[BlobRefEdge],
        atoms_read: u64,
        storage_prefix: Option<&str>,
    ) -> Result<BlobRefCompleteness, SchemaError> {
        let complete_key = build_storage_key(storage_prefix, BLOB_REF_COMPLETE_KEY);
        self.main_store
            .delete_item(&complete_key)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "clear blob reference completeness before bootstrap: {error}"
                ))
            })?;
        let edge_prefix = build_storage_key(storage_prefix, BLOB_REF_PREFIX);
        let old_rows: Vec<(String, serde_json::Value)> = self
            .main_store
            .scan_items_with_prefix(&edge_prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "scan old blob reference edges before bootstrap: {error}"
                ))
            })?;
        for (key, _) in old_rows {
            self.main_store.delete_item(&key).await.map_err(|error| {
                SchemaError::InvalidData(format!(
                    "clear old blob reference edge before bootstrap: {error}"
                ))
            })?;
        }
        let mut expected = HashSet::new();
        for edge in edges {
            let key = edge.storage_key(storage_prefix);
            if !expected.insert(key.clone()) {
                continue;
            }
            self.main_store
                .put_item(&key, edge)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("put blob reference edge: {error}"))
                })?;
        }
        let written: Vec<(String, BlobRefEdge)> = self
            .main_store
            .scan_items_with_prefix(&edge_prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("audit blob reference bootstrap: {error}"))
            })?;
        let actual: HashSet<String> = written.into_iter().map(|(key, _)| key).collect();
        if actual != expected {
            return Err(SchemaError::InvalidData(format!(
                "blob reference bootstrap audit failed: expected {} edge keys, found {}",
                expected.len(),
                actual.len()
            )));
        }
        let proof = BlobRefCompleteness {
            version: 1,
            atoms_read,
            edges_written: expected.len() as u64,
        };
        self.main_store
            .put_item(&complete_key, &proof)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("persist blob reference completeness: {error}"))
            })?;
        Ok(proof)
    }

    /// Rebuild one storage domain from its canonical immutable atom bodies.
    /// Any unreadable atom aborts before the completeness marker is written.
    pub async fn bootstrap_blob_ref_edges_from_atoms_on_isolated_copy(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<BlobRefCompleteness, SchemaError> {
        let (start, end) = Self::kind_plane_scan_bounds(storage_prefix, "atom:");
        let rows = self
            .main_store
            .inner()
            .scan_range(start.as_bytes(), end.as_bytes())
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "scan atoms for blob reference bootstrap: {error}"
                ))
            })?;
        let mut edges = Vec::new();
        for (key, value) in &rows {
            let key = String::from_utf8_lossy(key);
            let uuid = Self::atom_uuid_from_body_key(&key).ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "blob reference bootstrap found malformed atom key {key:?}"
                ))
            })?;
            let atom = self.decode_atom_bytes(value).await.map_err(|error| {
                SchemaError::InvalidData(format!(
                    "blob reference bootstrap cannot decode atom {uuid}: {error}"
                ))
            })?;
            edges.extend(
                crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
                    .into_iter()
                    .map(|blob_ref| BlobRefEdge::atom(uuid, &blob_ref)),
            );
        }
        self.bootstrap_blob_ref_edges_on_isolated_copy(&edges, rows.len() as u64, storage_prefix)
            .await
    }
}
