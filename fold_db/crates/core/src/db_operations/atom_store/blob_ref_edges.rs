//! Node-global blob roots owned independently by immutable atoms and live tips.

use super::AtomStore;
use crate::hex::hex_lower;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod bootstrap;
mod tips;
pub(crate) use tips::TipBlobChanges;

const BLOB_REF_PREFIX: &str = "bref:v1:e:";
const LEGACY_COMPLETE_KEY: &str = "bref:v1:complete";
pub const BLOB_REF_COMPLETE_KEY: &str = "bref:v2:complete";
const BLOB_REF_DOMAIN: &[u8] = b"lastdb:blob-ref-edge-source:v1\0";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobRefEdge {
    pub blob_ref: String,
    pub atom_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tip_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub atom_scope: Option<String>,
}

impl BlobRefEdge {
    #[must_use]
    pub fn atom(atom_uuid: &str, blob_ref: &str) -> Self {
        Self {
            blob_ref: blob_ref.to_string(),
            atom_uuid: atom_uuid.to_string(),
            tip_key: None,
            atom_scope: None,
        }
    }

    pub(crate) fn storage_key(&self, storage_prefix: Option<&str>) -> String {
        let target = hex_lower(Sha256::digest(self.blob_ref.as_bytes()));
        let mut source = Sha256::new();
        if let Some(key) = &self.tip_key {
            source.update(b"lastdb:blob-ref-tip-source:v2\0");
            source.update(key.as_bytes());
        } else if let Some(scope) = self.atom_scope.as_deref().or(storage_prefix) {
            source.update(b"lastdb:blob-ref-scoped-atom-source:v2\0");
            source.update(scope.as_bytes());
            source.update([0]);
            source.update(self.atom_uuid.as_bytes());
        } else {
            // Preserve existing personal atom identities for exact reclaim.
            source.update(BLOB_REF_DOMAIN);
            source.update(self.atom_uuid.as_bytes());
        }
        format!(
            "{BLOB_REF_PREFIX}{target}\0{}",
            hex_lower(source.finalize())
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobRefLookup {
    pub complete: bool,
    pub edges: Vec<BlobRefEdge>,
}

/// A whole-home proof covers all scoped atom bodies and selected live tips.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobRefCompleteness {
    pub version: u8,
    pub atoms_read: u64,
    #[serde(default)]
    pub tips_read: u64,
    pub edges_written: u64,
}

impl AtomStore {
    /// Raw maintenance cannot preserve exact source ownership. Rebuild it
    /// offline before file-blob reclaim can resume.
    pub(crate) async fn revoke_blob_ref_completeness(&self) -> Result<(), SchemaError> {
        self.main_store
            .inner()
            .batch_delete(vec![
                BLOB_REF_COMPLETE_KEY.as_bytes().to_vec(),
                LEGACY_COMPLETE_KEY.as_bytes().to_vec(),
            ])
            .await?;
        Ok(())
    }

    pub async fn blob_ref_edges_complete(
        &self,
        _storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let proof = self
            .main_store
            .get_item::<BlobRefCompleteness>(BLOB_REF_COMPLETE_KEY)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("read blob reference completeness: {error}"))
            })?;
        Ok(proof.is_some_and(|proof| proof.version == 2))
    }

    pub async fn blob_ref_edges_for_blob(
        &self,
        blob_ref: &str,
        storage_prefix: Option<&str>,
    ) -> Result<BlobRefLookup, SchemaError> {
        let target = hex_lower(Sha256::digest(blob_ref.as_bytes()));
        let prefix = format!("{BLOB_REF_PREFIX}{target}\0");
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
        let prefix = format!("{BLOB_REF_PREFIX}{target}\0");
        self.main_store
            .inner()
            .scan_prefix_paged(prefix.as_bytes(), 1)
            .await
            .map(|rows| !rows.is_empty())
            .map_err(|error| {
                SchemaError::InvalidData(format!("probe blob reference edges: {error}"))
            })
    }
}
