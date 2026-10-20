//! Canonical node-global blob roots, with exact scoped owner rows.

use super::AtomStore;
use crate::hex::hex_lower;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod tips;
pub(crate) use tips::TipBlobChanges;

type BlobRefRows = Vec<(Vec<u8>, Vec<u8>)>;

const BLOB_REF_PREFIX: &str = "bref:v2:e:";
const BLOB_OWNER_PREFIX: &str = "bref:v2:s:";
pub const BLOB_REF_COMPLETE_KEY: &str = "bref:v2:complete";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum BlobRefSource {
    Atom {
        atom_uuid: String,
        scope: Option<String>,
    },
    Tip {
        atom_uuid: String,
        key: String,
        scope: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlobRefEdge {
    pub blob_ref: String,
    pub source: BlobRefSource,
}

impl BlobRefEdge {
    #[must_use]
    pub fn atom(atom_uuid: &str, blob_ref: &str, scope: Option<&str>) -> Self {
        Self {
            blob_ref: blob_ref.to_string(),
            source: BlobRefSource::Atom {
                atom_uuid: atom_uuid.to_string(),
                scope: scope.map(str::to_string),
            },
        }
    }

    fn scope(&self) -> Option<&str> {
        match &self.source {
            BlobRefSource::Atom { scope, .. } | BlobRefSource::Tip { scope, .. } => {
                scope.as_deref()
            }
        }
    }

    fn suffix(&self) -> String {
        let target = hex_lower(Sha256::digest(self.blob_ref.as_bytes()));
        let (kind, owner) = match &self.source {
            BlobRefSource::Atom { atom_uuid, .. } => ("atom", atom_uuid.as_str()),
            BlobRefSource::Tip { key, .. } => ("tip", key.as_str()),
        };
        let mut source = Sha256::new();
        source.update(b"lastdb:blob-reference-owner:v2\0");
        for part in [kind, self.scope().unwrap_or(""), owner] {
            source.update((part.len() as u64).to_be_bytes());
            source.update(part.as_bytes());
        }
        format!("{target}\0{}", hex_lower(source.finalize()))
    }

    pub(crate) fn storage_key(&self) -> String {
        format!("{BLOB_REF_PREFIX}{}", self.suffix())
    }

    pub(crate) fn owner_key(&self) -> String {
        build_storage_key(
            self.scope(),
            &format!("{BLOB_OWNER_PREFIX}{}", self.suffix()),
        )
    }

    pub(crate) fn put_rows(&self) -> Result<BlobRefRows, SchemaError> {
        let value = serde_json::to_vec(self).map_err(|error| {
            SchemaError::InvalidData(format!("encode canonical blob reference: {error}"))
        })?;
        Ok(vec![
            (self.storage_key().into_bytes(), value.clone()),
            (self.owner_key().into_bytes(), value),
        ])
    }

    pub(crate) fn delete_mutations(&self) -> Vec<KvMutation> {
        vec![
            KvMutation::delete(self.storage_key().into_bytes()),
            KvMutation::delete(self.owner_key().into_bytes()),
        ]
    }

    pub(crate) fn target_from_owner_key(key: &[u8]) -> Option<Vec<u8>> {
        let key = std::str::from_utf8(key).ok()?;
        let bare = match crate::storage::current_keys::exact_scope(key).ok()? {
            Some(scope) => key.get(scope.len() + 1..)?,
            None => key,
        };
        let suffix = bare.strip_prefix(BLOB_OWNER_PREFIX)?;
        let (target, source) = suffix.split_once('\0')?;
        if target.len() != 64
            || source.len() != 64
            || !target
                .bytes()
                .chain(source.bytes())
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return None;
        }
        Some(format!("{BLOB_REF_PREFIX}{suffix}").into_bytes())
    }

    pub(crate) fn is_target_key(key: &[u8]) -> bool {
        key.starts_with(BLOB_REF_PREFIX.as_bytes())
    }

    pub(crate) fn scoped_owner_row(
        key: &[u8],
        value: &[u8],
        scope: &str,
    ) -> Result<Option<Self>, crate::storage::StorageError> {
        let prefix = build_storage_key(Some(scope), BLOB_OWNER_PREFIX);
        if !key.starts_with(prefix.as_bytes()) {
            return Ok(None);
        }
        let edge: Self = serde_json::from_slice(value).map_err(|error| {
            crate::storage::StorageError::BackendError(format!("decode blob owner row: {error}"))
        })?;
        if edge.scope() != Some(scope) || edge.owner_key().as_bytes() != key {
            return Err(crate::storage::StorageError::BackendError(
                "blob owner identity mismatch".into(),
            ));
        }
        Ok(Some(edge))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobRefLookup {
    pub complete: bool,
    pub edges: Vec<BlobRefEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlobRefCompleteness {
    pub version: u8,
}

impl AtomStore {
    pub async fn blob_ref_edges_complete(&self, _scope: Option<&str>) -> Result<bool, SchemaError> {
        let proof = self
            .main_store
            .get_item::<BlobRefCompleteness>(BLOB_REF_COMPLETE_KEY)
            .await?;
        Ok(proof.is_some_and(|proof| proof.version == 2))
    }

    pub async fn blob_ref_edges_for_blob(
        &self,
        blob_ref: &str,
        scope: Option<&str>,
    ) -> Result<BlobRefLookup, SchemaError> {
        let target = hex_lower(Sha256::digest(blob_ref.as_bytes()));
        let prefix = format!("{BLOB_REF_PREFIX}{target}\0");
        let edges = self
            .main_store
            .scan_items_with_prefix::<BlobRefEdge>(&prefix)
            .await?
            .into_iter()
            .map(|(_, edge)| edge)
            .collect();
        Ok(BlobRefLookup {
            complete: self.blob_ref_edges_complete(scope).await?,
            edges,
        })
    }

    pub async fn has_active_blob_refs(
        &self,
        blob_ref: &str,
        scope: Option<&str>,
    ) -> Result<bool, SchemaError> {
        if !self.blob_ref_edges_complete(scope).await? {
            return Ok(true);
        }
        let target = hex_lower(Sha256::digest(blob_ref.as_bytes()));
        let prefix = format!("{BLOB_REF_PREFIX}{target}\0");
        Ok(!self
            .main_store
            .inner()
            .scan_prefix_paged(prefix.as_bytes(), 1)
            .await?
            .is_empty())
    }
}
