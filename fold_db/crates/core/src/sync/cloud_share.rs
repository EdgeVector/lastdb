//! A foreign sender's data changes only through an exact receiver namespace.

use crate::storage::NamespacedStore;
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind};
use crate::sync::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const RECEIVER_BINDINGS_NAMESPACE: &str = "sync_foreign_share_bindings";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareTargetReadBinding {
    pub generation: u64,
    pub data_generation: u64,
    pub storage_prefix: String,
    pub share_grant: Option<crate::sharing::types::ShareAccessGrant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiverBinding {
    pub format_version: u32,
    pub sender_hash: String,
    pub account: CloudAccount,
    pub generation: u64,
    pub data_generation: u64,
    pub operation_id: Option<String>,
    pub storage_prefix: String,
    pub share_roots: Vec<String>,
    pub snapshots: BTreeMap<String, String>,
    #[serde(default)]
    pub writer_frontiers: BTreeMap<String, BTreeMap<String, u64>>,
}

impl ReceiverBinding {
    pub fn validate(&self) -> SyncResult<()> {
        let mut roots = self.share_roots.clone();
        roots.sort();
        roots.dedup();
        if self.format_version != 1
            || self.account.kind != CloudAccountKind::Personal
            || self.account.id != self.sender_hash
            || self.data_generation > self.generation
            || roots != self.share_roots
            || roots.is_empty()
            || roots
                .iter()
                .any(|root| sender_hash(root) != Some(self.sender_hash.as_str()))
            || self.storage_prefix
                != receiver_prefix(
                    &self.sender_hash,
                    self.data_generation,
                    &roots,
                    &self.snapshots,
                )?
            || (self.data_generation > 0
                && self.snapshots.keys().cloned().collect::<Vec<_>>() != roots)
            || (self.generation == 0) != self.operation_id.is_none()
            || (self.data_generation > 0
                && self.writer_frontiers.keys().cloned().collect::<Vec<_>>() != roots)
            || self
                .writer_frontiers
                .values()
                .any(|frontier| frontier.keys().any(String::is_empty))
            || self
                .operation_id
                .as_ref()
                .is_some_and(|id| uuid::Uuid::parse_str(id).is_err())
        {
            return Err(held("the foreign receiver binding is invalid"));
        }
        Ok(())
    }

    pub fn target(
        &self,
        share_grant: Option<crate::sharing::types::ShareAccessGrant>,
    ) -> ShareTargetReadBinding {
        ShareTargetReadBinding {
            generation: self.generation,
            data_generation: self.data_generation,
            storage_prefix: self.storage_prefix.clone(),
            share_grant,
        }
    }
}

pub fn sender_hash(prefix: &str) -> Option<&str> {
    let mut parts = prefix.split(':');
    if parts.next()? != "share" {
        return None;
    }
    let sender = parts.next()?;
    let opaque = parts.next()?;
    if parts.next().is_some()
        || opaque.is_empty()
        || sender.len() != 32
        || !sender.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some(sender)
}

pub(crate) fn receiver_prefix(
    sender: &str,
    data_generation: u64,
    roots: &[String],
    snapshots: &BTreeMap<String, String>,
) -> SyncResult<String> {
    if data_generation == 0 {
        return Ok(format!("from:{sender}"));
    }
    let identity =
        crate::hex::sha256_hex(serde_json::to_vec(&(data_generation, roots, snapshots))?);
    Ok(format!("from:{sender}:cloud:{identity}"))
}

pub fn validate_grant(target: &crate::sync::SyncTarget) -> SyncResult<()> {
    let grant = target
        .read_binding
        .as_ref()
        .and_then(|binding| binding.share_grant.as_ref())
        .ok_or_else(|| held("the retained share has no signed cloud read grant"))?;
    let public = crate::security::Ed25519PublicKey::from_base64(&grant.sender_pubkey)
        .map_err(|_| held("the retained share grant public key is invalid"))?;
    let signature = crate::security::KeyUtils::signature_from_base64(&grant.signature)
        .map_err(|_| held("the retained share grant signature is invalid"))?;
    let owner = crate::hex::sha256_hex(public.to_bytes());
    if grant.share_prefix != target.prefix
        || sender_hash(&target.prefix) != Some(&owner[..32])
        || grant.expires_at
            <= std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| held("the clock precedes the share grant epoch"))?
                .as_secs()
        || !public.verify(
            &crate::sharing::signing::share_access_grant_canonical_bytes(grant),
            &signature,
        )
    {
        return Err(held("the retained share cloud grant is invalid or expired"));
    }
    Ok(())
}

pub async fn read_bindings(
    store: &dyn NamespacedStore,
    senders: &[String],
) -> SyncResult<Vec<Option<ReceiverBinding>>> {
    let namespace = store.open_namespace(RECEIVER_BINDINGS_NAMESPACE).await?;
    let values = namespace
        .get_many(
            senders
                .iter()
                .map(|sender| sender.as_bytes().to_vec())
                .collect(),
        )
        .await?;
    if values.len() != senders.len() {
        return Err(held("the receiver binding batch is incomplete"));
    }
    senders
        .iter()
        .zip(values)
        .map(|(sender, value)| {
            let Some(value) = value else { return Ok(None) };
            if value.len() > 256 * 1024 {
                return Err(held("the receiver binding exceeds its metadata limit"));
            }
            let binding: ReceiverBinding = serde_json::from_slice(&value)?;
            binding.validate()?;
            if binding.sender_hash != *sender {
                return Err(held("the receiver binding belongs to another sender"));
            }
            Ok(Some(binding))
        })
        .collect()
}

pub(crate) fn held(message: &str) -> SyncError {
    SyncError::CloudPurgeHeld(message.into())
}

/// Every received record uses the same exact physical namespace as its seed.
pub fn rewrite_key(key: &[u8], root: &str, receiver: &str) -> SyncResult<Vec<u8>> {
    for native in ["", "emb:", "graveyard:emb:"] {
        let prefix = format!("{native}{root}:");
        if let Some(bare) = key.strip_prefix(prefix.as_bytes()) {
            let mut rewritten = format!("{native}{receiver}:").into_bytes();
            rewritten.extend_from_slice(bare);
            return Ok(rewritten);
        }
    }
    Err(held(
        "a foreign share record escaped its exact signed scope",
    ))
}
