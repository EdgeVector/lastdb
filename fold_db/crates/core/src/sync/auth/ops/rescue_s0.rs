use super::super::AuthClient;
use super::op_failed;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::s3::PresignedCreateOnlyUrl;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

const MAX_RESCUE_POINTERS: usize = 10_000;

/// Exact cloud identity for one immutable S0 rescue cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RescueS0Identity {
    pub db_hash: String,
    pub manifest_sha256: String,
    pub descriptor_name: String,
    pub descriptor_sha256: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RescueS0PrepareResponse {
    pub prepared_at_unix_secs: u64,
    pub ready_after_unix_secs: u64,
}

/// One create-only upload capability, or a proof that the object exists.
#[derive(Debug, Clone)]
pub struct RescueS0UploadPresign {
    pub already_present: bool,
    pub url: Option<PresignedCreateOnlyUrl>,
}

#[derive(Deserialize)]
struct RescueS0UploadResponse {
    key: String,
    already_present: bool,
    urls: Vec<PresignedCreateOnlyUrl>,
}

#[derive(Deserialize)]
struct RescueS0ChunkReceipt {
    sha256: String,
    bytes: u64,
    receipt_key: String,
}

#[derive(Deserialize)]
struct RescueS0PageProof {
    page_prefix: String,
    count: usize,
    pin_page_key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RescueS0Pointer {
    pub version: u32,
    pub source_scope: String,
    pub db_hash: String,
    pub manifest_sha256: String,
    pub store_uuid: String,
    pub epoch: u64,
    pub counter: u64,
    pub descriptor_name: String,
    pub descriptor_sha256: String,
}

impl RescueS0Pointer {
    pub fn validate(&self) -> SyncResult<()> {
        if self.version != 1
            || self.source_scope != "primary_only"
            || self.counter == 0
            || self.store_uuid.is_empty()
            || self.db_hash
                != crate::storage::laststore::cloud_db_hash_for_store_uuid(&self.store_uuid)
            || !crate::hex::is_lower_hex_sha256(&self.manifest_sha256)
            || !crate::hex::is_lower_hex_sha256(&self.descriptor_sha256)
            || self.descriptor_name
                != format!(
                    "lastdb-recovery-v1-{}-{}-{}.enc",
                    self.db_hash, self.manifest_sha256, self.descriptor_sha256
                )
        {
            return Err(SyncError::Storage("invalid S0 rescue pointer".into()));
        }
        Ok(())
    }

    pub fn validate_descriptor(
        &self,
        descriptor: &crate::sync::engine::RecoveryDescriptorV1,
    ) -> SyncResult<()> {
        self.validate()?;
        if descriptor.mode != "s0_only"
            || descriptor.store_uuid != self.store_uuid
            || descriptor.db_hash != self.db_hash
            || descriptor.manifest_sha256 != self.manifest_sha256
            || descriptor.epoch != self.epoch
            || descriptor.counter != self.counter
        {
            return Err(SyncError::Storage(
                "S0 rescue pointer does not match recovery descriptor".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct RescueS0ListResponse {
    ok: bool,
    #[serde(default)]
    rescues: Vec<RescueS0Pointer>,
    has_more: Option<bool>,
    continuation_token: Option<String>,
    error: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RescueS0CommitResponse {
    pub key: String,
    pub rescue: RescueS0Pointer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RescueS0WaitReason {
    DeleteUrlDrain,
    UploadUrlAge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RescueS0CommitOutcome {
    Committed(RescueS0CommitResponse),
    Wait {
        reason: RescueS0WaitReason,
        ready_after_unix_secs: u64,
    },
}

fn require_ok(action: &str, value: serde_json::Value) -> SyncResult<serde_json::Value> {
    if value.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
        return Ok(value);
    }
    let detail = value
        .get("reason")
        .or_else(|| value.get("error"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    Err(op_failed(action, detail))
}

fn decode_response<T: serde::de::DeserializeOwned>(
    action: &str,
    value: serde_json::Value,
) -> SyncResult<T> {
    let value = require_ok(action, value)?;
    serde_json::from_value(value).map_err(|error| {
        SyncError::Serialization(format!("{action} response decode failed: {error}"))
    })
}

impl AuthClient {
    async fn rescue_s0_presign_upload(
        &self,
        identity: &RescueS0Identity,
        store_uuid: &str,
        sha256: &str,
        bytes: u64,
        chunk: bool,
    ) -> SyncResult<RescueS0UploadPresign> {
        if !crate::hex::is_lower_hex_sha256(sha256)
            || bytes == 0
            || identity.db_hash
                != crate::storage::laststore::cloud_db_hash_for_store_uuid(store_uuid)
        {
            return Err(SyncError::Storage(
                "invalid S0 rescue upload identity".into(),
            ));
        }
        let kind = if chunk { "chunks" } else { "manifests" };
        let action = if chunk {
            "rescue_s0_presign_chunk_upload"
        } else {
            "rescue_s0_presign_manifest_upload"
        };
        let mut body = serde_json::json!({
            "action": action,
            "db_hash": identity.db_hash,
            "manifest_sha256": identity.manifest_sha256,
            "descriptor_name": identity.descriptor_name,
            "descriptor_sha256": identity.descriptor_sha256,
            "backup_store_uuid": store_uuid,
            "estimated_size_bytes": bytes,
        });
        if chunk {
            body["chunk_sha256"] = serde_json::json!(sha256);
        } else if sha256 != identity.manifest_sha256 {
            return Err(SyncError::Storage(
                "S0 rescue manifest hash mismatch".into(),
            ));
        }
        let value = self
            .post_no_default_db_hash("/api/sync/presign", body)
            .await?;
        let response: RescueS0UploadResponse = decode_response(action, value)?;
        if response.key != format!("{}/backup/{kind}/{sha256}", identity.db_hash) {
            return Err(SyncError::Storage("S0 rescue upload key mismatch".into()));
        }
        let url = match (response.already_present, response.urls.len()) {
            (true, 0) => None,
            (false, 1) => Some(response.urls.into_iter().next().expect("one URL")),
            _ => return Err(SyncError::Storage("invalid S0 rescue upload URLs".into())),
        };
        Ok(RescueS0UploadPresign {
            already_present: response.already_present,
            url,
        })
    }

    pub async fn rescue_s0_presign_chunk_upload(
        &self,
        identity: &RescueS0Identity,
        store_uuid: &str,
        sha256: &str,
        bytes: u64,
    ) -> SyncResult<RescueS0UploadPresign> {
        self.rescue_s0_presign_upload(identity, store_uuid, sha256, bytes, true)
            .await
    }

    pub async fn rescue_s0_presign_manifest_upload(
        &self,
        identity: &RescueS0Identity,
        store_uuid: &str,
        bytes: u64,
    ) -> SyncResult<RescueS0UploadPresign> {
        self.rescue_s0_presign_upload(
            identity,
            store_uuid,
            &identity.manifest_sha256,
            bytes,
            false,
        )
        .await
    }

    pub async fn rescue_s0_confirm_chunk(
        &self,
        identity: &RescueS0Identity,
        store_uuid: &str,
        sha256: &str,
        bytes: u64,
    ) -> SyncResult<()> {
        if !crate::hex::is_lower_hex_sha256(sha256)
            || identity.db_hash
                != crate::storage::laststore::cloud_db_hash_for_store_uuid(store_uuid)
        {
            return Err(SyncError::Storage(
                "invalid S0 rescue chunk identity".into(),
            ));
        }
        let value = self
            .post_no_default_db_hash(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "rescue_s0_confirm_chunk",
                    "db_hash": identity.db_hash,
                    "manifest_sha256": identity.manifest_sha256,
                    "descriptor_name": identity.descriptor_name,
                    "descriptor_sha256": identity.descriptor_sha256,
                    "backup_store_uuid": store_uuid,
                    "chunk_sha256": sha256,
                }),
            )
            .await?;
        let receipt: RescueS0ChunkReceipt = decode_response("rescue_s0_confirm_chunk", value)?;
        if receipt.sha256 != sha256
            || receipt.bytes != bytes
            || receipt.receipt_key
                != format!(
                    "{}/rescue/s0/receipts/{}/{sha256}.json",
                    identity.db_hash, identity.manifest_sha256
                )
        {
            return Err(SyncError::Storage(
                "S0 rescue chunk receipt mismatch".into(),
            ));
        }
        Ok(())
    }

    /// Verify one bounded group of immutable chunk receipts and stored bytes.
    pub async fn rescue_s0_verify_page(
        &self,
        identity: &RescueS0Identity,
        store_uuid: &str,
        prefix: &str,
        chunks: &[String],
    ) -> SyncResult<()> {
        if prefix.len() != 2
            || !prefix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || chunks.is_empty()
            || chunks.len() > 512
            || chunks
                .iter()
                .any(|sha| !crate::hex::is_lower_hex_sha256(sha) || !sha.starts_with(prefix))
            || chunks.windows(2).any(|pair| pair[0] >= pair[1])
            || identity.db_hash
                != crate::storage::laststore::cloud_db_hash_for_store_uuid(store_uuid)
        {
            return Err(SyncError::Storage("invalid S0 rescue page request".into()));
        }
        let value = self
            .post_no_default_db_hash(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "rescue_s0_verify_page",
                    "db_hash": identity.db_hash,
                    "manifest_sha256": identity.manifest_sha256,
                    "descriptor_name": identity.descriptor_name,
                    "descriptor_sha256": identity.descriptor_sha256,
                    "backup_store_uuid": store_uuid,
                    "rescue_page_prefix": prefix,
                    "rescue_page_chunks": chunks,
                }),
            )
            .await?;
        let proof: RescueS0PageProof = decode_response("rescue_s0_verify_page", value)?;
        if proof.page_prefix != prefix
            || proof.count != chunks.len()
            || proof.pin_page_key != format!("{}/rescue/s0/pins/{prefix}.json", identity.db_hash)
        {
            return Err(SyncError::Storage("S0 rescue page proof mismatch".into()));
        }
        Ok(())
    }

    /// Read the complete account-root rescue list before accepting a commit.
    pub async fn rescue_s0_list(&self) -> SyncResult<Vec<RescueS0Pointer>> {
        let mut body = serde_json::json!({"action": "rescue_s0_list", "max_keys": 100});
        let mut pointers = Vec::new();
        let mut seen_tokens = HashSet::new();
        loop {
            let value = self
                .post_no_default_db_hash("/api/sync/list", body.clone())
                .await?;
            let response: RescueS0ListResponse = serde_json::from_value(value)?;
            if !response.ok {
                return Err(op_failed(
                    "rescue_s0_list",
                    response.reason.or(response.error),
                ));
            }
            if pointers.len().saturating_add(response.rescues.len()) > MAX_RESCUE_POINTERS {
                return Err(SyncError::Storage("too many S0 rescue pointers".into()));
            }
            for pointer in response.rescues {
                pointer.validate()?;
                pointers.push(pointer);
            }
            match (response.has_more, response.continuation_token) {
                (Some(false), None) => return Ok(pointers),
                (Some(true), Some(token))
                    if !token.is_empty() && seen_tokens.insert(token.clone()) =>
                {
                    body["continuation_token"] = serde_json::Value::String(token);
                }
                _ => {
                    return Err(SyncError::Storage(
                        "incomplete S0 rescue pointer list".into(),
                    ))
                }
            }
        }
    }

    /// Read one immutable account-root pointer by its manifest hash.
    pub async fn rescue_s0_get(&self, manifest_sha256: &str) -> SyncResult<RescueS0Pointer> {
        if !crate::hex::is_lower_hex_sha256(manifest_sha256) {
            return Err(SyncError::Storage("invalid S0 rescue manifest hash".into()));
        }
        let value = self
            .post_no_default_db_hash(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "rescue_s0_get",
                    "manifest_sha256": manifest_sha256,
                }),
            )
            .await?;
        #[derive(Deserialize)]
        struct GetResponse {
            key: String,
            rescue: RescueS0Pointer,
        }
        let response: GetResponse = decode_response("rescue_s0_get", value)?;
        if response.key != format!("rescue/s0/{manifest_sha256}.json")
            || response.rescue.manifest_sha256 != manifest_sha256
        {
            return Err(SyncError::Storage("S0 rescue pointer key mismatch".into()));
        }
        response.rescue.validate()?;
        Ok(response.rescue)
    }

    /// Create a server hold before any rescue chunk upload.
    pub async fn rescue_s0_prepare(
        &self,
        identity: &RescueS0Identity,
        store_uuid: &str,
    ) -> SyncResult<RescueS0PrepareResponse> {
        if store_uuid.is_empty()
            || identity.db_hash
                != crate::storage::laststore::cloud_db_hash_for_store_uuid(store_uuid)
        {
            return Err(SyncError::Storage(
                "rescue_s0_prepare store identity mismatch".into(),
            ));
        }
        let value = self
            .post_no_default_db_hash(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "rescue_s0_prepare",
                    "db_hash": identity.db_hash,
                    "manifest_sha256": identity.manifest_sha256,
                    "descriptor_name": identity.descriptor_name,
                    "descriptor_sha256": identity.descriptor_sha256,
                    "backup_store_uuid": store_uuid,
                }),
            )
            .await?;
        let response: RescueS0PrepareResponse = decode_response("rescue_s0_prepare", value)?;
        if response.ready_after_unix_secs < response.prepared_at_unix_secs {
            return Err(SyncError::Storage(
                "rescue_s0_prepare returned an invalid hold time".into(),
            ));
        }
        Ok(response)
    }

    /// Store the encrypted account-root descriptor under the prepared hold.
    pub async fn rescue_s0_descriptor_put(
        &self,
        identity: &RescueS0Identity,
        ciphertext: &[u8],
    ) -> SyncResult<()> {
        let value = self
            .post_no_default_db_hash(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "rescue_s0_descriptor_put",
                    "db_hash": identity.db_hash,
                    "manifest_sha256": identity.manifest_sha256,
                    "descriptor_name": identity.descriptor_name,
                    "descriptor_sha256": identity.descriptor_sha256,
                    "descriptor_base64": base64::engine::general_purpose::STANDARD.encode(ciphertext),
                }),
            )
            .await?;
        #[derive(Deserialize)]
        struct PutResponse {
            key: String,
        }
        let response: PutResponse = decode_response("rescue_s0_descriptor_put", value)?;
        if response.key != format!("snapshots/{}", identity.descriptor_name) {
            return Err(SyncError::Storage(
                "rescue_s0_descriptor_put returned a different key".into(),
            ));
        }
        Ok(())
    }

    /// Commit the immutable rescue pointer. This never changes `backup/latest`.
    pub async fn rescue_s0_commit(
        &self,
        identity: &RescueS0Identity,
        store_uuid: &str,
        epoch: u64,
        counter: u64,
    ) -> SyncResult<RescueS0CommitOutcome> {
        let value = self
            .post_no_default_db_hash(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "rescue_s0_commit",
                    "db_hash": identity.db_hash,
                    "manifest_sha256": identity.manifest_sha256,
                    "descriptor_name": identity.descriptor_name,
                    "descriptor_sha256": identity.descriptor_sha256,
                    "backup_store_uuid": store_uuid,
                    "backup_epoch": epoch,
                    "backup_counter": counter,
                }),
            )
            .await?;
        if value.get("ok").and_then(serde_json::Value::as_bool) == Some(false)
            && value.get("code").and_then(serde_json::Value::as_str) == Some("RESCUE_S0_WAIT")
        {
            let reason = match value.get("reason").and_then(serde_json::Value::as_str) {
                Some("delete_url_drain") => RescueS0WaitReason::DeleteUrlDrain,
                Some("upload_url_age") => RescueS0WaitReason::UploadUrlAge,
                _ => {
                    return Err(SyncError::Storage(
                        "rescue_s0_commit returned an unknown wait reason".into(),
                    ))
                }
            };
            let ready_after_unix_secs = value
                .get("ready_after_unix_secs")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    SyncError::Storage("rescue_s0_commit wait omitted its deadline".into())
                })?;
            return Ok(RescueS0CommitOutcome::Wait {
                reason,
                ready_after_unix_secs,
            });
        }
        let response: RescueS0CommitResponse = decode_response("rescue_s0_commit", value)?;
        response.rescue.validate()?;
        if response.key != format!("rescue/s0/{}.json", identity.manifest_sha256)
            || response.rescue.version != 1
            || response.rescue.db_hash != identity.db_hash
            || response.rescue.manifest_sha256 != identity.manifest_sha256
            || response.rescue.store_uuid != store_uuid
            || response.rescue.epoch != epoch
            || response.rescue.counter != counter
            || response.rescue.descriptor_name != identity.descriptor_name
            || response.rescue.descriptor_sha256 != identity.descriptor_sha256
        {
            return Err(SyncError::Storage(
                "rescue_s0_commit returned a different rescue identity".into(),
            ));
        }
        Ok(RescueS0CommitOutcome::Committed(response))
    }
}
