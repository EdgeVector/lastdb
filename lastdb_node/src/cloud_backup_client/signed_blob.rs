//! Node-signed cold-blob upload and account-context client
//! (`POST /api/cloud/backups/v2/mutation-log/blob/bytes`).

use super::{
    AuthClient, BackupTransport, BackupTransportError as E, PresignedPut, TransportResult,
};
use crate::node_read::{authorization_post, PathPostTarget};
use crate::profile::endpoints::{environment_for_exemem_api_url, Environment};
use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::v2::context::{ContextReadRequest, ContextReadResponse};
use cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
use cloud_backup_protocol::v2::share_access::{ShareEnvironment, MUTATION_LOG_BLOB_BYTES_PATH};
use cloud_backup_protocol::v2::transfer::UploadGrant;
use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::{
    bytes_sha256, AccountId, CloudScope, DecimalU64, Digest, Id, ProtocolError,
};
use fold_db::security::Ed25519KeyPair;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const BLOB_BYTES_OP: &str = "cloud/backups/v2/mutation-log/blob/bytes";

/// Resolve the canonical `/api/cloud/backups/v2/` endpoint for `auth`.
pub fn canonical_backup_endpoint(auth: AuthClient) -> TransportResult<String> {
    BackupTransport::from_auth(auth).map(|t| t.api_endpoint("cloud/backups/v2/").to_string())
}

/// Verified parameters of one signed `FileMarker` blob upload.
#[derive(Clone, Copy)]
pub struct BlobUploadTarget<'a> {
    pub scope: &'a CloudScope,
    pub path: &'a BlobPath,
    pub bytes: DecimalU64,
    pub sha256: &'a Digest,
    pub member_revision: u64,
}

/// Dual `X-API-Key` + Ed25519 `LastDBNode` client for account context and cold-blob uploads.
#[derive(Clone)]
pub struct SignedBlobClient {
    transport: BackupTransport,
    keypair: Arc<Ed25519KeyPair>,
    environment: ShareEnvironment,
}

#[derive(Serialize)]
struct ContextWire<'a> {
    action: &'static str,
    account_id: &'a AccountId,
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    required_destinations: &'a [Destination],
}

#[derive(Serialize)]
struct BlobWire<'a> {
    action: &'static str,
    database_id: &'a Id,
    scope: &'a CloudScope,
    path: &'a BlobPath,
    bytes: DecimalU64,
    sha256: &'a Digest,
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum PrepareReply {
    AlreadyVerified { scope: CloudScope, path: BlobPath },
    Upload { grant: UploadGrant },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompleteReply {
    status: String,
    scope: CloudScope,
    path: BlobPath,
}

impl SignedBlobClient {
    pub fn from_auth_and_keypair(
        auth: AuthClient,
        keypair: Arc<Ed25519KeyPair>,
    ) -> TransportResult<Self> {
        let environment = match environment_for_exemem_api_url(&auth.base_url) {
            Some(Environment::Dev) => ShareEnvironment::Dev,
            Some(Environment::Prod) => ShareEnvironment::Prod,
            None => return Err(E::Configuration("environment")),
        };
        Ok(Self {
            transport: BackupTransport::from_auth(auth)?,
            keypair,
            environment,
        })
    }

    #[must_use]
    pub fn canonical_endpoint(&self) -> String {
        self.transport.api_endpoint("cloud/backups/v2/").to_string()
    }

    pub async fn context(
        &self,
        request: &ContextReadRequest,
    ) -> TransportResult<ContextReadResponse> {
        request.validate()?;
        let derived = crate::identity::user_hash_from_pubkey(&self.keypair.public_key_base64())
            .map_err(|_| E::Configuration("identity"))?;
        if derived != request.account_id.as_str() {
            return Err(E::Configuration("account identity"));
        }
        let context_id = Id::try_from(uuid::Uuid::new_v4().to_string())?;
        let scope = CloudScope {
            account_id: request.account_id.clone(),
            account_generation: DecimalU64::new(0),
        };
        let wire = ContextWire {
            action: "context",
            account_id: &request.account_id,
            required_destinations: &request.required_destinations,
        };
        let response: ContextReadResponse = self.signed_post(&scope, &context_id, 0, &wire).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    pub async fn upload_blob(
        &self,
        target: BlobUploadTarget<'_>,
        data: Vec<u8>,
    ) -> TransportResult<()> {
        target.scope.validate()?;
        target.path.validate()?;
        let len = u64::try_from(data.len()).map_err(|_| ProtocolError::invalid("blob.bytes"))?;
        if target.bytes.get() == 0
            || target.bytes.get() != len
            || bytes_sha256(&data) != *target.sha256
        {
            return Err(E::Integrity("upload source"));
        }
        let prepare_wire = BlobWire {
            action: "prepare",
            database_id: &target.path.database,
            scope: target.scope,
            path: target.path,
            bytes: target.bytes,
            sha256: target.sha256,
        };
        let prepared: PrepareReply = self
            .signed_post(
                target.scope,
                &target.path.database,
                target.member_revision,
                &prepare_wire,
            )
            .await?;
        match prepared {
            PrepareReply::AlreadyVerified { scope, path } => {
                if scope != *target.scope || path != *target.path {
                    return Err(E::InvalidResponse("blob verified identity"));
                }
                return Ok(());
            }
            PrepareReply::Upload { grant } => {
                grant.validate()?;
                if grant.transfer_mode.is_some() {
                    return Err(ProtocolError::invalid("blob.transfer_mode").into());
                }
                let put = PresignedPut {
                    sha256: target.sha256,
                    bytes: target.bytes.get(),
                    grant_url: grant.url.as_str(),
                    expires_ms: grant.expires_ms.get(),
                };
                if let Err(error) = self.transport.put_presigned(&put, data).await {
                    if !retryable_put(&error) {
                        return Err(error);
                    }
                }
            }
        }
        let complete_wire = BlobWire {
            action: "complete",
            database_id: &target.path.database,
            scope: target.scope,
            path: target.path,
            bytes: target.bytes,
            sha256: target.sha256,
        };
        let completed: CompleteReply = self
            .signed_post(
                target.scope,
                &target.path.database,
                target.member_revision,
                &complete_wire,
            )
            .await?;
        if completed.status != "verified"
            || completed.scope != *target.scope
            || completed.path != *target.path
        {
            return Err(E::InvalidResponse("blob complete reply"));
        }
        Ok(())
    }

    async fn signed_post<Q: Serialize, R: DeserializeOwned>(
        &self,
        scope: &CloudScope,
        database_id: &Id,
        member_revision: u64,
        request: &Q,
    ) -> TransportResult<R> {
        let body = cloud_backup_protocol::canonical_bytes(request)?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| u64::try_from(d.as_millis()).ok())
            .filter(|&ms| ms > 0)
            .ok_or(E::Configuration("clock"))?;
        let auth_value = authorization_post(
            &self.keypair,
            PathPostTarget {
                scope,
                database_id,
                environment: self.environment,
                path: MUTATION_LOG_BLOB_BYTES_PATH,
            },
            &body,
            member_revision,
            now_ms,
        )
        .map_err(|_| E::Configuration("node authorization"))?;
        let mut headers = HeaderMap::new();
        let parsed = HeaderValue::from_str(&auth_value)
            .map_err(|_| E::Configuration("authorization header"))?;
        headers.insert(reqwest::header::AUTHORIZATION, parsed);
        let url = self.transport.api_endpoint(BLOB_BYTES_OP);
        self.transport
            .post_bounded_with_headers(url, request, CONTROL_JSON_MAX_BYTES, headers)
            .await
    }
}

fn retryable_put(error: &E) -> bool {
    matches!(
        error,
        E::Timeout | E::Transport | E::Http(408 | 429 | 500 | 502 | 503 | 504)
    )
}
