//! Node-signed account-context client for cloud backup control plane calls.

use super::{
    authorization_post, AuthClient, BackupTransport, BackupTransportError as E, PathPostTarget,
    TransportResult,
};
use crate::profile::endpoints::{environment_for_exemem_api_url, Environment};
use cloud_backup_protocol::v2::context::{ContextReadRequest, ContextReadResponse};
use cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
use cloud_backup_protocol::v2::share_access::{ShareEnvironment, MUTATION_LOG_BLOB_BYTES_PATH};
use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::{AccountId, CloudScope, DecimalU64, Id};
use fold_db::security::Ed25519KeyPair;
use rand::RngCore;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Resolve the canonical `/api/cloud/backups/v2/` endpoint for `auth`.
pub fn canonical_backup_endpoint(auth: AuthClient) -> TransportResult<String> {
    BackupTransport::from_auth(auth).map(|t| t.api_endpoint("cloud/backups/v2/").to_string())
}

/// Dual `X-API-Key` + Ed25519 `LastDBNode` client for account context reads.
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

impl SignedBlobClient {
    pub(crate) fn from_auth_and_keypair(
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

    pub async fn context(
        &self,
        request: &ContextReadRequest,
    ) -> TransportResult<ContextReadResponse> {
        request.validate()?;
        let derived = crate::profile::user_hash_from_pubkey(&self.keypair.public_key_base64())
            .map_err(|_| E::Configuration("identity"))?;
        if derived != request.account_id.as_str() {
            return Err(E::Configuration("account identity"));
        }
        let context_id = ephemeral_authorization_database_id()?;
        let scope = CloudScope {
            account_id: request.account_id.clone(),
            account_generation: DecimalU64::new(0),
        };
        let wire = ContextWire {
            action: "context",
            account_id: &request.account_id,
            required_destinations: &request.required_destinations,
        };
        let response: ContextReadResponse = self.signed_post(&scope, &context_id, &wire).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    async fn signed_post<Q: Serialize, R: DeserializeOwned>(
        &self,
        scope: &CloudScope,
        database_id: &Id,
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
            now_ms,
        )
        .map_err(|_| E::Configuration("node authorization"))?;
        let mut headers = HeaderMap::new();
        let parsed = HeaderValue::from_str(&auth_value)
            .map_err(|_| E::Configuration("authorization header"))?;
        headers.insert(reqwest::header::AUTHORIZATION, parsed);
        let url = self
            .transport
            .api_endpoint("cloud/backups/v2/mutation-log/blob/bytes");
        self.transport
            .post_bounded_with_headers(url, body, CONTROL_JSON_MAX_BYTES, headers)
            .await
    }
}

/// Account context reads are not bound to one database; authorization still
/// requires a valid protocol database id, so mint an ephemeral Ed25519 identity.
fn ephemeral_authorization_database_id() -> std::result::Result<Id, E> {
    for _ in 0..8 {
        let mut seed = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut seed);
        let key = app_identity_crypto::SigningKey::from_bytes(&seed);
        let hex = app_identity_crypto::hex_lower(&key.verifying_key().to_bytes());
        if let Ok(id) = Id::try_from(hex) {
            return Ok(id);
        }
    }
    Err(E::Configuration("database id"))
}
