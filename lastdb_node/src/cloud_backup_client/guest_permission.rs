//! Scoped guest permissions and `LastDBNode` signed POST authorization.
use super::{BackupTransportError, TransportResult as Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cloud_backup_protocol::v2::share_access::{
    NodeReadToken, ShareHttpMethod, SHARE_NODE_AUTHORIZATION_SCHEME,
};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, Id, Text};
use fold_db::security::Ed25519KeyPair;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

const SCHEME: &str = "LastDBShare";
const SIGNING_PREFIX: &str = "lastdb-share-permission-v1\n";
const MAX_HEADER_BYTES: usize = 2048;

pub(crate) use cloud_backup_protocol::v2::share_access::ShareEnvironment;

/// Account, database, realm, and raw path of one signed POST.
#[derive(Clone, Copy)]
pub(crate) struct PathPostTarget<'a> {
    pub scope: &'a CloudScope,
    pub database_id: &'a Id,
    pub environment: ShareEnvironment,
    pub path: &'a str,
}

/// Authorization header value for one POST. A new nonce is used every call.
pub(crate) fn authorization_post(
    keypair: &Ed25519KeyPair,
    target: PathPostTarget<'_>,
    body: &[u8],
    now_ms: u64,
) -> std::result::Result<String, String> {
    if now_ms == 0 {
        return Err("node read is not bound to a time".into());
    }
    let mut nonce_bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
    let node_public_key = Digest::try_from(fold_db::hex::hex_lower(keypair.public_key_bytes()))
        .map_err(|_| "node key was refused")?;
    let token = NodeReadToken {
        version: Version::V2,
        environment: target.environment,
        database_id: target.database_id.clone(),
        stream: Stream {
            scope: target.scope.clone(),
            stream_id: target.database_id.clone(),
        },
        method: ShareHttpMethod::Post,
        path: Text::try_from(target.path.to_owned()).map_err(|_| "node read path was refused")?,
        issued_at_ms: DecimalU64::new(now_ms),
        nonce: Text::try_from(fold_db::hex::hex_lower(nonce_bytes))
            .map_err(|_| "node read nonce was refused")?,
        node_public_key,
    };
    let digest = token
        .digest(body)
        .map_err(|_| "node read digest was refused")?;
    let signature = keypair.sign(digest.as_str().as_bytes());
    let token_json = serde_json::to_vec(&token).map_err(|_| "node read token was refused")?;
    let encoded_token = URL_SAFE_NO_PAD.encode(token_json);
    let encoded_signature = URL_SAFE_NO_PAD.encode(signature.to_bytes());
    Ok(format!(
        "{SHARE_NODE_AUTHORIZATION_SCHEME} {encoded_token}.{encoded_signature}"
    ))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Claim {
    version: u8,
    environment: ShareEnvironment,
    scope: CloudScope,
    stream_id: Id,
    operation: String,
    expires_at: String,
    public_key: String,
}

pub(crate) struct GuestReadPermission {
    authorization: Zeroizing<String>,
    stream: Stream,
    environment: ShareEnvironment,
    expires_ms: u64,
    publisher_public_key: Digest,
}

impl GuestReadPermission {
    pub(crate) fn sign(
        stream: Stream,
        environment: ShareEnvironment,
        expires_at: &str,
        account_public_key: [u8; 32],
        sign: impl FnOnce(&[u8]) -> [u8; 64],
    ) -> Result<Self> {
        let claim = Claim {
            version: 1,
            environment,
            scope: stream.scope,
            stream_id: stream.stream_id,
            operation: "read".to_owned(),
            expires_at: expires_at.to_owned(),
            public_key: URL_SAFE_NO_PAD.encode(account_public_key),
        };
        claim.validated()?;
        let payload = URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&claim).map_err(|_| invalid("guest permission"))?);
        let message = format!("{SIGNING_PREFIX}{payload}");
        let signature = URL_SAFE_NO_PAD.encode(sign(message.as_bytes()));
        let authorization = Zeroizing::new(format!("{SCHEME} {payload}.{signature}"));
        Self::parse(&authorization)
    }

    pub(crate) fn parse(authorization: &str) -> Result<Self> {
        let (scheme, token) = authorization
            .split_once(' ')
            .filter(|_| authorization.len() <= MAX_HEADER_BYTES)
            .ok_or_else(|| invalid("guest permission"))?;
        if !scheme.eq_ignore_ascii_case(SCHEME) {
            return Err(invalid("guest permission"));
        }
        let (payload, signature) = token
            .split_once('.')
            .ok_or_else(|| invalid("guest permission"))?;
        let json = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| invalid("guest permission"))?;
        let claim: Claim =
            serde_json::from_slice(&json).map_err(|_| invalid("guest permission"))?;
        let (expires_ms, key, publisher_public_key) = claim.validated()?;
        let signature = decode_fixed::<64>(signature, "guest permission signature")?;
        let message = format!("{SIGNING_PREFIX}{payload}");
        verify_ed25519(&key, &signature, message.as_bytes())?;
        Ok(Self {
            authorization: Zeroizing::new(authorization.to_owned()),
            stream: Stream {
                scope: claim.scope,
                stream_id: claim.stream_id,
            },
            environment: claim.environment,
            expires_ms,
            publisher_public_key,
        })
    }

    pub(crate) fn authorization(&self) -> &str {
        &self.authorization
    }

    pub(crate) fn stream(&self) -> &Stream {
        &self.stream
    }

    pub(crate) fn environment(&self) -> ShareEnvironment {
        self.environment
    }

    pub(crate) fn publisher_public_key(&self) -> &Digest {
        &self.publisher_public_key
    }

    pub(crate) fn expires_ms(&self) -> u64 {
        self.expires_ms
    }
}

fn verify_ed25519(key: &[u8; 32], signature: &[u8; 64], message: &[u8]) -> Result<()> {
    let verifying_key = app_identity_crypto::VerifyingKey::from_bytes(key)
        .ok()
        .filter(|vk| vk.to_edwards().compress().to_bytes() == *key)
        .ok_or_else(|| invalid("guest permission signature"))?;
    app_identity_crypto::verify(&verifying_key, signature, message)
        .map_err(|_| invalid("guest permission signature"))
}

impl Claim {
    fn validated(&self) -> Result<(u64, [u8; 32], Digest)> {
        if self.version != 1 || self.operation != "read" {
            return Err(invalid("guest permission"));
        }
        let expires_ms = expires_ms(&self.expires_at)?;
        let key = decode_fixed::<32>(&self.public_key, "guest permission owner")?;
        self.scope
            .validate()
            .map_err(|_| invalid("guest permission owner"))?;
        if fold_db::hex::hex_lower(&Sha256::digest(key)[..16]) != self.scope.account_id.as_str() {
            return Err(invalid("guest permission owner"));
        }
        let account_public_key = Digest::try_from(fold_db::hex::hex_lower(key))?;
        Ok((expires_ms, key, account_public_key))
    }
}

fn decode_fixed<const N: usize>(value: &str, field: &'static str) -> Result<[u8; N]> {
    URL_SAFE_NO_PAD
        .decode(value)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| invalid(field))
}

fn expires_ms(value: &str) -> Result<u64> {
    let invalid_expiry = || invalid("guest permission expiry");
    if value.as_bytes().get(10) != Some(&b'T') || !value.ends_with('Z') {
        return Err(invalid_expiry());
    }
    let date = chrono::DateTime::parse_from_rfc3339(value).map_err(|_| invalid_expiry())?;
    if date.timestamp_subsec_nanos() >= 1_000_000_000 {
        return Err(invalid_expiry());
    }
    u64::try_from(date.timestamp_millis()).map_err(|_| invalid_expiry())
}

fn invalid(field: &'static str) -> BackupTransportError {
    BackupTransportError::Configuration(field)
}
