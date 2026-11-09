//! Stream read permission signed by the publisher's LastDB account key.
use super::{BackupTransportError, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cloud_backup_protocol::v2::Stream;
use cloud_backup_protocol::{CloudScope, Digest, Id, RecoveryDescriptor, RecoveryProfile};
use ring::signature::{UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

const SCHEME: &str = "LastDBShare";
const SIGNING_PREFIX: &str = "lastdb-share-permission-v1\n";
const MAX_HEADER_BYTES: usize = 2048;

/// The Exemem environment bound to a guest permission.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShareEnvironment {
    Dev,
    Prod,
}

impl ShareEnvironment {
    pub(super) fn registry(self) -> folddb_profile::endpoints::Environment {
        use folddb_profile::endpoints::Environment;
        match self {
            Self::Dev => Environment::Dev,
            Self::Prod => Environment::Prod,
        }
    }
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

/// A bearer credential for encrypted bytes. The publication key is separate.
pub struct GuestReadPermission {
    authorization: Zeroizing<String>,
    stream: Stream,
    environment: ShareEnvironment,
    expires_ms: u64,
    publisher_public_key: Digest,
}

impl GuestReadPermission {
    /// The callback signs the exact domain-separated bytes with the account key.
    pub fn sign(
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

    /// Validate the wire form and signature without contacting Exemem.
    pub fn parse(authorization: &str) -> Result<Self> {
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
        UnparsedPublicKey::new(&ED25519, key)
            .verify(message.as_bytes(), &signature)
            .map_err(|_| invalid("guest permission signature"))?;
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

    /// Exact Authorization header value. Keep it out of logs and URLs.
    pub fn authorization(&self) -> &str {
        &self.authorization
    }

    pub fn stream(&self) -> &Stream {
        &self.stream
    }

    pub fn environment(&self) -> ShareEnvironment {
        self.environment
    }

    pub fn publisher_public_key(&self) -> &Digest {
        &self.publisher_public_key
    }

    /// Signed permission expiry in Unix milliseconds.
    pub fn expires_ms(&self) -> u64 {
        self.expires_ms
    }

    pub(super) fn into_read_scope(self) -> (Stream, u64) {
        (self.stream, self.expires_ms)
    }
}

impl Claim {
    fn validated(&self) -> Result<(u64, [u8; 32], Digest)> {
        if self.version != 1 || self.operation != "read" {
            return Err(invalid("guest permission"));
        }
        let expires_ms = expires_ms(&self.expires_at)?;
        let key = decode_fixed::<32>(&self.public_key, "guest permission owner")?;
        let recovery = RecoveryDescriptor {
            profile: RecoveryProfile::MiniPortableV1,
            account_public_key: Digest::try_from(lastdb_model::hex::hex_lower(key))?,
        };
        recovery
            .validate_for(&self.scope)
            .map_err(|_| invalid("guest permission owner"))?;
        Ok((expires_ms, key, recovery.account_public_key))
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
