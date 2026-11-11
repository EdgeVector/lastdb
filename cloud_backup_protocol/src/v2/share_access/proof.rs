//! What a node signs on a share request.
//!
//! The digest binds the database id, the HTTP action, the raw body, and a
//! nonce. The service stores the nonce. This module does not.

use super::{
    exact_lower_hex, Ed25519Signature, SHARE_NODE_READ_AUTHORITY_DOMAIN, SHARE_REQUEST_SKEW_MS,
};
use crate::v2::{limits, Stream, Version};
use crate::{bytes_sha256, input_digest, DecimalU64, Digest, Id, ProtocolError, Text};
use serde::{Deserialize, Serialize};

/// HTTP method bound into a node-read proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShareHttpMethod {
    /// HTTP GET.
    #[serde(rename = "GET")]
    Get,
    /// HTTP POST.
    #[serde(rename = "POST")]
    Post,
    /// HTTP PUT.
    #[serde(rename = "PUT")]
    Put,
    /// HTTP DELETE.
    #[serde(rename = "DELETE")]
    Delete,
    /// HTTP HEAD.
    #[serde(rename = "HEAD")]
    Head,
}

/// Realm the signature is allowed to address.
///
/// DEV and PROD use the same routes. This value is the credential realm, not a
/// second path and not a feature flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareEnvironment {
    /// Development service.
    Dev,
    /// Production service.
    Prod,
}

impl ShareEnvironment {
    /// Wire spelling: `dev` or `prod`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Prod => "prod",
        }
    }

    /// Accept only `dev` or `prod`.
    pub fn parse(value: &str) -> Result<Self, ProtocolError> {
        match value {
            "dev" => Ok(Self::Dev),
            "prod" => Ok(Self::Prod),
            _ => Err(ProtocolError::invalid("share.node_read.environment")),
        }
    }
}

/// Signed statement carried in a `LastDBNode` authorization token.
///
/// The token JSON is this object. The Ed25519 signature is the second token
/// segment, not a field here. `digest` covers the raw HTTP body, not the token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeReadToken {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// `dev` or `prod`. The signature covers this value.
    pub environment: ShareEnvironment,
    /// Database the request addresses. It must equal `stream.stream_id`.
    pub database_id: Id,
    /// Account scope and database id.
    pub stream: Stream,
    /// HTTP method of the request.
    pub method: ShareHttpMethod,
    /// HTTP path of the request, including the `/api` prefix.
    pub path: Text<512>,
    /// Unix milliseconds when the node signed this proof.
    pub issued_at_ms: DecimalU64,
    /// 32 random bytes as 64 lowercase hex characters. One nonce per request.
    pub nonce: Text<64>,
    /// Member-list revision this request is bound to. Zero is the empty list.
    pub member_list_revision: DecimalU64,
    /// Ed25519 node key that signs this proof.
    pub node_public_key: Digest,
}

impl NodeReadToken {
    /// Check the path, time, nonce, and database binding.
    ///
    /// This does not verify the signature or consult a replay cache.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        if self.database_id != self.stream.stream_id {
            return Err(ProtocolError::invalid("share.node_read.database_id"));
        }
        validate_path(self.path.as_str())?;
        if self.issued_at_ms.get() == 0 {
            return Err(ProtocolError::invalid("share.node_read.issued_at_ms"));
        }
        if !exact_lower_hex(self.nonce.as_str(), 64) {
            return Err(ProtocolError::invalid("share.node_read.nonce"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }

    /// Reject a token aimed at another realm, method, or path.
    ///
    /// `path` is the raw request path, including `/api`. It is not a rewritten
    /// route key.
    pub fn require_target(
        &self,
        service: ShareEnvironment,
        method: ShareHttpMethod,
        path: &str,
    ) -> Result<(), ProtocolError> {
        if self.environment != service {
            return Err(ProtocolError::unauthorized("share.node_read.environment"));
        }
        if self.method != method || self.path.as_str() != path {
            return Err(ProtocolError::unauthorized("share.node_read.target"));
        }
        Ok(())
    }

    /// Domain-separated SHA-256 of this statement and the raw request body.
    ///
    /// The canonical array is `version`, `database_id`, `stream`, `method`,
    /// `path`, `body_sha256`, `issued_at_ms`, `nonce`, `member_list_revision`,
    /// `node_public_key`, `environment`. `body_sha256` is SHA-256 of `body`
    /// with no domain frame. Framing is a big-endian u32 domain length, the
    /// UTF-8 domain `lastdb:share:node-read-authority:v1`, a big-endian u64
    /// body length, and the RFC 8785 array.
    pub fn digest(&self, body: &[u8]) -> Result<Digest, ProtocolError> {
        self.validate()?;
        let body_sha256 = bytes_sha256(body);
        input_digest(
            SHARE_NODE_READ_AUTHORITY_DOMAIN,
            &(
                &self.version,
                &self.database_id,
                &self.stream,
                self.method,
                self.path.as_str(),
                &body_sha256,
                self.issued_at_ms,
                self.nonce.as_str(),
                self.member_list_revision,
                &self.node_public_key,
                self.environment,
            ),
        )
    }
}

/// A node statement plus the signature over its digest hex.
///
/// `signature` is the 128-character hex form. The `LastDBNode` token carries
/// the same signature as 64 raw bytes in the second segment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeReadProof {
    /// Statement that `digest` covers, aside from the raw HTTP body.
    pub token: NodeReadToken,
    /// Signature over `digest`'s lowercase hexadecimal bytes.
    pub signature: Ed25519Signature,
}

impl NodeReadProof {
    /// Check the statement. This does not verify the signature.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.token.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }

    /// Digest of the statement and the exact received HTTP body octets.
    ///
    /// `body` is the bytes on the wire. Do not parse them and serialize again
    /// before this call.
    pub fn digest(&self, body: &[u8]) -> Result<Digest, ProtocolError> {
        self.validate()?;
        self.token.digest(body)
    }

    /// JSON object encoded in the `LastDBNode` token, without the signature.
    pub fn token_json(&self) -> Result<Vec<u8>, ProtocolError> {
        self.token.validate()?;
        serde_json::to_vec(&self.token).map_err(|_| ProtocolError::invalid("share.node_read.token"))
    }

    /// Reject a clock reading outside ±[`SHARE_REQUEST_SKEW_MS`] and a spent nonce.
    ///
    /// `nonce_seen` is true when this database, node key, and nonce were
    /// already accepted and that acceptance window has not ended. The service
    /// stores that fact. This function does not.
    pub fn validate_fresh(&self, now_ms: u64, nonce_seen: bool) -> Result<(), ProtocolError> {
        self.validate()?;
        let issued = self.token.issued_at_ms.get();
        let earliest = now_ms.saturating_sub(SHARE_REQUEST_SKEW_MS);
        let latest = now_ms.saturating_add(SHARE_REQUEST_SKEW_MS);
        if issued < earliest || issued > latest {
            return Err(ProtocolError::unauthorized("share.node_read.issued_at_ms"));
        }
        if nonce_seen {
            return Err(ProtocolError::unauthorized("share.node_read.replay"));
        }
        Ok(())
    }

    /// Require the signed revision to be the revision stored for the database.
    ///
    /// Data routes use `authorize_request`, which includes this check. The
    /// member-list read does not: `member_list_read_view` lets an authorized
    /// node learn the revision without receiving the member list.
    pub fn require_current_revision(&self, current: DecimalU64) -> Result<(), ProtocolError> {
        if self.token.member_list_revision != current {
            return Err(ProtocolError::unauthorized("share.node_read.revision"));
        }
        Ok(())
    }
}

/// Body-less share proof from before the per-database cutover.
///
/// The JSON object is flat. It has no `database_id` and no `environment`.
/// [`digest`](Self::digest) does not cover the HTTP body. Use it only when
/// [`super::accepted_proof`] is [`super::AcceptedProof::Legacy`]. A database
/// with a cutover receipt refuses this object. Do not use Exemem #769's
/// private field order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyNodeReadProof {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Shared database this read addresses.
    pub stream: Stream,
    /// HTTP method of the read.
    pub method: ShareHttpMethod,
    /// HTTP path of the read, including the `/api` prefix.
    pub path: Text<512>,
    /// Unix milliseconds when the node signed this proof.
    pub issued_at_ms: DecimalU64,
    /// 32 random bytes as 64 lowercase hex characters.
    pub nonce: Text<64>,
    /// Member-list revision this read is bound to. Zero is the empty list.
    pub member_list_revision: DecimalU64,
    /// Ed25519 node key that signs this proof.
    pub node_public_key: Digest,
    /// Signature over `digest`'s lowercase hexadecimal bytes.
    pub signature: Ed25519Signature,
}

impl LegacyNodeReadProof {
    /// Check the path, time, and nonce. This does not verify the signature.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        validate_path(self.path.as_str())?;
        if self.issued_at_ms.get() == 0 {
            return Err(ProtocolError::invalid("share.node_read.issued_at_ms"));
        }
        if !exact_lower_hex(self.nonce.as_str(), 64) {
            return Err(ProtocolError::invalid("share.node_read.nonce"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }

    /// Pre-migration digest. The array is `version`, `stream`, `method`,
    /// `path`, `issued_at_ms`, `nonce`, `member_list_revision`, `node_public_key`.
    pub fn digest(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        input_digest(
            SHARE_NODE_READ_AUTHORITY_DOMAIN,
            &(
                &self.version,
                &self.stream,
                self.method,
                self.path.as_str(),
                self.issued_at_ms,
                self.nonce.as_str(),
                self.member_list_revision,
                &self.node_public_key,
            ),
        )
    }
}

fn validate_path(path: &str) -> Result<(), ProtocolError> {
    let Some(rest) = path.strip_prefix("/api/cloud/backups/v2/") else {
        return Err(ProtocolError::invalid("share.node_read.path"));
    };
    if rest.is_empty()
        || !path.is_ascii()
        || path.contains(['?', '#', '\\'])
        || rest
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(ProtocolError::invalid("share.node_read.path"));
    }
    Ok(())
}
