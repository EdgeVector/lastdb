//! What a reader node signs on a share read.

use super::{exact_lower_hex, Ed25519Signature, SHARE_NODE_READ_AUTHORITY_DOMAIN};
use crate::v2::{limits, Stream, Version};
use crate::{input_digest, DecimalU64, Digest, ProtocolError, Text};
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

/// Signed statement for one read.
///
/// `node_public_key`, `signature`, `issued_at_ms`, `nonce`, and
/// `member_list_revision` travel in `SHARE_NODE_HEADER`,
/// `SHARE_NODE_SIGNATURE_HEADER`, `SHARE_NODE_ISSUED_AT_HEADER`,
/// `SHARE_NODE_NONCE_HEADER`, and `SHARE_MEMBER_LIST_REVISION_HEADER`.
/// `method` and `path` are the HTTP request. `stream` is the request body.
/// The signature is not part of the digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeReadProof {
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

impl NodeReadProof {
    /// Check the path, time, and nonce. This does not verify the signature
    /// or consult a replay cache.
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

    /// Domain-separated SHA-256 of this proof with the signature removed.
    ///
    /// The canonical body is a JSON array, in order: `version`, `stream`,
    /// `method`, `path`, `issued_at_ms`, `nonce`, `member_list_revision`,
    /// `node_public_key`. Framing is a big-endian u32 domain length, the
    /// UTF-8 domain `lastdb:share:node-read-authority:v1`, a big-endian u64
    /// body length, and the RFC 8785 body. The result is lowercase hex.
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
