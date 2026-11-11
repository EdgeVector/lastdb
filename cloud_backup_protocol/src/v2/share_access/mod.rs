//! Share access v2 wire types.
//!
//! A member list names the Ed25519 node keys that may read or submit one
//! shared database. The publisher signs each new revision. A revoked key stays
//! on that revision until a later revision sets `regrant`. v1 does not rotate
//! keys. These types do no I/O, do not verify signatures, and do not cap the
//! member count.
//!
//! A signed request uses the `LastDBNode` authorization scheme. The token is
//! `NodeReadToken` JSON. The second segment is the raw 64-byte Ed25519
//! signature over `NodeReadProof::digest` of the exact HTTP body. The signing
//! key is the node key in that token. It is not the publisher account key.
//! Data routes call `authorize_request`. The member-list read calls
//! `member_list_read_view` so an authorized node can learn the revision
//! without receiving the member list.

mod cursor;
mod grant;
mod members;
mod proof;

pub use cursor::{DeliveryCursor, DeviceCursor, SnapshotReference, SnapshotReset};
pub use grant::{
    authorize, authorize_request, bind_request_scope, member_list_read_view, member_list_revision,
    MemberListReadView, ShareAccess,
};
pub use members::{
    MemberListReadRequest, MemberListReadResponse, MemberListRevision, MemberListUpdate,
    ShareMember, ShareMemberRole, ShareMemberState,
};
pub use proof::{NodeReadProof, NodeReadToken, ShareEnvironment, ShareHttpMethod};

use crate::{AccountId, Digest, ProtocolError};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};

/// Domain for `MemberListUpdate::digest`.
pub const SHARE_MEMBER_LIST_UPDATE_DOMAIN: &str = "lastdb:share:member-list-update:v1";
/// Domain for `NodeReadProof::digest`.
pub const SHARE_NODE_READ_AUTHORITY_DOMAIN: &str = "lastdb:share:node-read-authority:v1";
/// Authorization scheme for [`NodeReadToken`]. Bearer share passes are not this scheme.
pub const SHARE_NODE_AUTHORIZATION_SCHEME: &str = "LastDBNode";
/// Symmetric clock skew for a signed share request, in milliseconds.
pub const SHARE_REQUEST_SKEW_MS: u64 = 60_000;

/// Complete HTTP path for a publisher member-list update.
///
/// `BackupTransport` inserts `/api` itself, so the operation suffix is
/// `cloud/backups/v2/share/members/update`.
pub const SHARE_MEMBER_LIST_UPDATE_PATH: &str = "/api/cloud/backups/v2/share/members/update";
/// Complete HTTP path for a member-list read.
///
/// `BackupTransport` inserts `/api` itself, so the operation suffix is
/// `cloud/backups/v2/share/members/read`.
pub const SHARE_MEMBER_LIST_READ_PATH: &str = "/api/cloud/backups/v2/share/members/read";

/// Reader node public key, 64 lowercase hex characters.
pub const SHARE_NODE_HEADER: &str = "x-lastdb-share-node";
/// Ed25519 signature over the node-read digest's lowercase hex bytes.
pub const SHARE_NODE_SIGNATURE_HEADER: &str = "x-lastdb-share-node-signature";
/// Decimal Unix milliseconds bound into the node-read proof.
pub const SHARE_NODE_ISSUED_AT_HEADER: &str = "x-lastdb-share-node-issued-at-ms";
/// 32-byte nonce, 64 lowercase hex characters.
pub const SHARE_NODE_NONCE_HEADER: &str = "x-lastdb-share-node-nonce";
/// Member-list revision bound into the node-read proof, decimal.
pub const SHARE_MEMBER_LIST_REVISION_HEADER: &str = "x-lastdb-share-member-list-revision";

const SIGNATURE_HEX_LEN: usize = 128;
const PUBLIC_KEY_HEX_LEN: usize = 64;

/// Lowercase hex encoding of a 64-byte Ed25519 signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Ed25519Signature(String);

impl Ed25519Signature {
    /// Borrow the 128-character hex text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Ed25519Signature {
    type Error = ProtocolError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if exact_lower_hex(&value, SIGNATURE_HEX_LEN) {
            Ok(Self(value))
        } else {
            Err(ProtocolError::invalid("share.signature"))
        }
    }
}

impl<'de> Deserialize<'de> for Ed25519Signature {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_from(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

pub(super) fn exact_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// The publisher key is the account key: the first 16 bytes of SHA-256 over
/// the raw 32-byte key equal `account`.
pub(super) fn account_key_matches(
    public_key: &Digest,
    account: &AccountId,
) -> Result<(), ProtocolError> {
    let raw = raw_public_key(public_key)?;
    let hash = Sha256::digest(raw);
    let derived: String = hash[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if derived != account.as_str() {
        return Err(ProtocolError::invalid("share.publisher_public_key"));
    }
    Ok(())
}

fn raw_public_key(encoded: &Digest) -> Result<[u8; 32], ProtocolError> {
    if !exact_lower_hex(encoded.as_str(), PUBLIC_KEY_HEX_LEN) {
        return Err(ProtocolError::invalid("share.public_key"));
    }
    let bytes = encoded.as_str().as_bytes();
    let mut raw = [0u8; 32];
    for (index, slot) in raw.iter_mut().enumerate() {
        let pair = std::str::from_utf8(&bytes[index * 2..index * 2 + 2])
            .map_err(|_| ProtocolError::invalid("share.public_key"))?;
        *slot =
            u8::from_str_radix(pair, 16).map_err(|_| ProtocolError::invalid("share.public_key"))?;
    }
    Ok(raw)
}
