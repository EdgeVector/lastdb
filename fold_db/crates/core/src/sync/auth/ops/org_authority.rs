//! Signed Org authority controls do not enroll ordinary data writers.

use crate::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind};
use crate::sync::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};

pub mod validation;

/// Maximum decoded owner request. Epoch history has a separate count bound.
pub const ORG_AUTHORITY_BODY_LIMIT: usize = 1_048_576;
/// Leave room for Dynamo DB keys, duplicate hashes and attribute overhead.
pub const ORG_AUTHORITY_TERMINAL_JSON_LIMIT: usize = 350 * 1024;
pub const ORG_AUTHORITY_EPOCH_LIMIT: usize = 1000;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrgAuthoritySignedPayload {
    pub payload_jcs: String,
    #[serde(alias = "sig")]
    pub signature_base64url: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrgAuthorityEnvelope {
    pub org_root_spki_base64: String,
    pub epochs: Vec<OrgAuthoritySignedPayload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_enrollment: Option<OrgAuthoritySignedPayload>,
}

/// Omission derives the current tip; explicit null requires an absent tip.
#[derive(Clone, Default)]
pub enum OrgAuthorityExpectedEpoch {
    #[default]
    Unspecified,
    Absent,
    Exact(String),
}

impl<'de> Deserialize<'de> for OrgAuthorityExpectedEpoch {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match Option::<String>::deserialize(deserializer)? {
            Some(hash) => Self::Exact(hash),
            None => Self::Absent,
        })
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrgAuthorityPublishRequest {
    pub org_authority: OrgAuthorityEnvelope,
    #[serde(default)]
    pub account: Option<CloudAccount>,
    #[serde(default)]
    pub expected_epoch_hash: OrgAuthorityExpectedEpoch,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrgAuthorityTip {
    pub account: CloudAccount,
    pub absent: bool,
    pub epoch_hash: Option<String>,
    pub epoch_no: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrgAuthorityPublished {
    pub account: CloudAccount,
    pub epoch_hash: String,
    pub epoch_no: u64,
}

pub fn validate_account(account: &CloudAccount) -> SyncResult<()> {
    if account.kind != CloudAccountKind::Org || !valid_hash(&account.id) {
        return Err(invalid("an exact Org account is required"));
    }
    Ok(())
}

fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn invalid(message: &str) -> SyncError {
    SyncError::Storage(format!("org_authority_invalid: {message}"))
}

fn conflict() -> SyncError {
    SyncError::Storage(
        "cloud_org_epoch_conflict: the remote tip differs from the signed chain or expected tip"
            .into(),
    )
}
