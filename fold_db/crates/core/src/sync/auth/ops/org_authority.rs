//! Signed Org authority controls do not enroll ordinary data writers.

use super::super::AuthClient;
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind};
use crate::sync::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};
use serde_json::json;

mod validation;

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

impl AuthClient {
    /// A refreshed credential must remain bound to the endpoint that minted
    /// it. A changed home configuration cannot retarget an existing client.
    pub fn cloud_org_authority_endpoint_matches(&self, api_url: &str) -> bool {
        self.base_url.trim_end_matches('/') == api_url.trim_end_matches('/')
    }

    pub async fn cloud_org_authority_get(
        &self,
        account: &CloudAccount,
    ) -> SyncResult<OrgAuthorityTip> {
        validate_account(account)?;
        let tip: OrgAuthorityTip = self
            .purge_call(json!({"account":account}), "cloud_org_authority_get")
            .await?;
        if tip.account != *account
            || if tip.absent {
                tip.epoch_hash.is_some() || tip.epoch_no.is_some()
            } else {
                tip.epoch_no.is_none() || !tip.epoch_hash.as_deref().is_some_and(valid_hash)
            }
        {
            return Err(invalid(
                "the authority tip response has a different identity",
            ));
        }
        Ok(tip)
    }

    /// Verify the full chain once, bind the observed tip, and never retry a
    /// changed epoch automatically. Exact signed replay remains server-owned.
    pub async fn cloud_org_authority_publish(
        &self,
        request: &OrgAuthorityPublishRequest,
    ) -> SyncResult<OrgAuthorityPublished> {
        let (account, hashes) = validation::verify(request)?;
        let tip = self.cloud_org_authority_get(&account).await?;
        validation::bind_tip(request, &tip, &hashes)?;
        let expected = match &request.expected_epoch_hash {
            OrgAuthorityExpectedEpoch::Unspecified => tip.epoch_hash.as_deref().unwrap_or(""),
            OrgAuthorityExpectedEpoch::Absent => "",
            OrgAuthorityExpectedEpoch::Exact(value) => value,
        };
        let published: OrgAuthorityPublished = self
            .purge_call(
                json!({"account":&account,"expected_epoch_hash":expected,
                    "org_authority":&request.org_authority}),
                "cloud_org_authority_publish",
            )
            .await?;
        let terminal = hashes
            .last()
            .ok_or_else(|| invalid("the signed authority chain is empty"))?;
        if published.account != account
            || published.epoch_hash != *terminal
            || usize::try_from(published.epoch_no).ok() != hashes.len().checked_sub(1)
        {
            return Err(invalid(
                "the publication receipt differs from the signed tip",
            ));
        }
        Ok(published)
    }
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
