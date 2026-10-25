//! Existing recipient enrollment metadata only.
use crate::sync::cloud_purge::protocol::CloudAccount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CloudRecipientEnrollment {
    pub format_version: u32,
    pub account: CloudAccount,
    pub enrollment_id: String,
    pub recipient_pseudonym: String,
}
