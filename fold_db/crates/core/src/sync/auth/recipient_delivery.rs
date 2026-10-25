//! Frozen retired delivery metadata. No sender, presign or enrollment operation.
use crate::sync::cloud_purge::protocol::CloudAccount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RecipientDeliveryTarget {
    Inbox {
        target_user_hash: String,
        file_name: String,
    },
    Bulletin {
        recipient_pseudonym: String,
        message_id: String,
    },
    Messaging {
        target_pseudonym: String,
    },
}

/// This non-secret record belongs beside the staged message. A retry reuses
/// it exactly; it never assigns an old message to a newer recipient generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecipientDeliveryBinding {
    pub format_version: u32,
    pub account: CloudAccount,
    pub generation: u64,
    pub operation_id: Option<String>,
    #[serde(default)]
    pub sender_account: Option<CloudAccount>,
    #[serde(default)]
    pub sender_generation: u64,
    pub delivery_nonce: String,
    pub target: RecipientDeliveryTarget,
}
