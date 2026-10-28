use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShareScope {
    Schema(String),
    SchemaField(String, String),
    AllSchemas,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareRule {
    pub rule_id: String,
    pub recipient_pubkey: String,
    pub recipient_display_name: String,
    pub scope: ShareScope,
    pub share_prefix: String,
    pub share_e2e_secret: Vec<u8>,
    pub active: bool,
    pub created_at: u64,
    pub writer_pubkey: String,
    pub signature: String,
}

impl ShareRule {
    pub fn scope_matches(&self, target_schema_name: &str) -> bool {
        match &self.scope {
            ShareScope::AllSchemas => true,
            ShareScope::Schema(schema) | ShareScope::SchemaField(schema, _) => {
                schema == target_schema_name
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareSubscription {
    pub sender_pubkey: String,
    pub share_prefix: String,
    pub share_e2e_secret: Vec<u8>,
    pub accepted_at: u64,
    pub active: bool,
    /// Preserve an existing subscription's saved grant metadata.
    /// The retired cloud read endpoint no longer consumes this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub share_grant: Option<ShareAccessGrant>,
}

/// Saved grant fields inside an existing local share subscription.
///
/// Keep the stored shape readable without enabling the retired cloud access
/// endpoint or assigning current cloud authority to these fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareAccessGrant {
    /// The exact prefix this grant authorizes: `share:{sender_hash}:{opaque}`.
    pub share_prefix: String,
    /// Base64 Ed25519 public key of the issuing sender (same encoding used by
    /// [`ShareInvite::sender_pubkey`]). Its SHA-256 must hash to the
    /// `sender_hash` embedded in `share_prefix`.
    pub sender_pubkey: String,
    /// Original Unix-seconds expiry retained with the subscription.
    pub expires_at: u64,
    /// Original base64 Ed25519 signature, preserved without a new issuer.
    #[serde(default)]
    pub signature: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    #[default]
    Snapshot,
    Live,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryArtifactDescriptor {
    pub payload_version: String,
    pub payload_sha256: String,
    pub payload_signature: String,
    pub envelope_format: String,
    pub envelope_alg: String,
    pub envelope_enc: String,
}
