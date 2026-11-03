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
