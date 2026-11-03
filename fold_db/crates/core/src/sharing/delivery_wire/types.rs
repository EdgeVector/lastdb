//! Wire-format record types for a delivery slice.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastDbSlicePayload {
    pub version: String,
    pub provenance: SliceProvenance,
    pub schemas: Vec<SliceSchema>,
    pub field_keys: Vec<SliceFieldKey>,
    pub records: Vec<ContentAddressedValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliceProvenance {
    pub source: String,
    pub mode: String,
    pub created_at: u64,
    pub sender_public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliceSchema {
    pub schema_name: String,
    pub definition: Value,
    pub fields: Vec<String>,
}

/// One delivered field of one record: where it lives and the content address
/// of its value in `records`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliceFieldKey {
    pub schema_name: String,
    pub record_key: String,
    pub field_name: String,
    pub value_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentAddressedValue {
    pub value_ref: String,
    pub value: Value,
    pub content_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedLastDbSlicePayload {
    pub payload: LastDbSlicePayload,
    pub payload_sha256: String,
    pub signer_public_key: String,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JweEnvelope {
    pub protected: String,
    #[serde(default)]
    pub encrypted_key: String,
    pub iv: String,
    pub ciphertext: String,
    pub tag: String,
}
