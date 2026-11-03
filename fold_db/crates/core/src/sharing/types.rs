use serde::{Deserialize, Serialize};

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
