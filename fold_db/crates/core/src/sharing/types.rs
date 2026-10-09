use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::delivery_wire::JweEnvelope;
use crate::schema::types::operations::Query;

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
pub struct ShareInvite {
    pub sender_pubkey: String,
    pub sender_display_name: String,
    pub share_prefix: String,
    pub share_e2e_secret: Vec<u8>,
    pub scope_description: String,
    #[serde(default)]
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareSubscription {
    pub sender_pubkey: String,
    pub share_prefix: String,
    pub share_e2e_secret: Vec<u8>,
    pub accepted_at: u64,
    pub active: bool,
}

/// A sender-signed authorization for a recipient to read a share log from
/// cloud storage.
///
/// A share log lives at `/{share_prefix}/log/{seq}.enc` where `share_prefix`
/// is `share:{sender_hash}:{opaque_id}` and `sender_hash =
/// hex(SHA256(sender_pubkey_bytes)[..16])`. The log bytes are E2E-encrypted
/// under `share_e2e_secret`, so encryption is the real confidentiality
/// boundary. The *storage* layer still needs a least-privilege access rule so
/// a recipient can list/presign-read that one prefix without being able to
/// read arbitrary prefixes or org logs.
///
/// The **sender** owns their own `share:{their_hash}:…` namespace: the storage
/// service authorizes their writes/reads by matching the authenticated
/// `user_hash` against `sender_hash`, so a sender needs no grant. A
/// **recipient** (whose `user_hash != sender_hash`) presents this grant as a
/// read-only capability. The grant deliberately **omits the E2E secret** — it
/// proves *authorization to read the ciphertext*, never the key — so handing it
/// to the (untrusted) cloud storage service leaks no key material.
///
/// The signature binds the exact `share_prefix`, the `sender_pubkey`, and an
/// `expires_at` deadline to the sender's key. The storage service verifies:
/// (1) `share_prefix` parses as `share:{sender_hash}:{opaque}`, (2)
/// `hex(SHA256(sender_pubkey)[..16]) == sender_hash` (the sender can only
/// authorize prefixes in their own namespace), (3) the Ed25519 signature is
/// valid, and (4) `expires_at` is in the future. See
/// `signing::share_access_grant_canonical_bytes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareAccessGrant {
    /// The exact prefix this grant authorizes: `share:{sender_hash}:{opaque}`.
    pub share_prefix: String,
    /// Base64 Ed25519 public key of the issuing sender (same encoding used by
    /// [`ShareInvite::sender_pubkey`]). Its SHA-256 must hash to the
    /// `sender_hash` embedded in `share_prefix`.
    pub sender_pubkey: String,
    /// Unix-seconds expiry. The storage service rejects an expired grant so a
    /// leaked capability is time-boxed; recipients re-fetch a fresh grant.
    pub expires_at: u64,
    /// Base64 Ed25519 signature over
    /// [`signing::share_access_grant_canonical_bytes`].
    #[serde(default)]
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryRecord {
    pub schema_name: String,
    pub record_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    #[default]
    Snapshot,
    Live,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeliverySpec {
    /// Materialize the exact output of a schema query.
    Query { query: Query },
    /// A named saved query plus its resolved query body. The name is retained
    /// for preview/provenance; the resolved query is the executable contract.
    SavedQuery { name: String, query: Query },
    /// Historical compatibility for serialized delivery specs that named a
    /// computed output. Live code does not produce new values of this variant.
    TransformOutput {
        view_name: String,
        fields: Vec<String>,
    },
    /// Back-compat with the pre-query deliver endpoint.
    Scope { scope: ShareScope },
}

impl DeliverySpec {
    pub fn label(&self) -> String {
        match self {
            Self::Query { query } => format!("query: {}", query.schema_name),
            Self::SavedQuery { name, .. } => format!("saved query: {name}"),
            Self::TransformOutput { view_name, .. } => format!("transform output: {view_name}"),
            Self::Scope { scope } => match scope {
                ShareScope::AllSchemas => "scope: all schemas".to_string(),
                ShareScope::Schema(schema) => format!("scope: schema {schema}"),
                ShareScope::SchemaField(schema, field) => format!("scope: field {schema}.{field}"),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliverySampleRecord {
    pub schema_name: String,
    pub record_key: String,
    pub fields: std::collections::BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryPreview {
    pub query_label: String,
    pub fields: Vec<String>,
    pub record_count: usize,
    pub sample: Vec<DeliverySampleRecord>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedDeliveryArtifact {
    pub envelope: JweEnvelope,
    pub content_key: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingDelivery {
    pub delivery_id: String,
    pub recipient_pubkey: String,
    pub recipient_display_name: String,
    pub spec: DeliverySpec,
    pub mode: DeliveryMode,
    /// Deprecated compatibility field for older clients. New callers should
    /// read `spec` and `preview.fields`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<ShareScope>,
    pub records: Vec<DeliveryRecord>,
    pub preview: DeliveryPreview,
    pub artifact: DeliveryArtifactDescriptor,
    pub status: String,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decided_at: Option<u64>,
    /// Recipient X25519 messaging public key (base64). Required on Mini for
    /// approve-send without a contact book (e.g. admin kanban-consumer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messaging_public_key: Option<String>,
    /// Recipient messaging pseudonym (UUID). Target for bulletin/messaging
    /// connect. Required on Mini when `messaging_public_key` is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messaging_pseudonym: Option<String>,
}
