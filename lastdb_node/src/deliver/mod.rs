//! Consent-gated deliver stage / list / approve on LastDB Mini.
//!
//! Stages a query-defined `lastdb.slice.v1` into the local outbox (no network
//! until approve). On approve, seals a `delivery_slice` to the recipient's
//! messaging X25519 key and POSTs it to Exemem `messaging/connect` using the
//! node's cloud API key — Exemem remains a blind relay.

use crate::host::Host;
use crate::seal::seal_and_encrypt_message;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use fold_db::access::AccessContext;
use fold_db::clock::unix_secs;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::{FieldPredicate, QueryOrderBy, SortOrder};
use fold_db::sharing::delivery_wire::{
    encrypt_signed_slice_jwe, sign_slice_payload, JweEnvelope, LASTDB_SLICE_PAYLOAD_VERSION,
};
use fold_db::sharing::query_slice::{materialize_query_slice, QuerySliceLeg};
use fold_db::sharing::store::{
    get_pending_delivery_in_ops, get_staged_delivery_artifact_in_ops,
    list_pending_deliveries_in_ops, remove_pending_delivery_in_ops, store_pending_delivery_in_ops,
    store_staged_delivery_artifact_in_ops,
};
use fold_db::sharing::types::{
    DeliveryArtifactDescriptor, DeliveryMode, DeliveryPreview, DeliveryRecord,
    DeliverySampleRecord, DeliverySpec, PendingDelivery, StagedDeliveryArtifact,
};
use fold_db::storage::config::CloudSyncConfig;
use lastdb_host::envelope::{content_free, envelope, json_ok};
use lastdb_host::handlers::render;
use lastdb_host::handlers::resolve_schema_name;
use lastdb_host::HostError;
use lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use uuid::Uuid;

/// Messaging blob hard cap (matches messaging_service 64KB base64 ≈ 87382 chars).
const MAX_BLOB_B64_CHARS: usize = 87_382;
/// Object-store snapshot seal cap (base64 chars). Far above messaging; keeps
/// accidents from OOMing a Mini while allowing full-board slices.
const MAX_SNAPSHOT_BLOB_B64_CHARS: usize = 12 * 1024 * 1024;
/// Version tag inside the canonical query document (hash domain).
const CANONICAL_QUERY_V: u32 = 1;

/// Filter variants a deliver/snapshot leg may name in its remediation.
/// Tighter than the general `/api/query` scan gate (`is_key_restricted`):
/// point, multi-get, or range-under-one-hash only. Cross-hash `RangeKey` /
/// `RangePrefix` / `RangeRange` / `HashRange` still walk every hash group, and
/// `Page` / `PageAfter` / `SampleN` / the pattern variants are scans — none of
/// those belong on a deliver leg.
const DELIVER_KEYED_FILTER_VARIANTS: &[&str] = &[
    "HashKey",
    "HashRangeKey",
    "HashRangeKeys",
    "HashRangePrefix",
    "HashRangeRange",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliverySlicePayload {
    pub message_type: String,
    pub content_key: String,
    pub envelope: JweEnvelope,
    pub artifact: DeliveryArtifactDescriptor,
}

mod approve;
mod legs;
mod publish;
mod stage;
mod types;

pub use approve::{execute_approve_delivery, execute_reject_delivery};
pub use publish::execute_publish_snapshot;
pub use stage::{execute_list_deliveries, execute_stage_delivery};

fn error_json(status: u16, message: &str, ctx: &AccessContext) -> UdsResponse {
    render(Err(HostError::new(status, message)), ctx)
}

fn ok_json(value: &serde_json::Value, ctx: &AccessContext) -> UdsResponse {
    json_ok(&envelope(value, ctx.user_id.as_str()))
}

fn load_cloud_sync(home: &Path) -> Option<CloudSyncConfig> {
    let path = home.join(crate::host::CLOUD_SYNC_CONFIG_FILE);
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

/// Parse `/api/sharing/deliveries/{id}/approve|reject` path tails.
pub fn parse_delivery_action(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix("/api/sharing/deliveries/")?;
    let (id, action) = rest.split_once('/')?;
    if id.is_empty() {
        return None;
    }
    match action {
        "approve" | "reject" => Some((id, action)),
        _ => None,
    }
}

/// Content-free 404 helper re-export for routing fallthrough.
pub fn not_found() -> UdsResponse {
    content_free(404, "Not Found")
}
