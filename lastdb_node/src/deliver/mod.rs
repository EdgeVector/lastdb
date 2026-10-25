//! Local encrypted snapshot export and passive staged-record list/reject.
//! No recipient enrollment, messaging sender or new delivery stage is available.

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
    get_pending_delivery_in_ops, list_pending_deliveries_in_ops, remove_pending_delivery_in_ops,
};
use fold_db::sharing::types::{DeliveryArtifactDescriptor, DeliveryMode};
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
use std::collections::HashSet;
use uuid::Uuid;

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

mod filters;
mod legs;
mod pending;
mod publish;
mod types;

pub use pending::{execute_list_deliveries, execute_reject_delivery};
pub use publish::execute_publish_snapshot;

fn error_json(status: u16, message: &str, ctx: &AccessContext) -> UdsResponse {
    render(Err(HostError::new(status, message)), ctx)
}

fn ok_json(value: &serde_json::Value, ctx: &AccessContext) -> UdsResponse {
    json_ok(&envelope(value, ctx.user_id.as_str()))
}

/// Parse the local rejection path of an existing staged record.
pub fn parse_delivery_action(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix("/api/sharing/deliveries/")?;
    let (id, action) = rest.split_once('/')?;
    if id.is_empty() {
        return None;
    }
    match action {
        "reject" => Some((id, action)),
        _ => None,
    }
}

/// Content-free 404 helper re-export for routing fallthrough.
pub fn not_found() -> UdsResponse {
    content_free(404, "Not Found")
}
