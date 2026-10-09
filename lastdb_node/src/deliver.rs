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

#[derive(Debug, Deserialize)]
struct StageBody {
    recipient_pubkey: String,
    #[serde(default)]
    recipient_display_name: Option<String>,
    /// X25519 messaging public key (base64). Required for Mini approve-send.
    messaging_public_key: String,
    /// Messaging pseudonym UUID. Required for Mini approve-send.
    messaging_pseudonym: String,
    #[serde(default)]
    mode: DeliveryMode,
    /// Query-defined slice legs. Each leg is one schema query.
    #[serde(default)]
    legs: Vec<StageLeg>,
    /// Convenience: single schema + fields (expanded to one leg).
    #[serde(default)]
    schema_name: Option<String>,
    #[serde(default)]
    fields: Option<Vec<String>>,
    /// Optional query filter for the convenience schema_name path (e.g. SampleN / Page).
    #[serde(default)]
    filter: Option<HashRangeFilter>,
    /// Two-pass field predicates. Serialized as `where`, matching `/api/query`.
    #[serde(default, rename = "where")]
    field_predicates: Option<Vec<FieldPredicate>>,
    /// Convenience time window, e.g. "24h", "7d", "30m". Adds an `after`
    /// predicate against `since_field` (default `updated_at`).
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    since_field: Option<String>,
    /// Optional field ordering. Accepts either a field string or the core
    /// `{ "field": "...", "order": "desc" }` shape.
    #[serde(default)]
    order_by: Option<StageOrderBy>,
    /// Companion to string `order_by`.
    #[serde(default)]
    order: Option<SortOrder>,
    /// Optional column allow/deny sugar for kanban Card slices.
    #[serde(default)]
    columns_include: Option<Vec<String>>,
    #[serde(default)]
    columns_exclude: Option<Vec<String>>,
    /// Cap applied after field predicates/order_by. Defaults from max_records
    /// for two-pass requests.
    #[serde(default)]
    predicate_limit: Option<usize>,
    /// Cap records for messaging size (~64KB sealed blob). Applied as
    /// [`HashRangeFilter::SampleN`] when no explicit filter or two-pass
    /// predicate/order is set; otherwise it becomes `predicate_limit`.
    /// Prefer this for admin board summaries of large boards.
    #[serde(default)]
    max_records: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct StageLeg {
    schema_name: String,
    fields: Vec<String>,
    /// Optional hash keys (Hash schemas) — materialize only these records.
    /// When set, expands to one query leg per key with [`HashRangeFilter::HashKey`].
    #[serde(default)]
    hash_keys: Option<Vec<String>>,
    #[serde(default)]
    filter: Option<HashRangeFilter>,
    #[serde(default, rename = "where")]
    field_predicates: Option<Vec<FieldPredicate>>,
    #[serde(default)]
    order_by: Option<StageOrderBy>,
    #[serde(default)]
    order: Option<SortOrder>,
    #[serde(default)]
    predicate_limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum StageOrderBy {
    Field(String),
    Query(QueryOrderBy),
}

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

fn resolve_filter(
    explicit: Option<HashRangeFilter>,
    max_records: Option<usize>,
) -> Option<HashRangeFilter> {
    if explicit.is_some() {
        return explicit;
    }
    max_records.map(HashRangeFilter::SampleN)
}

fn parse_since_duration_secs(raw: &str) -> Result<u64, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("since must not be empty".into());
    }
    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (amount, unit) = trimmed.split_at(split_at);
    let amount: u64 = amount
        .parse()
        .map_err(|_| format!("invalid since duration '{raw}'"))?;
    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => 1,
        "m" | "min" | "mins" | "minute" | "minutes" => 60,
        "h" | "hr" | "hrs" | "hour" | "hours" => 60 * 60,
        "d" | "day" | "days" => 24 * 60 * 60,
        _ => return Err(format!("invalid since duration unit in '{raw}'")),
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| format!("since duration '{raw}' is too large"))
}

fn resolve_order_by(
    order_by: Option<&StageOrderBy>,
    order: Option<SortOrder>,
) -> Result<Option<QueryOrderBy>, String> {
    match order_by {
        None => Ok(None),
        Some(StageOrderBy::Field(field)) if field.trim().is_empty() => {
            Err("order_by field must not be empty".into())
        }
        Some(StageOrderBy::Field(field)) => Ok(Some(QueryOrderBy {
            field: field.clone(),
            order,
        })),
        Some(StageOrderBy::Query(query_order)) => {
            if order.is_some() && query_order.order.is_some() && order != query_order.order {
                return Err("order conflicts with order_by.order".into());
            }
            let mut resolved = query_order.clone();
            if resolved.order.is_none() {
                resolved.order = order;
            }
            Ok(Some(resolved))
        }
    }
}

fn stage_predicates(
    base: Option<&[FieldPredicate]>,
    since: Option<&str>,
    since_field: Option<&str>,
    columns_include: Option<&[String]>,
    columns_exclude: Option<&[String]>,
) -> Result<Option<Vec<FieldPredicate>>, String> {
    let mut predicates = base.map_or_else(Vec::new, <[FieldPredicate]>::to_vec);
    if let Some(raw_since) = since {
        let duration_secs = parse_since_duration_secs(raw_since)?;
        let threshold = unix_secs().saturating_sub(duration_secs);
        let field = since_field.unwrap_or("updated_at").trim();
        if field.is_empty() {
            return Err("since_field must not be empty".into());
        }
        predicates.push(FieldPredicate::After {
            field: field.to_string(),
            instant: Value::Number(threshold.into()),
        });
    }
    if let Some(columns) = columns_include.filter(|c| !c.is_empty()) {
        predicates.push(FieldPredicate::In {
            field: "column".to_string(),
            values: columns.iter().cloned().map(Value::String).collect(),
        });
    }
    if let Some(columns) = columns_exclude.filter(|c| !c.is_empty()) {
        let values: HashSet<&str> = columns.iter().map(String::as_str).collect();
        let allowed = ["backlog", "todo", "doing", "review", "done"]
            .into_iter()
            .filter(|column| !values.contains(column))
            .map(|column| Value::String(column.to_string()))
            .collect();
        predicates.push(FieldPredicate::In {
            field: "column".to_string(),
            values: allowed,
        });
    }
    Ok((!predicates.is_empty()).then_some(predicates))
}

fn two_pass_limit(
    predicates: Option<&[FieldPredicate]>,
    order_by: Option<&QueryOrderBy>,
    predicate_limit: Option<usize>,
    max_records: Option<usize>,
) -> Option<usize> {
    predicate_limit.or_else(|| {
        (predicates.is_some_and(|p| !p.is_empty()) || order_by.is_some()).then_some(max_records)?
    })
}

fn leg_filter(
    explicit: Option<HashRangeFilter>,
    max_records: Option<usize>,
    predicates: Option<&[FieldPredicate]>,
    order_by: Option<&QueryOrderBy>,
    predicate_limit: Option<usize>,
) -> Option<HashRangeFilter> {
    if predicates.is_some_and(|p| !p.is_empty()) || order_by.is_some() || predicate_limit.is_some()
    {
        return explicit;
    }
    resolve_filter(explicit, max_records)
}

fn legs_from_body(body: &StageBody) -> Result<Vec<QuerySliceLeg>, String> {
    if !body.legs.is_empty() {
        let mut out = Vec::new();
        for leg in &body.legs {
            if leg.fields.is_empty() {
                return Err(format!(
                    "deliver leg for schema '{}' has no fields",
                    leg.schema_name
                ));
            }
            match &leg.hash_keys {
                Some(keys) if !keys.is_empty() => {
                    for key in keys {
                        if key.is_empty() {
                            return Err("deliver hash_keys must be non-empty strings".into());
                        }
                        out.push(QuerySliceLeg {
                            schema_name: leg.schema_name.clone(),
                            fields: leg.fields.clone(),
                            filter: Some(HashRangeFilter::HashKey(key.clone())),
                            field_predicates: None,
                            order_by: None,
                            predicate_limit: None,
                        });
                    }
                }
                _ => {
                    let predicates =
                        stage_predicates(leg.field_predicates.as_deref(), None, None, None, None)?;
                    let order_by = resolve_order_by(leg.order_by.as_ref(), leg.order.clone())?;
                    let predicate_limit = two_pass_limit(
                        predicates.as_deref(),
                        order_by.as_ref(),
                        leg.predicate_limit,
                        body.max_records,
                    );
                    out.push(QuerySliceLeg {
                        schema_name: leg.schema_name.clone(),
                        fields: leg.fields.clone(),
                        filter: leg_filter(
                            leg.filter.clone(),
                            body.max_records,
                            predicates.as_deref(),
                            order_by.as_ref(),
                            predicate_limit,
                        ),
                        field_predicates: predicates,
                        order_by,
                        predicate_limit,
                    });
                }
            }
        }
        return Ok(out);
    }
    let schema = body
        .schema_name
        .clone()
        .ok_or_else(|| "deliver requires legs[] or schema_name + fields".to_string())?;
    let fields = body
        .fields
        .clone()
        .filter(|f| !f.is_empty())
        .ok_or_else(|| "deliver requires non-empty fields".to_string())?;
    let predicates = stage_predicates(
        body.field_predicates.as_deref(),
        body.since.as_deref(),
        body.since_field.as_deref(),
        body.columns_include.as_deref(),
        body.columns_exclude.as_deref(),
    )?;
    let order_by = resolve_order_by(body.order_by.as_ref(), body.order.clone())?;
    let predicate_limit = two_pass_limit(
        predicates.as_deref(),
        order_by.as_ref(),
        body.predicate_limit,
        body.max_records,
    );
    Ok(vec![QuerySliceLeg {
        schema_name: schema,
        fields,
        filter: leg_filter(
            body.filter.clone(),
            body.max_records,
            predicates.as_deref(),
            order_by.as_ref(),
            predicate_limit,
        ),
        field_predicates: predicates,
        order_by,
        predicate_limit,
    }])
}

/// Canonical LastDB deliver query document (exact recipe that produced the slice).
///
/// `snapshot_key = hex(sha256(canonical_json || 0x00 || recipient_id))` where
/// `recipient_id` is the messaging pseudonym UUID. Same query + recipient
/// always overwrites the same object-store slot (no append log).
#[derive(Debug, Clone, Serialize)]
struct CanonicalQuery {
    v: u32,
    legs: Vec<CanonicalQueryLeg>,
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalQueryLeg {
    schema_name: String,
    fields: Vec<String>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
    field_predicates: Option<Vec<FieldPredicate>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    order_by: Option<QueryOrderBy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    predicate_limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<HashRangeFilter>,
}

fn canonical_query_from_legs(legs: &[QuerySliceLeg]) -> CanonicalQuery {
    let mut out = Vec::with_capacity(legs.len());
    for leg in legs {
        let mut fields = leg.fields.clone();
        fields.sort();
        out.push(CanonicalQueryLeg {
            schema_name: leg.schema_name.clone(),
            fields,
            field_predicates: leg.field_predicates.clone(),
            order_by: leg.order_by.clone(),
            predicate_limit: leg.predicate_limit,
            filter: leg.filter.clone(),
        });
    }
    CanonicalQuery {
        v: CANONICAL_QUERY_V,
        legs: out,
    }
}

/// `snapshot_key = hex(sha256(canonical_query_json || 0x00 || recipient_id))`.
fn snapshot_key_for(
    canonical_query: &CanonicalQuery,
    recipient_id: &str,
) -> Result<String, String> {
    let json =
        serde_json::to_vec(canonical_query).map_err(|e| format!("canonical query json: {e}"))?;
    let mut h = Sha256::new();
    h.update(&json);
    h.update([0u8]);
    h.update(recipient_id.as_bytes());
    Ok(fold_db::hex::hex_lower(h.finalize()))
}

fn validate_stage_recipient(body: &StageBody) -> Result<[u8; 32], String> {
    if body.mode != DeliveryMode::Snapshot {
        return Err("Mini deliver currently supports snapshot mode only".into());
    }
    if Uuid::parse_str(&body.messaging_pseudonym).is_err() {
        return Err("messaging_pseudonym must be a UUID".into());
    }
    match B64.decode(body.messaging_public_key.as_bytes()) {
        Ok(b) if b.len() == 32 => {
            let mut out = [0u8; 32];
            out.copy_from_slice(&b);
            Ok(out)
        }
        _ => Err("messaging_public_key must be base64 of 32-byte X25519 key".into()),
    }
}

/// Does this leg's filter restrict the deliver/snapshot read to a bounded
/// set of keys? See [`DELIVER_KEYED_FILTER_VARIANTS`] for the allowed shapes.
fn deliver_filter_is_keyed(filter: Option<&HashRangeFilter>) -> bool {
    matches!(
        filter,
        Some(
            HashRangeFilter::HashKey(_)
                | HashRangeFilter::HashRangeKey { .. }
                | HashRangeFilter::HashRangeKeys(_)
                | HashRangeFilter::HashRangePrefix { .. }
                | HashRangeFilter::HashRangeRange { .. }
        )
    )
}

/// `400 deliver_unkeyed_leg_not_allowed` — mirrors the shape of
/// `full_schema_scan_not_allowed` (`lastdb_host::handlers`) so a caller sees
/// the same `kind` + `remediation` convention on every rejected access
/// pattern, not a one-off message.
fn unkeyed_leg_rejection(schema_name: &str) -> HostError {
    let message = format!(
        "deliver_unkeyed_leg_not_allowed: deliver does not two-pass scan schema \
         '{schema_name}'; pass hash_keys or a key-restricted filter on every leg \
         (X-LastDB-Allow-Full-Scan is not honored on this path)"
    );
    let body = serde_json::json!({
        "ok": false,
        "kind": "deliver_unkeyed_leg_not_allowed",
        "error": "deliver requires a key-restricted filter on each leg",
        "message": message,
        "schema_name": schema_name,
        "remediation": {
            "action": "add_key_filter",
            "request_key": "legs[].hash_keys | legs[].filter",
            "filter_variants": DELIVER_KEYED_FILTER_VARIANTS,
        }
    });
    HostError::structured(400, message, body)
}

fn resolve_legs(host: &Host, body: &StageBody) -> Result<Vec<QuerySliceLeg>, HostError> {
    let mut legs = legs_from_body(body).map_err(|e| HostError::new(400, e))?;
    for leg in &mut legs {
        match resolve_schema_name(host, &leg.schema_name)? {
            Some(canonical) => leg.schema_name = canonical,
            None => {
                return Err(HostError::new(
                    404,
                    format!("Schema not found: {}", leg.schema_name),
                ));
            }
        }
    }
    for leg in &legs {
        if !deliver_filter_is_keyed(leg.filter.as_ref()) {
            return Err(unkeyed_leg_rejection(&leg.schema_name));
        }
    }
    Ok(legs)
}

/// `POST /api/sharing/snapshot` — seal a query snapshot for object-store overwrite.
///
/// Does **not** append to the Exemem messaging mailbox. Callers PUT
/// `encrypted_blob` to `s3://…/delivery-snapshots/{snapshot_key}` (or equivalent)
/// replacing any previous object at that key.
pub async fn execute_publish_snapshot(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let body: StageBody = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => return error_json(400, &format!("invalid body: {e}"), ctx),
    };
    let messaging_pk = match validate_stage_recipient(&body) {
        Ok(pk) => pk,
        Err(e) => return error_json(400, &e, ctx),
    };
    let legs = match resolve_legs(host, &body) {
        Ok(l) => l,
        Err(e) => return render(Err(e), ctx),
    };
    if legs.is_empty() {
        return error_json(400, "snapshot requires at least one query leg", ctx);
    }

    let canonical = canonical_query_from_legs(&legs);
    let snapshot_key = match snapshot_key_for(&canonical, &body.messaging_pseudonym) {
        Ok(k) => k,
        Err(e) => return error_json(500, &e, ctx),
    };

    let owner = AccessContext::owner(host.public_key());
    let payload = match materialize_query_slice(
        &host.db,
        &legs,
        &owner,
        legs.first()
            .map_or_else(|| "query".into(), |l| format!("query: {}", l.schema_name)),
        host.public_key(),
    )
    .await
    {
        Ok(p) => p,
        Err(e) => return error_json(400, &format!("materialize slice failed: {e}"), ctx),
    };
    if payload.molecules.is_empty() {
        return error_json(400, "delivery snapshot has no matching records", ctx);
    }

    let record_count = {
        let mut seen: HashSet<(String, String)> = HashSet::new();
        for mol in &payload.molecules {
            seen.insert((mol.schema_name.clone(), mol.record_key.clone()));
        }
        seen.len()
    };

    let signed = match sign_slice_payload(payload, host.keypair.as_ref()) {
        Ok(s) => s,
        Err(e) => return error_json(500, &format!("sign slice failed: {e}"), ctx),
    };
    let mut content_key = [0u8; 32];
    OsRng.fill_bytes(&mut content_key);
    let envelope = match encrypt_signed_slice_jwe(&signed, &content_key) {
        Ok(e) => e,
        Err(e) => return error_json(500, &format!("encrypt slice failed: {e}"), ctx),
    };
    let artifact = DeliveryArtifactDescriptor {
        payload_version: LASTDB_SLICE_PAYLOAD_VERSION.to_string(),
        payload_sha256: signed.payload_sha256,
        payload_signature: signed.signature,
        envelope_format: "jwe".to_string(),
        envelope_alg: "dir".to_string(),
        envelope_enc: "A256GCM".to_string(),
    };
    let slice_payload = DeliverySlicePayload {
        message_type: "delivery_slice".to_string(),
        content_key: B64.encode(content_key),
        envelope,
        artifact,
    };
    let sealed =
        match seal_and_encrypt_message(&messaging_pk, &slice_payload, host.keypair.as_ref()) {
            Ok(b) => b,
            Err(e) => return error_json(500, &format!("seal snapshot failed: {e}"), ctx),
        };
    let encrypted_blob = B64.encode(&sealed);
    if encrypted_blob.len() > MAX_SNAPSHOT_BLOB_B64_CHARS {
        return error_json(
            400,
            &format!(
                "sealed snapshot is {} base64 chars (max {MAX_SNAPSHOT_BLOB_B64_CHARS}); shrink the query",
                encrypted_blob.len()
            ),
            ctx,
        );
    }

    ok_json(
        &serde_json::json!({
            "snapshot_key": snapshot_key,
            "recipient_id": body.messaging_pseudonym,
            "canonical_query": canonical,
            "encrypted_blob": encrypted_blob,
            "record_count": record_count,
            "message_type": "delivery_slice",
            "transport": "object_snapshot",
            "note": "overwrite object-store key delivery-snapshots/{snapshot_key}; no messaging append",
        }),
        ctx,
    )
}

/// `POST /api/sharing/deliver` — stage a snapshot delivery (no network).
pub async fn execute_stage_delivery(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let body: StageBody = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => return error_json(400, &format!("invalid body: {e}"), ctx),
    };
    if let Err(e) = validate_stage_recipient(&body) {
        return error_json(400, &e, ctx);
    }

    let legs = match resolve_legs(host, &body) {
        Ok(l) => l,
        Err(e) => return render(Err(e), ctx),
    };

    let owner = AccessContext::owner(host.public_key());
    let payload = match materialize_query_slice(
        &host.db,
        &legs,
        &owner,
        legs.first()
            .map_or_else(|| "query".into(), |l| format!("query: {}", l.schema_name)),
        host.public_key(),
    )
    .await
    {
        Ok(p) => p,
        Err(e) => return error_json(400, &format!("materialize slice failed: {e}"), ctx),
    };

    if payload.molecules.is_empty() {
        return error_json(400, "delivery snapshot has no matching records", ctx);
    }

    // Build preview samples from atoms (first 3 record keys).
    let mut field_names: HashSet<String> = HashSet::new();
    let mut records: Vec<DeliveryRecord> = Vec::new();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut by_record: BTreeMap<(String, String), BTreeMap<String, serde_json::Value>> =
        BTreeMap::new();
    let atoms: BTreeMap<_, _> = payload
        .atoms
        .iter()
        .map(|a| (a.atom_ref.clone(), a.value.clone()))
        .collect();
    for mol in &payload.molecules {
        field_names.insert(mol.field_name.clone());
        let key = (mol.schema_name.clone(), mol.record_key.clone());
        if seen.insert(key.clone()) {
            records.push(DeliveryRecord {
                schema_name: mol.schema_name.clone(),
                record_key: mol.record_key.clone(),
                fields: None,
            });
        }
        if let Some(v) = atoms.get(&mol.atom_ref) {
            by_record
                .entry(key)
                .or_default()
                .insert(mol.field_name.clone(), v.clone());
        }
    }
    let mut sample = Vec::new();
    for ((schema, record_key), fields) in by_record.into_iter().take(3) {
        sample.push(DeliverySampleRecord {
            schema_name: schema,
            record_key,
            fields,
        });
    }
    let mut fields: Vec<String> = field_names.into_iter().collect();
    fields.sort();
    let preview = DeliveryPreview {
        query_label: payload.provenance.source.clone(),
        fields,
        record_count: records.len(),
        sample,
    };

    let signed = match sign_slice_payload(payload, host.keypair.as_ref()) {
        Ok(s) => s,
        Err(e) => return error_json(500, &format!("sign slice failed: {e}"), ctx),
    };
    let mut content_key = [0u8; 32];
    OsRng.fill_bytes(&mut content_key);
    let envelope = match encrypt_signed_slice_jwe(&signed, &content_key) {
        Ok(e) => e,
        Err(e) => return error_json(500, &format!("encrypt slice failed: {e}"), ctx),
    };

    let delivery_id = Uuid::new_v4().to_string();
    let artifact = DeliveryArtifactDescriptor {
        payload_version: LASTDB_SLICE_PAYLOAD_VERSION.to_string(),
        payload_sha256: signed.payload_sha256,
        payload_signature: signed.signature,
        envelope_format: "jwe".to_string(),
        envelope_alg: "dir".to_string(),
        envelope_enc: "A256GCM".to_string(),
    };
    let staged = StagedDeliveryArtifact {
        envelope,
        content_key: content_key.to_vec(),
    };
    if let Err(e) =
        store_staged_delivery_artifact_in_ops(host.db.db_ops(), &delivery_id, &staged).await
    {
        return error_json(500, &format!("store staged artifact: {e}"), ctx);
    }

    // Spec retained for preview/provenance; Mini stages from legs only.
    let primary = legs.first().expect("legs non-empty");
    let delivery = PendingDelivery {
        delivery_id: delivery_id.clone(),
        recipient_pubkey: body.recipient_pubkey,
        recipient_display_name: body
            .recipient_display_name
            .unwrap_or_else(|| "recipient".into()),
        spec: DeliverySpec::Query {
            query: primary.to_query(),
        },
        mode: DeliveryMode::Snapshot,
        scope: None,
        records,
        preview,
        artifact,
        status: "pending".to_string(),
        created_at: unix_secs(),
        decided_at: None,
        messaging_public_key: Some(body.messaging_public_key),
        messaging_pseudonym: Some(body.messaging_pseudonym),
    };
    if let Err(e) = store_pending_delivery_in_ops(host.db.db_ops(), &delivery).await {
        let _ = remove_pending_delivery_in_ops(host.db.db_ops(), &delivery_id).await;
        return error_json(500, &format!("store pending delivery: {e}"), ctx);
    }

    ok_json(
        &serde_json::json!({
            "delivery": delivery,
            "note": "staged only — no network until POST /api/sharing/deliveries/{id}/approve",
        }),
        ctx,
    )
}

/// `GET /api/sharing/deliveries` — list pending staged deliveries.
pub async fn execute_list_deliveries(ctx: &AccessContext, host: &Host) -> UdsResponse {
    match list_pending_deliveries_in_ops(host.db.db_ops()).await {
        Ok(deliveries) => ok_json(&serde_json::json!({ "deliveries": deliveries }), ctx),
        Err(e) => error_json(500, &format!("list deliveries: {e}"), ctx),
    }
}

/// `POST /api/sharing/deliveries/{id}/approve` — seal + send via Exemem messaging.
pub async fn execute_approve_delivery(
    delivery_id: &str,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let delivery = match get_pending_delivery_in_ops(host.db.db_ops(), delivery_id).await {
        Ok(Some(d)) => d,
        Ok(None) => return error_json(404, &format!("Delivery not found: {delivery_id}"), ctx),
        Err(e) => return error_json(500, &format!("get delivery: {e}"), ctx),
    };
    if delivery.status != "pending" {
        return error_json(
            400,
            &format!("delivery {delivery_id} is already {}", delivery.status),
            ctx,
        );
    }
    let Some(messaging_pk_b64) = delivery.messaging_public_key.as_ref() else {
        return error_json(
            400,
            "delivery has no messaging_public_key — re-stage with messaging keys",
            ctx,
        );
    };
    let Some(messaging_pseudonym) = delivery.messaging_pseudonym.as_ref() else {
        return error_json(
            400,
            "delivery has no messaging_pseudonym — re-stage with messaging keys",
            ctx,
        );
    };
    let pk_bytes = match B64.decode(messaging_pk_b64.as_bytes()) {
        Ok(b) if b.len() == 32 => b,
        _ => return error_json(500, "stored messaging_public_key is invalid", ctx),
    };
    let mut target_pk = [0u8; 32];
    target_pk.copy_from_slice(&pk_bytes);

    let staged = match get_staged_delivery_artifact_in_ops(host.db.db_ops(), delivery_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return error_json(
                500,
                "pending snapshot delivery missing staged wire artifact; re-stage",
                ctx,
            )
        }
        Err(e) => return error_json(500, &format!("get staged artifact: {e}"), ctx),
    };

    let slice_payload = DeliverySlicePayload {
        message_type: "delivery_slice".to_string(),
        content_key: B64.encode(&staged.content_key),
        envelope: staged.envelope,
        artifact: delivery.artifact.clone(),
    };

    let sealed = match seal_and_encrypt_message(&target_pk, &slice_payload, host.keypair.as_ref()) {
        Ok(b) => b,
        Err(e) => return error_json(500, &format!("seal delivery failed: {e}"), ctx),
    };
    let encrypted_blob = B64.encode(&sealed);
    if encrypted_blob.len() > MAX_BLOB_B64_CHARS {
        return error_json(
            400,
            &format!(
                "sealed delivery is {} base64 chars (max {MAX_BLOB_B64_CHARS}); shrink the query slice",
                encrypted_blob.len()
            ),
            ctx,
        );
    }

    let cloud =
        match load_cloud_sync(&host.home) {
            Some(c) if !c.api_url.is_empty() && !c.api_key.is_empty() => c,
            Some(_) => return error_json(
                400,
                "cloud_sync.json present but api_key empty — re-run lastdb connect / re-register",
                ctx,
            ),
            None => {
                return error_json(
                    400,
                    "cloud_sync.json required to send via Exemem messaging",
                    ctx,
                )
            }
        };

    if let Err(e) = post_messaging_connect(
        &cloud.api_url,
        &cloud.api_key,
        messaging_pseudonym,
        &encrypted_blob,
    )
    .await
    {
        return error_json(502, &format!("messaging send failed: {e}"), ctx);
    }

    if let Err(e) = remove_pending_delivery_in_ops(host.db.db_ops(), delivery_id).await {
        return error_json(500, &format!("sent but failed to clear outbox: {e}"), ctx);
    }

    ok_json(
        &serde_json::json!({
            "delivery_id": delivery_id,
            "shared": delivery.records.len(),
            "message_type": "delivery_slice",
        }),
        ctx,
    )
}

/// `POST /api/sharing/deliveries/{id}/reject` — discard staged delivery.
pub async fn execute_reject_delivery(
    delivery_id: &str,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match get_pending_delivery_in_ops(host.db.db_ops(), delivery_id).await {
        Ok(None) => return error_json(404, &format!("Delivery not found: {delivery_id}"), ctx),
        Ok(Some(_)) => {}
        Err(e) => return error_json(500, &format!("get delivery: {e}"), ctx),
    }
    if let Err(e) = remove_pending_delivery_in_ops(host.db.db_ops(), delivery_id).await {
        return error_json(500, &format!("remove delivery: {e}"), ctx);
    }
    ok_json(
        &serde_json::json!({ "delivery_id": delivery_id, "status": "rejected" }),
        ctx,
    )
}

async fn post_messaging_connect(
    api_url: &str,
    api_key: &str,
    target_pseudonym: &str,
    encrypted_blob: &str,
) -> Result<(), String> {
    let base = api_url.trim_end_matches('/');
    // Exemem HTTP API mounts messaging under `/api/...` (see exemem-stack
    // MessagingConnect route). Do not call bare `/messaging/connect` — that
    // 404s on API Gateway.
    let url = if base.ends_with("/api") {
        format!("{base}/messaging/connect")
    } else {
        format!("{base}/api/messaging/connect")
    };
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .header("X-API-Key", api_key)
        .json(&serde_json::json!({
            "target_pseudonym": target_pseudonym,
            "encrypted_blob": encrypted_blob,
        }))
        .send()
        .await
        .map_err(|e| format!("http error: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("status {status}: {body}"));
    }
    Ok(())
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
