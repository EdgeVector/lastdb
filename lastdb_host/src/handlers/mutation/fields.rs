//! Mutation field validation and aggregate schema/receipt helpers (shared).

use super::*;

// ---------------------------------------------------------------------------
// Mutation field validation (shared)
// ---------------------------------------------------------------------------

/// Collect every writable field name on `schema`. Mirror of the query side's
/// queryable-field collection, but tighter: `transform_fields` are computed at
/// read time and rejected as mutation targets; only plain `fields` and
/// `ref_fields` keys (which a caller can legitimately mutate, even though the
/// value shape is a reference object) are included. Value-shape validation for
/// refs is intentionally out of scope here — this gate only refuses unknown
/// field NAMES, not bad shapes.
#[must_use]
pub fn mutation_writable_field_names(schema: &Schema) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    if let Some(plain) = schema.fields.as_ref() {
        names.extend(plain.iter().cloned());
    }
    names.extend(schema.ref_fields.keys().cloned());
    names.sort();
    names.dedup();
    names
}

pub(in super::super) fn mutation_field_is_writable(schema: &Schema, field: &str) -> bool {
    schema
        .fields
        .as_ref()
        .is_some_and(|plain| plain.iter().any(|name| name == field))
        || schema.ref_fields.contains_key(field)
}

/// Diff the keys of `fields_and_values` against the schema's writable surface.
/// `Some((unknown, available))` when at least one key is unknown, `None` when
/// every key is legal or the map is empty (no field names to check).
#[must_use]
pub fn mutation_unknown_fields(
    schema: &Schema,
    fields_and_values: &HashMap<String, Value>,
) -> Option<(Vec<String>, Vec<String>)> {
    if fields_and_values.is_empty() {
        return None;
    }
    let mut unknown: Vec<String> = fields_and_values
        .keys()
        .filter(|field| !mutation_field_is_writable(schema, field))
        .cloned()
        .collect();
    if unknown.is_empty() {
        None
    } else {
        // HashMap iteration order is non-deterministic; sort so the error
        // payload is stable across runs (and so tests can pin order).
        unknown.sort();
        let available = mutation_writable_field_names(schema);
        Some((unknown, available))
    }
}

/// Build the discriminated `unknown_fields` rejection body — `{ok, error:
/// "unknown_fields", message, schema_name, unknown_fields, available_fields}`.
/// This is the ONE body shape every socket surface emits for an unknown-field
/// rejection (the full node's HTTP route renders it as a 400 JSON response;
/// the owner socket carries it as the raw body of a 400), so a CLI can branch
/// on a single contract failure regardless of which host fired. Clients (e.g.
/// fkanban's legacy-surfaces write fallback) depend on `error` being exactly
/// `"unknown_fields"` and on the offending names appearing in `message`.
#[must_use]
pub fn unknown_fields_body(
    schema_name: &str,
    unknown: &[String],
    available: &[String],
    predicate: &str,
) -> Value {
    let quoted_unknown = unknown
        .iter()
        .map(|f| format!("'{f}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let plural = if unknown.len() == 1 { "" } else { "s" };
    let available_summary = if available.is_empty() {
        "<none>".to_string()
    } else {
        available.join(", ")
    };
    let message = format!(
        "Field{plural} {quoted_unknown} {predicate} '{schema_name}'. Available: {available_summary}",
    );
    serde_json::json!({
        "ok": false,
        "error": "unknown_fields",
        "message": message,
        "schema_name": schema_name,
        "unknown_fields": unknown,
        "available_fields": available,
    })
}

/// Gate a mutation's field names against the resolved schema BEFORE the write:
/// a mutation naming a field the schema doesn't carry gets the structured
/// `unknown_fields` 400 instead of surfacing from deep inside the write path as
/// an opaque (and, pre-gate, unlogged) 500 — the failure mode that stalled
/// every fkanban board write after the 2026-07-12 minimal-node cutover (fbrain
/// `papercut-lastdbd-post-cutover-board-mutation-500`). Follows the
/// supersession chain so the gate agrees with the executor (#618). A schema
/// lookup failure does NOT block the write — the write path stays authoritative
/// for its own errors.
pub(in super::super) async fn validate_mutation_fields<H: HostNode>(
    host: &H,
    requested: &str,
    canonical: &str,
    fields_and_values: &HashMap<String, Value>,
) -> Result<(), HostError> {
    let Ok(Some(schema)) = host
        .fold_db()
        .schema_manager()
        .get_schema_following_supersession(canonical)
        .await
    else {
        return Ok(());
    };
    if let Some((unknown, available)) = mutation_unknown_fields(&schema, fields_and_values) {
        tracing::info!(
            target: "lastdb_host::handlers",
            schema = %requested,
            unknown_fields = ?unknown,
            "execute_mutation: rejecting unknown fields"
        );
        return Err(HostError::new(
            400,
            unknown_fields_body(requested, &unknown, &available, "not writable on schema")
                .to_string(),
        ));
    }
    Ok(())
}

/// Approximate serialized byte size of a mutation's field payload — the QoS lane
/// signal for a write (a large blob write lands on [`Lane::Bulk`], a small row
/// write on [`Lane::Interactive`]). Uses each value's compact JSON length, which
/// is a cheap, allocation-light proxy for the bytes the write will persist.
pub(in super::super) fn fields_payload_bytes(fields: &HashMap<String, Value>) -> usize {
    fields
        .values()
        .map(|v| match v {
            // Avoid re-serializing a large string just to measure it.
            Value::String(s) => s.len(),
            other => other.to_string().len(),
        })
        .sum()
}

pub(in super::super) fn resolve_required_schema_name<H: HostNode>(
    host: &H,
    requested: &str,
) -> Result<String, HostError> {
    resolve_schema_name(host, requested)?
        .ok_or_else(|| HostError::new(404, format!("Schema not found: {requested}")))
}

pub(in super::super) fn resolve_aggregate_schema_triplet<H: HostNode>(
    host: &H,
    source: &str,
    target: &str,
    member: &str,
) -> Result<(String, String, String), HostError> {
    Ok((
        resolve_required_schema_name(host, source)?,
        resolve_required_schema_name(host, target)?,
        resolve_required_schema_name(host, member)?,
    ))
}

pub(in super::super) fn resolve_aggregate_set_schema_names<H: HostNode>(
    host: &H,
    mut aggregate: AggregateSet,
) -> Result<AggregateSet, HostError> {
    aggregate.target_schema_name =
        resolve_required_schema_name(host, &aggregate.target_schema_name)?;
    aggregate.member_schema_name =
        resolve_required_schema_name(host, &aggregate.member_schema_name)?;
    Ok(aggregate)
}

pub(in super::super) fn require_durable_aggregate_receipt(
    receipt: ResidentCommitReceipt,
    operation: &str,
) -> Result<ResidentCommitReceipt, HostError> {
    if receipt.durability != ResidentDurability::Durable {
        return Err(HostError::internal(format!(
            "{operation} returned a queued receipt"
        )));
    }
    Ok(receipt)
}

pub(in super::super) fn source_mutation_id(
    receipt: &ResidentCommitReceipt,
) -> Result<String, HostError> {
    receipt
        .mutation_ids
        .first()
        .cloned()
        .ok_or_else(|| HostError::internal("Mutation returned no IDs"))
}
