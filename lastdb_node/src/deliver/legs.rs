use super::*;

use super::types::*;
use fold_db::sharing::delivery_wire::LastDbSlicePayload;

pub(super) fn resolve_filter(
    explicit: Option<HashRangeFilter>,
    max_records: Option<usize>,
) -> Option<HashRangeFilter> {
    if explicit.is_some() {
        return explicit;
    }
    max_records.map(HashRangeFilter::SampleN)
}

pub(super) fn parse_since_duration_secs(raw: &str) -> Result<u64, String> {
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

pub(super) fn resolve_order_by(
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

pub(super) fn stage_predicates(
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

pub(super) fn two_pass_limit(
    predicates: Option<&[FieldPredicate]>,
    order_by: Option<&QueryOrderBy>,
    predicate_limit: Option<usize>,
    max_records: Option<usize>,
) -> Option<usize> {
    predicate_limit.or_else(|| {
        (predicates.is_some_and(|p| !p.is_empty()) || order_by.is_some()).then_some(max_records)?
    })
}

pub(super) fn leg_filter(
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

pub(super) fn legs_from_body(body: &StageBody) -> Result<Vec<QuerySliceLeg>, String> {
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
pub(super) struct CanonicalQuery {
    v: u32,
    legs: Vec<CanonicalQueryLeg>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct CanonicalQueryLeg {
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

pub(super) fn canonical_query_from_legs(legs: &[QuerySliceLeg]) -> CanonicalQuery {
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
pub(super) fn snapshot_key_for(
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

pub(super) fn validate_stage_recipient(body: &StageBody) -> Result<[u8; 32], String> {
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
pub(super) fn deliver_filter_is_keyed(filter: Option<&HashRangeFilter>) -> bool {
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
pub(super) fn unkeyed_leg_rejection(schema_name: &str) -> HostError {
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

pub(super) fn resolve_legs(host: &Host, body: &StageBody) -> Result<Vec<QuerySliceLeg>, HostError> {
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

/// Signed + encrypted slice ready to wrap in a wire payload.
pub(super) struct SealedSlice {
    pub(super) content_key: [u8; 32],
    pub(super) envelope: JweEnvelope,
    pub(super) artifact: DeliveryArtifactDescriptor,
}

/// Materialize the legs as the node owner; empty results are a 400.
pub(super) async fn materialize_nonempty(
    host: &Host,
    legs: &[QuerySliceLeg],
    ctx: &AccessContext,
) -> Result<LastDbSlicePayload, UdsResponse> {
    let owner = AccessContext::owner(host.public_key());
    let payload = materialize_query_slice(
        &host.db,
        legs,
        &owner,
        legs.first()
            .map_or_else(|| "query".into(), |l| format!("query: {}", l.schema_name)),
        host.public_key(),
    )
    .await
    .map_err(|e| error_json(400, &format!("materialize slice failed: {e}"), ctx))?;
    if payload.molecules.is_empty() {
        return Err(error_json(
            400,
            "delivery snapshot has no matching records",
            ctx,
        ));
    }
    Ok(payload)
}

/// Sign the payload with the node key and encrypt it under a fresh content key.
pub(super) fn sign_and_encrypt_slice(
    payload: LastDbSlicePayload,
    host: &Host,
    ctx: &AccessContext,
) -> Result<SealedSlice, UdsResponse> {
    let signed = sign_slice_payload(payload, host.keypair.as_ref())
        .map_err(|e| error_json(500, &format!("sign slice failed: {e}"), ctx))?;
    let mut content_key = [0u8; 32];
    OsRng.fill_bytes(&mut content_key);
    let envelope = encrypt_signed_slice_jwe(&signed, &content_key)
        .map_err(|e| error_json(500, &format!("encrypt slice failed: {e}"), ctx))?;
    let artifact = DeliveryArtifactDescriptor {
        payload_version: LASTDB_SLICE_PAYLOAD_VERSION.to_string(),
        payload_sha256: signed.payload_sha256,
        payload_signature: signed.signature,
        envelope_format: "jwe".to_string(),
        envelope_alg: "dir".to_string(),
        envelope_enc: "A256GCM".to_string(),
    };
    Ok(SealedSlice {
        content_key,
        envelope,
        artifact,
    })
}
