//! Themed module split from the parent.

use super::*;

pub(in crate::handlers) fn looks_like_schema_identity(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Human/product-facing schema name for an access-policy rejection.
///
/// A canonical identity hash remains useful as a secondary machine identifier,
/// but it must never be the only or primary name shown to the caller. Prefer
/// the declared descriptive name, then a non-hash spelling the caller used,
/// then a non-hash runtime name. Old unnamed hash-only schemas get an explicit
/// label instead of masquerading as a useful product name.
pub(in crate::handlers) fn product_schema_name(requested: &str, schema: Option<&Schema>) -> String {
    schema
        .and_then(|schema| schema.descriptive_name.clone())
        .or_else(|| (!looks_like_schema_identity(requested)).then(|| requested.to_string()))
        .or_else(|| {
            schema.and_then(|schema| {
                (!looks_like_schema_identity(&schema.name)).then(|| schema.name.clone())
            })
        })
        .or_else(|| {
            schema
                .and_then(|schema| schema.owner_app_id.as_deref())
                .map(|owner| format!("{owner}/<unnamed-schema>"))
        })
        .unwrap_or_else(|| "<unnamed-product-schema>".to_string())
}

pub(in crate::handlers) fn full_schema_scan_rejection(
    requested: &str,
    schema: Option<&Schema>,
) -> HostError {
    let schema_name = product_schema_name(requested, schema);
    let schema_id = schema.map_or(requested, |schema| schema.name.as_str());
    let hash_field = schema
        .and_then(|schema| schema.key.as_ref())
        .and_then(|key| key.hash_field.as_deref());
    let range_field = schema
        .and_then(|schema| schema.key.as_ref())
        .and_then(|key| key.range_field.as_deref());
    let message = format!(
        "full_schema_scan_not_allowed: unfiltered query on product schema \
         '{schema_name}' is not supported; add a keyed filter using the \
         structured remediation (admin/offline bulk may explicitly opt in \
         with X-LastDB-Allow-Full-Scan: 1)"
    );
    let body = serde_json::json!({
        "ok": false,
        "kind": "full_schema_scan_not_allowed",
        "error": "product apps are keyed-access only; unfiltered schema queries are not supported",
        "message": message,
        "schema_name": schema_name,
        "schema_id": schema_id,
        "remediation": {
            "action": "add_key_filter",
            "request_key": "filter",
            "hash_field": hash_field,
            "range_field": range_field,
            "filter_variants": KEYED_FILTER_VARIANTS,
            "admin_override": {
                "header": "X-LastDB-Allow-Full-Scan",
                "value": "1",
                "scope": "admin_offline_bulk_only"
            }
        }
    });
    HostError::structured(400, message, body)
}
