//! Framework-agnostic request parsing for the owner-socket data routes.
//!
//! Every helper here is **I4** (side-channel closure): a parse failure never
//! echoes a caller-supplied byte (a schema name, namespace, or offending value)
//! back onto the wire. These are the exact helpers both socket route executors
//! (`lastdb_node::exec`, `fold_db_node::server::uds_exec`) parse requests with;
//! keeping one copy means the wire contract cannot silently drift between the
//! minimal daemon and the full node.
//!
//! A refusal is a typed [`Reject`](crate::reject::Reject) rather than the
//! former content-free `400 "Bad Request"`: it names which parse rule refused
//! and which key it refused on, using only compile-time constants. See
//! [`crate::reject`] for why naming the rule keeps I4 intact.

use fold_db::schema::types::KeyValue;
use lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use serde_json::Value;

use crate::reject::{Reject, WireKey};

/// Parse the request body as a JSON object. A non-object top-level JSON value
/// (array, string, number) is rejected the same as malformed bytes — the data
/// routes all expect an object.
///
/// # Errors
/// Returns a [`RejectKind::MalformedBody`](crate::reject::RejectKind::MalformedBody)
/// `400` when the body is not a JSON object.
pub fn body_object(req: &UdsRequest) -> Result<serde_json::Map<String, Value>, UdsResponse> {
    match serde_json::from_slice::<Value>(&req.body) {
        Ok(Value::Object(map)) => Ok(map),
        Ok(_) | Err(_) => Err(Reject::malformed_body().response()),
    }
}

/// Pull an optional non-negative pagination field out of a JSON object, removing
/// it so the remainder can deserialize into a `deny_unknown_fields` `Query`.
///
/// An absent or `null` field is `None`; a non-negative integer is `Some(n)`;
/// anything else (negative, fractional, string) is rejected. The error names the
/// key but never echoes the offending value (**I4**).
///
/// The key is a [`WireKey`] rather than a `&str` so the rejection can name it
/// from the closed vocabulary — a caller-supplied string could never reach the
/// wire through this path.
///
/// # Errors
/// Returns an [`RejectKind::InvalidValue`](crate::reject::RejectKind::InvalidValue)
/// `400` when the field is present, non-null, and not a non-negative integer.
pub fn take_pagination(
    obj: &mut serde_json::Map<String, Value>,
    field: WireKey,
) -> Result<Option<usize>, UdsResponse> {
    let Some(raw) = obj.remove(field.as_str()) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    match raw.as_u64() {
        Some(n) => Ok(Some(n as usize)),
        None => Err(Reject::invalid(field).response()),
    }
}

/// The keys `POST /api/query` genuinely requires, in the order a caller is told
/// about them.
///
/// This list must contain **exactly** the `Query` fields serde would reject as
/// missing — no more. `filter` is deliberately absent: it is
/// `Option<HashRangeFilter>`, and serde's `missing_field` helper resolves a
/// missing `Option<T>` to `None` instead of erroring, so an unfiltered body
/// parses fine and must keep reaching the scan guard — the layer that actually
/// decides whether an unfiltered read is permitted. Listing it here would
/// reject at parse time a request the guard would have allowed.
///
/// [`tests::require_keys_matches_what_serde_actually_requires`] pins both
/// directions against the real `Query` type.
pub const QUERY_REQUIRED_KEYS: &[WireKey] = &[WireKey::SchemaName, WireKey::Fields];

/// The keys `POST /api/mutation` genuinely requires, in the order a caller is
/// told about them.
///
/// `type` leads because `Operation` is internally tagged (`#[serde(tag =
/// "type")]`): with it absent serde never reaches any other field, so reporting
/// a later key first would send the caller to fix something that is not yet the
/// problem.
///
/// `source_file_name`, `expected`, and `convergence` are deliberately absent —
/// all three are `Option<_>`, which serde resolves to `None` when missing, so
/// requiring any of them would reject at parse time a mutation the node would
/// have applied. Same rule, and the same past mistake, as `filter` on
/// [`QUERY_REQUIRED_KEYS`].
///
/// [`tests::mutation_required_keys_match_what_serde_actually_requires`] pins
/// both directions against the real `Operation` type.
pub const MUTATION_REQUIRED_KEYS: &[WireKey] = &[
    WireKey::OperationType,
    WireKey::Schema,
    WireKey::FieldsAndValues,
    WireKey::KeyValue,
    WireKey::MutationType,
];

/// Check that every key a route requires is present in the parsed body, naming
/// the first missing one.
///
/// This runs *before* the body deserializes into its typed request so the
/// caller learns which key is missing. `serde` reports a missing field as a
/// generic deserialization failure, which is how `POST /api/query` came to
/// answer the 11 bytes `Bad Request` to the most natural first query anyone
/// writes by hand — schema plus filter, no `fields`. `required` is a fixed list
/// owned by the route, so nothing here is caller-derived.
///
/// A key present with a `null` value counts as present: whether `null` is legal
/// is the typed request's business, not this check's.
///
/// # Errors
/// Returns a [`RejectKind::MissingRequiredKey`](crate::reject::RejectKind::MissingRequiredKey)
/// `400` naming the first absent key, in `required` order.
pub fn require_keys(
    obj: &serde_json::Map<String, Value>,
    required: &[WireKey],
) -> Result<(), UdsResponse> {
    match first_missing_key(obj, required) {
        Some(key) => Err(Reject::missing(key).response()),
        None => Ok(()),
    }
}

/// The first key in `required` absent from `obj`, in `required` order.
///
/// The same check [`require_keys`] performs, exposed without rendering a
/// response, for routes that must record telemetry or a log line around the
/// refusal before answering. Keeping one implementation means the two routes
/// cannot disagree about what "missing" means — notably that a key present with
/// a `null` value counts as present.
#[must_use]
pub fn first_missing_key(
    obj: &serde_json::Map<String, Value>,
    required: &[WireKey],
) -> Option<WireKey> {
    required
        .iter()
        .find(|key| !obj.contains_key(key.as_str()))
        .copied()
}

/// Pull an optional query cursor out of a JSON object, removing it so the
/// remainder can deserialize into a `deny_unknown_fields` `Query`.
///
/// The cursor is the same structured key object query rows return:
/// `{ "hash": string|null, "range": string|null }`. A malformed value is
/// rejected naming the `cursor` key, with the shape to echo back.
///
/// # Errors
/// Returns an [`RejectKind::InvalidValue`](crate::reject::RejectKind::InvalidValue)
/// `400` when `cursor` is present, non-null, and not a key object.
pub fn take_cursor(
    obj: &mut serde_json::Map<String, Value>,
) -> Result<Option<KeyValue>, UdsResponse> {
    let Some(raw) = obj.remove(WireKey::Cursor.as_str()) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    serde_json::from_value::<KeyValue>(raw)
        .map(Some)
        .map_err(|_| Reject::invalid(WireKey::Cursor).response())
}

/// Whether a `?key=...` flag in the request target is truthy. An absent flag, an
/// empty value, or anything other than `true`/`1` reads as `false`.
#[must_use]
pub fn query_flag(target: &str, key: &str) -> bool {
    let Some(qs) = target.split_once('?').map(|(_, q)| q) else {
        return false;
    };
    qs.split('&').any(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        k == key && env_flag::truthy(v)
    })
}

/// Pull the percent-decoded value of a `?key=...` query-string parameter from
/// the request target, returning `None` when the key is absent. The first
/// occurrence wins; a bad `%`-escape is left verbatim.
#[must_use]
pub fn query_value(target: &str, key: &str) -> Option<String> {
    let qs = target.split_once('?').map(|(_, q)| q)?;
    qs.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (k == key).then(|| percent_decode(v))
    })
}

/// Minimal `application/x-www-form-urlencoded` value decode: `+` → space and
/// `%XX` → byte. A truncated or non-hex `%` escape is left verbatim. Lossy UTF-8
/// so a malformed byte sequence can never panic the route.
#[must_use]
pub fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(h), Some(l)) = (hi, lo) {
                    out.push((h * 16 + l) as u8);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parse the `min_score` value: absent/blank → `Ok(None)`; a finite number in
/// `[0.0, 1.0]` → `Ok(Some(v))`; anything else → `Err(())` (the route turns that
/// into a content-free 400, never echoing the offending value — **I4**).
///
/// # Errors
/// Returns `Err(())` for a non-numeric, non-finite, or out-of-`[0,1]` value.
#[allow(
    clippy::result_unit_err,
    reason = "the sole caller (a socket route) maps any Err to a fixed content-free 400 (I4) \
              and never inspects an error payload; a unit error keeps the wire contract exact"
)]
pub fn parse_min_score(raw: Option<&str>) -> Result<Option<f64>, ()> {
    let Some(s) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let value = s.parse::<f64>().map_err(|_| ())?;
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(());
    }
    Ok(Some(value))
}

/// Extract a single `?`/`#`-stripped, percent-decoded path segment for
/// `GET /api/schema/{name}`. `None` when absent/empty/nested.
#[must_use]
pub fn schema_name_from_target(target: &str) -> Option<String> {
    let end = target.find(['?', '#']).unwrap_or(target.len());
    let path = &target[..end];
    let raw = path.strip_prefix("/api/schema/")?;
    (!raw.is_empty() && !raw.contains('/')).then(|| percent_decode(raw))
}

/// Extract the single path segment after `prefix` from a request target (query
/// string stripped, percent-decoded). `None` when absent/empty/nested. Used by
/// the `/api/history/{uuid}` and `/api/atom/{uuid}` routes.
#[must_use]
pub fn path_tail(target: &str, prefix: &str) -> Option<String> {
    let end = target.find(['?', '#']).unwrap_or(target.len());
    let path = &target[..end];
    let raw = path.strip_prefix(prefix)?;
    (!raw.is_empty() && !raw.contains('/')).then(|| percent_decode(raw))
}
