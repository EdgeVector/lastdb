//! Hard upper bound on record field content size.
//!
//! **Product default (Tom, 2026-10-12):** one global ceiling of 256 KiB for new
//! writes. It was 64 KiB before. The warm set byte limit bounds resident
//! content, and a sealed 256 KiB record is about 341 KiB, under the 400 KiB
//! DynamoDB item limit.
//!
//! A record may pack many small fields, up to the ceiling. It is still **not a
//! blob store**: a list or log that grows is one record per entry in a
//! HashRange schema, and large opaque bytes belong in a file blob (CAS) with a
//! pointer in the record. See [`FIELD_VALUE_TOO_LARGE_HINT`].
//!
//! ## Configuration
//!
//! | | |
//! |--|--|
//! | Env | `LASTDB_MAX_FIELD_VALUE_BYTES` |
//! | Default | [`DEFAULT_MAX_FIELD_VALUE_BYTES`] (256 KiB) |
//! | Absolute max | [`ABSOLUTE_MAX_FIELD_VALUE_BYTES`] (1 MiB) — cannot raise higher via env |
//! | Measure | serialized JSON of field content (`Value::to_string`), **before** encryption |
//!
//! Effective limit is also on `GET /api/status` → `limits.max_field_value_bytes`
//! and `lastdb status` (`Limits: max_peer_field_value=…`). The record model
//! checks it only on one field value of a peer record.
//!
//! See `fold_db/docs/FIELD_VALUE_SIZE_LIMIT.md`.

use std::sync::OnceLock;

use serde_json::Value;

use crate::schema::types::SchemaError;

/// The fixed numbers and text are model data; this module resolves and logs.
pub use lastdb_model::content_limit::{
    field_value_byte_len, ABSOLUTE_MAX_FIELD_VALUE_BYTES, DEFAULT_MAX_FIELD_VALUE_BYTES,
    FIELD_VALUE_TOO_LARGE_HINT, HEADROOM_ALARM_FRACTION, MAX_FIELD_VALUE_BYTES_ENV,
    MIN_MAX_FIELD_VALUE_BYTES,
};

static EFFECTIVE_LIMIT: OnceLock<usize> = OnceLock::new();

/// Effective field value size limit for this process (bytes).
///
/// Resolved once from [`MAX_FIELD_VALUE_BYTES_ENV`], clamped to
/// \[`MIN_MAX_FIELD_VALUE_BYTES`, `ABSOLUTE_MAX_FIELD_VALUE_BYTES`\],
/// defaulting to [`DEFAULT_MAX_FIELD_VALUE_BYTES`].
#[must_use]
pub fn max_field_value_bytes() -> usize {
    *EFFECTIVE_LIMIT.get_or_init(resolve_effective_limit)
}

fn resolve_effective_limit() -> usize {
    let limit = parse_max_field_value_bytes(std::env::var(MAX_FIELD_VALUE_BYTES_ENV).ok());
    if limit != DEFAULT_MAX_FIELD_VALUE_BYTES {
        tracing::info!(
            target: "fold_db::content_limit",
            limit,
            default = DEFAULT_MAX_FIELD_VALUE_BYTES,
            absolute_max = ABSOLUTE_MAX_FIELD_VALUE_BYTES,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "peer field value size limit overridden from env"
        );
    }
    limit
}

/// Parse env value → clamped limit. `None` / empty / unparseable → default.
///
/// Accepts plain decimal bytes (`262144`) or a trailing unit:
/// `k`/`kb`/`kib`, `m`/`mb`/`mib` (case-insensitive).
#[must_use]
pub fn parse_max_field_value_bytes(raw: Option<String>) -> usize {
    let Some(raw) = raw else {
        return DEFAULT_MAX_FIELD_VALUE_BYTES;
    };
    let s = raw.trim();
    if s.is_empty() {
        return DEFAULT_MAX_FIELD_VALUE_BYTES;
    }
    let parsed = parse_byte_size(s).unwrap_or_else(|| {
        tracing::warn!(
            raw = %s,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "invalid LASTDB_MAX_FIELD_VALUE_BYTES; using default {}",
            DEFAULT_MAX_FIELD_VALUE_BYTES
        );
        DEFAULT_MAX_FIELD_VALUE_BYTES
    });
    clamp_field_value_limit(parsed)
}

fn parse_byte_size(s: &str) -> Option<usize> {
    let lower = s.to_ascii_lowercase();
    let (num, mult) = if let Some(rest) = lower.strip_suffix("kib") {
        (rest.trim(), 1024usize)
    } else if let Some(rest) = lower.strip_suffix("mib") {
        (rest.trim(), 1024 * 1024)
    } else if let Some(rest) = lower.strip_suffix("kb") {
        (rest.trim(), 1024)
    } else if let Some(rest) = lower.strip_suffix("mb") {
        (rest.trim(), 1024 * 1024)
    } else if let Some(rest) = lower.strip_suffix('k') {
        (rest.trim(), 1024)
    } else if let Some(rest) = lower.strip_suffix('m') {
        (rest.trim(), 1024 * 1024)
    } else {
        (lower.as_str(), 1)
    };
    let n: usize = num.trim().parse().ok()?;
    n.checked_mul(mult)
}

fn clamp_field_value_limit(n: usize) -> usize {
    if n < MIN_MAX_FIELD_VALUE_BYTES {
        tracing::warn!(
            requested = n,
            min = MIN_MAX_FIELD_VALUE_BYTES,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "field value limit below minimum; clamping"
        );
        return MIN_MAX_FIELD_VALUE_BYTES;
    }
    if n > ABSOLUTE_MAX_FIELD_VALUE_BYTES {
        tracing::warn!(
            requested = n,
            absolute_max = ABSOLUTE_MAX_FIELD_VALUE_BYTES,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "field value limit above absolute max; clamping (use file-blob/CAS for larger payloads)"
        );
        return ABSOLUTE_MAX_FIELD_VALUE_BYTES;
    }
    n
}

/// The write-path choke point: enforce the limit **and** record what happened,
/// with the schema name attached in both directions.
///
/// This exists because the two halves used to be separate calls, and only the
/// accepting half knew the schema name. A rejection therefore produced no node
/// log at all — the `413` body reaches the owner, but an operator reading
/// `lastdbd.err.log` after the fact sees nothing, and the client that failed is
/// unidentifiable. (`render` only logs `5xx`, so the typed `413` passes it by.)
///
/// Three outcomes, all named:
/// - **rejected** → `ERROR`, with schema, size, limit, and the overage.
/// - **accepted but low on headroom** → `ERROR` (see [`HEADROOM_ALARM_FRACTION`]).
/// - **accepted but over the default limit** → `WARN`
///   (see [`observe_field_value_over_default`]).
pub fn enforce_field_value_limit(schema_name: &str, content: &Value) -> Result<(), SchemaError> {
    let size = field_value_byte_len(content);
    let limit = max_field_value_bytes();
    if size > limit {
        tracing::error!(
            target: "lastdb::record_size",
            schema = schema_name,
            size_bytes = size,
            limit_bytes = limit,
            over_by_bytes = size - limit,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "field value REJECTED: exceeds the effective limit; the write returns \
             413 field_value_too_large and no data is stored"
        );
        return Err(SchemaError::FieldValueTooLarge { size, limit });
    }
    observe_field_value_headroom(schema_name, size, limit);
    observe_field_value_over_default(schema_name, content);
    Ok(())
}

/// Report an **accepted** write that is close enough to the effective ceiling
/// that continued growth will reject it.
///
/// Silent below [`HEADROOM_ALARM_FRACTION`] of the limit, so an ordinary write
/// costs one comparison and logs nothing.
fn observe_field_value_headroom(schema_name: &str, size: usize, limit: usize) {
    let threshold = (limit as f64 * HEADROOM_ALARM_FRACTION) as usize;
    if size <= threshold {
        return;
    }
    tracing::error!(
        target: "lastdb::record_size",
        schema = schema_name,
        size_bytes = size,
        limit_bytes = limit,
        remaining_bytes = limit - size,
        threshold_bytes = threshold,
        "field value is within {}% of the limit and was still accepted; if this \
         document grows on every write it will be REJECTED once it crosses — bound it \
         now, or store one record per entry",
        ((1.0 - HEADROOM_ALARM_FRACTION) * 100.0).round() as u32
    );
}

/// Whether a write of `size` bytes only succeeds because the limit is raised.
#[must_use]
pub fn is_over_default_limit(size: usize) -> bool {
    size > DEFAULT_MAX_FIELD_VALUE_BYTES && max_field_value_bytes() > DEFAULT_MAX_FIELD_VALUE_BYTES
}

/// Log a record write that the **default** limit would have rejected.
///
/// While `LASTDB_MAX_FIELD_VALUE_BYTES` is raised above the default, these
/// writes succeed silently — so there is no signal about which clients depend on
/// the raise, and lowering the limit back becomes a guess. This turns the raised
/// window into a *measurement* window: every over-default write names itself.
///
/// `schema_name` is the join key for attribution: request-ops telemetry
/// (`lastdb ops`) records `client` per schema, so a schema logged here maps back
/// to the app that wrote it.
///
/// Silent when the limit is at (or below) the default, because then the hard
/// check in [`enforce_field_value_limit`] already rejects these writes
/// loudly and there is nothing extra to observe.
pub fn observe_field_value_over_default(schema_name: &str, content: &Value) {
    let size = field_value_byte_len(content);
    if !is_over_default_limit(size) {
        return;
    }
    tracing::warn!(
        target: "lastdb::record_size",
        schema = schema_name,
        size_bytes = size,
        default_limit_bytes = DEFAULT_MAX_FIELD_VALUE_BYTES,
        effective_limit_bytes = max_field_value_bytes(),
        "field value exceeds the DEFAULT limit and is accepted only because \
         LASTDB_MAX_FIELD_VALUE_BYTES is raised; this write would fail at the default"
    );
}
