//! Hard upper bound on atom field content size.
//!
//! **Won't-undo product default (Tom, 2026-07-24):** 64 KiB.
//! Preference: `preference-lastdb-atom-size-hard-limit-64kib`.
//!
//! Atoms are structured field values, **not a blob store**. Large / opaque
//! bytes belong in file-blob / CAS; the atom holds only a pointer (hash, size).
//!
//! ## Configuration
//!
//! | | |
//! |--|--|
//! | Env | `LASTDB_MAX_ATOM_CONTENT_BYTES` |
//! | Default | [`DEFAULT_MAX_ATOM_CONTENT_BYTES`] (64 KiB) |
//! | Absolute max | [`ABSOLUTE_MAX_ATOM_CONTENT_BYTES`] (1 MiB) — cannot raise higher via env |
//! | Measure | serialized JSON of field content (`Value::to_string`), **before** encryption |
//!
//! Effective limit is also on `GET /api/status` → `limits.max_atom_content_bytes`
//! and `lastdb status` (`Limits: max_atom_content=…`).
//!
//! See `fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md`.

use std::sync::OnceLock;

use serde_json::Value;

use crate::schema::types::SchemaError;

/// Product default hard limit (64 KiB). Prefer [`max_atom_content_bytes`] at
/// runtime so env overrides apply.
pub const DEFAULT_MAX_ATOM_CONTENT_BYTES: usize = 64 * 1024;

/// Absolute ceiling even if `LASTDB_MAX_ATOM_CONTENT_BYTES` is set higher.
/// Keeps the abuse fence while allowing limited ops headroom.
pub const ABSOLUTE_MAX_ATOM_CONTENT_BYTES: usize = 1024 * 1024;

/// Floor for the configured limit (reject absurdly small ops knobs).
pub const MIN_MAX_ATOM_CONTENT_BYTES: usize = 1024;

/// Env var that raises/lowers the atom content limit within
/// \[`MIN_MAX_ATOM_CONTENT_BYTES`, `ABSOLUTE_MAX_ATOM_CONTENT_BYTES`\].
pub const MAX_ATOM_CONTENT_BYTES_ENV: &str = "LASTDB_MAX_ATOM_CONTENT_BYTES";

/// Back-compat alias for the **default** limit (not the effective runtime
/// value). New code should call [`max_atom_content_bytes`].
pub const MAX_ATOM_CONTENT_BYTES: usize = DEFAULT_MAX_ATOM_CONTENT_BYTES;

static EFFECTIVE_LIMIT: OnceLock<usize> = OnceLock::new();

/// Effective atom content size limit for this process (bytes).
///
/// Resolved once from [`MAX_ATOM_CONTENT_BYTES_ENV`], clamped to
/// \[`MIN_MAX_ATOM_CONTENT_BYTES`, `ABSOLUTE_MAX_ATOM_CONTENT_BYTES`\],
/// defaulting to [`DEFAULT_MAX_ATOM_CONTENT_BYTES`].
#[must_use]
pub fn max_atom_content_bytes() -> usize {
    *EFFECTIVE_LIMIT.get_or_init(resolve_effective_limit)
}

fn resolve_effective_limit() -> usize {
    let limit = parse_max_atom_content_bytes(std::env::var(MAX_ATOM_CONTENT_BYTES_ENV).ok());
    if limit != DEFAULT_MAX_ATOM_CONTENT_BYTES {
        tracing::info!(
            target: "fold_db::atom",
            limit,
            default = DEFAULT_MAX_ATOM_CONTENT_BYTES,
            absolute_max = ABSOLUTE_MAX_ATOM_CONTENT_BYTES,
            env = MAX_ATOM_CONTENT_BYTES_ENV,
            "atom content size limit overridden from env"
        );
    }
    limit
}

/// Parse env value → clamped limit. `None` / empty / unparseable → default.
///
/// Accepts plain decimal bytes (`65536`) or a trailing unit:
/// `k`/`kb`/`kib`, `m`/`mb`/`mib` (case-insensitive).
#[must_use]
pub fn parse_max_atom_content_bytes(raw: Option<String>) -> usize {
    let Some(raw) = raw else {
        return DEFAULT_MAX_ATOM_CONTENT_BYTES;
    };
    let s = raw.trim();
    if s.is_empty() {
        return DEFAULT_MAX_ATOM_CONTENT_BYTES;
    }
    let parsed = parse_byte_size(s).unwrap_or_else(|| {
        tracing::warn!(
            raw = %s,
            env = MAX_ATOM_CONTENT_BYTES_ENV,
            "invalid LASTDB_MAX_ATOM_CONTENT_BYTES; using default {}",
            DEFAULT_MAX_ATOM_CONTENT_BYTES
        );
        DEFAULT_MAX_ATOM_CONTENT_BYTES
    });
    clamp_atom_content_limit(parsed)
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

fn clamp_atom_content_limit(n: usize) -> usize {
    if n < MIN_MAX_ATOM_CONTENT_BYTES {
        tracing::warn!(
            requested = n,
            min = MIN_MAX_ATOM_CONTENT_BYTES,
            env = MAX_ATOM_CONTENT_BYTES_ENV,
            "atom content limit below minimum; clamping"
        );
        return MIN_MAX_ATOM_CONTENT_BYTES;
    }
    if n > ABSOLUTE_MAX_ATOM_CONTENT_BYTES {
        tracing::warn!(
            requested = n,
            absolute_max = ABSOLUTE_MAX_ATOM_CONTENT_BYTES,
            env = MAX_ATOM_CONTENT_BYTES_ENV,
            "atom content limit above absolute max; clamping (use file-blob/CAS for larger payloads)"
        );
        return ABSOLUTE_MAX_ATOM_CONTENT_BYTES;
    }
    n
}

/// Serialized byte length of atom content JSON (`Value::to_string`).
#[must_use]
pub fn atom_content_byte_len(content: &Value) -> usize {
    content.to_string().len()
}

/// Fraction of the **effective** limit above which an accepted write is
/// reported as running out of headroom.
///
/// A single-row list index that is rewritten in full on every mutation grows
/// monotonically, so it does not fail until the write that crosses the ceiling
/// — and that write is a half-commit, not a clean refusal. Reporting at 80%
/// turns "it wedged" into "it will wedge", with bytes remaining, while there is
/// still room to bound the document.
pub const HEADROOM_ALARM_FRACTION: f64 = 0.8;

/// A fraction outside `(0.5, 1.0)` is either constant noise or never fires.
/// Enforced at compile time rather than in a test — both operands are constants,
/// so a runtime assertion could never fail anyway.
const _: () = assert!(HEADROOM_ALARM_FRACTION > 0.5 && HEADROOM_ALARM_FRACTION < 1.0);

/// Reject atom content larger than the effective limit ([`max_atom_content_bytes`]).
///
/// Returns [`SchemaError::AtomContentTooLarge`] with the measured size and limit.
///
/// Prefer [`enforce_atom_content_limit`] at a write path: this function alone
/// records nothing, so a rejection it produces is invisible in the node log.
pub fn ensure_atom_content_within_limit(content: &Value) -> Result<(), SchemaError> {
    let size = atom_content_byte_len(content);
    let limit = max_atom_content_bytes();
    if size > limit {
        return Err(SchemaError::AtomContentTooLarge { size, limit });
    }
    Ok(())
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
///   (see [`observe_atom_content_over_default`]).
pub fn enforce_atom_content_limit(schema_name: &str, content: &Value) -> Result<(), SchemaError> {
    let size = atom_content_byte_len(content);
    let limit = max_atom_content_bytes();
    if size > limit {
        tracing::error!(
            target: "lastdb::atom_size",
            schema = schema_name,
            size_bytes = size,
            limit_bytes = limit,
            over_by_bytes = size - limit,
            env = MAX_ATOM_CONTENT_BYTES_ENV,
            "atom content REJECTED: exceeds the effective limit; the write returns \
             413 atom_content_too_large and no data is stored"
        );
        return Err(SchemaError::AtomContentTooLarge { size, limit });
    }
    observe_atom_content_headroom(schema_name, size, limit);
    observe_atom_content_over_default(schema_name, content);
    Ok(())
}

/// Report an **accepted** write that is close enough to the effective ceiling
/// that continued growth will reject it.
///
/// Silent below [`HEADROOM_ALARM_FRACTION`] of the limit, so an ordinary write
/// costs one comparison and logs nothing.
fn observe_atom_content_headroom(schema_name: &str, size: usize, limit: usize) {
    #[allow(clippy::cast_precision_loss)]
    let threshold = (limit as f64 * HEADROOM_ALARM_FRACTION) as usize;
    if size <= threshold {
        return;
    }
    tracing::error!(
        target: "lastdb::atom_size",
        schema = schema_name,
        size_bytes = size,
        limit_bytes = limit,
        remaining_bytes = limit - size,
        threshold_bytes = threshold,
        "atom content is within {}% of the limit and was still accepted; a document \
         that grows on every write will be REJECTED once it crosses — bound it now",
        ((1.0 - HEADROOM_ALARM_FRACTION) * 100.0) as u32
    );
}

/// Whether a write of `size` bytes only succeeds because the limit is raised.
#[must_use]
pub fn is_over_default_limit(size: usize) -> bool {
    size > DEFAULT_MAX_ATOM_CONTENT_BYTES
        && max_atom_content_bytes() > DEFAULT_MAX_ATOM_CONTENT_BYTES
}

/// Log an atom write that the **default** limit would have rejected.
///
/// While `LASTDB_MAX_ATOM_CONTENT_BYTES` is raised above the default, these
/// writes succeed silently — so there is no signal about which clients depend on
/// the raise, and lowering the limit back becomes a guess. This turns the raised
/// window into a *measurement* window: every over-default write names itself.
///
/// `schema_name` is the join key for attribution: request-ops telemetry
/// (`lastdb ops`) records `client` per schema, so a schema logged here maps back
/// to the app that wrote it.
///
/// Silent when the limit is at (or below) the default, because then the hard
/// check in [`ensure_atom_content_within_limit`] already rejects these writes
/// loudly and there is nothing extra to observe.
pub fn observe_atom_content_over_default(schema_name: &str, content: &Value) {
    let size = atom_content_byte_len(content);
    if !is_over_default_limit(size) {
        return;
    }
    tracing::warn!(
        target: "lastdb::atom_size",
        schema = schema_name,
        size_bytes = size,
        default_limit_bytes = DEFAULT_MAX_ATOM_CONTENT_BYTES,
        effective_limit_bytes = max_atom_content_bytes(),
        "atom content exceeds the DEFAULT limit and is accepted only because \
         LASTDB_MAX_ATOM_CONTENT_BYTES is raised; this write would fail at the default"
    );
}
