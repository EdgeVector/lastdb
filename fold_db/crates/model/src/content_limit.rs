//! The fixed numbers and text of the field value size limit.
//!
//! These are data. Core resolves the effective limit for a process from the
//! environment and logs the write outcome (`fold_db::content_limit`).
//!
//! **Product default (Tom, 2026-10-12):** one global ceiling of 256 KiB for new
//! writes. See `fold_db/docs/FIELD_VALUE_SIZE_LIMIT.md`.

use serde_json::Value;

/// Product default hard limit (256 KiB). Prefer core's
/// `max_field_value_bytes` at runtime so env overrides apply.
pub const DEFAULT_MAX_FIELD_VALUE_BYTES: usize = 256 * 1024;

/// Absolute ceiling even if `LASTDB_MAX_FIELD_VALUE_BYTES` is set higher.
/// Keeps the abuse fence while allowing limited ops headroom.
pub const ABSOLUTE_MAX_FIELD_VALUE_BYTES: usize = 1024 * 1024;

/// Floor for the configured limit (reject absurdly small ops knobs).
pub const MIN_MAX_FIELD_VALUE_BYTES: usize = 1024;

/// Env var name that raises/lowers the field value limit within
/// \[`MIN_MAX_FIELD_VALUE_BYTES`, `ABSOLUTE_MAX_FIELD_VALUE_BYTES`\].
/// The model only names it; core reads it.
pub const MAX_FIELD_VALUE_BYTES_ENV: &str = "LASTDB_MAX_FIELD_VALUE_BYTES";

/// Guidance for a rejected write, by the shape of the data. The `413` body
/// `hint` and the error message both carry this text.
pub const FIELD_VALUE_TOO_LARGE_HINT: &str = "For a list or log that grows, write one record \
    per entry in a HashRange schema (hash = partition, range = entry key). For large opaque \
    bytes, store a file blob (CAS) and keep a pointer in the record; many small fields may be \
    packed into one record up to the limit.";

/// Guidance for a record of a home above its 64 KiB cap
/// (`record_value::MAX_RECORD_BYTES`). The `413` body `hint` and the error
/// message both carry this text.
pub const RECORD_TOO_LARGE_HINT: &str = "A record holds every field of one key, and its \
    encoded value must fit the 64 KiB record cap. Move a large field to a file blob (CAS) and \
    keep its pointer in the record, or write a list that grows as one record per entry in a \
    HashRange schema (hash = partition, range = entry key).";

/// Serialized byte length of field value JSON (`Value::to_string`).
#[must_use]
pub fn field_value_byte_len(content: &Value) -> usize {
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
