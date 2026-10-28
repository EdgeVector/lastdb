//! Redaction macros for `tracing` fields.
//!
//! Two macros, two purposes:
//!
//! - [`redact!`] — **opaque**. Replaces the value with the literal
//!   `<redacted>`. Use for things you never want to see again: passwords,
//!   raw API keys, encrypted blob contents.
//! - [`redact_id!`] — **correlatable**. Hashes the value with xxhash64 and
//!   formats it as `<id:abcd1234>` (8 hex chars, low 32 bits). Use for
//!   identifiers you need to follow across log lines without exposing the
//!   underlying string (user hashes, document IDs, schema names in
//!   sensitive contexts).
//!
//! Both return a value, so they slot directly into `tracing` field
//! positions:
//!
//! ```ignore
//! tracing::info!(
//!     user.hash = %observability::redact_id!(&user_hash),
//!     api.key = %observability::redact!(&api_key),
//!     "request received",
//! );
//! ```

/// Replace a sensitive value with the literal `<redacted>`.
///
/// Returns `&'static str` so it composes with `tracing`'s field syntax:
/// `tracing::info!(api.key = %observability::redact!(value), "...")`.
///
/// The `$value` expression is intentionally **not** evaluated — there is no
/// reason to compute a string we will never look at, and we want to avoid
/// accidentally materializing a secret on the stack.
#[macro_export]
macro_rules! redact {
    ($value:expr) => {{
        // Suppress unused-variable lints if the caller passes a let-binding.
        let _ = &$value;
        "<redacted>"
    }};
}

/// Replace a sensitive identifier with `<id:HHHHHHHH>` where `HHHHHHHH` is
/// the lowercase 8-hex-character (low 32-bit) xxhash64 of the input.
///
/// Returns `String`. Accepts anything that implements `AsRef<[u8]>` (so
/// `&str`, `String`, `&[u8]`, `Vec<u8>` all work).
#[macro_export]
macro_rules! redact_id {
    ($value:expr) => {{
        let __obs_redact_id_value = &$value;
        let __obs_redact_id_bytes: &[u8] =
            ::std::convert::AsRef::<[u8]>::as_ref(__obs_redact_id_value);
        let __obs_redact_id_hash =
            $crate::xxhash_rust::xxh64::xxh64(__obs_redact_id_bytes, 0) as u32;
        format!("<id:{:08x}>", __obs_redact_id_hash)
    }};
}
