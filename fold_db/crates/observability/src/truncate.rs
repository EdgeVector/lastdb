//! UTF-8-safe string truncation for log call sites.
//!
//! Plain `&s[..max_bytes]` (or its `&s[..s.len().min(max_bytes)]`
//! variant) panics when `max_bytes` lands inside a multi-byte UTF-8
//! character — e.g. an em dash (3 bytes), an accented Latin letter
//! (2 bytes), an emoji (4 bytes), or a CJK ideograph (3 bytes).
//!
//! In `tracing::debug!` / `tracing::warn!` sites that interpolate
//! user-supplied or LLM-derived content (queries, prompts,
//! serialized tool results, descriptive names), this is reachable
//! from production input — and on actix workers the panic just
//! kills the worker thread and the client sees `Empty reply from
//! server`. Use [`truncate_on_char_boundary`] at every byte-capped
//! log site instead.

/// Truncate `s` to at most `max_bytes`, snapping the cut down to the
/// nearest UTF-8 char boundary. Returns the whole string when it is
/// already within the cap.
///
/// The returned slice is always a valid `&str` — never panics, never
/// allocates, never returns a string longer than `max_bytes`.
pub fn truncate_on_char_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Cap on the visible portion of a user-supplied query/term when it is
/// logged. Anything beyond this is replaced with a
/// `…(N chars truncated)` marker so the body of the log line never
/// carries the full natural-language input (which on a log-forwarding
/// node would become a content leak).
pub const QUERY_LOG_MAX_BYTES: usize = 80;

/// Render a user-supplied query/term for safe inclusion in a log
/// message: the first [`QUERY_LOG_MAX_BYTES`] bytes (snapped to a char
/// boundary) followed by `…(N chars truncated)` when the original was
/// longer.
///
/// Use at every `tracing::*!` site that interpolates a raw query
/// string, search term, or other user-supplied free-form text — pair
/// with [`crate::redact_id!`] for the accompanying user identifier so
/// the line emits neither raw content nor full hash.
pub fn truncate_query_for_log(s: &str) -> String {
    let head = truncate_on_char_boundary(s, QUERY_LOG_MAX_BYTES);
    if head.len() < s.len() {
        format!("{}…({} chars truncated)", head, s.len() - head.len())
    } else {
        head.to_string()
    }
}
