//! Canonical env-flag truthy vocabulary.
//!
//! Feature and gate flags used to reimplement ad-hoc parsers (`1`/`true` only
//! in one crate, `1`/`true`/`yes`/`on` in another). Operators who set
//! `FEATURE=on` or `FEATURE=yes` then saw silent no-ops on half the flags.
//!
//! # Canonical vocabulary
//!
//! After `trim` + ASCII lowercasing, a truthy value is `1`, `true`, `yes`, or
//! `on`. Whitespace around the value is ignored. Unrecognized, empty, and
//! missing values are not truthy.
//!
//! # Default-on / default-off semantics
//!
//! This crate only owns recognition of enablement strings. Callers still
//! decide what happens when the variable is unset (default-off vs default-on).
#![warn(missing_docs)]

/// Returns `true` when `raw` is a canonical truthy flag value.
///
/// Accepts `1` / `true` / `yes` / `on` (case-insensitive, surrounding whitespace
/// ignored).
#[must_use]
pub fn truthy(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// `true` when the process environment variable `name` is set to a truthy value.
///
/// Missing or unreadable variables, empty strings, and unrecognized values all
/// yield `false` (default-off).
#[must_use]
pub fn var_truthy(name: &str) -> bool {
    std::env::var(name).ok().is_some_and(|v| truthy(&v))
}

/// Parse env var `name` as `T` after trimming whitespace, or `None` when unset,
/// unreadable, or not a valid `T`.
///
/// Use this for numeric tuning knobs (`usize`, `u64`, ...). A malformed value
/// is treated like an unset one so the caller keeps its default.
#[must_use]
pub fn var_parsed<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name).ok()?.trim().parse().ok()
}

/// Like [`var_parsed`], falling back to `default` when the variable is unset or
/// malformed.
#[must_use]
pub fn var_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    var_parsed(name).unwrap_or(default)
}
