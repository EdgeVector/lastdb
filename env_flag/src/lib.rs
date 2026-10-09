//! Canonical env-flag truthy / falsy vocabulary for the fold monorepo.
//!
//! Feature and gate flags used to reimplement ad-hoc parsers (`1`/`true` only
//! in one crate, `1`/`true`/`yes`/`on` in another). Operators who set
//! `FEATURE=on` or `FEATURE=yes` then saw silent no-ops on half the flags.
//!
//! # Canonical vocabulary
//!
//! After `trim` + ASCII lowercasing:
//!
//! | Kind | Accepted values |
//! |------|-----------------|
//! | **Truthy** | `1`, `true`, `yes`, `on` |
//! | **Falsy** | `0`, `false`, `no`, `off` |
//!
//! Whitespace around the value is ignored. Unrecognized non-empty strings are
//! neither truthy nor falsy (`parse` returns `None`). Empty / missing env
//! values are not truthy.
//!
//! # Default-on / default-off semantics
//!
//! This crate only owns **recognition** of enablement strings. Callers still
//! decide what happens when the variable is unset (default-off vs default-on).

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

/// Returns `true` when `raw` is a canonical falsy flag value.
///
/// Accepts `0` / `false` / `no` / `off` (case-insensitive, surrounding
/// whitespace ignored).
#[must_use]
pub fn falsy(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "off"
    )
}

/// Parse a flag string into `Some(true)` / `Some(false)` / `None`.
///
/// Use this when the caller needs a tri-state (e.g. env override that keeps a
/// product default when the value is unrecognized).
#[must_use]
pub fn parse(raw: &str) -> Option<bool> {
    if truthy(raw) {
        Some(true)
    } else if falsy(raw) {
        Some(false)
    } else {
        None
    }
}

/// `true` when the process environment variable `name` is set to a truthy value.
///
/// Missing or unreadable variables, empty strings, and unrecognized values all
/// yield `false` (default-off). Does not change default-on semantics — callers
/// that need default-on should use [`var_parse`] and supply their own default.
#[must_use]
pub fn var_truthy(name: &str) -> bool {
    std::env::var(name).ok().is_some_and(|v| truthy(&v))
}

/// Parse env var `name` as a tri-state bool, or `None` when unset / unreadable /
/// unrecognized.
#[must_use]
pub fn var_parse(name: &str) -> Option<bool> {
    std::env::var(name).ok().as_deref().and_then(parse)
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
