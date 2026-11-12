//! Canonical environment-variable flag and typed scalar parsers.
#![warn(missing_docs)]

/// Returns `true` when `raw` is `1`, `true`, `yes`, or `on` (ASCII case-insensitive).
#[must_use]
pub fn truthy(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Returns `true` when environment variable `name` is set to a [`truthy`] value.
#[must_use]
pub fn var_truthy(name: &str) -> bool {
    std::env::var(name).ok().is_some_and(|v| truthy(&v))
}

/// Parses environment variable `name` as `T` after trimming whitespace.
#[must_use]
pub fn var_parsed<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name).ok()?.trim().parse().ok()
}

/// Parses environment variable `name` as `T`, or returns `default`.
#[must_use]
pub fn var_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    var_parsed(name).unwrap_or(default)
}
