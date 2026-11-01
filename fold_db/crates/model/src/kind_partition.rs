//! Exact current storage-key layouts and administrative family bounds.
//!
//! Flat kinds use `kind\0id`. No reader substitutes another key spelling.

/// Kinds whose flat keys use `kind\0rest`.
pub const KIND_PARTITION_KINDS: &[&str] = &["schemaidx", "conflict", "history", "dellog"];

/// The separator between the kind and the rest of a flat key.
pub const KIND_PARTITION_SEP: char = '\0';

/// `kind\0rest` — the write form for a listed flat kind.
#[must_use]
pub fn anchored(kind: &str, rest: &str) -> String {
    format!("{kind}{KIND_PARTITION_SEP}{rest}")
}

/// Match the current separator and any current compound form for this kind.
#[must_use]
pub fn starts_with_kind(bare: &str, kind: &str) -> bool {
    if matches!(kind, "tv" | "tvr" | "tvr-meta") || !bare.starts_with(kind) {
        return false;
    }
    let rest = &bare[kind.len()..];
    if !KIND_PARTITION_KINDS.contains(&kind) {
        return rest.starts_with(':');
    }
    rest.starts_with(KIND_PARTITION_SEP)
}

/// Match a family selector against exact current key layouts.
#[must_use]
pub fn colon_prefix_matches(bare: &str, known_with_colon: &str) -> bool {
    let Some(kind) = known_with_colon.strip_suffix(':') else {
        return bare.starts_with(known_with_colon);
    };
    starts_with_kind(bare, kind)
}

/// Rest after the current kind separator. No alternate spelling is accepted.
#[must_use]
pub fn rest_of<'a>(key: &'a str, kind: &str) -> Option<&'a str> {
    let bare = strip_org(key).1;
    if !starts_with_kind(bare, kind) {
        return None;
    }
    Some(&bare[kind.len() + 1..])
}

fn match_kind(bare: &str) -> Option<&'static str> {
    KIND_PARTITION_KINDS
        .iter()
        .copied()
        .find(|kind| starts_with_kind(bare, kind))
}

/// Split a production org key `{sha256-hex}:{rest}` into `(storage_prefix, rest)`.
///
/// Returns `None` for a personal key or any shorter prefix. Works on bytes so a
/// non-ASCII char straddling byte 64 cannot panic the `&str` slice: once
/// `bytes[..64]` are all ASCII hex digits and `bytes[64]` is `:`, those offsets
/// are char boundaries.
pub fn split_org_storage_prefix(key: &str) -> Option<(&str, &str)> {
    let bytes = key.as_bytes();
    if bytes.len() > 65 && bytes[64] == b':' && bytes[..64].iter().all(u8::is_ascii_hexdigit) {
        Some((&key[..64], &key[65..]))
    } else {
        None
    }
}

fn strip_org(key: &str) -> (Option<&str>, &str) {
    if let Some((prefix, rest)) = split_org_storage_prefix(key) {
        return (Some(prefix), rest);
    }
    // Catalog instances and share receive can use compound scope prefixes.
    // Peel a scope only before an exact current flat/compound kind.
    if match_kind(key).is_some() {
        return (None, key);
    }
    let mut search = 0usize;
    while let Some(rel) = key[search..].find(':') {
        let colon = search + rel;
        let bare = &key[colon + 1..];
        if match_kind(bare).is_some() {
            return (Some(&key[..colon]), bare);
        }
        search = colon + 1;
    }
    (None, key)
}

/// Return the half-open byte range for one physical prefix.
///
/// The bytes name one prefix, field key, generation, or other exact identity.
#[must_use]
pub fn exact_prefix_bounds(prefix: &str) -> (String, String) {
    (prefix.to_string(), exclusive_prefix_end(prefix))
}

fn exclusive_prefix_end(prefix: &str) -> String {
    let mut bytes = prefix.as_bytes().to_vec();
    match bytes.last_mut() {
        Some(last) if *last < 0xff => *last += 1,
        Some(_) => bytes.push(0),
        None => {}
    }
    String::from_utf8(bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}
