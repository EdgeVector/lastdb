//! Exact current storage-key layouts and administrative family bounds.
//!
//! A flat kind uses `kind\0id`.

/// The separator between the kind and the rest of a flat key.
pub const KIND_PARTITION_SEP: char = '\0';

/// `kind\0rest` — the write form of a flat kind (`dellog`).
#[must_use]
pub fn anchored(kind: &str, rest: &str) -> String {
    format!("{kind}{KIND_PARTITION_SEP}{rest}")
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
