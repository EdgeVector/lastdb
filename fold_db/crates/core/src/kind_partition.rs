//! Kind-as-partition keys for remaining flat LastStore kinds.
//!
//! Decision `decision-2026-09-06-lastdb-sorted-groups-and-two-read-verbs`: a
//! flat key's kind is its partition. `tv:abc` becomes `tv\0abc` so LastStore
//! `partition_of` returns `tv\0` instead of hashing the whole id.
//!
//! Write the anchored form. Dual-read the colon form for one release. Compaction
//! (sibling card) rewrites old keys. Do not flip
//! `LASTDB_READS_REQUIRE_PARTITION` here.
//!
//! Already-partitioned encodings keep their existing `\0` (atom bodies under
//! `AtomKeyEncoding::PartitionPrefix`, sparse `mord:{M}\0…`, `aref:` edges
//! with a separator after the atom). This module only rewrites a kind whose
//! first separator is still `:`.

/// Kinds whose remaining flat form is `kind:rest` and whose write form is
/// `kind\0rest`. Longest token first so `gcatoms-probe-ref` wins a prefix race.
pub const KIND_PARTITION_KINDS: &[&str] = &[
    "gcatoms-probe-ref",
    "schemaidx",
    "conflict",
    "history",
    "dellog",
    "aloc",
    "mord",
    "moc",
    "atom",
    "aref",
    "tv",
];

/// The partition separator LastStore hashes on.
pub const KIND_PARTITION_SEP: char = '\0';

/// `kind\0rest` — the write form for a listed flat kind.
#[must_use]
pub fn anchored(kind: &str, rest: &str) -> String {
    format!("{kind}{KIND_PARTITION_SEP}{rest}")
}

/// `kind:rest` — the pre-anchor form kept for dual-read.
#[must_use]
pub fn flat(kind: &str, rest: &str) -> String {
    format!("{kind}:{rest}")
}

/// True when `bare` starts with `kind` followed by `:` or `\0`.
#[must_use]
pub fn starts_with_kind(bare: &str, kind: &str) -> bool {
    if !bare.starts_with(kind) {
        return false;
    }
    matches!(bare.as_bytes().get(kind.len()), Some(&b':' | &0))
}

/// Match a catalog prefix that still uses a trailing colon (`"tv:"`, `"atom:"`)
/// against either the colon form or the kind-as-partition form.
///
/// `mk:` is not a kind-partition kind, so `mk\0` does not match `"mk:"`.
#[must_use]
pub fn colon_prefix_matches(bare: &str, known_with_colon: &str) -> bool {
    if bare.starts_with(known_with_colon) {
        return true;
    }
    let Some(kind) = known_with_colon.strip_suffix(':') else {
        return false;
    };
    if !KIND_PARTITION_KINDS.contains(&kind) {
        return false;
    }
    starts_with_kind(bare, kind)
}

/// Rest after `kind:` or `kind\0`. `None` when `key` is not that kind.
#[must_use]
pub fn rest_of<'a>(key: &'a str, kind: &str) -> Option<&'a str> {
    let bare = strip_org(key).1;
    if !starts_with_kind(bare, kind) {
        return None;
    }
    Some(&bare[kind.len() + 1..])
}

/// The other encoding of a kind-as-partition key, when one exists.
///
/// - `tv\0id` ↔ `tv:id`
/// - `history\0MOL:1` ↔ `history:MOL:1`
/// - `atom:mk:M:h\0uuid` → `None` (already a slot partition, not a kind partition)
/// - `mord:M\0nanos` → `None` (sparse order log)
#[must_use]
pub fn form_twin(key: &str) -> Option<String> {
    let (org, bare) = strip_org(key);
    let twin = form_twin_bare(bare)?;
    Some(join_org(org, &twin))
}

/// Rewrite `key` so its kind separator matches `like` (a key or prefix).
///
/// Scan results then start with the prefix the caller asked for.
#[must_use]
pub fn rewrite_key_like(key: &str, like: &str) -> String {
    let Some(want) = kind_sep(like) else {
        return key.to_string();
    };
    let Some(have) = kind_sep(key) else {
        return key.to_string();
    };
    if want == have {
        return key.to_string();
    }
    form_twin(key).unwrap_or_else(|| key.to_string())
}

/// Logical rest used to dedupe a dual-read walk (`kind` + rest, org included).
#[must_use]
pub fn logical_row_id(key: &str) -> String {
    let (org, bare) = strip_org(key);
    match match_kind(bare) {
        Some(kind) => {
            let rest = &bare[kind.len() + 1..];
            join_org(org, &format!("{kind}\x1f{rest}"))
        }
        None => key.to_string(),
    }
}

fn form_twin_bare(bare: &str) -> Option<String> {
    let kind = match_kind(bare)?;
    let sep = *bare.as_bytes().get(kind.len())?;
    let rest = &bare[kind.len() + 1..];
    if sep == b':' && rest.contains(KIND_PARTITION_SEP) {
        return None;
    }
    let other = if sep == 0 { ':' } else { KIND_PARTITION_SEP };
    Some(format!("{kind}{other}{rest}"))
}

fn match_kind(bare: &str) -> Option<&'static str> {
    KIND_PARTITION_KINDS
        .iter()
        .copied()
        .find(|kind| starts_with_kind(bare, kind))
}

fn kind_sep(key: &str) -> Option<u8> {
    let bare = strip_org(key).1;
    let kind = match_kind(bare)?;
    bare.as_bytes().get(kind.len()).copied()
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
    // Production org ids are 64 hex. Tests and share receive use short
    // prefixes (`my_org`, `from:{sender}`). Peel `{prefix}:` only when the
    // remainder is a listed kind, so `tv:v1` stays a kind key.
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

/// Inclusive start and exclusive end covering `kind\0` and leftover `kind:` rows.
///
/// `atom:` sorts after `atom\0`, so a colon-only range misses every new write.
/// `mk:` is not a kind-as-partition kind; the range stays the colon prefix.
#[must_use]
pub fn colon_plane_bounds(colon_prefix: &str) -> (String, String) {
    let start = match form_twin(colon_prefix) {
        Some(twin) if twin.as_bytes().contains(&0) => twin,
        _ => colon_prefix.to_string(),
    };
    let colon_form = if colon_prefix.as_bytes().contains(&0) {
        form_twin(colon_prefix).unwrap_or_else(|| colon_prefix.to_string())
    } else {
        colon_prefix.to_string()
    };
    (start, exclusive_prefix_end(&colon_form))
}

/// Return the half-open byte range for one physical prefix.
///
/// This range does not join a kind-partition key with its legacy twin. Use it
/// when a caller must stay inside one molecule, generation, or other identity.
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

/// Point-read order: anchored write form first, then the colon tail.
#[must_use]
pub fn read_forms(key: &str) -> Vec<String> {
    let Some(twin) = form_twin(key) else {
        return vec![key.to_string()];
    };
    if kind_sep(key) == Some(0) {
        vec![key.to_string(), twin]
    } else {
        vec![twin, key.to_string()]
    }
}

/// Union a prefix walk with its kind-form twin. Anchored rows win.
#[must_use]
pub fn merge_scan_rows(
    requested_prefix: &str,
    primary: Vec<(Vec<u8>, Vec<u8>)>,
    twin: Vec<(Vec<u8>, Vec<u8>)>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::with_capacity(primary.len() + twin.len());
    for (key, value) in primary {
        if let Ok(text) = std::str::from_utf8(&key) {
            seen.insert(logical_row_id(text));
        }
        out.push((key, value));
    }
    for (key, value) in twin {
        let Ok(text) = std::str::from_utf8(&key) else {
            continue;
        };
        if !seen.insert(logical_row_id(text)) {
            continue;
        }
        out.push((rewrite_key_like(text, requested_prefix).into_bytes(), value));
    }
    out
}

fn join_org(org: Option<&str>, bare: &str) -> String {
    match org {
        Some(org) => format!("{org}:{bare}"),
        None => bare.to_string(),
    }
}
