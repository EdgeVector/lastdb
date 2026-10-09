//! Atom GC home scanners: physical kind-plane walks and atom/locator collectors. Moved verbatim from `atom_gc.rs`.

use super::*;

pub(crate) fn exclusive_prefix_end(prefix: &str) -> String {
    let mut end = prefix.to_string();
    if let Some(last) = end.pop() {
        let next = char::from_u32(last as u32 + 1).unwrap_or('\u{e000}');
        end.push(next);
        end
    } else {
        "\0".to_string()
    }
}

pub(crate) fn kind_plane_bounds(colon_prefix: &str) -> (String, String) {
    let start = match colon_prefix.strip_suffix(':') {
        Some(kind) if fold_db::kind_partition::KIND_PARTITION_KINDS.contains(&kind) => {
            fold_db::kind_partition::anchored(kind, "")
        }
        _ => colon_prefix.to_string(),
    };
    (start, exclusive_prefix_end(colon_prefix))
}

/// Walk `kind\0` and leftover `kind:` rows via physical handles.
///
/// A HashGroup `scan_prefix("atom:")` has no `\0`, so it visits every group
/// (~200s on the Mini gate). Physical pages only resolve occupied handles.
pub(crate) async fn scan_kind_plane(
    kv: &dyn KvStore,
    colon_prefix: &str,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
    let (start, end) = kind_plane_bounds(colon_prefix);
    let mut cursor: Option<PhysicalScanCursor> = None;
    let mut rows = Vec::new();
    loop {
        let page = kv
            .scan_range_physical_paged(start.as_bytes(), end.as_bytes(), cursor.as_ref(), 256, 8)
            .await
            .map_err(|e| e.to_string())?;
        if page.next_cursor.is_some() && page.next_cursor == cursor {
            return Err("physical cursor did not advance".into());
        }
        rows.extend(page.rows);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(rows)
}

pub(crate) async fn collect_atom_refs_from_prefix(
    store: &dyn NamespacedStore,
    namespaces: &BTreeSet<String>,
    namespaces_scanned: &mut BTreeSet<String>,
    namespace: &str,
    prefix: &str,
    refs: &mut HashSet<String>,
) -> Result<(), String> {
    if !namespaces.contains(namespace) {
        return Ok(());
    }
    let kv = store
        .open_namespace(namespace)
        .await
        .map_err(|e| format!("open namespace {namespace}: {e}"))?;
    let rows = scan_kind_plane(kv.as_ref(), prefix)
        .await
        .map_err(|e| format!("scan {namespace}/{prefix}: {e}"))?;
    if !rows.is_empty() {
        namespaces_scanned.insert(namespace.to_string());
    }
    for (_, value) in rows {
        collect_atom_uuid_strings(&value, refs);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn collect_atom_body_rows(
    store: &dyn NamespacedStore,
    namespaces: &BTreeSet<String>,
    namespaces_scanned: &mut BTreeSet<String>,
    namespace: &str,
    prefix: Option<&str>,
    copies_by_uuid: &mut BTreeMap<String, Vec<AtomCopy>>,
    flat_atom_body_keys: &mut u64,
    prefixed_atom_body_keys: &mut u64,
    opaque_atom_body_keys: &mut u64,
) -> Result<(), String> {
    if !namespaces.contains(namespace) {
        return Ok(());
    }
    let kv = store
        .open_namespace(namespace)
        .await
        .map_err(|e| format!("open namespace {namespace}: {e}"))?;
    let rows = match prefix {
        Some(prefix) => scan_kind_plane(kv.as_ref(), prefix)
            .await
            .map_err(|e| format!("scan {namespace}/{prefix}: {e}"))?,
        None => kv
            .scan_prefix(b"")
            .await
            .map_err(|e| format!("scan {namespace}: {e}"))?,
    };
    if !rows.is_empty() {
        namespaces_scanned.insert(namespace.to_string());
    }
    for (key_bytes, value) in rows {
        let key = String::from_utf8_lossy(&key_bytes).into_owned();
        let suffix = fold_db::kind_partition::rest_of(&key, "atom")
            .or_else(|| key.strip_prefix("atom:"))
            .unwrap_or(key.as_str());
        let uuid = atom_uuid_of_suffix(suffix).to_string();
        if suffix.contains('\0') {
            *prefixed_atom_body_keys += 1;
        } else {
            *flat_atom_body_keys += 1;
        }
        let content_sha256 = content_hash_of(&value);
        if content_sha256.is_none() {
            *opaque_atom_body_keys += 1;
        }
        let shape = AtomKeyShape::of_base_key(&key);
        copies_by_uuid.entry(uuid).or_default().push(AtomCopy {
            namespace: namespace.to_string(),
            key: format!("{namespace}:{key}"),
            stored_key: key.clone(),
            base_key: key,
            shape,
            content_sha256,
        });
    }
    Ok(())
}

/// SHA-256 of a body's canonical decoded form, or `None` when the value is not
/// readable content.
///
/// Hashes the **re-serialized parse**, not the stored bytes: an unhashed byte
/// comparison would trip over key ordering or whitespace drift between a body
/// the writer serialized and one the rekey round-tripped through
/// `serde_json::Value`. A value that does not parse is opaque — sealed at rest,
/// or not a body at all — and gets `None` so the rules refuse the group instead
/// of comparing ciphertext nonces.
pub(crate) fn content_hash_of(value: &[u8]) -> Option<String> {
    let parsed = serde_json::from_slice::<Value>(value).ok()?;
    Some(sha256_hex(&serde_json::to_vec(&parsed).ok()?))
}

/// `uuid → partition` from the home's locator rows.
pub(crate) async fn collect_atom_locators(
    store: &dyn NamespacedStore,
    namespaces: &BTreeSet<String>,
    namespaces_scanned: &mut BTreeSet<String>,
) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    for namespace in ["atom_locators", "main"] {
        if !namespaces.contains(namespace) {
            continue;
        }
        let kv = store
            .open_namespace(namespace)
            .await
            .map_err(|e| format!("open namespace {namespace}: {e}"))?;
        // Write form is `aloc\0{uuid}`; dual-read still serves `aloc:{uuid}`.
        // `scan_prefix("aloc:")` misses the NUL form (and walks every HashGroup
        // because the prefix has no `\0`). Walk both planes physically.
        let rows = scan_kind_plane(kv.as_ref(), "aloc:")
            .await
            .map_err(|e| format!("scan {namespace}/aloc: {e}"))?;
        if !rows.is_empty() {
            namespaces_scanned.insert(namespace.to_string());
        }
        for (key_bytes, value) in rows {
            let key = String::from_utf8_lossy(&key_bytes).into_owned();
            let Some(uuid) = fold_db::kind_partition::rest_of(&key, "aloc")
                .or_else(|| key.strip_prefix("aloc:"))
            else {
                continue;
            };
            // The value *is* the partition prefix, stored as a JSON string.
            let Ok(Value::String(partition)) = serde_json::from_slice::<Value>(&value) else {
                continue;
            };
            out.insert(uuid.to_string(), partition);
        }
    }
    Ok(out)
}

/// The atom key encoding this home's durable marker names.
///
/// Read from the home, never from `LASTDB_ATOM_KEY_ENCODING`: an env var set in
/// the operator's shell must not decide what a destructive pass deletes.
///
/// Deliberately *not* gated on `list_namespaces` containing `main`. `main` is a
/// logical collection — the store routes its keys to physical collections by
/// prefix — so it does not appear in the namespace listing, and gating on it
/// would report every home as unmarked. That failure is not neutral: unmarked
/// means "treat as flat", which inverts which copy the rules call canonical.
pub(crate) async fn read_encoding_marker(
    store: &dyn NamespacedStore,
) -> Result<Option<HomeAtomKeyEncoding>, String> {
    let Ok(kv) = store.open_namespace("main").await else {
        return Ok(None);
    };
    let raw = kv
        .get(b"amigr:atom_key_encoding_v1")
        .await
        .map_err(|e| format!("read atom key encoding marker: {e}"))?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let Ok(parsed) = serde_json::from_slice::<Value>(&raw) else {
        return Ok(None);
    };
    let encoding = parsed.get("encoding").and_then(Value::as_str);
    Ok(match encoding {
        Some(s) if s.eq_ignore_ascii_case("partition_prefix") => {
            Some(HomeAtomKeyEncoding::PartitionPrefix)
        }
        Some(s) if s.eq_ignore_ascii_case("flat") => Some(HomeAtomKeyEncoding::Flat),
        // A marker spelling this build does not know is not a decision. Treated
        // as unmarked, which blocks execution against a prefixed home.
        _ => None,
    })
}

pub(crate) async fn collect_schema_names(
    store: &dyn NamespacedStore,
    namespaces: &BTreeSet<String>,
    namespaces_scanned: &mut BTreeSet<String>,
) -> Result<BTreeSet<String>, String> {
    let mut out = BTreeSet::new();
    if !namespaces.contains("schemas") {
        return Ok(out);
    }
    let kv = store
        .open_namespace("schemas")
        .await
        .map_err(|e| format!("open namespace schemas: {e}"))?;
    let rows = kv
        .scan_prefix(b"")
        .await
        .map_err(|e| format!("scan schemas: {e}"))?;
    if !rows.is_empty() {
        namespaces_scanned.insert("schemas".into());
    }
    for (key, value) in rows {
        out.insert(String::from_utf8_lossy(&key).into_owned());
        if let Ok(val) = serde_json::from_slice::<Value>(&value) {
            if let Some(name) = val.get("name").and_then(Value::as_str) {
                out.insert(name.to_string());
            }
            if let Some(name) = val.get("descriptive_name").and_then(Value::as_str) {
                out.insert(name.to_string());
            }
        }
    }
    Ok(out)
}

pub(crate) fn atom_uuid_of_suffix(suffix: &str) -> &str {
    match suffix.rsplit_once('\0') {
        Some((_, uuid)) => uuid,
        None => suffix,
    }
}

pub(crate) fn collect_atom_uuid_strings(bytes: &[u8], sink: &mut HashSet<String>) {
    let Ok(val) = serde_json::from_slice::<Value>(bytes) else {
        return;
    };
    fn walk(v: &Value, sink: &mut HashSet<String>) {
        match v {
            Value::Object(map) => {
                for key in [
                    "atom_uuid",
                    "new_atom_uuid",
                    "old_atom_uuid",
                    "conflict_loser_atom",
                ] {
                    if let Some(Value::String(uuid)) = map.get(key) {
                        if !uuid.is_empty() {
                            sink.insert(uuid.clone());
                        }
                    }
                }
                for child in map.values() {
                    walk(child, sink);
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, sink);
                }
            }
            _ => {}
        }
    }
    walk(&val, sink);
}
