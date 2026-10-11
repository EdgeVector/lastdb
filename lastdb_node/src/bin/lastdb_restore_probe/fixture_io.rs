use std::path::Path;

use fold_db::hex::sha256_hex;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("parse {}: {e}", path.display()))
}

pub(crate) fn rows_by_id(
    value: &Value,
    key: Option<&[u8; 32]>,
) -> Result<BTreeMap<String, Value>, String> {
    let results = value
        .get("results")
        .or_else(|| value.pointer("/data/results"))
        .and_then(Value::as_array)
        .ok_or_else(|| "query JSON missing results array".to_string())?;
    let mut out = BTreeMap::new();
    for result in results {
        let fields = result.get("fields").unwrap_or(result);
        let id = field_plain(fields, "id", key)?
            .ok_or_else(|| format!("query row missing string id: {fields}"))?;
        out.insert(id, fields.clone());
    }
    Ok(out)
}

pub(crate) fn field_str<'a>(row: &'a Value, field: &str) -> Option<&'a str> {
    row.get(field).and_then(Value::as_str)
}

pub(crate) fn load_content_key(path: Option<&Path>) -> Result<Option<[u8; 32]>, String> {
    let Some(path) = path else {
        return Ok(None);
    };
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    if bytes.len() != 32 {
        return Err(format!(
            "{} must be 32 bytes, got {}",
            path.display(),
            bytes.len()
        ));
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes);
    let e2e = fold_db::crypto::E2eKeys::from_ed25519_seed(&seed)
        .map_err(|e| format!("E2E derive from {}: {e}", path.display()))?;
    Ok(Some(e2e.encryption_key()))
}

pub(crate) fn open_field_text(raw: &str, key: Option<&[u8; 32]>) -> Result<String, String> {
    if !raw.starts_with("ENC:") {
        return Ok(raw.to_string());
    }
    let Some(key) = key else {
        return Err("ENC: field present but --identity-key was not provided".to_string());
    };
    let opened = fold_db::atom::open_content_value(key, Value::String(raw.to_string()))
        .map_err(|e| format!("open ENC: field: {e}"))?;
    match opened {
        Value::String(s) => Ok(s),
        other => Ok(other.to_string()),
    }
}

pub(crate) fn field_plain(
    row: &Value,
    field: &str,
    key: Option<&[u8; 32]>,
) -> Result<Option<String>, String> {
    let Some(raw) = field_str(row, field) else {
        return Ok(None);
    };
    Ok(Some(open_field_text(raw, key)?))
}

pub(crate) fn field_sha256(
    row: &Value,
    field: &str,
    key: Option<&[u8; 32]>,
) -> Result<String, String> {
    let value =
        field_plain(row, field, key)?.ok_or_else(|| format!("row missing string field {field}"))?;
    Ok(sha256_hex(value.as_bytes()))
}

pub(crate) fn u64_field(value: &Value, field: &str) -> u64 {
    value.get(field).and_then(Value::as_u64).unwrap_or(0)
}

pub(crate) fn mini_search_plane_honest(semantic: &Value) -> bool {
    if native_index_served_hit(semantic, "") {
        return false;
    }
    let message = semantic
        .get("message")
        .or_else(|| semantic.get("error"))
        .and_then(Value::as_str)
        .unwrap_or("");
    semantic
        .get("search_plane_required")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || semantic
            .get("retired")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        || message.contains("search_plane_required")
        || (!semantic.get("ok").and_then(Value::as_bool).unwrap_or(true)
            && message.to_ascii_lowercase().contains("lastseek"))
}

pub(crate) fn native_index_served_hit(semantic: &Value, phrase: &str) -> bool {
    semantic.get("ok").and_then(Value::as_bool).unwrap_or(false)
        && !semantic
            .get("degraded")
            .and_then(Value::as_bool)
            .unwrap_or(true)
        && semantic
            .get("results")
            .and_then(Value::as_array)
            .is_some_and(|results| {
                results.iter().any(|result| {
                    let match_type = result
                        .pointer("/metadata/match_type")
                        .and_then(Value::as_str);
                    let served = match_type == Some("semantic") || match_type == Some("index");
                    let value = result.get("value").and_then(Value::as_str).unwrap_or("");
                    served && value.contains(phrase)
                })
            })
}
