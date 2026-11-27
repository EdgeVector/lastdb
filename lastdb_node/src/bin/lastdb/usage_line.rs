//! The cloud usage lines of `lastdb status`, from `cloud_sync.databases.<id>.usage` of `/api/status`,
//! and the relay contract line, from `cloud_sync.relay`.

use serde_json::Value;

use super::snapshot_line::size;

/// One line for each database this device put something in. The numbers are what this device put and
/// the relay accepted, less what the trims and the sweeps it asked for released. They are not what the
/// account is billed: the entries and files of another device of the owner are not in them, and a
/// sweep also releases files that another device put.
fn push_usage_lines(sync: &Value, out: &mut Vec<String>) {
    let Some(databases) = sync.get("databases").and_then(Value::as_object) else {
        return;
    };
    out.extend(databases.iter().filter_map(|(id, database)| {
        let usage = database.get("usage").filter(|usage| usage.is_object())?;
        let field = |name: &str| usage[name].as_u64().unwrap_or(0);
        let held = field("entry_bytes")
            .saturating_add(field("blob_bytes"))
            .saturating_sub(field("released_bytes").saturating_add(field("released_blob_bytes")));
        let unconfirmed = field("unconfirmed_blobs");
        let cut_off = if unconfirmed == 0 {
            String::new()
        } else {
            let unconfirmed_size = size(field("unconfirmed_blob_bytes"));
            format!("; {unconfirmed} files ({unconfirmed_size}) were cut off before their upload ended and are not counted")
        };
        let label = id.get(..8).unwrap_or(id);
        let put = format!(
            "put {} entries ({}) and {} files ({}); trims released {} entries ({}); sweeps released {} files ({})",
            field("entries"),
            size(field("entry_bytes")),
            field("blobs"),
            size(field("blob_bytes")),
            field("released_entries"),
            size(field("released_bytes")),
            field("released_blobs"),
            size(field("released_blob_bytes")),
        );
        if usage["complete"].as_bool() == Some(true) {
            let held_size = size(held);
            return Some(format!(
                "Cloud usage {label}: this device {put}; {held_size} on the relay from this device (other devices are not counted){cut_off}"
            ));
        }
        let since = i64::try_from(field("since_ms"))
            .ok()
            .and_then(chrono::DateTime::from_timestamp_millis)
            .map_or_else(
                || "an unknown time".to_owned(),
                |when| when.format("%Y-%m-%d %H:%MZ").to_string(),
            );
        Some(format!(
            "Cloud usage {label} since {since} (this device uploaded before the account began; earlier uploads are not counted): {put}{cut_off}"
        ))
    }));
}

/// One line per database whose durable upload queue is blocked behind a failed or refused entry.
fn blocked_upload_lines(sync: &Value) -> Vec<String> {
    let Some(databases) = sync.get("databases").and_then(Value::as_object) else {
        return Vec::new();
    };
    databases
        .iter()
        .filter_map(|(id, database)| {
            let blocked = database.pointer("/upload/oldest_blocked")?.as_object()?;
            let seq = blocked.get("writer_seq")?.as_u64()?;
            let reason = blocked.get("reason")?.as_str()?;
            let label = id.get(..8).unwrap_or(id);
            Some(format!(
                "Cloud upload blocked ({label}, entry {seq}): {reason}"
            ))
        })
        .collect()
}

/// Blocked upload, usage, and relay contract lines of `lastdb status`.
pub(crate) fn status_tail_lines(sync: &Value) -> Vec<String> {
    let mut lines = blocked_upload_lines(sync);
    push_usage_lines(sync, &mut lines);
    lines.extend(relay_contract_line(sync));
    lines
}

/// `Relay contract: 1.2, serves contract_probe` from `cloud_sync.relay`, once the relay answered.
/// Features the relay does not serve follow as `does not serve ...`.
fn relay_contract_line(sync: &Value) -> Option<String> {
    let relay = sync.get("relay")?;
    let revision = relay.get("revision")?.as_u64()?;
    let mut line = format!("Relay contract: {}.{revision}", relay_protocol::PROTOCOL);
    let features = relay
        .get("features")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let named = |supported: bool| {
        features
            .iter()
            .filter(|feature| feature.get("supported").and_then(Value::as_bool) == Some(supported))
            .filter_map(|feature| feature.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(", ")
    };
    for (supported, verb) in [(true, "serves"), (false, "does not serve")] {
        let names = named(supported);
        if !names.is_empty() {
            line.push_str(&format!(", {verb} {names}"));
        }
    }
    Some(line)
}
