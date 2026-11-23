//! The snapshot lines of `lastdb status`, from the `cloud_sync.snapshot` object of `/api/status`.

use serde_json::Value;

/// The lines that name the newest snapshot of the personal database. A node that syncs through a
/// relay address in its environment has no saved cloud connection, so the line does not wait for one.
pub(crate) fn snapshot_lines(sync: &Value) -> Vec<String> {
    let Some(snapshot) = sync.get("snapshot").filter(|snapshot| snapshot.is_object()) else {
        // A daemon of an older build reports no snapshot object. Say nothing rather than "none yet".
        return Vec::new();
    };
    let state = snapshot["state"].as_str().unwrap_or("none");
    let mut lines = vec![match facts(snapshot) {
        Some(facts) => format!("Snapshot: {facts}"),
        None if state == "in_progress" => {
            "Snapshot: none yet, the first snapshot of this home is in progress".to_owned()
        }
        None => "Snapshot: none yet".to_owned(),
    }];
    if state == "in_progress" {
        if let Some(message) = snapshot["message"].as_str() {
            lines.push(format!("Snapshot uploading: {message}"));
        }
    }
    if state == "blocked" {
        let message = snapshot["message"]
            .as_str()
            .unwrap_or("the relay refused it");
        lines.push(format!("Snapshot blocked: {message}"));
    }
    lines
}

/// When it was taken, how many files and bytes, where it is, and how far it covers this device.
fn facts(snapshot: &Value) -> Option<String> {
    let taken_at_ms = i64::try_from(snapshot["taken_at_ms"].as_u64()?).ok()?;
    let when = chrono::DateTime::from_timestamp_millis(taken_at_ms)?
        .with_timezone(&chrono::Local)
        .format("%Y-%m-%d %H:%M:%S %z");
    Some(format!(
        "{when}, {} files, {}, on {}, covers this device through entry {}",
        snapshot["files"].as_u64()?,
        size(snapshot["bytes"].as_u64()?),
        snapshot["relay"].as_str()?,
        snapshot["through_seq"].as_u64()?,
    ))
}

/// A length with one decimal in the largest binary unit that fits.
pub(crate) fn size(bytes: u64) -> String {
    for (name, unit) in [("GiB", 1u64 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)] {
        if bytes >= unit {
            let tenths = u128::from(bytes) * 10 / u128::from(unit);
            return format!("{}.{} {name}", tenths / 10, tenths % 10);
        }
    }
    format!("{bytes} B")
}
