//! The cloud usage lines of `lastdb status`, from `cloud_sync.databases.<id>.usage` of `/api/status`.

use serde_json::Value;

use super::snapshot_line::size;

/// One line for each database this device put something in. The numbers are what this device put and
/// the relay accepted, less what the trims it asked for released. They are not what the account is
/// billed: the entries and files of another device of the owner are not in them.
pub(crate) fn usage_lines(sync: &Value) -> Vec<String> {
    let Some(databases) = sync.get("databases").and_then(Value::as_object) else {
        return Vec::new();
    };
    databases
        .iter()
        .filter_map(|(id, database)| {
            let usage = database.get("usage").filter(|usage| usage.is_object())?;
            let field = |name: &str| usage[name].as_u64().unwrap_or(0);
            let held = (field("entry_bytes") + field("blob_bytes")).saturating_sub(field("released_bytes"));
            let unconfirmed = field("unconfirmed_blobs");
            let cut_off = if unconfirmed == 0 {
                String::new()
            } else {
                format!(
                    "; {unconfirmed} files ({}) were cut off before their upload ended and are not counted",
                    size(field("unconfirmed_blob_bytes"))
                )
            };
            Some(format!(
                "Cloud usage {}: this device put {} entries ({}) and {} files ({}); trims released {} entries ({}); \
                 {} on the relay from this device (other devices are not counted){cut_off}",
                id.get(..8).unwrap_or(id),
                field("entries"),
                size(field("entry_bytes")),
                field("blobs"),
                size(field("blob_bytes")),
                field("released_entries"),
                size(field("released_bytes")),
                size(held),
            ))
        })
        .collect()
}
