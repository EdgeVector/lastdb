//! The text of the `409` that refuses an install while the molecule drain still
//! removes the keys of an earlier schema of the same name
//! (`SchemaError::SchemaBeingDropped`).

/// The message for `reason`: `draining`, `waiting_for_conflict_records` or
/// `drain_task_off`. `molecules` is the number of molecules the install would bind
/// that the drain still works on.
pub(crate) fn message(schema: &str, reason: &str, molecules: u64) -> String {
    let wait = match reason {
        "drain_task_off" => format!(
            "the molecule drain task is off (LASTDB_MOLECULE_DRAIN_MS=0), so nothing erases \
             the {molecules} molecule(s) that the drop left. Set LASTDB_MOLECULE_DRAIN_MS \
             above 0 and restart the node, then retry"
        ),
        "waiting_for_conflict_records" => format!(
            "{molecules} molecule(s) wait for kept conflict records to be released; retry in \
             a few minutes"
        ),
        _ => format!(
            "the molecule drain is erasing {molecules} molecule(s); retry after it has \
             finished (`lastdb status` shows its progress)"
        ),
    };
    format!("schema '{schema}' is still being dropped; {wait}")
}
