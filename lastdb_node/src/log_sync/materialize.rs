//! Apply VerifiedHistory LWW winners into local stores.
//!
//! Phase 1 records the materialized view under log-sync for inspection; folding
//! into FoldDB record APIs is left open (DESIGN).

use std::path::Path;

use mutation_log::VerifiedHistory;

use crate::profile::private_fs as disk;

pub(super) fn apply_history_records(
    home: &Path,
    database_id: &str,
    history: &VerifiedHistory,
) -> Result<(), String> {
    // DESIGN: map (namespace, record_key) → FoldDB schema/key write. Namespace
    // may be a schema id or app label; cloud-api.md does not pin the mapping.
    // Persist a JSON snapshot of winners for daemon observability until that
    // mapping is decided.
    let path = home
        .join("log-sync")
        .join(database_id)
        .join("materialized.json");
    let mut rows = Vec::new();
    for ((namespace, key), record) in history.records() {
        rows.push(serde_json::json!({
            "namespace": namespace,
            "record_key": key,
            "tombstone": record.value.is_empty(),
            "lamport": record.position.lamport,
            "node_key": record.position.node_key.to_hex(),
            "device_id": record.position.device_id.to_string(),
            "value_len": record.value.len(),
        }));
    }
    let bytes = serde_json::to_vec(&rows).map_err(|_| "cannot encode materialized")?;
    disk::write_private(&path, &bytes).map_err(|_| "cannot write materialized".into())
}
