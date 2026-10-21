use super::*;

/// `lastdb status` line for the dependency parts of the native generation receipt.
///
/// Every publication writes the receipt in full, and a receipt holds at most
/// `dependency_parts_limit` parts. The line shows the count, the limit, the count
/// at which maintenance starts, the peak, and the last maintenance run. Always
/// emits one line, so a daemon that predates the field reads differently from one
/// with a small count.
pub fn storage_parts_status_line(snapshot: &StatusSnapshot) -> String {
    let Some(parts) = snapshot.storage_parts.as_ref() else {
        return "Storage parts: unsupported by daemon (no part count)".to_string();
    };
    let percent = parts.dependency_parts.saturating_mul(100) / parts.dependency_parts_limit.max(1);
    let maintenance = if !parts.auto_maintenance {
        "automatic maintenance off".to_string()
    } else if parts.maintenance_running {
        "maintenance running".to_string()
    } else {
        format!("maintenance starts at {}", parts.dependency_trigger)
    };
    let last = if parts.maintenance_runs == 0 {
        "no run yet".to_string()
    } else {
        format!(
            "last run {} -> {} parts in {} ms",
            parts.maintenance_last_parts_before,
            parts.maintenance_last_parts_after,
            parts.maintenance_last_millis
        )
    };
    let mut line = format!(
        "Storage parts: {} / {} ({percent}%); {maintenance}; peak {}; runs {} failures {}; {last}",
        parts.dependency_parts,
        parts.dependency_parts_limit,
        parts.dependency_parts_peak,
        parts.maintenance_runs,
        parts.maintenance_failures,
    );
    if let Some(error) = parts.maintenance_last_error.as_deref() {
        line.push_str(&format!("; last error: {error}"));
    }
    line
}
