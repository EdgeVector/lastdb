//! Client timeout defaults and atomic product-file writes.

use super::*;

pub(crate) fn truncate_label(label: &str, max: usize) -> String {
    if label.chars().count() <= max {
        return label.to_string();
    }
    let mut out: String = label.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// Client deadline for `db inventory`: explicit `--timeout SECS` wins; else the
/// shared admin-scan budget (server `LASTDB_UDS_ADMIN_TIMEOUT_SECS` / 600s).
pub(crate) fn inventory_client_timeout(timeout_secs: Option<u64>) -> Duration {
    match timeout_secs {
        Some(secs) if secs > 0 => Duration::from_secs(secs),
        _ => admin_scan_client_timeout(),
    }
}

/// Default client deadline for the cheap `lastdb status` health read.
///
/// Shares [`lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS`] — one named 30s
/// CLI UDS default. `/api/status` is *usually* milliseconds, but its
/// `status_sync` phase costs tens of seconds on a cold node (measured
/// 2026-08-08: 50.4s on a primary four minutes past restart, and a
/// `status_sync_us` phase that was ~100% of a 73s call). A health check should
/// stay snappy without lying about a node that is merely slow, so this is
/// generous compared to a warm read and still bounded.
pub(crate) const DEFAULT_STATUS_CLIENT_TIMEOUT_SECS: u64 =
    lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS;

/// Default client deadline for the `lastdb status` daemon probe: the 30s
/// health default, unless the operator exported a usable
/// `LASTDB_UDS_ADMIN_TIMEOUT_SECS`.
///
/// The deadline error this probe prints ([`format_socket_io_error`]) prescribes
/// exactly that variable as the always-applicable remediation, so the status
/// client deadline has to honor it — otherwise the operator raises the budget
/// as told and still times out at 30s. Unset (or unusable) keeps the snappy
/// health default; a health check must not silently inherit the 600s
/// admin-scan budget nobody asked for.
pub(crate) fn status_default_client_timeout() -> Duration {
    lastdb_node::exec::admin_handler_timeout_override()
        .unwrap_or(Duration::from_secs(DEFAULT_STATUS_CLIENT_TIMEOUT_SECS))
}

/// Client deadline for the `/api/status` reads behind `lastdb status` and
/// `lastdb ops`: explicit `--timeout SECS` wins, else `default`.
///
/// Both commands used to share one hardcoded 3s deadline with no way to raise
/// it. That made `lastdb ops` — the command both CLAUDE.md files tell operators
/// and agents to run *before* escalating, i.e. against a node already slow
/// enough to warrant telemetry — fail exactly when it was needed, and the
/// failure text pointed at a `--timeout` flag this command did not accept.
/// `ops` now defaults to the shared admin-scan budget for that reason: it is a
/// diagnostic, and giving up before the daemon answers is the whole defect.
pub(crate) fn status_client_timeout(timeout_secs: Option<u64>, default: Duration) -> Duration {
    match timeout_secs {
        Some(secs) if secs > 0 => Duration::from_secs(secs),
        _ => default,
    }
}

/// Write `bytes` to `path` via a sibling temp file + rename so readers never
/// see a truncated / empty product file from a mid-write failure.
pub(crate) fn write_product_file_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create out dir {}: {e}", parent.display()))?;
        }
    }
    let tmp = {
        let mut name = path.as_os_str().to_owned();
        name.push(format!(".tmp.{}", std::process::id()));
        PathBuf::from(name)
    };
    match std::fs::write(&tmp, bytes) {
        Ok(()) => match std::fs::rename(&tmp, path) {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                Err(format!("rename inventory out to {}: {e}", path.display()))
            }
        },
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(format!("write inventory temp {}: {e}", tmp.display()))
        }
    }
}
