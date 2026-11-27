//! Changes to an existing job file: the backup, the rewrite for `service adopt`, and the
//! restore. A rewrite that fails part way puts the backup back, so the job never names a
//! program that a rollback removes.

use std::path::Path;

use super::service::{ctl, definition_path, is_mac, plist_buddy};

/// Save the job file to `backup` before [`rewrite_definition`] changes it.
pub(super) fn backup_definition(label: &str, backup: &Path) -> Result<(), String> {
    let path = definition_path(label)?;
    let old = std::fs::read(&path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    super::state::atomic_write(backup, &old, 0o600)
}

/// Rewrite an existing job to run `<home>/current/lastdbd`. Everything else in the job
/// stays. [`backup_definition`] saved the old file first; on any failure the job file is
/// put back from it, so a half-done rewrite never stays.
pub(super) fn rewrite_definition(
    label: &str,
    home: &Path,
    stop_secs: u64,
    backup: &Path,
) -> Result<(), String> {
    let Err(e) = edit_definition(label, home, stop_secs) else {
        return Ok(());
    };
    match restore_definition(label, backup) {
        Ok(()) => Err(format!("{e}; the job file was put back")),
        Err(r) => Err(format!("{e}; putting the job file back failed too: {r}")),
    }
}

fn edit_definition(label: &str, home: &Path, stop_secs: u64) -> Result<(), String> {
    let path = definition_path(label)?;
    let program = format!("{}/current/lastdbd", home.display());
    if is_mac() {
        let set = plist_buddy(&path, &format!("Set :ProgramArguments:0 {program}"))?;
        if !set.ok {
            return Err(format!(
                "cannot rewrite {}: {}",
                path.display(),
                set.stderr.trim()
            ));
        }
        let timeout = stop_secs + 60;
        let have = plist_buddy(&path, "Print :ExitTimeOut")?;
        let command = if have.ok {
            format!("Set :ExitTimeOut {timeout}")
        } else {
            format!("Add :ExitTimeOut integer {timeout}")
        };
        let write = plist_buddy(&path, &command)?;
        if !write.ok {
            return Err(format!(
                "cannot rewrite {}: {}",
                path.display(),
                write.stderr.trim()
            ));
        }
        return Ok(());
    }
    let old = std::fs::read(&path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let text = String::from_utf8(old).map_err(|_| "the unit is not UTF-8".to_string())?;
    let mut done = false;
    let mut lines: Vec<String> = text
        .lines()
        .map(|line| match line.trim().strip_prefix("ExecStart=") {
            Some(rest) if !done => {
                done = true;
                let tail = rest.split_once(char::is_whitespace).map_or("", |(_, t)| t);
                format!("ExecStart={program} {tail}").trim_end().to_owned()
            }
            _ => line.to_owned(),
        })
        .collect();
    if !done {
        return Err(format!("{} has no ExecStart to rewrite", path.display()));
    }
    lines.retain(|l| !l.trim().starts_with("TimeoutStopSec="));
    if let Some(at) = lines.iter().position(|l| l.trim() == "[Service]") {
        lines.insert(at + 1, format!("TimeoutStopSec={}", stop_secs + 60));
    }
    let mut out = lines.join("\n");
    out.push('\n');
    super::state::atomic_write(&path, out.as_bytes(), 0o644)?;
    let reload = ctl("systemctl", &["--user", "daemon-reload"])?;
    if reload.ok {
        Ok(())
    } else {
        Err(format!(
            "systemctl daemon-reload failed: {}",
            reload.stderr.trim()
        ))
    }
}

/// Put a saved job file back.
pub(super) fn restore_definition(label: &str, backup: &Path) -> Result<(), String> {
    let path = definition_path(label)?;
    let old =
        std::fs::read(backup).map_err(|e| format!("cannot read {}: {e}", backup.display()))?;
    super::state::atomic_write(&path, &old, 0o644)?;
    if !is_mac() {
        let reload = ctl("systemctl", &["--user", "daemon-reload"])?;
        if !reload.ok {
            return Err(format!(
                "the job file is back, but systemctl daemon-reload failed: {}",
                reload.stderr.trim()
            ));
        }
    }
    Ok(())
}
