use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use lastdb_node::offline_home::existing_primary_homes;

pub(crate) fn write_primary_pids_out(argv: &[String]) -> Option<PathBuf> {
    argv.windows(2)
        .find_map(|pair| (pair[0] == "--write-primary-pids").then(|| PathBuf::from(&pair[1])))
}

pub(crate) fn write_primary_pid_census(out: &Path) -> Result<(), String> {
    write_pid_census(out, live_primary_lastdbd_pids())
}

/// A census that could not be taken is an error, never an empty file: an
/// empty `before` and an empty `after` would compare equal and let the
/// verdict claim `primary_pid_set_unchanged=true` without ever seeing a pid.
pub(crate) fn write_pid_census(
    out: &Path,
    census: Result<BTreeSet<String>, String>,
) -> Result<(), String> {
    let pids = census?;
    let mut text = String::new();
    for pid in &pids {
        text.push_str(pid);
        text.push('\n');
    }
    std::fs::write(out, text).map_err(|e| format!("write {}: {e}", out.display()))
}

/// Pids that are actually serving the live `~/.lastdb` (or `~/.folddb`)
/// primary — not repair copies, smoke COWs, safe-upgrade helpers, or
/// this probe's own throwaway daemons.
pub(crate) fn live_primary_lastdbd_pids() -> Result<BTreeSet<String>, String> {
    let homes = existing_primary_homes()?;
    live_primary_lastdbd_pids_from(pids_holding_primary_sockets(&homes), || {
        pids_from_process_table(&homes)
    })
}

/// `lsof` on the primary socket is the sandbox-safe source; the process
/// table is only consulted when the socket read names nobody. A process-table
/// read that fails propagates as `Err` so a denied `ps` can never yield an
/// empty-but-`Ok` census.
pub(crate) fn live_primary_lastdbd_pids_from(
    socket_holders: BTreeSet<String>,
    process_table: impl FnOnce() -> Result<BTreeSet<String>, String>,
) -> Result<BTreeSet<String>, String> {
    if socket_holders.is_empty() {
        process_table()
    } else {
        Ok(socket_holders)
    }
}

pub(crate) fn pids_holding_primary_sockets(homes: &[PathBuf]) -> BTreeSet<String> {
    let mut pids = BTreeSet::new();
    for home in homes {
        let sock = home.join("data").join("folddb.sock");
        if !sock.exists() {
            continue;
        }
        let output = std::process::Command::new("lsof")
            .args(["-t", "--"])
            .arg(&sock)
            .output();
        let Ok(output) = output else {
            continue;
        };
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let pid = line.trim();
            if pid.bytes().all(|b| b.is_ascii_digit()) && !pid.is_empty() {
                pids.insert(pid.to_string());
            }
        }
    }
    pids
}

pub(crate) fn pids_from_process_table(homes: &[PathBuf]) -> Result<BTreeSet<String>, String> {
    let output = std::process::Command::new("ps")
        .args(["-ax", "-o", "pid=,command="])
        .output()
        .map_err(|e| format!("ps: {e}"))?;
    pids_from_ps_output(&output, homes)
}

/// A `ps` that ran but exited non-zero (a seatbelt-denied sysctl prints an
/// error and exits 1) has not listed the table; treat it like a spawn failure
/// rather than an empty table.
pub(crate) fn pids_from_ps_output(
    output: &std::process::Output,
    homes: &[PathBuf],
) -> Result<BTreeSet<String>, String> {
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("ps {}: {}", output.status, stderr.trim()));
    }
    let mut pids = BTreeSet::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((pid, command)) = line.split_once(char::is_whitespace) else {
            continue;
        };
        let pid = pid.trim();
        if !pid.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        if command_serves_primary_home(command.trim(), homes) {
            pids.insert(pid.to_string());
        }
    }
    Ok(pids)
}

/// A process is the live primary iff it is actually `lastdbd` and either
/// `--data-dir` is a primary home or (no `--data-dir`) the binary itself
/// lives under a primary home. `~/.lastdb-test-copies/...` must not match
/// `~/.lastdb`.
pub(crate) fn command_serves_primary_home(command: &str, primary_homes: &[PathBuf]) -> bool {
    if !is_lastdbd_process(command) {
        return false;
    }
    if let Some(dir) = data_dir_arg(command) {
        return primary_homes.iter().any(|home| path_is_primary(&dir, home));
    }
    let bin = first_token(command);
    primary_homes
        .iter()
        .any(|home| path_is_primary(Path::new(bin), home))
}

pub(crate) fn is_lastdbd_process(command: &str) -> bool {
    Path::new(first_token(command))
        .file_name()
        .is_some_and(|name| name == "lastdbd")
}

pub(crate) fn first_token(command: &str) -> &str {
    command
        .split_whitespace()
        .next()
        .unwrap_or(command)
        .trim_matches(|c| c == '"' || c == '\'')
}

pub(crate) fn data_dir_arg(command: &str) -> Option<PathBuf> {
    let mut tokens = command.split_whitespace();
    while let Some(token) = tokens.next() {
        if let Some(rest) = token.strip_prefix("--data-dir=") {
            return Some(PathBuf::from(rest));
        }
        if token == "--data-dir" {
            return tokens.next().map(PathBuf::from);
        }
    }
    None
}

pub(crate) fn path_is_primary(path: &Path, home: &Path) -> bool {
    path == home || path.starts_with(home)
}

pub(crate) fn read_pid_set(path: &Path) -> Result<BTreeSet<String>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}
