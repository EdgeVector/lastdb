//! G0: the proof that the home is not served, before any store read.
//!
//! The planner reads a home that no daemon may write to. This guard asks for
//! four proofs. Any failure is a refusal with exit 2.
//!
//! 1. The flags. A home under `~/.lastdb` or `~/.folddb` needs both
//!    `--stopped-primary` and `--i-know-this-is-primary`.
//! 2. The sockets. A connect to each socket path of the home must fail with
//!    `ECONNREFUSED` or `ENOENT`. Any other result refuses, including a
//!    success and a timeout.
//! 3. The processes. No `lastdbd` names this home, and no process holds a
//!    socket file of the home open.
//! 4. The files. `identity.key` has 32 bytes and the layout descriptor says
//!    plain packaging in the hash-group layout.

use std::collections::BTreeSet;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc;
use std::time::Duration;

use lastdb_node::offline_home::refuse_primary;
use lastdb_uds::uds::{FULL_SOCKET_FILE_NAME, SOCKET_FILE_NAME};
use laststore::{LayoutMode, PackagingMode};
use serde::{Deserialize, Serialize};

use super::ReapError;

/// How long one socket connect may take before the guard refuses.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Process names that serve a home.
const DAEMON_NAMES: [&str; 3] = ["lastdbd", "lastdb_server", "folddb_server"];

/// The guard result, as plan.json records it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct GuardReport {
    pub primary_path: bool,
    pub sockets: Vec<String>,
    pub process_check: String,
    pub identity_key_bytes: u64,
    pub layout: String,
}

/// The flags of the stopped-primary proof.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Flags {
    pub stopped_primary: bool,
    pub i_know_this_is_primary: bool,
}

/// What the process table and `lsof` said.
#[derive(Debug, Clone)]
pub(crate) struct ProcessView {
    /// Pids that hold a socket file of the home open. `Err` when `lsof` is
    /// missing.
    pub socket_holders: Result<Vec<String>, String>,
    /// The output of `ps -ax -o pid=,command=`. `Err` when `ps` failed.
    pub ps: Result<String, String>,
}

/// Gate 1: the flags.
pub(crate) fn check_flags(primary_path: bool, flags: Flags) -> Result<(), ReapError> {
    if !primary_path {
        return Ok(());
    }
    if flags.stopped_primary && flags.i_know_this_is_primary {
        return Ok(());
    }
    Err(ReapError::Refused(
        "this home is under ~/.lastdb or ~/.folddb: pass both --stopped-primary and \
         --i-know-this-is-primary after the daemon is stopped"
            .to_string(),
    ))
}

/// The socket paths of a home.
pub(crate) fn socket_paths(home: &Path, store_root: &Path) -> BTreeSet<PathBuf> {
    let mut dirs = vec![
        home.to_path_buf(),
        store_root.to_path_buf(),
        home.join("data"),
    ];
    dirs.dedup();
    dirs.iter()
        .flat_map(|dir| {
            [SOCKET_FILE_NAME, FULL_SOCKET_FILE_NAME]
                .into_iter()
                .map(|name| dir.join(name))
        })
        .collect()
}

/// Gate 2: one connect result. Only a refusal or a missing file passes.
pub(crate) fn classify_connect(result: &std::io::Result<()>) -> Result<&'static str, String> {
    match result {
        Ok(()) => Err("a node accepts connections".to_string()),
        Err(error) => match error.raw_os_error() {
            Some(code) if code == libc::ECONNREFUSED => Ok("refused"),
            Some(code) if code == libc::ENOENT => Ok("absent"),
            _ => Err(format!("unexpected connect result: {error}")),
        },
    }
}

/// Run `connect` in a thread and classify its result. A connect that does not
/// return within `timeout` refuses.
pub(crate) fn probe_with(
    connect: impl FnOnce() -> std::io::Result<()> + Send + 'static,
    timeout: Duration,
) -> Result<&'static str, String> {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = sender.send(connect());
    });
    match receiver.recv_timeout(timeout) {
        Ok(result) => classify_connect(&result),
        Err(_) => Err("connect timed out".to_string()),
    }
}

fn probe_socket(path: &Path) -> Result<&'static str, String> {
    let target = path.to_path_buf();
    probe_with(
        move || UnixStream::connect(&target).map(|_| ()),
        CONNECT_TIMEOUT,
    )
}

/// Probe every socket path. A refusal names the first path that fails.
pub(crate) fn probe_sockets(paths: &BTreeSet<PathBuf>) -> Result<Vec<String>, ReapError> {
    let mut seen = Vec::new();
    for path in paths {
        match probe_socket(path) {
            Ok(state) => seen.push(format!("{}: {state}", path.display())),
            Err(why) => {
                return Err(ReapError::Refused(format!(
                    "socket {}: {why}; the daemon may be running",
                    path.display()
                )));
            }
        }
    }
    Ok(seen)
}

/// Read the process table and the holders of the socket files.
pub(crate) fn live_process_view(paths: &BTreeSet<PathBuf>) -> ProcessView {
    let mut holders = Vec::new();
    let mut lsof_error = None;
    for path in paths.iter().filter(|path| path.exists()) {
        match Command::new("lsof").args(["-t", "--"]).arg(path).output() {
            Ok(output) => holders.extend(
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .map(str::trim)
                    .filter(|pid| !pid.is_empty() && pid.bytes().all(|b| b.is_ascii_digit()))
                    .map(str::to_string),
            ),
            Err(error) => lsof_error = Some(format!("lsof: {error}")),
        }
    }
    let ps = Command::new("ps")
        .args(["-ax", "-o", "pid=,command="])
        .output()
        .map_err(|error| format!("ps: {error}"))
        .and_then(|output| {
            if output.status.success() {
                Ok(String::from_utf8_lossy(&output.stdout).into_owned())
            } else {
                Err(format!("ps exited with {}", output.status))
            }
        });
    ProcessView {
        socket_holders: lsof_error.map_or(Ok(holders), Err),
        ps,
    }
}

fn data_dir_arg(command: &str) -> Option<PathBuf> {
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

fn same_dir(left: &Path, right: &Path) -> bool {
    let canonical =
        |path: &Path| std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let (left, right) = (canonical(left), canonical(right));
    left == right || left.starts_with(&right) || right.starts_with(&left)
}

/// The pids of daemons in `ps` text that serve this home.
///
/// A daemon with `--data-dir` serves that directory. A daemon without it
/// serves the default home, `~/.lastdb`, so it counts only for a primary
/// path. This check does not see a daemon under another binary name.
pub(crate) fn daemons_serving(
    ps: &str,
    home: &Path,
    store_root: &Path,
    primary_path: bool,
) -> Vec<String> {
    let mut found = Vec::new();
    for line in ps.lines() {
        let Some((pid, command)) = line.trim().split_once(char::is_whitespace) else {
            continue;
        };
        let command = command.trim();
        let name = command
            .split_whitespace()
            .next()
            .and_then(|token| Path::new(token.trim_matches(['"', '\''])).file_name())
            .and_then(|name| name.to_str());
        if !name.is_some_and(|name| DAEMON_NAMES.contains(&name)) {
            continue;
        }
        let serves = match data_dir_arg(command) {
            Some(dir) => [home, store_root]
                .iter()
                .any(|target| same_dir(&dir, target)),
            None => primary_path,
        };
        if serves {
            found.push(format!("{pid} {command}"));
        }
    }
    found
}

/// Gate 3: no daemon serves the home and no process holds a socket file.
pub(crate) fn check_processes(
    view: &ProcessView,
    home: &Path,
    store_root: &Path,
    primary_path: bool,
) -> Result<String, ReapError> {
    let ps = view.ps.as_ref().map_err(|error| {
        ReapError::Refused(format!("cannot read the process table ({error}); no proof"))
    })?;
    let mut method = String::from("ps");
    match &view.socket_holders {
        Ok(holders) if !holders.is_empty() => {
            return Err(ReapError::Refused(format!(
                "a process holds a socket file of the home open: pid {}",
                holders.join(",")
            )));
        }
        Ok(_) => method.push_str("+lsof"),
        Err(why) => method.push_str(&format!(" (lsof not used: {why})")),
    }
    let daemons = daemons_serving(ps, home, store_root, primary_path);
    if !daemons.is_empty() {
        return Err(ReapError::Refused(format!(
            "a daemon serves this home: {}",
            daemons.join("; ")
        )));
    }
    Ok(method)
}

/// Gate 4: the files.
pub(crate) fn check_files(home: &Path, store_root: &Path) -> Result<(u64, String), ReapError> {
    let key_path = home.join("identity.key");
    let size = std::fs::metadata(&key_path)
        .map_err(|error| {
            ReapError::Refused(format!("identity.key at {}: {error}", key_path.display()))
        })?
        .len();
    if size != 32 {
        return Err(ReapError::Refused(format!(
            "identity.key has {size} bytes, not 32"
        )));
    }
    let layout = laststore::describe_home(store_root)
        .map_err(|error| ReapError::Refused(format!("layout descriptor: {error}")))?
        .ok_or_else(|| {
            ReapError::Refused(format!(
                "no layout descriptor at {}",
                store_root.join("laststore-layout-v1").display()
            ))
        })?;
    if layout.packaging != PackagingMode::Plain || layout.layout_mode != LayoutMode::HashGroup {
        return Err(ReapError::Refused(format!(
            "layout is {:?} in {:?} mode, not plain hash-group",
            layout.packaging, layout.layout_mode
        )));
    }
    Ok((
        size,
        format!(
            "plain/hash_group/{:?}/epoch{}",
            layout.hash_group_key, layout.layout_epoch
        ),
    ))
}

/// Run all four proofs.
pub(crate) fn prove(
    home: &Path,
    store_root: &Path,
    flags: Flags,
    view: &ProcessView,
) -> Result<GuardReport, ReapError> {
    let primary_path = refuse_primary(home).is_err() || refuse_primary(store_root).is_err();
    check_flags(primary_path, flags)?;
    let (identity_key_bytes, layout) = check_files(home, store_root)?;
    let sockets = probe_sockets(&socket_paths(home, store_root))?;
    let process_check = check_processes(view, home, store_root, primary_path)?;
    Ok(GuardReport {
        primary_path,
        sockets,
        process_check,
        identity_key_bytes,
        layout,
    })
}
