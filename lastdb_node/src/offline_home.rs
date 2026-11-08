//! Primary-home guards and key loading for the current storage recovery proof.
//!
//! One definition of the primary-home guard keeps every tool on the same
//! protected home.

use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fold_db::crypto::E2eKeys;
use fold_db::security::Ed25519KeyPair;
use serde::Serialize;

use crate::session_ledger::{read_last_record, read_shutdown_flush_receipt};

fn user_home() -> Result<PathBuf, String> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| "HOME unset".to_string())
}

/// Refuse `path` when it is, or lies under, the primary home `~/.lastdb`.
///
/// A path that does not exist yet is resolved through its nearest existing
/// ancestor, so a missing path under a primary home is still refused.
pub fn refuse_primary(path: &Path) -> Result<(), String> {
    refuse_primary_under(&user_home()?, path)
}

fn refuse_primary_under(user_home: &Path, path: &Path) -> Result<(), String> {
    let abs = canonicalize_lenient(path);
    let primary_abs = canonicalize_lenient(&user_home.join(folddb_profile::paths::LASTDB_DIR));
    if abs.starts_with(&primary_abs) {
        return Err(format!(
            "refusing primary path {}: use a home outside ~/.lastdb",
            abs.display()
        ));
    }
    Ok(())
}

/// Canonicalize `path`, resolving a missing tail through the nearest existing
/// ancestor so symlinked prefixes (`/var` vs `/private/var`) still compare equal.
fn canonicalize_lenient(path: &Path) -> PathBuf {
    let mut tail = Vec::new();
    let mut cursor = path;
    loop {
        if let Ok(base) = std::fs::canonicalize(cursor) {
            return tail.iter().rev().fold(base, |acc, part| acc.join(part));
        }
        match (cursor.parent(), cursor.file_name()) {
            (Some(parent), Some(name)) => {
                tail.push(name.to_os_string());
                cursor = parent;
            }
            _ => return path.to_path_buf(),
        }
    }
}

/// Load the account E2E content keys and the device signing key from the
/// `identity.key` seed in `data_dir`.
pub fn load_e2e_keys(data_dir: &Path) -> Result<(E2eKeys, Arc<Ed25519KeyPair>), String> {
    let identity_path = data_dir.join("identity.key");
    let bytes = std::fs::read(&identity_path)
        .map_err(|e| format!("read {}: {e}", identity_path.display()))?;
    let seed: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
        format!(
            "identity.key has invalid length {} (expected 32)",
            bytes.len()
        )
    })?;
    let e2e = E2eKeys::from_ed25519_seed(&seed).map_err(|e| format!("e2e keys: {e}"))?;
    let keypair =
        Arc::new(Ed25519KeyPair::from_secret_key(&seed).map_err(|e| format!("ed25519: {e}"))?);
    Ok((e2e, keypair))
}

/// What the preflight of a stopped home checked.
#[derive(Debug, Serialize)]
pub struct Preflight {
    pub daemon_alive: bool,
    pub socket_closed: bool,
    pub last_session_clean: bool,
    pub shutdown_flush_receipt: bool,
}

/// Refuse unless the home is stopped and flushed.
///
/// 1. No daemon is alive for the home, and the daemon socket accepts no
///    connection.
/// 2. The last session ended cleanly and left a valid shutdown flush receipt,
///    so the previous daemon flushed everything before it stopped.
///
/// The store open that follows takes the run home owner lock (`LOCK`), which
/// excludes a running daemon and every other offline tool.
pub fn preflight_stopped_home(home: &Path, store_root: &Path) -> Result<Preflight, String> {
    if crate::crash_attribution::live_daemon_pid_alive(home) {
        return Err("refusing: a daemon is alive for this home; stop it first".to_string());
    }
    let socket = lastdb_uds::uds::socket_path(store_root);
    if UnixStream::connect(&socket).is_ok() {
        return Err(format!(
            "refusing: the database socket is open at {}",
            socket.display()
        ));
    }
    let last_session_clean = read_last_record(home).is_some_and(|record| record.ended_clean());
    if !last_session_clean {
        return Err(
            "refusing: the last session did not end cleanly. Start the daemon and stop it \
             gracefully, then retry."
                .to_string(),
        );
    }
    if let Err(error) = read_shutdown_flush_receipt(home) {
        return Err(format!(
            "refusing: no valid shutdown flush receipt ({error}). Start the daemon and stop it \
             gracefully, then retry."
        ));
    }
    Ok(Preflight {
        daemon_alive: false,
        socket_closed: true,
        last_session_clean: true,
        shutdown_flush_receipt: true,
    })
}
