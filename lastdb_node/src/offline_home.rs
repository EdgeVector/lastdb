//! Primary-home guards and stopped-home preflight for storage recovery.

use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::shutdown_receipt::read_shutdown_flush_receipt;

/// Refuse `path` when it is, or lies under, the primary home `~/.lastdb`.
pub fn refuse_primary(path: &Path) -> Result<(), String> {
    let user_home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| "HOME unset".to_string())?;
    let abs = canonicalize_lenient(path);
    let primary_abs = canonicalize_lenient(&user_home.join(crate::profile::paths::LASTDB_DIR));
    if abs.starts_with(&primary_abs) {
        return Err(format!(
            "refusing primary path {}: use a home outside ~/.lastdb",
            abs.display()
        ));
    }
    Ok(())
}

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

#[derive(Debug, Serialize)]
pub struct Preflight {
    pub socket_closed: bool,
    pub shutdown_flush_receipt: bool,
}

/// Refuse unless the home is stopped and flushed.
pub fn preflight_stopped_home(home: &Path, store_root: &Path) -> Result<Preflight, String> {
    let socket = crate::lastdb_uds::uds::socket_path(store_root);
    if UnixStream::connect(&socket).is_ok() {
        return Err(format!(
            "refusing: the database socket is open at {}",
            socket.display()
        ));
    }
    if let Err(error) = read_shutdown_flush_receipt(home) {
        return Err(format!(
            "refusing: no valid shutdown flush receipt ({error}). Start the daemon and stop it \
             gracefully, then retry."
        ));
    }
    Ok(Preflight {
        socket_closed: true,
        shutdown_flush_receipt: true,
    })
}
