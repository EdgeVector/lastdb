//! Checks that run before the store opens.
//!
//! The daemon takes no file lock on the store. The only sign that it runs is
//! a socket that accepts a connection. `maintenance.lock` stops a second
//! maintenance run. It does not stop the daemon.

use super::ReapError;
use crate::home_has_frame_aead_segments;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

const LAYOUT_FILE: &str = "laststore-layout-v1";
const LOCK_FILE: &str = "maintenance.lock";
/// The lock file of the offline atom rewrite. It is in the home.
const ATOM_REWRITE_LOCK_FILE: &str = "laststore_atom_rewrite.lock";
/// Sockets that the daemon binds in the store root.
const SOCKET_FILES: &[&str] = &["folddb.sock", "folddb-full.sock"];
/// A socket path cannot be longer than this on macOS and Linux.
const SOCKET_PATH_LIMIT: usize = 100;

/// Find the store root. `home` is the store root, or the home that holds it
/// in `data/`.
pub(super) fn resolve_store_root(home: &Path) -> Result<PathBuf, ReapError> {
    if home.join(LAYOUT_FILE).is_file() {
        return Ok(home.to_path_buf());
    }
    let nested = home.join("data");
    if nested.join(LAYOUT_FILE).is_file() {
        return Ok(nested);
    }
    Err(ReapError::Refused(format!(
        "no {LAYOUT_FILE} under {} or {}",
        home.display(),
        nested.display()
    )))
}

/// True when nothing can answer on the socket path.
fn socket_is_quiet(path: &Path) -> bool {
    match UnixStream::connect(path) {
        Ok(_) => false,
        Err(error) => match error.kind() {
            ErrorKind::NotFound | ErrorKind::ConnectionRefused => true,
            // A path that is too long cannot hold a bound socket.
            ErrorKind::InvalidInput => path.as_os_str().len() >= SOCKET_PATH_LIMIT,
            _ => false,
        },
    }
}

/// Refuse a store that a daemon may use, or that this reap cannot rewrite.
///
/// The store must have a layout file with plain packaging and no frame
/// segment. No socket in the store root may accept a connection. Any socket
/// error other than "not found" and "refused" also refuses.
pub(super) fn refuse_unless_stopped_plain(root: &Path) -> Result<(), ReapError> {
    let layout = std::fs::read_to_string(root.join(LAYOUT_FILE))?;
    if !layout.lines().any(|line| line == "packaging=plain") {
        return Err(ReapError::Refused("packaging is not plain".to_string()));
    }
    if home_has_frame_aead_segments(root) {
        return Err(ReapError::Refused("frame segments are present".to_string()));
    }
    for name in SOCKET_FILES {
        if !socket_is_quiet(&root.join(name)) {
            return Err(ReapError::Refused(format!(
                "the socket {name} accepts a connection or cannot be probed"
            )));
        }
    }
    Ok(())
}

/// Refuse a store root that has no `data` directory.
///
/// The store open makes that directory when it is absent. A run without
/// `execute` must make nothing, and a root without data has nothing to reap.
pub(super) fn refuse_unless_data_dir(root: &Path) -> Result<(), ReapError> {
    if root.join("data").is_dir() {
        return Ok(());
    }
    Err(ReapError::Refused(format!(
        "{} has no data directory",
        root.display()
    )))
}

/// Exclusive locks on the lock files of the offline tools. The locks end
/// when this drops.
pub(super) struct StoreLock {
    _files: Vec<File>,
}

/// Take one lock file without waiting. The result is `None` when the file is
/// absent and `create` is false.
fn lock_file(path: &Path, create: bool, what: &str) -> Result<Option<File>, ReapError> {
    let opened = if create {
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
    } else {
        match File::open(path) {
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            other => other,
        }
    };
    let file = opened.map_err(|error| ReapError::Refused(format!("{what}: {error}")))?;
    rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
        .map_err(|error| ReapError::Refused(format!("{what} is held: {error}")))?;
    Ok(Some(file))
}

/// Take the locks of the store without waiting.
///
/// `maintenance.lock` is in the store root. A write run creates it when it is
/// missing. A count run does not create a file. It locks the file only if the
/// file exists. The offline atom rewrite of `lastdb` locks
/// `laststore_atom_rewrite.lock` in the home, the parent of the store root.
/// Both kinds of run lock that file only if it exists.
pub(super) fn lock_store(root: &Path, create: bool) -> Result<StoreLock, ReapError> {
    let mut files = Vec::new();
    files.extend(lock_file(
        &root.join(LOCK_FILE),
        create,
        "maintenance lock",
    )?);
    let real = std::fs::canonicalize(root)?;
    if let Some(home) = real.parent() {
        let path = home.join(ATOM_REWRITE_LOCK_FILE);
        files.extend(lock_file(&path, false, "atom rewrite lock")?);
    }
    Ok(StoreLock { _files: files })
}
