//! How far a private file write waits for the drive.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

/// How far a private file write goes before it returns.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Flush {
    /// The file and its name are on the medium: a power cut keeps them. On macOS this
    /// is `F_FULLFSYNC` twice, which empties the whole drive cache each time.
    Full,
    /// The file reaches the drive before its name does, and both before every later
    /// write (`F_BARRIERFSYNC` on macOS, `fdatasync` elsewhere). A crash of the process
    /// or of the system loses nothing. A power cut can lose this write, never leave a
    /// torn file under the name.
    Ordered,
    /// Nothing waits for the drive. For a file that only this run of the program reads
    /// back, or that is rebuilt when it is lost.
    None,
}

/// Create the empty file `path` (mode 0600) when it does not exist. Nothing waits for
/// the drive: the file is a signal between processes on this machine, not a record.
pub(crate) fn touch_marker(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => return Ok(()),
        Ok(_) => {
            let message = "private file must be a regular file";
            return Err(io::Error::new(io::ErrorKind::InvalidData, message));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map(drop)
}

pub(super) fn sync_file(file: &File, flush: Flush) -> io::Result<()> {
    match flush {
        Flush::Full => file.sync_all(),
        Flush::Ordered => ordered(file),
        Flush::None => Ok(()),
    }
}

pub(super) fn sync_folder(folder: &Path, flush: Flush) -> io::Result<()> {
    if flush == Flush::None {
        return Ok(());
    }
    let directory = File::open(folder)?;
    if flush == Flush::Full {
        return directory.sync_all();
    }
    ordered_folder(&directory)
}

/// `F_BARRIERFSYNC`: the data reaches the drive, ordered before later writes, without
/// emptying the drive cache. A file system without it gets the full flush. The same
/// ordered write that `laststore` uses for a commit.
#[cfg(target_os = "macos")]
fn ordered(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: `fcntl` with `F_BARRIERFSYNC` takes no argument and reads no memory; the
    // descriptor stays open for the call because `file` is borrowed.
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_BARRIERFSYNC) };
    if rc == -1 {
        return file.sync_all();
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn ordered(file: &File) -> io::Result<()> {
    file.sync_data()
}

#[cfg(target_os = "macos")]
fn ordered_folder(directory: &File) -> io::Result<()> {
    ordered(directory)
}

/// A folder entry has no data apart from its metadata, so `fdatasync` is not enough.
#[cfg(not(target_os = "macos"))]
fn ordered_folder(directory: &File) -> io::Result<()> {
    directory.sync_all()
}
