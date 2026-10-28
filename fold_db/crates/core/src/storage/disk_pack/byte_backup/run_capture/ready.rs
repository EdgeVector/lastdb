//! A paired cut is immutable only after both native homes and its marker are durable.
use super::*;
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;

const READY: &[u8] = b"ready-v1\n";
pub(super) fn publish(root: &Path) -> StorageResult<()> {
    // The child capture helpers already flush their directories. This barrier
    // includes the node link before the completion marker can become durable.
    File::open(root)?.sync_all()?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(root.join("pair-ready"))?;
    file.write_all(READY)?;
    file.sync_all()?;
    File::open(root)?.sync_all()?;
    Ok(())
}
pub(super) fn complete(root: &Path) -> StorageResult<bool> {
    let mut file = match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(root.join("pair-ready"))
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || meta.nlink() != 1
        || meta.len() != READY.len() as u64
    {
        return Err(invalid("run paired completion marker"));
    }
    let mut bytes = [0u8; 9];
    file.read_exact(&mut bytes)?;
    if bytes != READY {
        return Err(invalid("run paired completion marker"));
    }
    Ok(true)
}
