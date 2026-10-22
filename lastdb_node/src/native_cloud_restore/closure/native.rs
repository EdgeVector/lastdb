use super::*;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;

pub(super) fn install_file(
    session: &RestoreSession,
    object: &Path,
    entry: &NativeFileEntry,
) -> Result<()> {
    let path = session.physical().join(entry.path.as_str());
    let mut parent = session.physical();
    let components: Vec<_> = entry.path.as_str().split('/').collect();
    for component in &components[..components.len() - 1] {
        parent.push(component);
        match fs::create_dir(&parent) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        if !fs::symlink_metadata(&parent)?.is_dir() {
            return Err(RestoreError::Invalid("native parent"));
        }
    }
    if path.try_exists()? {
        download::verify_file(&path, &entry.native_sha256, entry.native_bytes.get())?;
    } else {
        let pending = session.root.join("native.pending");
        if pending.try_exists()? && !fs::symlink_metadata(&pending)?.is_file() {
            return Err(RestoreError::Invalid("native temporary"));
        }
        let mut output = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&pending)?;
        download::check_capacity(&session.root, entry.native_bytes.get())?;
        let mut input = File::open(object)?;
        input.seek(SeekFrom::Start(entry.extent.offset.get()))?;
        let mut left = entry.native_bytes.get();
        let mut buffer = vec![0; cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize];
        while left > 0 {
            let count = left.min(buffer.len() as u64) as usize;
            input.read_exact(&mut buffer[..count])?;
            output.write_all(&buffer[..count])?;
            left -= count as u64;
        }
        output.sync_all()?;
        download::verify_file(&pending, &entry.native_sha256, entry.native_bytes.get())?;
        fs::rename(&pending, &path)?;
        File::open(&parent)?.sync_all()?;
        File::open(&session.root)?.sync_all()?;
    }
    let ledger = session
        .root
        .join("entries")
        .join(download::path_key(entry.path.as_str()));
    if ledger.try_exists()? {
        let existing: NativeFileEntry = session::read_json(&ledger, 256 * 1024)?;
        if existing != *entry {
            return Err(RestoreError::Invalid("saved native entry"));
        }
    } else {
        session.remove_spool_file(&ledger.with_extension("pending"))?;
        session.reserve_spool(4096)?;
        if let Err(error) = session::atomic_json(&ledger, entry) {
            if ledger.with_extension("pending").try_exists()? {
                session.add_spool_file(&ledger.with_extension("pending"))?;
            }
            return Err(error);
        }
        session.add_spool_file(&ledger)?;
    }
    Ok(())
}
