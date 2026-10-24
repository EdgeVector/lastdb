use std::fs;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use zeroize::Zeroizing;

/// A caller-owned, empty private parent keeps registration data outside stores.
pub(super) fn destination(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or("credential destination needs a parent")?;
    let directory = super::super::files::open_directory(parent)?;
    let metadata = directory
        .metadata()
        .map_err(|_| "cannot inspect credential parent")?;
    if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o777 != 0o700 {
        return Err("credential parent must be an owner-only directory".into());
    }
    for entry in
        rustix::fs::Dir::read_from(&directory).map_err(|_| "cannot read credential parent")?
    {
        let entry = entry.map_err(|_| "cannot inspect credential parent")?;
        if !matches!(entry.file_name().to_bytes(), b"." | b"..") {
            return Err("credential parent must be empty".into());
        }
    }
    Ok(())
}

pub(super) fn invite(path: &Path) -> Result<Zeroizing<String>, String> {
    if !path.is_absolute() {
        return Err("invite file must be absolute".into());
    }
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| "cannot open protected invite file")?;
    let before = file.metadata().map_err(|_| "cannot inspect invite file")?;
    if !before.is_file()
        || before.uid() != unsafe { libc::geteuid() }
        || before.mode() & 0o777 != 0o600
        || before.nlink() != 1
        || before.len() > 1024
    {
        return Err("invite must be a bounded private owner file".into());
    }
    let mut bytes = Zeroizing::new(Vec::new());
    (&file)
        .take(1025)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read invite file")?;
    let after = file.metadata().map_err(|_| "cannot inspect invite file")?;
    if bytes.len() > 1024
        || before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
    {
        return Err("invite file changed or exceeds its limit".into());
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| "invalid invite encoding")?
        .trim();
    if text.is_empty() || !text.is_ascii() || text.bytes().any(|b| b.is_ascii_control()) {
        return Err("invalid invite text".into());
    }
    Ok(Zeroizing::new(text.to_owned()))
}
