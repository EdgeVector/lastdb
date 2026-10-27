use super::*;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;

pub(super) fn directory_present(path: &Path) -> StorageResult<bool> {
    present(path, true)
}
pub(super) fn regular_present(path: &Path) -> StorageResult<bool> {
    present(path, false)
}
fn present(path: &Path, directory: bool) -> StorageResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(meta) if directory && meta.is_dir() || !directory && meta.is_file() => Ok(true),
        Ok(_) => Err(invalid("recovery member type")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn read_bytes(path: &Path, expected: u64) -> StorageResult<Vec<u8>> {
    let mut file = open_bytes(path, expected)?;
    let capacity = usize::try_from(expected).map_err(|_| invalid("local file addressable size"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| invalid("local file allocation"))?;
    bytes.resize(capacity, 0);
    file.read_exact(&mut bytes)?;
    let mut extra = [0u8; 1];
    if bytes.len() as u64 != expected || file.read(&mut extra)? != 0 {
        return Err(invalid("recovery ciphertext changed"));
    }
    Ok(bytes)
}

fn open_bytes(path: &Path, expected: u64) -> StorageResult<File> {
    if !regular_present(path)? {
        return Err(invalid("missing recovered ciphertext"));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() || file.metadata()?.len() != expected {
        return Err(invalid("recovery byte bounds"));
    }
    Ok(file)
}

pub(super) fn read_json<T: serde::de::DeserializeOwned>(
    path: &Path,
    maximum: u64,
) -> StorageResult<T> {
    if !regular_present(path)? {
        return Err(invalid("recovery metadata missing"));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let mut bytes = Vec::new();
    file.take(maximum + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(invalid("recovery metadata bounds"));
    }
    serde_json::from_slice(&bytes).map_err(|_| invalid("recovery metadata"))
}
