use super::*;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

pub fn prepare_directory(data_root: &Path, expected: &RecoveryDirectory) -> StorageResult<()> {
    if expected.version != 1 {
        return Err(invalid("recovery directory version"));
    }
    let root = data_root.join(DIRECTORY);
    for path in [&root, &root.join("entries"), &root.join("objects")] {
        match fs::DirBuilder::new().mode(0o700).create(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        if !directory_present(path)? {
            return Err(invalid("recovery directory"));
        }
    }
    let path = root.join("directory.json");
    if regular_present(&path)? {
        let actual: RecoveryDirectory = read_json(&path, METADATA_MAX_BYTES)?;
        if actual != *expected {
            return Err(invalid("recovery directory identity"));
        }
        File::open(&path)?.sync_all()?;
    } else {
        save_json(&path, expected)?;
    }
    File::open(&root)?.sync_all()?;
    File::open(data_root)?.sync_all()?;
    Ok(())
}

pub fn save_entry(data_root: &Path, entry: &RecoveryFile) -> StorageResult<()> {
    entry.validate()?;
    let root = data_root.join(DIRECTORY);
    if !directory_present(&root)? || !directory_present(&root.join("entries"))? {
        return Err(invalid("recovery directory"));
    }
    let path = root.join("entries").join(entry.key());
    if regular_present(&path)? {
        let actual: RecoveryFile = read_json(&path, METADATA_MAX_BYTES)?;
        if actual != *entry {
            return Err(invalid("recovery entry differs"));
        }
        File::open(&path)?.sync_all()?;
        File::open(root.join("entries"))?.sync_all()?;
        return Ok(());
    }
    save_json(&path, entry)
}

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

pub(super) fn open_bytes(path: &Path, expected: u64) -> StorageResult<File> {
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

fn save_json(path: &Path, value: &impl Serialize) -> StorageResult<()> {
    let bytes =
        cloud_backup_protocol::canonical_bytes(value).map_err(|_| invalid("recovery metadata"))?;
    if bytes.len() as u64 > METADATA_MAX_BYTES {
        return Err(invalid("recovery metadata bounds"));
    }
    let pending = path.with_extension("pending");
    regular_present(&pending)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&pending)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(pending, path)?;
    File::open(path.parent().ok_or_else(|| invalid("recovery parent"))?)?.sync_all()?;
    Ok(())
}
