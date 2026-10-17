use super::manifest::Entry;
use crate::disk_pack::corrupt;
use crate::{durability, Result};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

pub(super) fn validate_path(path: &Path) -> Result<()> {
    if path.as_os_str().len() > 512 || path.components().count() > 8 || path.as_os_str().is_empty()
    {
        return Err(corrupt("backup path length"));
    }
    if path
        .components()
        .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(corrupt("backup path escapes its root"));
    }
    Ok(())
}

pub(super) fn open(root: &Path, relative: &Path) -> Result<File> {
    validate_path(relative)?;
    let mut path = root.to_path_buf();
    for component in relative.components() {
        path.push(component);
        if fs::symlink_metadata(&path)?.file_type().is_symlink() {
            return Err(corrupt("backup file path contains a symlink"));
        }
    }
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(corrupt("backup input is not a regular file"));
    }
    Ok(file)
}

pub(super) fn copy(
    source: &Path,
    destination: &Path,
    relative: &Path,
    length: u64,
    expected: Option<&[u8; 32]>,
) -> Result<Entry> {
    let mut input = open(source, relative)?;
    if input.metadata()?.len() != length {
        return Err(corrupt("backup input length changed"));
    }
    let path = destination.join(relative);
    fs::create_dir_all(
        path.parent()
            .ok_or_else(|| corrupt("backup parent missing"))?,
    )?;
    let mut output = OpenOptions::new().create_new(true).write(true).open(path)?;
    let mut hash = Sha256::new();
    let mut remaining = length;
    let mut bytes = [0; 65_536];
    while remaining > 0 {
        let count = (remaining.min(bytes.len() as u64)) as usize;
        input.read_exact(&mut bytes[..count])?;
        output.write_all(&bytes[..count])?;
        hash.update(&bytes[..count]);
        remaining -= count as u64;
    }
    if input.metadata()?.len() != length {
        return Err(corrupt("backup input length changed"));
    }
    let digest: [u8; 32] = hash.finalize().into();
    if expected.is_some_and(|expected| *expected != digest) {
        return Err(corrupt("backup file checksum differs"));
    }
    durability::sync_dirty_file(&output)?;
    Ok(Entry {
        path: relative.to_path_buf(),
        bytes: length,
        digest,
    })
}

pub(super) fn write(root: &Path, name: &str, bytes: &[u8]) -> Result<Entry> {
    let relative = Path::new(name);
    validate_path(relative)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(root.join(relative))?;
    file.write_all(bytes)?;
    durability::sync_dirty_file(&file)?;
    Ok(Entry {
        path: relative.into(),
        bytes: bytes.len() as u64,
        digest: Sha256::digest(bytes).into(),
    })
}

pub(super) fn staging(destination: &Path) -> Result<PathBuf> {
    let destination = self::destination(destination)?;
    let parent = destination
        .parent()
        .ok_or_else(|| corrupt("backup destination needs a parent"))?;
    let parent = fs::canonicalize(parent)?;
    let path = parent.join(format!(".disk-backup-pending-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&path)?;
    Ok(path)
}

pub(super) fn destination(destination: &Path) -> Result<PathBuf> {
    let name = destination
        .file_name()
        .ok_or_else(|| corrupt("backup destination needs a file name"))?;
    let parent = destination
        .parent()
        .ok_or_else(|| corrupt("backup destination needs a parent"))?;
    let path = fs::canonicalize(parent)?.join(name);
    match fs::symlink_metadata(&path) {
        Ok(_) => Err(crate::Error::Config(
            "backup destination already exists".into(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(path),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn reserve_output(destination: &Path, bytes: u64, reserve: u64) -> Result<()> {
    let parent = destination
        .parent()
        .ok_or_else(|| corrupt("backup destination needs a parent"))?;
    let available = rustix::fs::statvfs(parent).map_err(std::io::Error::from)?;
    let available = (available.f_bavail as u64)
        .checked_mul(available.f_frsize as u64)
        .ok_or_else(|| corrupt("backup filesystem capacity overflow"))?;
    let required = bytes
        .checked_add(reserve)
        .ok_or_else(|| corrupt("backup capacity quote overflow"))?;
    if available < required {
        return Err(crate::Error::WriteCapacity(
            "backup output exceeds destination filesystem capacity".into(),
        ));
    }
    Ok(())
}

pub(super) fn sync_tree<'a>(root: &Path, paths: impl Iterator<Item = &'a Path>) -> Result<()> {
    let mut parents = BTreeSet::new();
    parents.insert(root.to_path_buf());
    for relative in paths {
        validate_path(relative)?;
        let mut path = root.join(relative);
        while path.pop() && path.starts_with(root) {
            parents.insert(path.clone());
            if path == root {
                break;
            }
        }
    }
    for parent in parents.iter().rev() {
        durability::sync_directory(&File::open(parent)?)?;
    }
    Ok(())
}

pub(super) fn publish(staging: &Path, destination: &Path) -> Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        staging,
        rustix::fs::CWD,
        destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(std::io::Error::from)?;
    durability::sync_directory(&File::open(
        destination
            .parent()
            .ok_or_else(|| corrupt("backup parent missing"))?,
    )?)
}
