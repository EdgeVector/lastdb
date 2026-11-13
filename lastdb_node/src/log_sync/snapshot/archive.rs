//! A bounded archive of one database's pinned, encrypted native files.

use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;

use cloud_backup_protocol::RelativePath;
use fold_db::crypto::envelope::{decrypt_envelope_with_aad, encrypt_envelope_with_aad};
use fold_db::storage::disk_pack::{snapshot::DatabaseSnapshot, CAS_DIR, NODE_MARKER, RUN_DIR};
use zeroize::Zeroizing;

use crate::log_envelope::DatabaseContentKey;

const MAGIC: &[u8; 8] = b"LDBSNAP1";
const MAX_FILES: usize = 8192;
pub(super) const MAX_BYTES: usize = 128 * 1024 * 1024;

fn aad(database: &str, through: u64) -> Vec<u8> {
    let mut bytes = b"lastdb:signed-database-snapshot:v1\0".to_vec();
    bytes.extend_from_slice(database.as_bytes());
    bytes.extend_from_slice(&through.to_be_bytes());
    bytes
}

pub(super) fn encode(
    cut: &DatabaseSnapshot,
    key: &DatabaseContentKey,
    through: u64,
) -> Result<Vec<u8>, String> {
    let files = cut.files().map_err(|e| e.to_string())?;
    if files.len() > MAX_FILES {
        return Err("snapshot exceeds the file count bound".into());
    }
    let capacity = files
        .iter()
        .try_fold(12usize, |total, (path, size)| {
            total
                .checked_add(10 + path.as_os_str().len())?
                .checked_add(usize::try_from(*size).ok()?)
        })
        .filter(|n| *n < MAX_BYTES - 64)
        .ok_or("snapshot exceeds the byte bound")?;
    let mut data = Zeroizing::new(Vec::with_capacity(capacity));
    data.extend_from_slice(MAGIC);
    data.extend_from_slice(&(files.len() as u32).to_be_bytes());
    for (path, size) in files {
        let path_text = path.to_str().ok_or("snapshot path is not UTF-8")?;
        check_path(path_text)?;
        data.extend_from_slice(&(path_text.len() as u16).to_be_bytes());
        data.extend_from_slice(&size.to_be_bytes());
        data.extend_from_slice(path_text.as_bytes());
        let file = cut.open_file(&path).map_err(|e| e.to_string())?;
        let read = file
            .take(size + 1)
            .read_to_end(&mut data)
            .map_err(|e| e.to_string())?;
        if read as u64 != size {
            return Err("snapshot source file length changed".into());
        }
    }
    encrypt_envelope_with_aad(
        key.encryption_bytes(),
        &aad(key.database_id(), through),
        &data,
    )
    .map_err(|_| "snapshot encryption failed".into())
}

pub(super) fn decode(
    sealed: &[u8],
    key: &DatabaseContentKey,
    through: u64,
    destination: &Path,
) -> Result<(), String> {
    if sealed.len() > MAX_BYTES {
        return Err("snapshot exceeds the byte bound".into());
    }
    let data = Zeroizing::new(
        decrypt_envelope_with_aad(
            key.encryption_bytes(),
            &aad(key.database_id(), through),
            sealed,
        )
        .map_err(|_| "snapshot authentication failed")?,
    );
    unpack(&data, destination).map_err(|e| e.to_string())
}

pub(super) fn opens(sealed: &[u8], key: &DatabaseContentKey, through: u64) -> bool {
    decrypt_envelope_with_aad(
        key.encryption_bytes(),
        &aad(key.database_id(), through),
        sealed,
    )
    .is_ok_and(|bytes| {
        let bytes = Zeroizing::new(bytes);
        bytes.starts_with(MAGIC)
    })
}

fn unpack(bytes: &[u8], destination: &Path) -> std::io::Result<()> {
    let invalid =
        || std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid snapshot archive");
    let mut data = Cursor::new(bytes);
    let mut magic = [0; 8];
    data.read_exact(&mut magic)?;
    let count = u32::from_be_bytes(read(&mut data)?) as usize;
    if &magic != MAGIC || !(5..=MAX_FILES).contains(&count) {
        return Err(invalid());
    }
    fs::DirBuilder::new().mode(0o700).create(destination)?;
    for name in [RUN_DIR, CAS_DIR] {
        let home = destination.join(name);
        fs::DirBuilder::new().mode(0o700).create(&home)?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(home.join("runs"))?;
        write_file(&home.join("LOCK"), &[])?;
    }
    for _ in 0..count {
        let length = u16::from_be_bytes(read(&mut data)?) as usize;
        let size = usize::try_from(u64::from_be_bytes(read(&mut data)?)).map_err(|_| invalid())?;
        let mut path = vec![0; length];
        data.read_exact(&mut path)?;
        let path = std::str::from_utf8(&path).map_err(|_| invalid())?;
        check_path(path).map_err(|_| invalid())?;
        let start = data.position() as usize;
        let end = start
            .checked_add(size)
            .filter(|n| *n <= bytes.len())
            .ok_or_else(invalid)?;
        write_file(&destination.join(path), &bytes[start..end])?;
        data.set_position(end as u64);
    }
    if data.position() as usize != bytes.len() {
        return Err(invalid());
    }
    for name in [RUN_DIR, CAS_DIR] {
        File::open(destination.join(name).join("runs"))?.sync_all()?;
        File::open(destination.join(name))?.sync_all()?;
    }
    File::open(destination)?.sync_all()
}

fn check_path(path: &str) -> Result<(), String> {
    RelativePath::try_from(path.to_owned()).map_err(|_| "invalid snapshot path")?;
    let allowed = path == NODE_MARKER
        || [RUN_DIR, CAS_DIR].iter().any(|home| {
            path.strip_prefix(&format!("{home}/"))
                .is_some_and(|relative| {
                    matches!(relative, "CURRENT" | "laststore-run-home-v2")
                        || relative
                            .strip_prefix("runs/")
                            .and_then(|run| run.strip_suffix(".ldrun"))
                            .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok())
                })
        });
    allowed
        .then_some(())
        .ok_or_else(|| "unexpected snapshot path".into())
}

fn read<const N: usize>(reader: &mut impl Read) -> std::io::Result<[u8; N]> {
    let mut bytes = [0; N];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn write_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}
