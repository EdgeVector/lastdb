//! Small-file persistence and store helpers for the S0 publisher. Moved verbatim from `cloud_rescue_publish.rs`.

use super::*;

pub(super) fn read_small_regular(path: &Path, max_bytes: u64) -> Result<Option<Vec<u8>>, String> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read {} metadata: {error}", path.display())),
    };
    if !meta.file_type().is_file() || meta.len() > max_bytes {
        return Err(format!("{} must be a bounded regular file", path.display()));
    }
    fs::read(path)
        .map(Some)
        .map_err(|error| format!("read {}: {error}", path.display()))
}

pub(super) fn save_once(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path.parent().ok_or("S0 rescue plan has no parent")?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| format!("create S0 rescue plan file: {error}"))?;
    tmp.write_all(bytes)
        .map_err(|error| format!("write S0 rescue plan file: {error}"))?;
    tmp.as_file()
        .sync_all()
        .map_err(|error| format!("sync S0 rescue plan file: {error}"))?;
    tmp.persist_noclobber(path)
        .map_err(|error| format!("place S0 rescue plan file: {}", error.error))?;
    File::open(parent)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| format!("sync S0 rescue plan directory: {error}"))
}

pub(super) fn save_replace(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path.parent().ok_or("S0 rescue proof has no parent")?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| format!("create S0 rescue proof file: {error}"))?;
    tmp.write_all(bytes)
        .map_err(|error| format!("write S0 rescue proof file: {error}"))?;
    tmp.as_file()
        .sync_all()
        .map_err(|error| format!("sync S0 rescue proof file: {error}"))?;
    tmp.persist(path)
        .map_err(|error| format!("replace S0 rescue proof file: {}", error.error))?;
    File::open(parent)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| format!("sync S0 rescue proof directory: {error}"))
}

pub(super) fn source_key(home: &Path) -> Result<[u8; 32], String> {
    let path = home.join(lastdb_node::host::IDENTITY_KEY_FILE);
    let bytes = read_small_regular(&path, 32)?
        .ok_or_else(|| "stopped copy has no identity key".to_string())?;
    if bytes.len() != 32 {
        return Err("stopped-copy identity key must be 32 bytes".into());
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes);
    fold_db::crypto::E2eKeys::from_ed25519_seed(&seed)
        .map(|keys| keys.encryption_key())
        .map_err(|error| format!("derive S0 rescue encryption key: {error}"))
}

pub(super) fn open_store(
    home: &Path,
    e2e_key: [u8; 32],
) -> Result<fold_db::storage::LastStoreNamespacedStore, String> {
    fold_db::storage::LastStoreNamespacedStore::open_with_data_key_and_high_water(
        &home.join("data"),
        e2e_key,
        home.join("laststore_high_water.json"),
    )
    .map_err(|error| format!("open stopped-copy LastStore: {error}"))
}

pub(super) fn ordered_refs(
    refs: impl Iterator<Item = BackupChunkRef>,
) -> Result<Vec<String>, String> {
    let mut rows = refs
        .map(|chunk| {
            serde_json::to_string(&chunk)
                .map_err(|error| format!("encode S0 rescue chunk reference: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort();
    Ok(rows)
}
