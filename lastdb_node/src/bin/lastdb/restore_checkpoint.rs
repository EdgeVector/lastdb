//! Durable S0 identity for mutation-tail retry. Never consult cloud latest on resume.
use fold_db::sync::engine::LastStoreCloudRestoreReport;
use serde::{Deserialize, Serialize};
use std::{fs, io::Write, path::Path};

const FILE: &str = "restore-s0-checkpoint.json";
const MAX_BYTES: u64 = 64 * 1024 * 1024;
const MAX_FILES: usize = 100_000;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    version: u32,
    checksum: String,
    source_db_hash: String,
    account_public_key: String,
    files: Vec<InstalledFile>,
    manifest_sha256: String,
    latest_key: String,
    counter: u64,
    cut_csn: u64,
    restored_epoch: u64,
    manifests_walked: usize,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InstalledFile {
    path: std::path::PathBuf,
    bytes: u64,
    sha256: String,
}

fn account_public_key(home: &Path) -> Result<String, String> {
    let seed = fs::read(home.join(lastdb_node::host::IDENTITY_KEY_FILE))
        .map_err(|_| "restore identity unavailable")?;
    fold_db::security::Ed25519KeyPair::from_secret_key(&seed)
        .map(|key| fold_db::hex::hex_lower(key.public_key_bytes()))
        .map_err(|_| "invalid restore identity".into())
}

pub(super) fn capture_files(
    home: &Path,
    store: &fold_db::storage::LastStoreNamespacedStore,
    installed_chunks: usize,
) -> Result<Vec<InstalledFile>, String> {
    let scan = store
        .scan_backup_chunks(None)
        .map_err(|_| "cannot enumerate installed S0 chunks")?;
    if !scan.unresolvable.is_empty()
        || scan.candidates.len() > MAX_FILES
        || scan.candidates.len() < installed_chunks
    {
        return Err("installed S0 chunk inventory is incomplete or exceeds its limit".into());
    }
    scan.candidates
        .into_iter()
        .map(|candidate| {
            let path = candidate
                .path
                .strip_prefix(home)
                .map_err(|_| "S0 chunk is outside destination")?
                .to_path_buf();
            Ok(InstalledFile {
                path,
                bytes: candidate.chunk.bytes,
                sha256: candidate.chunk.sha256,
            })
        })
        .collect()
}

fn validate_files(home: &Path, files: &[InstalledFile]) -> Result<(), String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let canonical_home = home
        .canonicalize()
        .map_err(|_| "restore destination unavailable")?;
    for entry in files {
        if !entry.path.starts_with("data")
            || entry
                .path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            return Err("invalid checkpoint chunk path".into());
        }
        let path = home.join(&entry.path);
        let canonical = path
            .canonicalize()
            .map_err(|_| "installed S0 chunk is missing")?;
        if !canonical.starts_with(&canonical_home) {
            return Err("S0 chunk escapes destination".into());
        }
        let file = fs::File::open(path).map_err(|_| "installed S0 chunk is unavailable")?;
        if file
            .metadata()
            .map_err(|_| "S0 chunk metadata unavailable")?
            .len()
            < entry.bytes
        {
            return Err("installed S0 chunk was truncated".into());
        }
        // Tail replay can append to an installed plain segment. Authenticate
        // the exact original prefix, never an unchecked replacement file.
        let mut prefix = file.take(entry.bytes);
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 65536];
        loop {
            let count = prefix
                .read(&mut buffer)
                .map_err(|_| "read installed S0 chunk failed")?;
            if count == 0 {
                break;
            }
            hash.update(&buffer[..count]);
        }
        if fold_db::hex::hex_lower(hash.finalize()) != entry.sha256 {
            return Err("installed S0 chunk digest mismatch".into());
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct Marker {
    store_uuid: String,
    backup_manifest_counter: u64,
    csn_high_water: u64,
    backup_epoch: u64,
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let file = fs::File::open(path).map_err(|_| "restore checkpoint or marker unavailable")?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "restore checkpoint read failed")?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err("restore checkpoint exceeds size limit".into());
    }
    Ok(bytes)
}

fn checksum(checkpoint: &Checkpoint) -> Result<String, String> {
    let mut payload = checkpoint.clone();
    payload.checksum.clear();
    let bytes = serde_json::to_vec(&payload).map_err(|_| "encode checkpoint checksum failed")?;
    Ok(fold_db::hex::sha256_hex(bytes))
}

fn validate(home: &Path, source: &str, checkpoint: &Checkpoint) -> Result<(), String> {
    if checkpoint.checksum != checksum(checkpoint)? {
        return Err("restore checkpoint checksum mismatch".into());
    }
    let marker: Marker =
        serde_json::from_slice(&read_bounded(&home.join("laststore_high_water.json"))?)
            .map_err(|_| "invalid restore high-water marker")?;
    let digest = &checkpoint.manifest_sha256;
    if checkpoint.files.len() > MAX_FILES {
        return Err("S0 checkpoint inventory exceeds limit".into());
    }
    if checkpoint.account_public_key != account_public_key(home)? {
        return Err("destination restore identity changed".into());
    }
    if checkpoint.version != 1
        || checkpoint.source_db_hash != source
        || fold_db::storage::laststore::cloud_db_hash_for_store_uuid(&marker.store_uuid) != source
        || checkpoint.counter == 0
        || marker.backup_manifest_counter != checkpoint.counter
        || marker.backup_epoch != checkpoint.restored_epoch
        || marker.csn_high_water < checkpoint.cut_csn
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err("restore checkpoint does not match source or installed S0 marker".into());
    }
    Ok(())
}

/// Written only after S0 chunk integrity and its high-water commit succeed.
/// A crash before this atomic commit refuses resume rather than guessing S0.
pub(super) fn save(
    home: &Path,
    source: &str,
    report: &LastStoreCloudRestoreReport,
    files: Vec<InstalledFile>,
) -> Result<(), String> {
    if !report.source_scope_verified {
        return Err("restore checkpoint requires verified source scope".into());
    }
    let mut checkpoint = Checkpoint {
        version: 1,
        checksum: String::new(),
        source_db_hash: source.into(),
        account_public_key: account_public_key(home)?,
        files,
        manifest_sha256: report.manifest_sha256.clone(),
        latest_key: report.latest_key.clone(),
        counter: report.counter,
        cut_csn: report.cut_csn,
        restored_epoch: report.restored_epoch,
        manifests_walked: report.manifests_walked,
    };
    checkpoint.checksum = checksum(&checkpoint)?;
    validate(home, source, &checkpoint)?;
    let bytes = serde_json::to_vec(&checkpoint).map_err(|_| "encode restore checkpoint failed")?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err("restore checkpoint exceeds size limit".into());
    }
    let temp = home.join(format!(".restore-s0-checkpoint-{}.tmp", std::process::id()));
    let result = (|| {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)
            .map_err(|_| "create restore checkpoint failed")?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|_| "persist restore checkpoint failed")?;
        fs::rename(&temp, home.join(FILE)).map_err(|_| "commit restore checkpoint failed")?;
        fs::File::open(home)
            .and_then(|dir| dir.sync_all())
            .map_err(|_| "sync restore checkpoint directory failed")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Report the installed S0, even if cloud latest advanced since its installation.
/// Caller must also verify the opened store before mutation-tail replay.
pub(super) fn load(
    home: &Path,
    source: &str,
    source_home: &Path,
) -> Result<LastStoreCloudRestoreReport, String> {
    let checkpoint: Checkpoint = serde_json::from_slice(&read_bounded(&home.join(FILE))?)
        .map_err(|_| "invalid restore checkpoint; use a fresh destination")?;
    validate(home, source, &checkpoint)?;
    if checkpoint.account_public_key != account_public_key(source_home)? {
        return Err("source restore identity changed".into());
    }
    validate_files(home, &checkpoint.files)?;
    Ok(LastStoreCloudRestoreReport {
        manifest_sha256: checkpoint.manifest_sha256,
        latest_key: checkpoint.latest_key,
        counter: checkpoint.counter,
        cut_csn: checkpoint.cut_csn,
        restored_epoch: checkpoint.restored_epoch,
        manifests_walked: checkpoint.manifests_walked,
        chunks_installed: 0,
        bytes_installed: 0,
        chunks_reused: 0,
        bytes_reused: 0,
        source_scope_verified: true,
        remote_read_only: false,
        mutation_log_replay: None,
        mutation_log_snapshot_frontier: None,
    })
}
