//! Offline exact authority diagnosis on an explicitly closed owned copy.
//! Ordinary LastStore open may create a layout descriptor or repair a torn tail.

use fold_db::crypto::{CryptoProvider, E2eKeys, LocalCryptoProvider};
use fold_db::db_operations::schema_store::SchemaStore;
use fold_db::storage::{EncryptingNamespacedStore, LastStoreNamespacedStore, NamespacedStore};
use std::fs::{File, Metadata, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(super) type FileIdentity = (u32, u32, u64, u64, u64, u64, i64, i64, i64, i64);
pub(super) type DirectoryIdentity = (u32, u32, u64, u64);

fn file_identity(meta: &Metadata) -> FileIdentity {
    (
        meta.uid(),
        meta.mode(),
        meta.dev(),
        meta.ino(),
        meta.nlink(),
        meta.size(),
        meta.mtime(),
        meta.mtime_nsec(),
        meta.ctime(),
        meta.ctime_nsec(),
    )
}

fn directory(path: &Path) -> Result<DirectoryIdentity, String> {
    let meta = std::fs::symlink_metadata(path).map_err(|_| "OWNED_DIRECTORY_UNAVAILABLE")?;
    if !meta.is_dir()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o022 != 0
        || meta.mode() & 0o500 != 0o500
        || path
            .canonicalize()
            .map_err(|_| "OWNED_DIRECTORY_UNAVAILABLE")?
            != path
    {
        return Err("OWNED_DIRECTORY_REFUSED".into());
    }
    Ok((meta.uid(), meta.mode(), meta.dev(), meta.ino()))
}

fn require_absent(path: &Path) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => Err("COPY_SOCKET_OR_ACTIVE_CONFIG_PRESENT".into()),
    }
}

pub(super) fn guard_home(home: &Path) -> Result<Vec<DirectoryIdentity>, String> {
    let temp = std::env::temp_dir()
        .canonicalize()
        .map_err(|_| "TEMP_ROOT_UNAVAILABLE")?;
    let private_tmp = Path::new("/private/tmp");
    if !home.is_absolute()
        || !(home.starts_with(&temp) || home.starts_with(private_tmp))
        || home == temp
        || home == private_tmp
    {
        return Err("EXPLICIT_TEMPORARY_COPY_REQUIRED".into());
    }
    let parent = home.parent().ok_or("OWNED_PARENT_REQUIRED")?;
    let guards = vec![
        directory(parent)?,
        directory(home)?,
        directory(&home.join("data"))?,
    ];
    for name in [
        "data/folddb.sock",
        "data/folddb-full.sock",
        "cloud_sync.json",
    ] {
        require_absent(&home.join(name))?;
    }
    Ok(guards)
}

fn schema_name() -> Result<String, String> {
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(4098)
        .read_to_end(&mut bytes)
        .map_err(|_| "SCHEMA_INPUT_FAILED")?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
    }
    if bytes.is_empty()
        || bytes.len() > 4096
        || bytes.iter().any(|b| *b == 0 || *b == b'\n' || *b == b'\r')
    {
        return Err("SCHEMA_INPUT_REFUSED".into());
    }
    String::from_utf8(bytes).map_err(|_| "SCHEMA_INPUT_NOT_UTF8".into())
}

pub(super) fn identity_key(home: &Path) -> Result<(File, FileIdentity, E2eKeys), String> {
    let path = home.join("identity.key");
    let before = std::fs::symlink_metadata(&path).map_err(|_| "IDENTITY_UNAVAILABLE")?;
    if !before.is_file()
        || before.uid() != unsafe { libc::geteuid() }
        || before.mode() & 0o777 != 0o600
        || before.nlink() != 1
        || before.size() != 32
    {
        return Err("IDENTITY_METADATA_REFUSED".into());
    }
    let expected = file_identity(&before);
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&path)
        .map_err(|_| "IDENTITY_OPEN_FAILED")?;
    let mut seed = [0u8; 32];
    let result = (|| {
        if file_identity(&file.metadata().map_err(|_| "IDENTITY_METADATA_FAILED")?) != expected {
            return Err("IDENTITY_CHANGED".into());
        }
        file.read_exact(&mut seed)
            .map_err(|_| "IDENTITY_READ_FAILED")?;
        let keys = E2eKeys::from_ed25519_seed(&seed).map_err(|_| "IDENTITY_DERIVATION_FAILED")?;
        identity_unchanged(home, &file, expected)?;
        Ok((file, expected, keys))
    })();
    seed.fill(0);
    result
}

pub(super) fn identity_unchanged(
    home: &Path,
    file: &File,
    expected: FileIdentity,
) -> Result<(), String> {
    if file_identity(&file.metadata().map_err(|_| "IDENTITY_METADATA_FAILED")?) != expected
        || file_identity(
            &std::fs::symlink_metadata(home.join("identity.key"))
                .map_err(|_| "IDENTITY_METADATA_FAILED")?,
        ) != expected
    {
        return Err("IDENTITY_CHANGED".into());
    }
    Ok(())
}

pub(super) fn run(
    data_dir: Option<PathBuf>,
    schema_name_stdin: bool,
    closed_owned_copy: bool,
) -> Result<(), String> {
    if !schema_name_stdin || !closed_owned_copy {
        return Err("EXPLICIT_ACK_REQUIRED".into());
    }
    let home = data_dir.ok_or("EXPLICIT_DATA_DIR_REQUIRED")?;
    let guards = guard_home(&home)?;
    let name = schema_name()?;
    let (seed_file, seed_guard, keys) = identity_key(&home)?;
    let opts = laststore::LastStoreOptions::hash_group_frame_aead(keys.encryption_key())
        .with_hash_group_key(laststore::HashGroupKey::PartitionPrefix);
    let raw: Arc<dyn NamespacedStore> = Arc::new(
        LastStoreNamespacedStore::open_with_options(&home.join("data"), opts)
            .map_err(|_| "STORE_OPEN_FAILED")?,
    );
    let crypto: Arc<dyn CryptoProvider> =
        Arc::new(LocalCryptoProvider::from_key(keys.encryption_key()));
    let wrapped: Arc<dyn NamespacedStore> = Arc::new(EncryptingNamespacedStore::new(raw, crypto));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "RUNTIME_UNAVAILABLE")?;
    let report = runtime
        .block_on(SchemaStore::probe_replay_authority(
            wrapped,
            &name,
            &keys.encryption_key(),
        ))
        .map_err(String::from)?;
    identity_unchanged(&home, &seed_file, seed_guard)?;
    if guard_home(&home)? != guards {
        return Err("OWNED_DIRECTORY_CHANGED".into());
    }
    let public = serde_json::json!({"schema": "lastdb_replay_authority_probe_v1", "ok": true,
        "cache_observed": false, "schema_heal_attempted": false, "authority_write_attempted": false,
        "cloud_calls": 0, "ordinary_store_open_may_write_layout_or_repair_tail": true, "authority": report});
    println!(
        "{}",
        serde_json::to_string(&public).map_err(|_| "PUBLIC_REPORT_FAILED")?
    );
    Ok(())
}
