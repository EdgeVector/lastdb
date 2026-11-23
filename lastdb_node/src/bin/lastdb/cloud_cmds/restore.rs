//! Offline signed cloud backup restore and source-free credential home recovery.

use super::enrollment_home::FreshHome;
use super::{print_backup, read_stdin_secret, BackupRestoreArgs, RecoverCredentialsArgs};
use base64::Engine as _;
use cloud_backup_protocol::AccountId;
use fold_db::security::Ed25519KeyPair;
use lastdb_node::cloud::{register_native_device, SavedCredentials, CLOUD_CREDENTIALS_FILE};
use lastdb_node::host::{user_hash_from_pubkey, Host, IDENTITY_KEY_FILE};
use lastdb_node::offline_home::DirInode;
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use rustix::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::Zeroizing;

const INTENT: &str = ".native_backup_registration.json";
const RECOVERY_ERRS: [&str; 4] = [
    "registration result is uncertain; retain this credentials home and its device intent; do not retry automatically",
    "registered account differs; retain the device intent",
    "cannot encode paused credentials; retain the device intent",
    "cannot commit paused credentials; retain the device intent",
];

/// Restore the personal database into a new home from the recovery phrase and
/// the relay. `home` is the home named by `--data-dir`, if any. It is used only
/// to find saved cloud credentials for the relay URL.
pub(super) fn run(home: Option<&Path>, args: BackupRestoreArgs) -> Result<(), String> {
    let phrase = read_stdin_secret(4096, "recovery phrase")?;
    let relay_override = args.relay_url;
    let destination = args.into;
    let json = args.json;
    let parent = destination.parent().ok_or("missing recovery parent")?;
    FreshHome::preflight_empty(&destination)?;
    let relay_url = relay_override
        .or_else(lastdb_node::log_sync::explicit_relay_url)
        .or_else(|| home.and_then(lastdb_node::log_sync::relay_base_url))
        .ok_or(
            "signed restore requires --relay-url, LASTDB_RELAY_URL, or saved cloud credentials under --data-dir",
        )?;
    let plan = SignedRestorePlan {
        phrase: &phrase,
        relay_url: &relay_url,
        parent,
        destination: &destination,
    };
    let restored = signed_restore_worker(|| async { restore_into_staged(plan).await })?;
    print_backup(
        &serde_json::json!({
            "ok": true,
            "database_id": restored.database_id,
            "devices": restored.devices,
            "through": restored.through,
            "replayed": restored.replayed,
            "snapshot": restored.snapshot,
            "peak_memory_bytes": peak_memory_bytes(),
            "destination": destination,
        }),
        json,
    )
}

/// The peak resident memory of this process (`VmHWM`), where the system reports it.
fn peak_memory_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

struct SignedRestorePlan<'a> {
    phrase: &'a str,
    relay_url: &'a str,
    parent: &'a Path,
    destination: &'a Path,
}

/// Make a home from the phrase, install the newest snapshot of the relay into it
/// when there is one, read what the streams hold after the snapshot (every entry
/// when there is none), and publish it. The recovery phrase is the only input: the node key,
/// the personal database id and the read key come from it.
async fn restore_into_staged(plan: SignedRestorePlan<'_>) -> Result<Restored, String> {
    let seed = Zeroizing::new(lastdb_node::cloud::seed_from_phrase(plan.phrase)?);
    let staged_path = plan
        .parent
        .join(format!(".lastdb-signed-restore-{}", uuid::Uuid::new_v4()));
    let mut fresh = FreshHome::preflight_empty(&staged_path)?.create()?;
    fresh.write(IDENTITY_KEY_FILE, seed.as_ref())?;
    fresh.commit()?;
    drop(fresh);
    let staged = StagedHome::open(staged_path)?;
    // The newest snapshot of the relay, if there is one, goes in before any store
    // is open on the staged home. Without one the home starts empty.
    let base =
        lastdb_node::log_sync::install_newest_snapshot(&staged.path, &seed, plan.relay_url).await?;
    let host = Arc::new(if base.is_some() {
        Host::boot_for_snapshot_restore(&staged.path).await?
    } else {
        Host::boot_for_signed_restore(&staged.path).await?
    });
    let report =
        lastdb_node::log_sync::restore_personal_database(&host, plan.relay_url, base.as_ref())
            .await;
    let owned_closed = host.shutdown_owned_databases().await;
    let owner_closed = host.db.shutdown().await;
    drop(host);
    let report = report?;
    owned_closed?;
    owner_closed.map_err(|e| e.to_string())?;
    let out = Restored {
        database_id: report.database_id,
        devices: report.devices,
        through: report.through,
        replayed: report.replayed,
        snapshot: base.as_ref().map(|base| {
            serde_json::json!({
                "snapshot_seq": base.snapshot_seq,
                "files": base.files,
                "bytes": base.bytes,
                "streams": base.streams(),
                "download_ms": base.download_ms,
            })
        }),
    };
    staged.publish(plan.destination).map(|()| out)
}

struct Restored {
    database_id: String,
    devices: usize,
    through: u64,
    replayed: u64,
    snapshot: Option<serde_json::Value>,
}

fn signed_restore_worker<T: Send, F>(make_future: impl FnOnce() -> F + Send) -> Result<T, String>
where
    F: std::future::Future<Output = Result<T, String>>,
{
    let stack = fold_db::env_flag::var_or("RUST_MIN_STACK", 0usize).max(64 * 1024 * 1024);
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("lastdb-signed-restore".to_owned())
            .stack_size(stack)
            .spawn_scoped(scope, move || {
                crate::block_on("signed restore", make_future())
            })
            .map_err(|_| "cannot start signed restore worker".to_owned())?
            .join()
            .map_err(|_| "signed restore worker stopped".to_owned())?
    })
}

struct StagedHome {
    path: PathBuf,
    inode: DirInode,
}

impl StagedHome {
    fn open(path: PathBuf) -> Result<Self, String> {
        let inode = DirInode::of_dir(&path).map_err(|_| "the staged recovery home changed")?;
        Ok(Self { path, inode })
    }

    fn publish(&self, destination: &Path) -> Result<(), String> {
        if !self.inode.matches_dir(&self.path) {
            return Err("the staged recovery home changed".into());
        }
        File::open(&self.path)
            .and_then(|f| f.sync_all())
            .map_err(|e| format!("cannot sync the staged recovery home: {e}"))?;
        fs::renameat_with(
            fs::CWD,
            &self.path,
            fs::CWD,
            destination,
            fs::RenameFlags::NOREPLACE,
        )
        .map_err(|e| format!("cannot publish the recovered home: {e}"))?;
        File::open(destination.parent().ok_or("missing recovery parent")?)
            .and_then(|f| f.sync_all())
            .map_err(|e| format!("cannot sync the recovered home parent: {e}"))
    }
}

impl Drop for StagedHome {
    fn drop(&mut self) {
        self.inode.remove_dir_if_owned(&self.path);
    }
}

pub(super) fn recover_credentials_home(
    source: Option<&Path>,
    args: RecoverCredentialsArgs,
) -> Result<(), String> {
    if source.is_some() || !args.identity_seed_stdin {
        return Err("recover-credentials-home requires seed stdin without --data-dir".into());
    }
    let env = Environment::try_from_str(&args.env)
        .map_err(|_| "credential environment must be dev or prod")?;
    let account =
        AccountId::try_from(args.expected_user_hash).map_err(|_| "invalid expected account")?;
    let (seed, pair) = verified_seed_pair(account.as_str())?;
    let mut target = FreshHome::preflight_empty_parent(&args.into)?.create()?;
    let device = uuid::Uuid::new_v4().to_string();
    let intent = serde_json::to_vec(&serde_json::json!({
        "version": 1, "account_id": account, "environment": env.as_str(),
        "device_id": device, "registration_attempted": true,
    }))
    .map_err(|_| "cannot encode registration intent")?;
    target.write(IDENTITY_KEY_FILE, seed.as_ref())?;
    target.write(INTENT, &intent)?;
    target.commit()?;
    register_into(env, &pair, &device, account.as_str(), &mut target)
        .map_err(|i| RECOVERY_ERRS[i])?;
    if args.json {
        let receipt = serde_json::json!({
            "schema": "lastdb.credentials-recovery.v1", "ok": true,
            "environment": env.as_str(), "account_id": account, "device_id": device,
            "credentials_only": true, "cloud_sync_off": true,
            "data_directory_created": false, "local_store_opened": false,
            "api_key_output": false, "identity_seed_output": false,
        });
        serde_json::to_writer(std::io::stdout().lock(), &receipt)
            .map_err(|_| "cannot write recovery receipt")?;
        println!();
    } else {
        println!("Prepared credentials-only home; Cloud Sync stays Off.");
    }
    Ok(())
}

fn register_into(
    env: Environment,
    pair: &Ed25519KeyPair,
    device: &str,
    expected: &str,
    target: &mut FreshHome,
) -> Result<(), usize> {
    let (api_key, user_hash) = crate::block_on(
        "account registration",
        register_native_device(env, pair, device),
    )
    .map_err(|_| 0usize)?;
    if user_hash != expected {
        return Err(1);
    }
    let bytes = SavedCredentials::new(exemem_api_url_for(env), api_key)
        .to_json()
        .map_err(|_| 2usize)?;
    target
        .write(CLOUD_CREDENTIALS_FILE, &bytes)
        .and_then(|()| target.commit())
        .map_err(|_| 3usize)
}

fn verified_seed_pair(
    expected: &str,
) -> Result<(Zeroizing<[u8; 32]>, Ed25519KeyPair), &'static str> {
    let mut seed_buf = Zeroizing::new(Vec::new());
    std::io::stdin()
        .take(129)
        .read_to_end(&mut seed_buf)
        .map_err(|_| "cannot_read_seed_stdin")?;
    if seed_buf.len() > 128 {
        return Err("seed_stdin_exceeds_supported_size");
    }
    let text = std::str::from_utf8(&seed_buf).map_err(|_| "invalid_seed_encoding")?;
    let raw = Zeroizing::new(
        base64::engine::general_purpose::STANDARD
            .decode(text.trim())
            .map_err(|_| "invalid_seed_encoding")?,
    );
    let arr: [u8; 32] = raw.as_slice().try_into().map_err(|_| "invalid_seed_size")?;
    let seed = Zeroizing::new(arr);
    let pair =
        Ed25519KeyPair::from_secret_key(seed.as_ref()).map_err(|_| "invalid_identity_seed")?;
    let hash = user_hash_from_pubkey(&pair.public_key_base64())
        .map_err(|_| "cannot_derive_account_hash")?;
    (hash == expected)
        .then_some((seed, pair))
        .ok_or("identity_account_mismatch")
}
