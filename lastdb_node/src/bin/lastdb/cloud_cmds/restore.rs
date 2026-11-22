//! Offline signed cloud backup restore and source-free credential home recovery.

use super::enrollment_home::FreshHome;
use super::{print_backup, read_stdin_secret, BackupRestoreArgs, RecoverCredentialsArgs};
use base64::Engine as _;
use cloud_backup_protocol::v2::{context::ContextReadRequest, Version};
use cloud_backup_protocol::{AccountId, Id};
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

pub(super) fn run(home: &Path, args: BackupRestoreArgs) -> Result<(), String> {
    let phrase = read_stdin_secret(4096, "recovery phrase")?;
    let database_id = args.database.clone();
    let database = Id::try_from(args.database.clone())
        .map_err(|_| "signed restore requires a 64-hex database id".to_owned())?;
    let device_ids = args.device_ids;
    let relay_override = args.relay_url;
    let generation = args.generation;
    let destination = args.into;
    let json = args.json;
    let parent = destination.parent().ok_or("missing recovery parent")?;
    FreshHome::preflight_empty(&destination)?;
    let relay_url = relay_override
        .or_else(|| lastdb_node::log_sync::relay_base_url(home))
        .ok_or(
            "signed restore requires --relay-url, LASTDB_RELAY_URL, or saved cloud credentials under --data-dir",
        )?;
    let plan = SignedRestorePlan {
        home,
        phrase: &phrase,
        database_id: &database_id,
        device_ids: &device_ids,
        relay_url: &relay_url,
        generation,
        parent,
        destination: &destination,
    };
    let restored = signed_restore_worker(|| async { restore_into_staged(plan).await })?;
    print_backup(
        &serde_json::json!({
            "ok": true, "database_id": database, "name": restored.name,
            "through": restored.through,
            "replayed": restored.replayed,
            "destination": destination,
        }),
        json,
    )
}

struct SignedRestorePlan<'a> {
    home: &'a Path,
    phrase: &'a str,
    database_id: &'a str,
    device_ids: &'a [String],
    relay_url: &'a str,
    generation: Option<u64>,
    parent: &'a Path,
    destination: &'a Path,
}

async fn restore_into_staged(plan: SignedRestorePlan<'_>) -> Result<Restored, String> {
    let account_id = lastdb_node::cloud::account_from_phrase(plan.phrase)?;
    let seed = Zeroizing::new(lastdb_node::cloud::seed_from_phrase(plan.phrase)?);
    let pair = Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| "invalid recovery identity".to_owned())?;
    let mut scope = lastdb_node::cloud::client_from_home_with_keypair(plan.home, Arc::new(pair))?
        .context(&ContextReadRequest {
            version: Version::V2,
            account_id,
            required_destinations: Vec::new(),
        })
        .await
        .map_err(|e| e.to_string())?
        .scope;
    if let Some(generation) = plan.generation {
        scope.account_generation = cloud_backup_protocol::DecimalU64::new(generation);
    }
    let _ = scope; // Phase 1 pulls from the relay; ExeMem scope is account-check only.
    let staged_path = plan
        .parent
        .join(format!(".lastdb-signed-restore-{}", uuid::Uuid::new_v4()));
    let mut fresh = FreshHome::preflight_empty(&staged_path)?.create()?;
    fresh.write(IDENTITY_KEY_FILE, seed.as_ref())?;
    fresh.commit()?;
    drop(fresh);
    let staged = StagedHome::open(staged_path)?;
    let streams = plan
        .device_ids
        .iter()
        .map(|id| lastdb_node::log_sync::OwnedStream {
            device_id: id.clone(),
        })
        .collect();
    let report =
        lastdb_node::log_sync::restore_owned_database(lastdb_node::log_sync::RestoreOwnedParams {
            home: &staged.path,
            relay_url: plan.relay_url.to_owned(),
            seed: &seed,
            database_id: plan.database_id.to_owned(),
            streams,
        })
        .await?;
    // FoldDB already holds winners under home/data; boot validates the layout.
    let host = Arc::new(Host::boot(&staged.path).await?);
    let owned_closed = host.shutdown_owned_databases().await;
    let owner_closed = host.db.shutdown().await;
    drop(host);
    owned_closed?;
    owner_closed.map_err(|e| e.to_string())?;
    let out = Restored {
        name: "recovered".to_owned(),
        through: report.through,
        replayed: report.replayed,
    };
    staged.publish(plan.destination).map(|()| out)
}

struct Restored {
    name: String,
    through: u64,
    replayed: u64,
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
