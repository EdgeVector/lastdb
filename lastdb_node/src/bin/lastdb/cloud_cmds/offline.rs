//! Offline native backup setup and signed owner-prefix home restore.
use super::*;
use cloud_backup_protocol::v2::{context::ContextReadRequest, Version};
use cloud_backup_protocol::{CloudScope, DecimalU64, Id};
use lastdb_node::Host;
use std::fs::{self as std_fs, File};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::Zeroizing;

pub(super) fn run_byte_backup(home: &Path, action: ByteBackupCommand) -> Result<(), String> {
    match action {
        ByteBackupCommand::Restore(args) => restore(home, args),
        _ => Err("invalid offline backup command".into()),
    }
}

pub(super) fn run_setup(source: Option<&Path>, action: &ByteBackupCommand) -> Result<(), String> {
    if let ByteBackupCommand::Connect(a) = action {
        if a.existing {
            if !a.api_key_stdin || a.dev || a.invite_code_stdin || a.restored {
                return Err(
                    "existing connection requires only an environment and API key stdin".into(),
                );
            }
            let home = source.ok_or("existing connection requires an explicit --data-dir")?;
            let env = a
                .env
                .as_deref()
                .ok_or("existing connection requires --env")?;
            return super::recovery_home::run_existing(
                home,
                env,
                a.expected_user_hash.as_deref(),
                a.json,
            );
        }
    }
    lastdb_node::byte_cloud_backup::require_enabled().map_err(|e| e.to_string())?;
    match action {
        ByteBackupCommand::Init(_) if source.is_some() => {
            Err("native backup init uses --into without --data-dir".into())
        }
        ByteBackupCommand::Init(a) => initialize(&a.into, &a.recovery_phrase_out, a.json),
        ByteBackupCommand::Connect(a) => {
            if !a.dev || a.env.is_some() || a.api_key_stdin || a.expected_user_hash.is_some() {
                return Err("native backup connect requires --dev".into());
            }
            if a.restored && a.invite_code_stdin {
                return Err("restored account reconnect does not accept a new invite".into());
            }
            super::connect(
                source.ok_or("native backup connect requires an explicit --data-dir")?,
                a.invite_code_stdin,
                a.restored,
                a.json,
            )
        }
        _ => Err("invalid native backup setup command".into()),
    }
}

fn initialize(destination: &Path, phrase_path: &Path, json: bool) -> Result<(), String> {
    if phrase_path != destination.join(PHRASE_FILE) {
        return Err("--recovery-phrase-out must name <into>/recovery-phrase.txt".into());
    }
    let target = FreshHome::preflight_empty(destination)?;
    let pair = Ed25519KeyPair::generate().map_err(|_| "cannot create fresh native identity")?;
    let seed = Zeroizing::new(pair.secret_key_bytes());
    let phrase = Zeroizing::new(
        bip39::Mnemonic::from_entropy(seed.as_ref())
            .map_err(|_| "cannot encode fresh native recovery phrase")?
            .to_string(),
    );
    let account = lastdb_node::identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    let mut target = target.create()?;
    target.write(IDENTITY_KEY_FILE, seed.as_ref())?;
    target.write(PHRASE_FILE, phrase.as_bytes())?;
    target.commit()?;
    print_backup(
        &serde_json::json!({
            "schema": "lastdb.native-backup-init.v1", "ok": true, "environment": "dev",
            "account_id": account, "home": destination, "recovery_phrase_file": phrase_path,
            "cloud_sync_off": true, "local_store_opened": false, "network_request": false,
            "identity_seed_output": false, "recovery_phrase_output": false,
        }),
        json,
    )
}

fn restore(home: &Path, args: BackupRestoreArgs) -> Result<(), String> {
    let phrase = super::read_stdin_secret(4096, "recovery phrase")?;
    super::block_on_cloud("signed recovery", async {
        let account_id = lastdb_node::backup_access::account_from_phrase(&phrase)?;
        let client = lastdb_node::byte_cloud_backup::client_from_home(home)?;
        let request = ContextReadRequest {
            version: Version::V2,
            account_id,
            required_destinations: Vec::new(),
        };
        let mut scope = client
            .context(&request)
            .await
            .map_err(|e| e.to_string())?
            .scope;
        if let Some(value) = args.generation {
            scope.account_generation = DecimalU64::new(value);
        }
        restore_signed(home, scope, &phrase, args).await
    })
}

async fn restore_signed(
    connection_home: &Path,
    scope: CloudScope,
    phrase: &str,
    args: BackupRestoreArgs,
) -> Result<(), String> {
    let destination = args.into;
    let database =
        Id::try_from(args.database).map_err(|_| "database ID must be a canonical UUID")?;
    let parent = destination.parent().ok_or("missing recovery parent")?;
    FreshHome::preflight_empty(&destination)?;
    let staged_path = parent.join(format!(".lastdb-signed-restore-{}", uuid::Uuid::new_v4()));
    let seed = Zeroizing::new(lastdb_node::backup_access::seed_from_phrase(phrase)?);
    let mut fresh = FreshHome::preflight_empty(&staged_path)?.create()?;
    fresh.write(IDENTITY_KEY_FILE, seed.as_ref())?;
    fresh.commit()?;
    drop(fresh);
    let staged = StagedHome::open(staged_path)?;
    let host = Arc::new(Host::boot(&staged.path).await?);
    let result =
        lastdb_node::log_sync::recover_owned(&host, connection_home, scope, &database, "recovered")
            .await;
    let owned_closed = host.shutdown_owned_databases().await;
    let owner_closed = host.db.shutdown().await;
    drop(host);
    let through = result?;
    owned_closed?;
    owner_closed.map_err(|e| e.to_string())?;
    staged.publish(&destination)?;
    print_backup(
        &serde_json::json!({
            "ok": true,
            "database_id": database,
            "name": "recovered",
            "through_delivery_seq": through,
            "destination": destination,
        }),
        args.json,
    )
}

struct StagedHome {
    path: PathBuf,
    inode: (u64, u64),
}

impl StagedHome {
    fn open(path: PathBuf) -> Result<Self, String> {
        let meta = std_fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
        if !meta.is_dir() {
            return Err("the staged recovery home changed".into());
        }
        Ok(Self {
            path,
            inode: (meta.dev(), meta.ino()),
        })
    }

    fn owns_path(&self) -> bool {
        std_fs::symlink_metadata(&self.path)
            .is_ok_and(|meta| meta.is_dir() && (meta.dev(), meta.ino()) == self.inode)
    }

    fn publish(&self, destination: &Path) -> Result<(), String> {
        if !self.owns_path() {
            return Err("the staged recovery home changed".into());
        }
        File::open(&self.path)
            .and_then(|f| f.sync_all())
            .map_err(|e| format!("cannot sync the staged recovery home: {e}"))?;
        rustix::fs::renameat_with(
            rustix::fs::CWD,
            &self.path,
            rustix::fs::CWD,
            destination,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(|e| format!("cannot publish the recovered home: {e}"))?;
        File::open(destination.parent().ok_or("missing recovery parent")?)
            .and_then(|f| f.sync_all())
            .map_err(|e| format!("cannot sync the recovered home parent: {e}"))
    }
}

impl Drop for StagedHome {
    fn drop(&mut self) {
        if self.owns_path() {
            let _ = std_fs::remove_dir_all(&self.path);
        }
    }
}
