//! Signed database restore, backup connection setup, and recovery-home commands.
use super::*;
use cloud_backup_protocol::v2::{context::ContextReadRequest, Version};
use cloud_backup_protocol::Id;
use lastdb_node::host::{Host, IDENTITY_KEY_FILE};
use lastdb_node::offline_home::{
    dir_entry_names, entry_present, open_directory_excluding, open_private_at, require_absolute,
    require_mode_0700_directory, require_owned_directory, same_inode, DIR_FLAGS,
};
use rustix::fs::{self, AtFlags, Mode, OFlags};
use std::ffi::OsString;
use std::fs::File;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::sync::Arc;
use zeroize::Zeroizing;

#[path = "cloud_cmds/recovery_home.rs"]
mod recovery_home;
#[path = "cloud_cmds/setup.rs"]
mod setup;
#[path = "cloud_cmds/snapshot_wait.rs"]
mod snapshot_wait;

pub(super) fn cloud_command(data_dir: Option<PathBuf>, action: CloudCommand) -> Result<(), String> {
    let CloudCommand::BackupV2 { action } = action else {
        return recovery_home::command(data_dir.as_deref(), action);
    };
    match action {
        ByteBackupCommand::Init(a) => {
            if data_dir.is_some() {
                return Err("native backup init uses --into without --data-dir".into());
            }
            setup::initialize(&a.into, &a.recovery_phrase_out, a.json)
        }
        ByteBackupCommand::Connect(a) if a.existing => {
            if !a.api_key_stdin || a.dev || a.invite_code_stdin {
                return Err(
                    "existing connection requires only an environment and API key stdin".into(),
                );
            }
            let home = data_dir
                .as_deref()
                .ok_or("existing connection requires an explicit --data-dir")?;
            let env = a
                .env
                .as_deref()
                .ok_or("existing connection requires --env")?;
            setup::connect_existing(home, env, a.expected_user_hash.as_deref(), a.json)
        }
        ByteBackupCommand::Connect(a) => {
            if !a.dev || a.env.is_some() || a.api_key_stdin || a.expected_user_hash.is_some() {
                return Err("native backup connect requires --dev".into());
            }
            let home = data_dir
                .as_deref()
                .ok_or("native backup connect requires an explicit --data-dir")?;
            setup::connect(home, a.invite_code_stdin, a.json)
        }
        ByteBackupCommand::Restore(args) => {
            if data_dir.as_deref().is_none_or(|h| !h.is_absolute()) {
                return Err("V2 backup requires an explicit absolute --data-dir home".into());
            }
            run_restore(&lastdb_node::host::resolve_home(data_dir)?, args)
        }
        ByteBackupCommand::Snapshot { database, wait } => queue_snapshot(data_dir, &database, wait),
    }
}

fn queue_snapshot(data_dir: Option<PathBuf>, database: &str, wait: bool) -> Result<(), String> {
    let (home, socket) = resolve_client_home_and_socket(data_dir)?;
    require_reachable(&socket)?;
    let (id, nonce) = lastdb_node::log_sync::request_snapshot(&home, database)?;
    if wait {
        return snapshot_wait::until_head(&home, &id, &nonce);
    }
    println!("Queued signed snapshot for {id}");
    Ok(())
}

struct FreshHome {
    parent: File,
    name: OsString,
    directory: Option<File>,
    created_files: Vec<&'static str>,
    committed: bool,
}

impl FreshHome {
    fn preflight_empty(destination: &Path) -> Result<Self, String> {
        Self::preflight(destination, None)
    }

    fn preflight_empty_parent(destination: &Path) -> Result<Self, String> {
        let target = Self::preflight_empty(destination)?;
        require_mode_0700_directory(
            &target.parent,
            "credential parent must be an owner-only directory",
        )?;
        if !dir_entry_names(&target.parent, "cannot inspect credential parent")?.is_empty() {
            return Err("credential parent must be empty".into());
        }
        Ok(target)
    }

    fn preflight_source_free_dev(destination: &Path) -> Result<Self, String> {
        let tmp = if cfg!(target_os = "macos") {
            "/private/tmp"
        } else {
            "/tmp"
        };
        let parent = destination.parent().ok_or("missing DEV recovery parent")?;
        if parent.parent() != Some(Path::new(tmp))
            || parent
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .is_none_or(|n| !n.starts_with("lastdb-dev-recovery."))
        {
            return Err("DEV recovery requires a fresh private temporary parent".into());
        }
        Self::preflight_empty_parent(destination)
    }

    fn preflight(destination: &Path, source: Option<&File>) -> Result<Self, String> {
        require_absolute(destination)?;
        let name = destination
            .file_name()
            .ok_or("destination requires a home name")?;
        let parent = open_directory_excluding(
            destination.parent().ok_or("missing destination parent")?,
            source,
        )?;
        let mode = parent
            .metadata()
            .map_err(|_| "cannot inspect destination parent")?
            .permissions()
            .mode();
        if mode & 0o022 != 0 && mode & 0o1000 == 0 {
            return Err(
                "destination parent must prevent other users from replacing its entries".into(),
            );
        }
        if entry_present(&parent, name.to_str().ok_or("invalid home name")?)? {
            return Err("recovery home must not already exist".into());
        }
        Ok(Self {
            parent,
            name: name.to_os_string(),
            directory: None,
            created_files: Vec::new(),
            committed: false,
        })
    }

    fn create(mut self) -> Result<Self, String> {
        fs::mkdirat(&self.parent, &self.name, Mode::from_raw_mode(0o700))
            .map_err(|_| "cannot exclusively create recovery home")?;
        let dir: File = fs::openat(&self.parent, &self.name, DIR_FLAGS, Mode::empty())
            .map_err(|_| "cannot open new recovery home without symlinks")?
            .into();
        fs::fchmod(&dir, Mode::from_raw_mode(0o700)).map_err(|_| "cannot protect recovery home")?;
        require_owned_directory(&dir)?;
        self.directory = Some(dir);
        Ok(self)
    }

    fn dir(&self) -> &File {
        self.directory.as_ref().expect("created recovery home")
    }

    fn write(&mut self, name: &'static str, bytes: &[u8]) -> Result<(), String> {
        let mut file = open_private_at(
            self.dir(),
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL,
            Mode::from_raw_mode(0o600),
            "cannot exclusively create recovery credential file",
        )?;
        self.created_files.push(name);
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| "cannot write recovery credential file".into())
    }

    fn commit(&mut self) -> Result<(), String> {
        let dir = self.dir();
        if !same_inode(&self.parent, &self.name, dir) {
            return Err("recovery home changed during preparation".into());
        }
        const EXACT_ERR: &str = "recovery home requires the exact protected credential files";
        require_mode_0700_directory(dir, EXACT_ERR)?;
        let entries = dir_entry_names(dir, "cannot inspect final recovery-home entries")?;
        if self.created_files.is_empty()
            || entries.len() != self.created_files.len()
            || entries
                .iter()
                .any(|n| !self.created_files.iter().any(|f| f.as_bytes() == n))
        {
            return Err(EXACT_ERR.into());
        }
        dir.sync_all()
            .and_then(|()| self.parent.sync_all())
            .map_err(|_| "cannot sync recovery home".to_owned())?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for FreshHome {
    fn drop(&mut self) {
        if self.committed || self.directory.is_none() {
            return;
        }
        for name in &self.created_files {
            let _ = fs::unlinkat(self.dir(), *name, AtFlags::empty());
        }
        if same_inode(&self.parent, &self.name, self.dir()) {
            let _ = fs::unlinkat(&self.parent, &self.name, AtFlags::REMOVEDIR);
        }
    }
}

fn run_restore(home: &Path, args: BackupRestoreArgs) -> Result<(), String> {
    let phrase = read_stdin_secret(4096, "recovery phrase")?;
    let database = Id::try_from(args.database)
        .map_err(|_| "signed restore requires a canonical database id".to_owned())?;
    let destination = args.into;
    let parent = destination.parent().ok_or("missing recovery parent")?;
    FreshHome::preflight_empty(&destination)?;
    let restored = signed_restore_worker(|| async {
        let account_id = lastdb_node::cloud::account_from_phrase(&phrase)?;
        let mut scope = lastdb_node::cloud::client_from_home(home)?
            .context(&ContextReadRequest {
                version: Version::V2,
                account_id,
                required_destinations: Vec::new(),
            })
            .await
            .map_err(|e| e.to_string())?
            .scope;
        if let Some(generation) = args.generation {
            scope.account_generation = cloud_backup_protocol::DecimalU64::new(generation);
        }
        let staged_path = parent.join(format!(".lastdb-signed-restore-{}", uuid::Uuid::new_v4()));
        let seed = Zeroizing::new(lastdb_node::cloud::seed_from_phrase(&phrase)?);
        let mut fresh = FreshHome::preflight_empty(&staged_path)?.create()?;
        fresh.write(IDENTITY_KEY_FILE, seed.as_ref())?;
        fresh.commit()?;
        drop(fresh);
        let staged = StagedHome::open(staged_path)?;
        let base = lastdb_node::log_sync::restore_snapshot_base(
            &staged.path,
            home,
            &scope,
            &database,
            &seed,
        )
        .await?;
        let host = Arc::new(if base.is_some() {
            Host::boot_for_snapshot_restore(&staged.path).await?
        } else {
            Host::boot_for_signed_restore(&staged.path).await?
        });
        let res =
            lastdb_node::log_sync::recover_owned(&host, home, scope, &database, "recovered", base)
                .await;
        let owned_closed = host.shutdown_owned_databases().await;
        let owner_closed = host.db.shutdown().await;
        drop(host);
        let out = res?;
        owned_closed?;
        owner_closed.map_err(|e| e.to_string())?;
        staged.publish(&destination).map(|()| out)
    })?;
    snapshot_wait::print_backup(
        &serde_json::json!({
            "ok": true, "database_id": database, "name": restored.name,
            "through_delivery_seq": restored.through,
            "snapshot_through": restored.snapshot_through, "replayed": restored.replayed,
            "destination": destination,
        }),
        args.json,
    )
}

// Replay uses the same native write path as the daemon. Run it off the
// process entry thread with the same stack allowance as a daemon worker.
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
                block_on_cloud("signed restore", make_future())
            })
            .map_err(|_| "cannot start signed restore worker".to_owned())?
            .join()
            .map_err(|_| "signed restore worker stopped".to_owned())?
    })
}

struct StagedHome {
    path: PathBuf,
    inode: (u64, u64),
}

impl StagedHome {
    fn open(path: PathBuf) -> Result<Self, String> {
        let m = std::fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
        if !m.is_dir() {
            return Err("the staged recovery home changed".into());
        }
        Ok(Self {
            path,
            inode: (m.dev(), m.ino()),
        })
    }

    fn owns_path(&self) -> bool {
        std::fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.is_dir() && (m.dev(), m.ino()) == self.inode)
    }

    fn publish(&self, destination: &Path) -> Result<(), String> {
        if !self.owns_path() {
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
        if self.owns_path() {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

fn block_on_cloud<T>(
    label: &str,
    future: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| format!("cannot construct {label} runtime"))?
        .block_on(future)
}

fn read_stdin_no_control(limit: usize, label: &str) -> Result<Zeroizing<String>, String> {
    let text = read_stdin_secret(limit, label)?;
    (!text.chars().any(char::is_control))
        .then_some(text)
        .ok_or_else(|| format!("{label} is invalid"))
}

fn read_stdin_secret(limit: usize, label: &str) -> Result<Zeroizing<String>, String> {
    let mut bytes = Zeroizing::new(Vec::new());
    std::io::stdin()
        .lock()
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| format!("cannot read {label}"))?;
    if bytes.len() > limit {
        return Err(format!("{label} exceeds its byte limit"));
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| format!("{label} must be UTF-8"))?
        .trim();
    (!text.is_empty())
        .then(|| Zeroizing::new(text.to_owned()))
        .ok_or_else(|| format!("{label} is empty"))
}
