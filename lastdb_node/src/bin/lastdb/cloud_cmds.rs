//! Explicit V2 backup and credentials-only recovery-home commands.
use super::*;
use fold_db::security::Ed25519KeyPair;
use lastdb_node::cloud::{register_native_device, SavedCredentials, CLOUD_CREDENTIALS_FILE};
use lastdb_node::host::IDENTITY_KEY_FILE;
use lastdb_node::offline_home::{
    dir_entry_names, entry_present, open_directory_excluding, open_private_at, read_private_file,
    read_private_file_bound, require_absolute, require_mode_0700_directory,
    require_owned_directory, same_inode, DIR_FLAGS,
};
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use rustix::fs::{self, AtFlags, Mode, OFlags};
use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use zeroize::Zeroizing;

#[path = "cloud_cmds/offline.rs"]
mod offline;
#[path = "cloud_cmds/recovery_home.rs"]
mod recovery_home;
#[path = "cloud_cmds/setup.rs"]
mod setup_files;

const PHRASE_FILE: &str = "recovery-phrase.txt";

pub(super) fn cloud_command(data_dir: Option<PathBuf>, action: CloudCommand) -> Result<(), String> {
    let CloudCommand::BackupV2 { action } = action else {
        return recovery_home::command(data_dir.as_deref(), action);
    };
    if matches!(
        action,
        ByteBackupCommand::Init(_) | ByteBackupCommand::Connect(_)
    ) {
        return offline::run_setup(data_dir.as_deref(), &action);
    }
    if data_dir.as_deref().is_none_or(|h| !h.is_absolute()) {
        return Err("V2 backup requires an explicit absolute --data-dir home".into());
    }
    offline::run_byte_backup(&lastdb_node::host::resolve_home(data_dir)?, action)
}

fn connect(home: &Path, invite_stdin: bool, restored: bool, json: bool) -> Result<(), String> {
    let dir = setup_files::PreparedHome::open(home, restored)?;
    let seed = dir.seed()?;
    let pair = Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| "invalid native backup identity")?;
    let account = lastdb_node::identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    let api_url = exemem_api_url_for(Environment::Dev);
    if restored {
        dir.device_id()?;
    }
    let emit = |registered| {
        print_backup(
            &serde_json::json!({
                "schema": "lastdb.native-backup-connect.v1", "ok": true, "environment": "dev",
                "account_id": account, "home": home, "credential_file": CLOUD_CREDENTIALS_FILE,
                "cloud_sync_off": true, "local_store_opened": restored,
                "local_store_read_only": restored, "database_writer_opened": false,
                "registration_attempted": registered, "credentials_saved": true,
                "api_key_output": false, "restored_home": restored, "automatic_backup_enabled": false,
            }),
            json,
        )
    };
    if dir.finish_saved_credentials(api_url)? {
        return emit(false);
    }
    let invite = invite_stdin
        .then(|| read_stdin_no_control(4096, "native enrollment invite"))
        .transpose()?;
    let device_id = dir.device_id()?;
    let fut = register_native_device(
        Environment::Dev,
        &pair,
        &device_id,
        invite.as_deref().map(String::as_str),
    );
    let (api_key, user_hash) = block_on_cloud("native enrollment", fut).map_err(|e| {
        format!("{e}; registration may have committed; retry only by an explicit request")
    })?;
    if user_hash != account
        || dir.seed()?.as_ref() != seed.as_ref()
        || dir.device_id()? != device_id
    {
        return Err("native enrollment identity changed; registration may have committed".into());
    }
    let creds = SavedCredentials::new(api_url, api_key)
        .key_json()
        .map_err(|_| "cannot encode paused native credentials")?;
    dir.save_credentials(&creds)
        .map_err(|e| format!("{e}; registration may have committed; retain this home"))?;
    emit(true)
}

pub(super) fn block_on_cloud<T>(
    label: &str,
    future: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| format!("cannot construct {label} runtime"))?
        .block_on(future)
}

fn print_backup(value: &serde_json::Value, json: bool) -> Result<(), String> {
    if !json {
        println!("V2 backup");
    }
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|_| "cannot encode V2 result")?
    );
    Ok(())
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

fn require_disjoint(source: &Path, destination: &Path) -> Result<(), String> {
    require_absolute(source)?;
    require_absolute(destination)?;
    (!source.starts_with(destination) && !destination.starts_with(source))
        .then_some(())
        .ok_or_else(|| "source and recovery home paths must not overlap".into())
}

pub(super) struct FreshHome {
    parent: File,
    name: OsString,
    directory: Option<File>,
    created_files: Vec<(&'static str, File, Zeroizing<Vec<u8>>)>,
    committed: bool,
}

impl FreshHome {
    pub(super) fn preflight_empty(destination: &Path) -> Result<Self, String> {
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
        let tmp = Path::new(if cfg!(target_os = "macos") {
            "/private/tmp"
        } else {
            "/tmp"
        });
        let parent = destination.parent().ok_or("missing DEV recovery parent")?;
        if parent.parent() != Some(tmp)
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

    pub(super) fn create(mut self) -> Result<Self, String> {
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

    pub(super) fn write(&mut self, name: &'static str, bytes: &[u8]) -> Result<(), String> {
        let mut file = open_private_at(
            self.dir(),
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL,
            Mode::from_raw_mode(0o600),
            "cannot exclusively create recovery credential file",
        )?;
        let clone = file
            .try_clone()
            .map_err(|_| "cannot retain new credential file")?;
        self.created_files
            .push((name, clone, Zeroizing::new(bytes.to_vec())));
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| "cannot write recovery credential file")?;
        let observed = Zeroizing::new(read_private_file(self.dir(), name, bytes.len() as u64)?);
        (observed.as_slice() == bytes)
            .then_some(())
            .ok_or_else(|| "recovery credential read-back does not match".into())
    }

    pub(super) fn commit(&mut self) -> Result<(), String> {
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
                .any(|n| !self.created_files.iter().any(|(f, _, _)| f.as_bytes() == n))
        {
            return Err(EXACT_ERR.into());
        }
        for (name, fd, expected) in &self.created_files {
            let bytes = Zeroizing::new(read_private_file_bound(
                dir,
                name,
                expected.len() as u64,
                Some(fd),
            )?);
            if bytes.as_slice() != expected.as_slice() {
                return Err("final recovery credential contents do not match".into());
            }
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
        for (name, fd, _) in &self.created_files {
            if same_inode(self.dir(), *name, fd) {
                let _ = fs::unlinkat(self.dir(), *name, AtFlags::empty());
            }
        }
        if same_inode(&self.parent, &self.name, self.dir()) {
            let _ = fs::unlinkat(&self.parent, &self.name, AtFlags::REMOVEDIR);
        }
    }
}
