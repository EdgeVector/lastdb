//! Descriptor-bound fresh-home enrollment and existing-account connection setup.
use super::{
    print_backup, read_stdin_no_control, CLOUD_CREDENTIALS_FILE, IDENTITY_KEY_FILE, PHRASE_FILE,
};
use cloud_backup_protocol::{AccountId, Id};
use fold_db::security::Ed25519KeyPair;
use lastdb_node::cloud::{SavedConnection, SavedCredentials};
use lastdb_node::identity::user_hash_from_pubkey;
use lastdb_node::offline_home::{
    dir_entry_names, entry_present, open_directory, open_private_at, read_private_file,
    require_mode_0700_directory, require_owned_directory, same_inode,
};
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use rustix::fs::{self, Mode, OFlags};
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use zeroize::Zeroizing;

const DEVICE_FILE: &str = ".native_backup_device_id";
const LOCK_FILE: &str = ".native_backup_enrollment.lock";
const PENDING_FILE: &str = ".native_backup_credentials.pending";
const FILE_ERR: &str = "native enrollment file must be a private regular owner file";
const MODE_ERR: &str = "native enrollment directory must be owner-only with mode0700";
const ALLOWED: [&str; 6] = [
    IDENTITY_KEY_FILE,
    PHRASE_FILE,
    DEVICE_FILE,
    LOCK_FILE,
    PENDING_FILE,
    CLOUD_CREDENTIALS_FILE,
];

pub(super) struct PreparedHome {
    parent: File,
    name: OsString,
    directory: File,
    _lock: File,
}

impl PreparedHome {
    pub(super) fn open(home: &Path) -> Result<Self, String> {
        let parent = open_directory(home.parent().ok_or("native enrollment needs a home")?)?;
        require_owned_directory(&parent)?;
        let directory = open_directory(home)?;
        require_mode_0700_directory(&directory, MODE_ERR)?;
        let name = home
            .file_name()
            .ok_or("native enrollment needs an explicit home name")?
            .to_os_string();
        let flags = OFlags::RDWR | OFlags::CREATE | OFlags::NONBLOCK;
        let _lock = open_private_at(
            &directory,
            LOCK_FILE,
            flags,
            Mode::from_raw_mode(0o600),
            FILE_ERR,
        )?;
        _lock
            .try_lock()
            .map_err(|_| "native enrollment is already active for this home")?;
        let prepared = Self {
            parent,
            name,
            directory,
            _lock,
        };
        prepared.verify_entries()?;
        prepared.seed().map(|_| prepared)
    }

    fn verify_entries(&self) -> Result<(), String> {
        if !same_inode(&self.parent, &self.name, &self.directory) {
            return Err("selected native enrollment home changed".into());
        }
        let entries = dir_entry_names(&self.directory, "cannot inspect native enrollment home")?;
        if entries.len() > ALLOWED.len()
            || entries
                .iter()
                .any(|n| !ALLOWED.iter().any(|a| a.as_bytes() == n))
        {
            return Err("native enrollment home contains a store or an unexpected entry".into());
        }
        Ok(())
    }

    pub(super) fn seed(&self) -> Result<Zeroizing<[u8; 32]>, String> {
        if !same_inode(&self.parent, &self.name, &self.directory) {
            return Err("selected native enrollment home changed".into());
        }
        let seed: Zeroizing<[u8; 32]> = read_private_file(&self.directory, IDENTITY_KEY_FILE, 32)?
            .as_slice()
            .try_into()
            .map(Zeroizing::new)
            .map_err(|_| "native identity must contain 32 bytes".to_owned())?;
        let words = Zeroizing::new(read_private_file(&self.directory, PHRASE_FILE, 4096)?);
        let phrase =
            std::str::from_utf8(&words).map_err(|_| "invalid protected recovery phrase")?;
        let recovered = Zeroizing::new(
            lastdb_node::cloud::seed_from_phrase(phrase)
                .map_err(|_| "invalid protected recovery phrase")?,
        );
        (recovered.as_ref() == seed.as_ref())
            .then_some(seed)
            .ok_or_else(|| "protected recovery phrase does not match native identity".into())
    }

    pub(super) fn device_id(&self) -> Result<String, String> {
        self.verify_entries()?;
        if !entry_present(&self.directory, DEVICE_FILE)? {
            self.write_new(DEVICE_FILE, uuid::Uuid::new_v4().to_string().as_bytes())?;
        }
        let bytes = read_private_file(&self.directory, DEVICE_FILE, 36)?;
        let text = String::from_utf8(bytes).map_err(|_| "invalid saved native device ID")?;
        let id = uuid::Uuid::parse_str(&text).map_err(|_| "invalid saved native device ID")?;
        if id.get_version() != Some(uuid::Version::Random) || id.to_string() != text {
            return Err("native device ID must be a canonical UUIDv4".into());
        }
        sync_ro(&self.directory, DEVICE_FILE)?;
        self.sync().map(|()| text)
    }

    pub(super) fn finish_saved_credentials(&self, api_url: &str) -> Result<bool, String> {
        self.verify_entries()?;
        let name = match (
            entry_present(&self.directory, CLOUD_CREDENTIALS_FILE)?,
            entry_present(&self.directory, PENDING_FILE)?,
        ) {
            (true, true) => return Err("native enrollment credential state is ambiguous".into()),
            (true, false) => CLOUD_CREDENTIALS_FILE,
            (false, true) => PENDING_FILE,
            (false, false) => return Ok(false),
        };
        let bytes = Zeroizing::new(read_private_file(&self.directory, name, 65_536)?);
        let creds = SavedCredentials::parse(&bytes)
            .map_err(|_| "invalid saved native credentials; registration may have committed")?;
        if creds.api_url != api_url || creds.api_key.is_empty() {
            return Err("saved native credentials do not match the compiled DEV endpoint".into());
        }
        sync_ro(&self.directory, name)?;
        if name == PENDING_FILE {
            self.publish()?;
        } else {
            self.sync()?;
        }
        Ok(true)
    }

    pub(super) fn save_credentials(&self, bytes: &[u8]) -> Result<(), String> {
        self.write_new(PENDING_FILE, bytes)?;
        self.publish()
    }

    fn publish(&self) -> Result<(), String> {
        self.verify_entries()?;
        if entry_present(&self.directory, CLOUD_CREDENTIALS_FILE)? {
            return Err("refusing to replace paused native credentials".into());
        }
        fs::renameat(
            &self.directory,
            PENDING_FILE,
            &self.directory,
            CLOUD_CREDENTIALS_FILE,
        )
        .map_err(|_| "cannot publish paused native credentials")?;
        self.sync()
    }

    fn write_new(&self, name: &str, bytes: &[u8]) -> Result<(), String> {
        self.verify_entries()?;
        let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL;
        let mut file = open_private_at(
            &self.directory,
            name,
            flags,
            Mode::from_raw_mode(0o600),
            FILE_ERR,
        )?;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| "cannot write protected native enrollment file")?;
        self.sync()
    }

    fn sync(&self) -> Result<(), String> {
        self.verify_entries()?;
        self.directory
            .sync_all()
            .and_then(|()| self.parent.sync_all())
            .map_err(|_| "cannot sync native enrollment home".into())
    }
}

fn sync_ro(dir: &File, name: &str) -> Result<(), String> {
    open_private_at(
        dir,
        name,
        OFlags::RDONLY | OFlags::NONBLOCK,
        Mode::empty(),
        FILE_ERR,
    )?
    .sync_all()
    .map_err(|_| "cannot sync saved native enrollment file".into())
}

pub(super) fn run_existing(
    home: &Path,
    env: &str,
    expected: Option<&str>,
    json: bool,
) -> Result<(), String> {
    if !home.is_absolute() || !matches!(env, "dev" | "prod") {
        return Err(
            "existing connection requires an absolute home and dev or prod environment".into(),
        );
    }
    let expected = expected
        .map(|v| AccountId::try_from(v.to_owned()).map_err(|_| "invalid expected account identity"))
        .transpose()?;
    let api_key = read_stdin_no_control(4096, "existing account API key")?;
    if api_key.is_empty() || api_key.len() > 4096 || api_key.trim() != api_key.as_str() {
        return Err("existing account API key was refused".into());
    }
    let account_id = home_account(home)?;
    if expected.as_ref().is_some_and(|id| id != &account_id) {
        return Err("expected account identity does not match this home".into());
    }
    save_existing(home, env, &account_id, &api_key)?;
    print_backup(
        &serde_json::json!({
            "ok": true,
            "environment": env,
            "account_id": account_id,
            "credentials_saved": true,
            "registration_attempted": false,
        }),
        json,
    )
}

fn home_account(home: &Path) -> Result<AccountId, String> {
    let directory = open_directory(home)?;
    require_owned_directory(&directory)?;
    let seed = read_private_file(&directory, IDENTITY_KEY_FILE, 32)?;
    if seed.len() != 32 {
        return Err("identity must contain exactly 32 bytes".into());
    }
    let pair = Ed25519KeyPair::from_secret_key(&seed).map_err(|_| "invalid identity")?;
    let account = user_hash_from_pubkey(&pair.public_key_base64())?;
    AccountId::try_from(account).map_err(|_| "invalid account identity".into())
}

fn save_existing(
    home: &Path,
    env: &str,
    account_id: &AccountId,
    api_key: &str,
) -> Result<(), String> {
    let environment =
        Environment::try_from_str(env).map_err(|_| "recovery environment must be dev or prod")?;
    let api_url = exemem_api_url_for(environment);
    let endpoint = lastdb_node::cloud_backup_client::byte_backup::ByteBackupClient::from_auth(
        lastdb_node::cloud_backup_client::AuthClient::new(api_url.to_owned(), api_key.to_owned()),
    )
    .map_err(|error| error.to_string())?
    .canonical_endpoint();
    let directory = lastdb_node::offline_home::Directory::open(home).map_err(|e| e.to_string())?;
    let previous = directory
        .read(CLOUD_CREDENTIALS_FILE)
        .map_err(|e| e.to_string())?;
    let mut creds = match &previous {
        Some(bytes) => {
            let saved =
                SavedCredentials::parse(bytes).map_err(|_| "invalid source cloud credentials")?;
            if saved.api_url != api_url || saved.api_key.as_str() != api_key {
                return Err("saved cloud credentials do not match this connection".into());
            }
            saved
        }
        None => SavedCredentials::new(api_url, api_key.to_owned()),
    };
    let source_home = match &creds.connection {
        Some(saved) => saved.source_home.clone(),
        None => Id::try_from(uuid::Uuid::new_v4().to_string())
            .map_err(|_| "cannot name this home".to_owned())?,
    };
    let connection = SavedConnection {
        account_id: account_id.clone(),
        source_home,
        environment: env.to_owned(),
        endpoint,
    };
    if creds
        .connection
        .as_ref()
        .is_some_and(|saved| saved != &connection)
    {
        return Err("saved cloud connection does not match this account".into());
    }
    creds.connection = Some(connection);
    let encoded = creds
        .to_json()
        .map_err(|_| "cannot encode cloud credentials")?;
    directory
        .publish(
            CLOUD_CREDENTIALS_FILE,
            previous.as_ref().map(|bytes| bytes.as_slice()),
            &encoded,
        )
        .map_err(|e| e.to_string())
}
