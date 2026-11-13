//! Store-free native backup home initialization, DEV enrollment, and existing connections.
use super::snapshot_wait::print_backup;
use super::{block_on_cloud, read_stdin_no_control, FreshHome};
use cloud_backup_protocol::{AccountId, Id};
use fold_db::security::Ed25519KeyPair;
use lastdb_node::cloud::{
    register_native_device, SavedConnection, SavedCredentials, CLOUD_CREDENTIALS_FILE,
};
use lastdb_node::host::{user_hash_from_pubkey, IDENTITY_KEY_FILE};
use lastdb_node::offline_home::{
    dir_entry_names, entry_present, open_directory, open_private_at, read_private_file,
    require_mode_0700_directory, require_owned_directory, same_inode, Directory,
};
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use rustix::fs::{self, Mode, OFlags};
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use zeroize::Zeroizing;

const PHRASE_FILE: &str = "recovery-phrase.txt";
const DEVICE_FILE: &str = ".native_backup_device_id";
const LOCK_FILE: &str = ".native_backup_enrollment.lock";
const PENDING_FILE: &str = ".native_backup_credentials.pending";
const FILE_ERR: &str = "native enrollment file must be a private regular owner file";
const ALLOWED: [&str; 6] = [
    IDENTITY_KEY_FILE,
    PHRASE_FILE,
    DEVICE_FILE,
    LOCK_FILE,
    PENDING_FILE,
    CLOUD_CREDENTIALS_FILE,
];

pub(super) fn initialize(destination: &Path, phrase_path: &Path, json: bool) -> Result<(), String> {
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
    let account = user_hash_from_pubkey(&pair.public_key_base64())?;
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

pub(super) fn connect(home: &Path, invite_stdin: bool, json: bool) -> Result<(), String> {
    let dir = PreparedHome::open(home)?;
    let seed = dir.seed()?;
    let pair = Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| "invalid native backup identity")?;
    let account = user_hash_from_pubkey(&pair.public_key_base64())?;
    let api_url = exemem_api_url_for(Environment::Dev);
    let emit = |registered: bool| -> Result<(), String> {
        print_backup(
            &serde_json::json!({
                "schema": "lastdb.native-backup-connect.v1", "ok": true, "environment": "dev",
                "account_id": account, "home": home, "credential_file": CLOUD_CREDENTIALS_FILE,
                "cloud_sync_off": true, "local_store_opened": false,
                "local_store_read_only": false, "database_writer_opened": false,
                "registration_attempted": registered, "credentials_saved": true,
                "api_key_output": false, "restored_home": false, "automatic_backup_enabled": false,
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
    dir.write_new(PENDING_FILE, &creds)
        .and_then(|()| dir.publish())
        .map_err(|e| format!("{e}; registration may have committed; retain this home"))?;
    emit(true)
}

pub(super) fn connect_existing(
    home: &Path,
    env_str: &str,
    expected: Option<&str>,
    json: bool,
) -> Result<(), String> {
    let env = Environment::try_from_str(env_str)
        .map_err(|_| "existing connection requires an absolute home and dev or prod environment")?;
    let expected = expected
        .map(|v| AccountId::try_from(v.to_owned()).map_err(|_| "invalid expected account identity"))
        .transpose()?;
    let api_key = read_stdin_no_control(4096, "existing account API key")?;
    let dir = Directory::open(home).map_err(|e| e.to_string())?;
    let seed = dir
        .read(IDENTITY_KEY_FILE)
        .map_err(|e| e.to_string())?
        .ok_or("missing home identity")?;
    if seed.len() != 32 {
        return Err("identity must contain exactly 32 bytes".into());
    }
    let pair = Ed25519KeyPair::from_secret_key(&seed).map_err(|_| "invalid identity")?;
    let account_id = AccountId::try_from(user_hash_from_pubkey(&pair.public_key_base64())?)
        .map_err(|_| "invalid account identity".to_owned())?;
    if expected.as_ref().is_some_and(|id| id != &account_id) {
        return Err("expected account identity does not match this home".into());
    }
    let api_url = exemem_api_url_for(env);
    let auth = lastdb_node::cloud_backup_client::AuthClient::new(
        api_url.to_owned(),
        api_key.as_str().to_owned(),
    );
    let endpoint = lastdb_node::cloud_backup_client::byte_backup::ByteBackupClient::from_auth(auth)
        .map_err(|e| e.to_string())?
        .canonical_endpoint();
    let prev = dir
        .read(CLOUD_CREDENTIALS_FILE)
        .map_err(|e| e.to_string())?;
    let mut creds = match &prev {
        Some(bytes) => {
            let saved =
                SavedCredentials::parse(bytes).map_err(|_| "invalid source cloud credentials")?;
            if saved.api_url != api_url || saved.api_key.as_str() != api_key.as_str() {
                return Err("saved cloud credentials do not match this connection".into());
            }
            saved
        }
        None => SavedCredentials::new(api_url, api_key.as_str().to_owned()),
    };
    let source_home = match &creds.connection {
        Some(saved) => saved.source_home.clone(),
        None => Id::try_from(uuid::Uuid::new_v4().to_string())
            .map_err(|_| "cannot name this home".to_owned())?,
    };
    let conn = SavedConnection {
        account_id: account_id.clone(),
        source_home,
        environment: env_str.to_owned(),
        endpoint,
    };
    if creds
        .connection
        .as_ref()
        .is_some_and(|saved| saved != &conn)
    {
        return Err("saved cloud connection does not match this account".into());
    }
    creds.connection = Some(conn);
    let encoded = creds
        .to_json()
        .map_err(|_| "cannot encode cloud credentials")?;
    dir.publish(
        CLOUD_CREDENTIALS_FILE,
        prev.as_deref().map(Vec::as_slice),
        &encoded,
    )
    .map_err(|e| e.to_string())?;
    print_backup(
        &serde_json::json!({
            "ok": true, "environment": env_str, "account_id": account_id,
            "credentials_saved": true, "registration_attempted": false,
        }),
        json,
    )
}

struct PreparedHome {
    parent: File,
    name: OsString,
    directory: File,
    _lock: File,
}

impl PreparedHome {
    fn open(home: &Path) -> Result<Self, String> {
        let parent = open_directory(home.parent().ok_or("native enrollment needs a home")?)?;
        require_owned_directory(&parent)?;
        let directory = open_directory(home)?;
        require_mode_0700_directory(
            &directory,
            "native enrollment directory must be owner-only with mode0700",
        )?;
        let name = home
            .file_name()
            .ok_or("native enrollment needs an explicit home name")?
            .to_os_string();
        let lock = open_private_at(
            &directory,
            LOCK_FILE,
            OFlags::RDWR | OFlags::CREATE | OFlags::NONBLOCK,
            Mode::from_raw_mode(0o600),
            FILE_ERR,
        )?;
        lock.try_lock()
            .map_err(|_| "native enrollment is already active for this home")?;
        let prepared = Self {
            parent,
            name,
            directory,
            _lock: lock,
        };
        prepared.verify_entries().map(|()| prepared)
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

    fn seed(&self) -> Result<Zeroizing<[u8; 32]>, String> {
        self.verify_entries()?;
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

    fn device_id(&self) -> Result<String, String> {
        self.verify_entries()?;
        if !entry_present(&self.directory, DEVICE_FILE)? {
            self.write_new(DEVICE_FILE, uuid::Uuid::new_v4().to_string().as_bytes())?;
        }
        let text = String::from_utf8(read_private_file(&self.directory, DEVICE_FILE, 36)?)
            .map_err(|_| "invalid saved native device ID")?;
        let id = uuid::Uuid::parse_str(&text).map_err(|_| "invalid saved native device ID")?;
        if id.get_version() != Some(uuid::Version::Random) || id.to_string() != text {
            return Err("native device ID must be a canonical UUIDv4".into());
        }
        self.sync().map(|()| text)
    }

    fn finish_saved_credentials(&self, api_url: &str) -> Result<bool, String> {
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
        if name == PENDING_FILE {
            self.publish()
        } else {
            self.sync()
        }
        .map(|()| true)
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
        let mut file = open_private_at(
            &self.directory,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL,
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
