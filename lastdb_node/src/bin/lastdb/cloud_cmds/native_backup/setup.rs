//! Explicit account enrollment and paused credentials. No database writer opens.

use super::*;
use fold_db::security::Ed25519KeyPair;
use folddb_profile::endpoints::{exemem_api_url_for, Environment};
use lastdb_node::cloud::{
    register_native_device, CLOUD_RESUME_REQUIRED_FILE, CLOUD_SYNC_PAUSED_FILE,
};
use lastdb_node::host::IDENTITY_KEY_FILE;
use std::io::Read;

#[path = "setup/files.rs"]
mod files;
use super::super::recovery_home::files as recovery_files;

const PHRASE_FILE: &str = "recovery-phrase.txt";

pub(crate) fn is_setup(action: &NativeBackupCommand) -> bool {
    matches!(
        action,
        NativeBackupCommand::Init { .. } | NativeBackupCommand::Connect { .. }
    )
}

pub(crate) fn setup(source: Option<&Path>, action: NativeBackupCommand) -> Result<(), String> {
    lastdb_node::native_cloud_backup::require_enabled().map_err(|error| error.to_string())?;
    match action {
        NativeBackupCommand::Init {
            into,
            recovery_phrase_out,
            json,
        } => {
            if source.is_some() {
                return Err("native backup init uses --into without --data-dir".into());
            }
            initialize(&into, &recovery_phrase_out, json)
        }
        NativeBackupCommand::Connect {
            dev,
            prod,
            existing,
            invite_code_stdin,
            restored,
            json,
        } => {
            if !matches!(
                (dev, prod, existing),
                (true, false, false) | (false, true, true)
            ) {
                return Err("native backup connect requires --dev or --prod --existing".into());
            }
            if restored && invite_code_stdin {
                return Err("restored account reconnect does not accept a new invite".into());
            }
            connect(
                source.ok_or("native backup connect requires an explicit --data-dir")?,
                invite_code_stdin,
                restored,
                existing,
                json,
            )
        }
        _ => Err("invalid native backup setup command".into()),
    }
}

fn initialize(destination: &Path, phrase_path: &Path, json: bool) -> Result<(), String> {
    if phrase_path != destination.join(PHRASE_FILE) {
        return Err("--recovery-phrase-out must name <into>/recovery-phrase.txt".into());
    }
    let target = recovery_files::FreshHome::preflight_empty(destination)?;
    let pair = Ed25519KeyPair::generate().map_err(|_| "cannot create fresh native identity")?;
    let seed = zeroize::Zeroizing::new(pair.secret_key_bytes());
    let phrase = zeroize::Zeroizing::new(
        bip39::Mnemonic::from_entropy(seed.as_ref())
            .map_err(|_| "cannot encode fresh native recovery phrase")?
            .to_string(),
    );
    let account = lastdb_identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    let mut target = target.create()?;
    target.write(IDENTITY_KEY_FILE, seed.as_ref())?;
    target.write(PHRASE_FILE, phrase.as_bytes())?;
    target.write(CLOUD_RESUME_REQUIRED_FILE, b"")?;
    target.commit()?;
    print(
        &serde_json::json!({
            "schema": "lastdb.native-backup-init.v1", "ok": true, "environment": "dev",
            "account_id": account, "home": destination, "recovery_phrase_file": phrase_path,
            "cloud_sync_off": true, "local_store_opened": false, "network_request": false,
            "identity_seed_output": false, "recovery_phrase_output": false,
        }),
        json,
    )
}

fn connect(
    home: &Path,
    invite_stdin: bool,
    restored: bool,
    existing: bool,
    json: bool,
) -> Result<(), String> {
    let environment = if existing {
        Environment::Prod
    } else {
        Environment::Dev
    };
    let directory = if existing {
        files::PreparedHome::open_existing(home)?
    } else if restored {
        files::PreparedHome::open_restored(home)?
    } else {
        files::PreparedHome::open(home)?
    };
    let seed = directory.seed()?;
    let pair = Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| "invalid native backup identity")?;
    let account = lastdb_identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    let api_url = exemem_api_url_for(environment);
    if restored || existing {
        directory.device_id()?;
    }
    if directory.finish_saved_credentials(api_url)? {
        return connected(
            &account,
            home,
            false,
            (restored, existing),
            environment,
            json,
        );
    }
    let invite = invite_stdin.then(read_invite).transpose()?;
    let device_id = directory.device_id()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot construct native enrollment runtime")?;
    let registered = runtime
        .block_on(register_native_device(
            environment,
            &pair,
            &device_id,
            invite.as_deref().map(String::as_str),
        ))
        .map_err(|error| {
            format!("{error}; registration may have committed; retry only by an explicit request")
        })?;
    if registered.user_hash != account
        || directory.seed()?.as_ref() != seed.as_ref()
        || directory.device_id()? != device_id
    {
        return Err("native enrollment identity changed; registration may have committed".into());
    }
    let credentials = zeroize::Zeroizing::new(
        serde_json::to_vec(&serde_json::json!({
            "api_url": api_url, "api_key": registered.api_key,
        }))
        .map_err(|_| "cannot encode paused native credentials")?,
    );
    directory
        .save_credentials(&credentials)
        .map_err(|error| format!("{error}; registration may have committed; retain this home"))?;
    connected(
        &account,
        home,
        true,
        (restored, existing),
        environment,
        json,
    )
}

fn validate_saved_credentials(api_url: &str, api_key: &str, account: &str) -> Result<(), String> {
    use fold_db::sync::auth::{native_backup::NativeBackupClient, AuthClient, SyncAuth};
    let http = fold_db::sync::build_shared_http_client()
        .map_err(|_| "cannot construct native credential transport")?;
    let auth = AuthClient::new(
        std::sync::Arc::new(http),
        api_url.to_owned(),
        SyncAuth::ApiKey(api_key.to_owned()),
    )
    .without_db_auto_claim();
    let client =
        NativeBackupClient::from_auth(auth).map_err(|_| "invalid saved native account endpoint")?;
    let account = cloud_backup_protocol::AccountId::try_from(account.to_owned())
        .map_err(|_| "invalid saved account identity")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot construct native credential validation runtime")?;
    runtime
        .block_on(client.context(
            &cloud_backup_protocol::context::ContextReadRequest {
                version: cloud_backup_protocol::Version,
            },
            &account,
        ))
        .map(|_| ())
        .map_err(|_| "saved native credentials do not authorize the selected account".into())
}

fn connected(
    account: &str,
    home: &Path,
    registered: bool,
    mode: (bool, bool),
    environment: Environment,
    json: bool,
) -> Result<(), String> {
    let (restored, existing) = mode;
    print(
        &serde_json::json!({
            "schema": "lastdb.native-backup-connect.v1", "ok": true, "environment": environment.as_str(),
            "account_id": account, "home": home, "credential_file": CLOUD_SYNC_PAUSED_FILE,
            "cloud_sync_off": true, "local_store_opened": restored,
            "local_store_read_only": restored, "database_writer_opened": false,
            "registration_attempted": registered, "credentials_saved": true,
            "api_key_output": false, "legacy_recipient_enrolled": false,
            "restored_home": restored, "automatic_backup_enabled": false,
            "existing_home": existing, "identity_preserved": true,
            "native_runtime_activation_changed": false,
        }),
        json,
    )
}

fn read_invite() -> Result<zeroize::Zeroizing<String>, String> {
    let mut bytes = zeroize::Zeroizing::new(Vec::new());
    std::io::stdin()
        .lock()
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read native enrollment invite")?;
    if bytes.len() > 4096 {
        return Err("native enrollment invite exceeds its byte limit".into());
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| "native enrollment invite is not UTF-8")?
        .trim();
    if text.is_empty() || text.chars().any(char::is_control) {
        return Err("native enrollment invite must be nonempty without control characters".into());
    }
    Ok(zeroize::Zeroizing::new(text.to_owned()))
}
