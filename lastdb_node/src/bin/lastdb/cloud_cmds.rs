//! Explicit V2 backup and credentials-only recovery-home commands.
use super::*;
use cloud_backup_protocol::{AccountId, Id};
use fold_db::security::Ed25519KeyPair;
use lastdb_node::byte_cloud_backup::{ByteBackupRequest, SensitiveConnectionRequest};
use lastdb_node::cloud::{register_native_device, CLOUD_CREDENTIALS_FILE};
use lastdb_node::cloud_backup_client::{byte_backup::ByteBackupClient, AuthClient};
use lastdb_node::host::IDENTITY_KEY_FILE;
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use std::io::Read;
use zeroize::Zeroizing;

#[path = "cloud_cmds/byte_backup/offline.rs"]
mod offline;
#[path = "cloud_cmds/recovery_home.rs"]
mod recovery_home;
#[path = "cloud_cmds/byte_backup/setup/files.rs"]
mod setup_files;

use recovery_home::files as recovery_files;
pub(super) use recovery_home::files::FreshHome;

const PHRASE_FILE: &str = "recovery-phrase.txt";

pub(super) fn cloud_command(data_dir: Option<PathBuf>, action: CloudCommand) -> Result<(), String> {
    if let CloudCommand::BackupV2 {
        action: action @ (ByteBackupCommand::Init(_) | ByteBackupCommand::Connect(_)),
    } = &action
    {
        return run_setup(data_dir.as_deref(), action);
    }
    if recovery_home::handles(&action) {
        return recovery_home::command(data_dir.as_deref(), action);
    }
    if matches!(action, CloudCommand::BackupV2 { .. })
        && !data_dir.as_ref().is_some_and(|home| home.is_absolute())
    {
        return Err("V2 backup requires an explicit absolute --data-dir home".into());
    }
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let CloudCommand::BackupV2 { action } = action else {
        unreachable!("handled before home resolution");
    };
    run_byte_backup(&home, action)
}

fn run_byte_backup(home: &Path, action: ByteBackupCommand) -> Result<(), String> {
    let (request, json) = match action {
        ByteBackupCommand::Start(BackupStartArgs { request_id, json }) => {
            let request_id =
                Id::try_from(request_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()))
                    .map_err(|_| "V2 request ID must be a canonical UUID")?;
            eprintln!("V2 backup request ID: {}", request_id.as_str());
            (ByteBackupRequest::Start { request_id }, json)
        }
        ByteBackupCommand::Resume(BackupJsonArgs { json }) => (ByteBackupRequest::Resume, json),
        ByteBackupCommand::Status(BackupJsonArgs { json }) => (ByteBackupRequest::Status, json),
        ByteBackupCommand::Close(BackupJsonArgs { json }) => (ByteBackupRequest::Close, json),
        ByteBackupCommand::EnableAutomatic(BackupEnableArgs {
            log_interval_seconds,
            snapshot_interval_seconds,
            json,
        }) => (
            ByteBackupRequest::EnableAutomatic {
                log_interval_seconds,
                snapshot_interval_seconds,
            },
            json,
        ),
        ByteBackupCommand::DisableAutomatic(BackupJsonArgs { json }) => {
            (ByteBackupRequest::DisableAutomatic, json)
        }
        action => return offline::run(home, action),
    };
    let body = serde_json::to_value(request).map_err(|_| "cannot encode V2 owner request")?;
    let response = super::socket_client::post_json_admin(
        &home.join("data/folddb.sock"),
        "/api/sync/byte-backup-v2",
        &body,
    )?;
    print_backup(&parse_json_response(&response, "V2 backup")?, json)
}

fn run_setup(source: Option<&Path>, action: &ByteBackupCommand) -> Result<(), String> {
    if let ByteBackupCommand::Connect(args) = action {
        if args.existing {
            if !args.api_key_stdin || args.dev || args.invite_code_stdin || args.restored {
                return Err(
                    "existing connection requires only an environment and API key stdin".into(),
                );
            }
            return run_existing(
                source.ok_or("existing connection requires an explicit --data-dir")?,
                args.env
                    .as_deref()
                    .ok_or("existing connection requires --env")?,
                args.expected_user_hash.as_deref(),
                args.json,
            );
        }
    }
    lastdb_node::byte_cloud_backup::require_enabled().map_err(|error| error.to_string())?;
    match action {
        ByteBackupCommand::Init(args) => {
            if source.is_some() {
                return Err("native backup init uses --into without --data-dir".into());
            }
            initialize(&args.into, &args.recovery_phrase_out, args.json)
        }
        ByteBackupCommand::Connect(args) => {
            if !args.dev
                || args.existing
                || args.env.is_some()
                || args.api_key_stdin
                || args.expected_user_hash.is_some()
            {
                return Err("native backup connect requires --dev".into());
            }
            if args.restored && args.invite_code_stdin {
                return Err("restored account reconnect does not accept a new invite".into());
            }
            connect(
                source.ok_or("native backup connect requires an explicit --data-dir")?,
                args.invite_code_stdin,
                args.restored,
                args.json,
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

fn connect(home: &Path, invite_stdin: bool, restored: bool, json: bool) -> Result<(), String> {
    let directory = if restored {
        setup_files::PreparedHome::open_restored(home)?
    } else {
        setup_files::PreparedHome::open(home)?
    };
    let seed = directory.seed()?;
    let pair = Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| "invalid native backup identity")?;
    let account = lastdb_node::identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    let api_url = exemem_api_url_for(Environment::Dev);
    if restored {
        directory.device_id()?;
    }
    if directory.finish_saved_credentials(api_url)? {
        return print_connected(&account, home, false, restored, json);
    }
    let invite = invite_stdin
        .then(|| read_stdin_no_control(4096, "native enrollment invite"))
        .transpose()?;
    let device_id = directory.device_id()?;
    let registered = block_on_cloud(
        "native enrollment",
        register_native_device(
            Environment::Dev,
            &pair,
            &device_id,
            invite.as_deref().map(String::as_str),
        ),
    )
    .map_err(|error| {
        format!("{error}; registration may have committed; retry only by an explicit request")
    })?;
    if registered.user_hash != account
        || directory.seed()?.as_ref() != seed.as_ref()
        || directory.device_id()? != device_id
    {
        return Err("native enrollment identity changed; registration may have committed".into());
    }
    let credentials = lastdb_node::cloud::SavedCredentials::new(api_url, registered.api_key)
        .key_json()
        .map_err(|_| "cannot encode paused native credentials")?;
    directory
        .save_credentials(&credentials)
        .map_err(|error| format!("{error}; registration may have committed; retain this home"))?;
    print_connected(&account, home, true, restored, json)
}

fn validate_restored_credentials(
    api_url: &str,
    api_key: &str,
    account: &str,
) -> Result<(), String> {
    let auth = AuthClient::new(api_url.to_owned(), api_key.to_owned());
    let client = ByteBackupClient::from_auth(auth)
        .map_err(|_| "invalid restored native account endpoint")?;
    let account =
        AccountId::try_from(account.to_owned()).map_err(|_| "invalid restored account identity")?;
    block_on_cloud("restored credential validation", async move {
        client
            .context(&cloud_backup_protocol::v2::context::ContextReadRequest {
                version: cloud_backup_protocol::v2::Version::V2,
                account_id: account,
                required_destinations: Vec::new(),
            })
            .await
            .map(|_| ())
            .map_err(|_| "saved native credentials do not authorize the restored account".into())
    })
}

fn print_connected(
    account: &str,
    home: &Path,
    registered: bool,
    restored: bool,
    json: bool,
) -> Result<(), String> {
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
}

fn run_existing(
    home: &Path,
    environment: &str,
    expected: Option<&str>,
    json: bool,
) -> Result<(), String> {
    if !home.is_absolute() || !matches!(environment, "dev" | "prod") {
        return Err(
            "existing connection requires an absolute home and dev or prod environment".into(),
        );
    }
    let expected_user_hash = expected
        .map(|value| AccountId::try_from(value.to_owned()))
        .transpose()
        .map_err(|_| "invalid expected account identity")?;
    let api_key = read_stdin_no_control(4096, "existing account API key")?;
    let request = SensitiveConnectionRequest {
        environment: environment.to_owned(),
        expected_user_hash,
        api_key: api_key.as_str().to_owned(),
    };
    let body = Zeroizing::new(
        serde_json::to_vec(&request).map_err(|_| "cannot encode protected connection request")?,
    );
    let mut wire = Zeroizing::new(
        format!(
            "POST /api/sync/byte-backup-v2/connect HTTP/1.1\r\nHost: localhost\r\n{}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            super::socket_client::client_headers(),
            body.len(),
        )
        .into_bytes(),
    );
    wire.extend_from_slice(&body);
    let response = super::socket_client::request_with_timeout(
        &home.join("data/folddb.sock"),
        &wire,
        admin_scan_client_timeout(),
    )?;
    print_backup(
        &parse_json_response(&response, "V2 existing account connection")?,
        json,
    )
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

fn read_recovery_phrase() -> Result<Zeroizing<String>, String> {
    read_stdin_secret(4096, "recovery phrase")
}

fn read_stdin_no_control(limit: usize, label: &str) -> Result<Zeroizing<String>, String> {
    let text = read_stdin_secret(limit, label)?;
    if text.chars().any(char::is_control) {
        return Err(format!("{label} is invalid"));
    }
    Ok(text)
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
    if text.is_empty() {
        return Err(format!("{label} is empty"));
    }
    Ok(Zeroizing::new(text.to_owned()))
}
