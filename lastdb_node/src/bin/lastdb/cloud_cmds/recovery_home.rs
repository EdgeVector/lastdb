//! Protected credential homes: offline transfer or explicit source-free registration.
use std::fs;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

use base64::Engine as _;
use fold_db::security::Ed25519KeyPair;
use lastdb_node::cloud::{register_native_device, SavedCredentials, CLOUD_CREDENTIALS_FILE};
use lastdb_node::host::{user_hash_from_pubkey, IDENTITY_KEY_FILE};
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use zeroize::Zeroizing;

#[path = "recovery_home/files.rs"]
pub(super) mod files;
use super::{CloudCommand, PrepareRecoveryArgs, RecoverCredentialsArgs};
use files::{open_directory, read_private_file, FreshHome};

const API_KEY_LIMIT: usize = 4096;
const SEED_INPUT_LIMIT: usize = 128;
const INTENT: &str = ".native_backup_registration.json";

pub(super) fn handles(action: &CloudCommand) -> bool {
    matches!(
        action,
        CloudCommand::PrepareRecoveryHome(_)
            | CloudCommand::RecoverDevCredentialsHome { .. }
            | CloudCommand::RecoverCredentialsHome(_)
    )
}

pub(super) fn command(source: Option<&Path>, action: CloudCommand) -> Result<(), String> {
    match action {
        CloudCommand::RecoverCredentialsHome(args) => {
            if source.is_some() || !args.identity_seed_stdin {
                return Err(
                    "recover-credentials-home requires seed stdin without --data-dir".into(),
                );
            }
            recover_credentials_home(args)
        }
        CloudCommand::PrepareRecoveryHome(args) => {
            if !args.api_key_stdin {
                return Err("prepare-recovery-home requires --api-key-stdin".into());
            }
            prepare_recovery_home(source, &args)
        }
        CloudCommand::RecoverDevCredentialsHome {
            into,
            expected_user_hash,
            identity_seed_stdin,
            json,
        } => seed_registration_command(
            source,
            &into,
            &expected_user_hash,
            identity_seed_stdin,
            json,
        ),
        _ => unreachable!("recovery-home command dispatch"),
    }
}

fn emit_receipt(json: bool, receipt: &serde_json::Value, text: &str) -> Result<(), String> {
    if json {
        serde_json::to_writer(std::io::stdout().lock(), receipt)
            .map_err(|_| "cannot write recovery receipt")?;
        println!();
    } else {
        println!("{text}");
    }
    Ok(())
}

fn prepare_recovery_home(source: Option<&Path>, args: &PrepareRecoveryArgs) -> Result<(), String> {
    let source = source.ok_or("prepare-recovery-home requires an explicit --data-dir")?;
    let environment = Environment::try_from_str(&args.env)
        .map_err(|_| "recovery environment must be dev or prod")?;
    let source_dir = open_directory(source)?;
    files::require_owned_directory(&source_dir)?;
    files::require_disjoint(source, &args.into)?;
    let target = FreshHome::preflight(&args.into, &source_dir)?;
    let seed = read_private_file(&source_dir, IDENTITY_KEY_FILE, 32)?;
    if seed.len() != 32 {
        return Err("source identity must contain exactly 32 bytes".into());
    }
    if !files::entry_present(&source_dir, CLOUD_CREDENTIALS_FILE)? {
        return Err("source requires saved paused cloud credentials".into());
    }
    let raw = Zeroizing::new(read_private_file(
        &source_dir,
        CLOUD_CREDENTIALS_FILE,
        65_536,
    )?);
    let credentials =
        SavedCredentials::parse(&raw).map_err(|_| "invalid source cloud credentials")?;
    if credentials.api_url != exemem_api_url_for(environment) {
        return Err("saved cloud URL does not match the selected compiled environment".into());
    }
    let api_key = super::read_stdin_secret(API_KEY_LIMIT, "API key")?;
    if api_key.as_str() != credentials.api_key.as_str() {
        return Err("stdin API key does not match the source cloud credentials".into());
    }
    let bytes = credentials
        .key_json()
        .map_err(|_| "cannot encode recovery credentials")?;
    let mut target = target.create()?;
    target.write(IDENTITY_KEY_FILE, &seed)?;
    target.write(CLOUD_CREDENTIALS_FILE, &bytes)?;
    target.commit()?;
    emit_receipt(
        args.json,
        &serde_json::json!({
            "schema": "lastdb.recovery-home.v1", "ok": true, "environment": environment.as_str(),
            "credentials_only": true, "source_opened_read_only": true,
            "identity_matches_source": true, "api_key_matches_source": true,
            "saved_url_matches_environment": true, "cloud_sync_off": true,
            "data_directory_created": false, "files": 2, "home_mode": "0700", "file_mode": "0600",
        }),
        "Prepared credentials-only recovery home; Cloud Sync stays Off.",
    )
}

fn recover_credentials_home(args: RecoverCredentialsArgs) -> Result<(), String> {
    let environment = Environment::try_from_str(&args.env)
        .map_err(|_| "credential environment must be dev or prod")?;
    let account = cloud_backup_protocol::AccountId::try_from(args.expected_user_hash)
        .map_err(|_| "invalid expected account")?;
    let (seed, pair) = verified_seed_pair(account.as_str())?;
    let invite = args
        .invite_code_file
        .as_deref()
        .map(read_invite_file)
        .transpose()?;
    let mut target = FreshHome::preflight_empty_parent(&args.into)?.create()?;
    let device = uuid::Uuid::new_v4().to_string();
    let intent = serde_json::to_vec(&serde_json::json!({
        "version": 1, "account_id": account, "environment": environment.as_str(),
        "device_id": device, "registration_attempted": true,
    }))
    .map_err(|_| "cannot encode registration intent")?;
    target.write(IDENTITY_KEY_FILE, seed.as_ref())?;
    target.write(INTENT, &intent)?;
    target.commit()?;
    let registered = super::block_on_cloud(
        "account registration",
        register_native_device(environment, &pair, &device, invite.as_deref().map(String::as_str)),
    )
    .map_err(|_| {
        "registration result is uncertain; retain this credentials home and its device intent; do not retry automatically"
    })?;
    if registered.user_hash != account.as_str() {
        return Err("registered account differs; retain the device intent".into());
    }
    let bytes = SavedCredentials::new(exemem_api_url_for(environment), registered.api_key)
        .to_json()
        .map_err(|_| "cannot encode paused credentials; retain the device intent")?;
    target
        .write(CLOUD_CREDENTIALS_FILE, &bytes)
        .and_then(|()| target.commit())
        .map_err(|_| "cannot commit paused credentials; retain the device intent")?;
    emit_receipt(
        args.json,
        &serde_json::json!({
            "schema": "lastdb.credentials-recovery.v1", "ok": true,
            "environment": environment.as_str(), "account_id": account, "device_id": device,
            "credentials_only": true, "cloud_sync_off": true,
            "data_directory_created": false, "local_store_opened": false,
            "api_key_output": false, "identity_seed_output": false,
        }),
        "Prepared credentials-only home; Cloud Sync stays Off.",
    )
}

fn verified_seed_pair(
    expected: &str,
) -> Result<(Zeroizing<[u8; 32]>, Ed25519KeyPair), &'static str> {
    let seed = read_seed_stdin()?;
    let pair =
        Ed25519KeyPair::from_secret_key(seed.as_ref()).map_err(|_| "invalid_identity_seed")?;
    let user_hash = user_hash_from_pubkey(&pair.public_key_base64())
        .map_err(|_| "cannot_derive_account_hash")?;
    if user_hash != expected {
        return Err("identity_account_mismatch");
    }
    Ok((seed, pair))
}

fn read_seed_stdin() -> Result<Zeroizing<[u8; 32]>, &'static str> {
    let mut bytes = Zeroizing::new(Vec::new());
    std::io::stdin()
        .take((SEED_INPUT_LIMIT + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot_read_seed_stdin")?;
    if bytes.len() > SEED_INPUT_LIMIT {
        return Err("seed_stdin_exceeds_supported_size");
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| "invalid_seed_encoding")?;
    let decoded = Zeroizing::new(
        base64::engine::general_purpose::STANDARD
            .decode(text.trim())
            .map_err(|_| "invalid_seed_encoding")?,
    );
    let value: [u8; 32] = decoded
        .as_slice()
        .try_into()
        .map_err(|_| "invalid_seed_size")?;
    Ok(Zeroizing::new(value))
}

fn read_invite_file(path: &Path) -> Result<Zeroizing<String>, String> {
    if !path.is_absolute() {
        return Err("invite file must be absolute".into());
    }
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| "cannot open protected invite file")?;
    let before = file.metadata().map_err(|_| "cannot inspect invite file")?;
    if !before.is_file()
        || before.uid() != unsafe { libc::geteuid() }
        || before.mode() & 0o777 != 0o600
        || before.nlink() != 1
        || before.len() > 1024
    {
        return Err("invite must be a bounded private owner file".into());
    }
    let mut bytes = Zeroizing::new(Vec::new());
    (&file)
        .take(1025)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read invite file")?;
    let after = file.metadata().map_err(|_| "cannot inspect invite file")?;
    if bytes.len() > 1024
        || (before.len(), before.mtime(), before.mtime_nsec())
            != (after.len(), after.mtime(), after.mtime_nsec())
    {
        return Err("invite file changed or exceeds its limit".into());
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| "invalid invite encoding")?
        .trim();
    if text.is_empty() || !text.is_ascii() || text.as_bytes().iter().any(u8::is_ascii_control) {
        return Err("invalid invite text".into());
    }
    Ok(Zeroizing::new(text.to_owned()))
}

type SeedFailure = (&'static str, bool, Option<String>);

fn seed_registration_command(
    source: Option<&Path>,
    destination: &Path,
    expected_user_hash: &str,
    seed_stdin: bool,
    json: bool,
) -> Result<(), String> {
    let result = recover_dev_seed(source, destination, expected_user_hash, seed_stdin);
    if json {
        let receipt = match &result {
            Ok(device_id) => serde_json::json!({
                "schema": "lastdb.dev-credentials-recovery.v1", "ok": true, "environment": "dev",
                "user_hash_matches_expected": true, "fresh_device_id": device_id,
                "credentials_only": true, "cloud_sync_off": true,
                "data_directory_created": false, "files": 2, "home_mode": "0700", "file_mode": "0600",
                "identity_seed_output": false, "api_key_output": false, "local_store_opened": false,
                "cloud_data_published": false, "device_registered": true,
                "remote_key_rotation_policy_verified": false, "credentials_home_committed": true,
            }),
            Err((code, committed, device_id)) => serde_json::json!({
                "schema": "lastdb.dev-credentials-recovery.v1", "ok": false, "environment": "dev",
                "code": code, "registration_may_have_committed": committed,
                "fresh_device_id": device_id, "identity_seed_output": false, "api_key_output": false,
                "local_store_opened": false, "cloud_data_published": false, "credentials_home_committed": false,
            }),
        };
        emit_receipt(true, &receipt, "")?;
    } else if result.is_ok() {
        println!(
            "Prepared DEV credentials-only home; one fresh device registered; Cloud Sync stays Off."
        );
    }
    result.map(|_| ()).map_err(|(code, committed, _)| {
        if committed {
            format!("{code}; remote registration may have committed; do not retry automatically")
        } else {
            code.to_owned()
        }
    })
}

fn recover_dev_seed(
    source: Option<&Path>,
    destination: &Path,
    expected_user_hash: &str,
    seed_stdin: bool,
) -> Result<String, SeedFailure> {
    let fail = |code| (code, false, None);
    if source.is_some() || !seed_stdin {
        return Err(fail("explicit_seed_stdin_without_data_dir_required"));
    }
    if cloud_backup_protocol::AccountId::try_from(expected_user_hash.to_owned()).is_err() {
        return Err(fail("invalid_expected_user_hash"));
    }
    let (seed, keypair) = verified_seed_pair(expected_user_hash).map_err(fail)?;
    let mut target = FreshHome::preflight_source_free_dev(destination)
        .map_err(|_| fail("invalid_fresh_private_destination"))?
        .create()
        .map_err(|_| fail("cannot_create_fresh_private_destination"))?;
    target
        .write(IDENTITY_KEY_FILE, seed.as_ref())
        .map_err(|_| fail("cannot_write_protected_identity"))?;
    let device_id = uuid::Uuid::new_v4().to_string();
    let after = |code| (code, true, Some(device_id.clone()));
    let registered = super::block_on_cloud(
        "account registration",
        register_native_device(Environment::Dev, &keypair, &device_id, None),
    )
    .map_err(|_| after("dev_registration_result_uncertain"))?;
    if registered.user_hash != expected_user_hash {
        return Err(after("registered_account_mismatch"));
    }
    let bytes = SavedCredentials::new(exemem_api_url_for(Environment::Dev), registered.api_key)
        .to_json()
        .map_err(|_| after("cannot_encode_protected_credentials"))?;
    target
        .write(CLOUD_CREDENTIALS_FILE, &bytes)
        .and_then(|()| target.commit())
        .map_err(|_| after("cannot_commit_protected_credentials"))?;
    Ok(device_id)
}
