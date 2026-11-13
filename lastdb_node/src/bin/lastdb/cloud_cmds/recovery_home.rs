//! Protected credential homes: offline transfer or explicit source-free registration.
use super::{
    block_on_cloud, read_stdin_secret, CloudCommand, FreshHome, PrepareRecoveryArgs,
    RecoverCredentialsArgs,
};
use base64::Engine as _;
use cloud_backup_protocol::AccountId;
use fold_db::security::Ed25519KeyPair;
use lastdb_node::cloud::{register_native_device, SavedCredentials, CLOUD_CREDENTIALS_FILE};
use lastdb_node::host::{user_hash_from_pubkey, IDENTITY_KEY_FILE};
use lastdb_node::offline_home::{
    entry_present, open_directory, read_private_file, require_absolute, require_owned_directory,
    require_private_file,
};
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use zeroize::Zeroizing;

const INTENT: &str = ".native_backup_registration.json";
const RECOVERY_ERRS: [&str; 4] = [
    "registration result is uncertain; retain this credentials home and its device intent; do not retry automatically",
    "registered account differs; retain the device intent",
    "cannot encode paused credentials; retain the device intent",
    "cannot commit paused credentials; retain the device intent",
];
const DEV_ERRS: [&str; 4] = [
    "dev_registration_result_uncertain",
    "registered_account_mismatch",
    "cannot_encode_protected_credentials",
    "cannot_commit_protected_credentials",
];

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
        } => {
            let result = recover_dev_seed(source, &into, &expected_user_hash, identity_seed_stdin);
            if json {
                let receipt = match &result {
                    Ok(id) => serde_json::json!({
                        "schema": "lastdb.dev-credentials-recovery.v1", "ok": true, "environment": "dev",
                        "user_hash_matches_expected": true, "fresh_device_id": id,
                        "credentials_only": true, "cloud_sync_off": true,
                        "data_directory_created": false, "files": 2, "home_mode": "0700", "file_mode": "0600",
                        "identity_seed_output": false, "api_key_output": false, "local_store_opened": false,
                        "cloud_data_published": false, "device_registered": true,
                        "remote_key_rotation_policy_verified": false, "credentials_home_committed": true,
                    }),
                    Err((code, committed, id)) => serde_json::json!({
                        "schema": "lastdb.dev-credentials-recovery.v1", "ok": false, "environment": "dev",
                        "code": code, "registration_may_have_committed": committed,
                        "fresh_device_id": id, "identity_seed_output": false, "api_key_output": false,
                        "local_store_opened": false, "cloud_data_published": false, "credentials_home_committed": false,
                    }),
                };
                emit_receipt(true, &receipt, "")?;
            } else if result.is_ok() {
                println!("Prepared DEV credentials-only home; one fresh device registered; Cloud Sync stays Off.");
            }
            result.map(|_| ()).map_err(|(code, committed, _)| {
                if committed {
                    format!("{code}; remote registration may have committed; do not retry automatically")
                } else {
                    code.to_owned()
                }
            })
        }
        CloudCommand::BackupV2 { .. } => unreachable!("handled by backup dispatch"),
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
    let env = Environment::try_from_str(&args.env)
        .map_err(|_| "recovery environment must be dev or prod")?;
    let src_dir = open_directory(source)?;
    require_owned_directory(&src_dir)?;
    require_absolute(source)?;
    require_absolute(&args.into)?;
    if source.starts_with(&args.into) || args.into.starts_with(source) {
        return Err("source and recovery home paths must not overlap".into());
    }
    let target = FreshHome::preflight(&args.into, Some(&src_dir))?;
    let seed = read_private_file(&src_dir, IDENTITY_KEY_FILE, 32)?;
    if seed.len() != 32 {
        return Err("source identity must contain exactly 32 bytes".into());
    }
    if !entry_present(&src_dir, CLOUD_CREDENTIALS_FILE)? {
        return Err("source requires saved paused cloud credentials".into());
    }
    let cred_bytes = Zeroizing::new(read_private_file(&src_dir, CLOUD_CREDENTIALS_FILE, 65_536)?);
    let creds =
        SavedCredentials::parse(&cred_bytes).map_err(|_| "invalid source cloud credentials")?;
    if creds.api_url != exemem_api_url_for(env) {
        return Err("saved cloud URL does not match the selected compiled environment".into());
    }
    if read_stdin_secret(4096, "API key")?.as_str() != creds.api_key.as_str() {
        return Err("stdin API key does not match the source cloud credentials".into());
    }
    let bytes = creds
        .key_json()
        .map_err(|_| "cannot encode recovery credentials")?;
    let mut target = target.create()?;
    target.write(IDENTITY_KEY_FILE, &seed)?;
    target.write(CLOUD_CREDENTIALS_FILE, &bytes)?;
    target.commit()?;
    emit_receipt(
        args.json,
        &serde_json::json!({
            "schema": "lastdb.recovery-home.v1", "ok": true, "environment": env.as_str(),
            "credentials_only": true, "source_opened_read_only": true,
            "identity_matches_source": true, "api_key_matches_source": true,
            "saved_url_matches_environment": true, "cloud_sync_off": true,
            "data_directory_created": false, "files": 2, "home_mode": "0700", "file_mode": "0600",
        }),
        "Prepared credentials-only recovery home; Cloud Sync stays Off.",
    )
}

fn recover_credentials_home(args: RecoverCredentialsArgs) -> Result<(), String> {
    let env = Environment::try_from_str(&args.env)
        .map_err(|_| "credential environment must be dev or prod")?;
    let account =
        AccountId::try_from(args.expected_user_hash).map_err(|_| "invalid expected account")?;
    let (seed, pair) = verified_seed_pair(account.as_str())?;
    let invite = args
        .invite_code_file
        .as_deref()
        .map(read_invite_file)
        .transpose()?;
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
    register_into(
        env,
        &pair,
        &device,
        invite.as_deref().map(String::as_str),
        account.as_str(),
        &mut target,
    )
    .map_err(|i| RECOVERY_ERRS[i])?;
    emit_receipt(
        args.json,
        &serde_json::json!({
            "schema": "lastdb.credentials-recovery.v1", "ok": true,
            "environment": env.as_str(), "account_id": account, "device_id": device,
            "credentials_only": true, "cloud_sync_off": true,
            "data_directory_created": false, "local_store_opened": false,
            "api_key_output": false, "identity_seed_output": false,
        }),
        "Prepared credentials-only home; Cloud Sync stays Off.",
    )
}

fn register_into(
    env: Environment,
    pair: &Ed25519KeyPair,
    device: &str,
    invite: Option<&str>,
    expected: &str,
    target: &mut FreshHome,
) -> Result<(), usize> {
    let (api_key, user_hash) = block_on_cloud(
        "account registration",
        register_native_device(env, pair, device, invite),
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

fn read_invite_file(path: &Path) -> Result<Zeroizing<String>, String> {
    if !path.is_absolute() {
        return Err("invite file must be absolute".into());
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| "cannot open protected invite file")?;
    require_private_file(
        &file,
        Some(1024),
        "invite must be a bounded private owner file",
    )?;
    let mut bytes = Zeroizing::new(Vec::new());
    (&file)
        .take(1025)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read invite file")?;
    if bytes.len() > 1024 {
        return Err("invite file changed or exceeds its limit".into());
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| "invalid invite encoding")?
        .trim();
    (!text.is_empty() && text.is_ascii() && !text.as_bytes().iter().any(u8::is_ascii_control))
        .then(|| Zeroizing::new(text.to_owned()))
        .ok_or_else(|| "invalid invite text".into())
}

fn recover_dev_seed(
    source: Option<&Path>,
    destination: &Path,
    expected: &str,
    seed_stdin: bool,
) -> Result<String, (&'static str, bool, Option<String>)> {
    let fail = |code: &'static str| (code, false, None::<String>);
    if source.is_some() || !seed_stdin {
        return Err(fail("explicit_seed_stdin_without_data_dir_required"));
    }
    AccountId::try_from(expected.to_owned()).map_err(|_| fail("invalid_expected_user_hash"))?;
    let (seed, pair) = verified_seed_pair(expected).map_err(fail)?;
    let mut target = FreshHome::preflight_source_free_dev(destination)
        .map_err(|_| fail("invalid_fresh_private_destination"))?
        .create()
        .map_err(|_| fail("cannot_create_fresh_private_destination"))?;
    target
        .write(IDENTITY_KEY_FILE, seed.as_ref())
        .map_err(|_| fail("cannot_write_protected_identity"))?;
    let id = uuid::Uuid::new_v4().to_string();
    match register_into(Environment::Dev, &pair, &id, None, expected, &mut target) {
        Ok(()) => Ok(id),
        Err(i) => Err((DEV_ERRS[i], true, Some(id))),
    }
}
