//! Protected credential homes: offline transfer or explicit DEV registration.
//! Neither action opens a local store or enables Cloud Sync.

use std::io::{Read, Write};
use std::path::Path;

use base64::Engine as _;
use fold_db::security::Ed25519KeyPair;
use lastdb_node::cloud::{register_dev_recovery_device, SavedCredentials, CLOUD_CREDENTIALS_FILE};
use lastdb_node::host::{user_hash_from_pubkey, IDENTITY_KEY_FILE};
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};

#[path = "recovery_home/files.rs"]
pub(super) mod files;
#[path = "recovery_home/native_registration.rs"]
mod native_registration;
use super::{CloudCommand, PrepareRecoveryArgs, RecoverCredentialsArgs};
use files::{open_directory, read_private_file, FreshHome};

const API_KEY_LIMIT: u64 = 4096;
const SEED_INPUT_LIMIT: u64 = 128;

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
        CloudCommand::RecoverCredentialsHome(RecoverCredentialsArgs {
            into,
            env,
            expected_user_hash,
            identity_seed_stdin,
            invite_code_file,
            json,
        }) => {
            if source.is_some() || !identity_seed_stdin {
                return Err(
                    "recover-credentials-home requires seed stdin without --data-dir".into(),
                );
            }
            native_registration::command(
                &into,
                &env,
                &expected_user_hash,
                invite_code_file.as_deref(),
                json,
            )
        }
        CloudCommand::PrepareRecoveryHome(PrepareRecoveryArgs {
            into,
            env,
            api_key_stdin,
            json,
        }) => {
            if !api_key_stdin {
                return Err("prepare-recovery-home requires --api-key-stdin".into());
            }
            run(source, &into, &env, json)
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

fn run(
    source: Option<&Path>,
    destination: &Path,
    environment: &str,
    json: bool,
) -> Result<(), String> {
    let source = source.ok_or("prepare-recovery-home requires an explicit --data-dir")?;
    let environment = Environment::try_from_str(environment)
        .map_err(|_| "recovery environment must be dev or prod")?;
    let source_dir = open_directory(source)?;
    files::require_owned_directory(&source_dir)?;
    files::require_disjoint(source, destination)?;
    let target = FreshHome::preflight(destination, &source_dir)?;
    let seed = read_private_file(&source_dir, IDENTITY_KEY_FILE, 32)?;
    if seed.len() != 32 {
        return Err("source identity must contain exactly 32 bytes".into());
    }
    let credentials = read_credentials(&source_dir)?;
    if credentials.api_url != exemem_api_url_for(environment) {
        return Err("saved cloud URL does not match the selected compiled environment".into());
    }
    let api_key = read_api_key()?;
    if api_key.is_empty() || api_key.as_str() != credentials.api_key.as_str() {
        return Err("stdin API key does not match the source cloud credentials".into());
    }
    // The backup consent names the source home, so the copy omits it.
    let bytes = credentials
        .without_connection()
        .to_json_pretty()
        .map_err(|_| "cannot encode recovery credentials")?;
    let mut target = target.create()?;
    target.write(IDENTITY_KEY_FILE, &seed)?;
    target.write(CLOUD_CREDENTIALS_FILE, &bytes)?;
    target.commit()?;
    print_receipt(environment, json)
}

fn read_credentials(source: &std::fs::File) -> Result<SavedCredentials, String> {
    if !files::entry_present(source, CLOUD_CREDENTIALS_FILE)? {
        return Err("source requires saved paused cloud credentials".into());
    }
    let bytes = zeroize::Zeroizing::new(read_private_file(source, CLOUD_CREDENTIALS_FILE, 65_536)?);
    SavedCredentials::parse(&bytes).map_err(|_| "invalid source cloud credentials".into())
}

fn read_api_key() -> Result<String, String> {
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(API_KEY_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read API key from stdin")?;
    if bytes.len() as u64 > API_KEY_LIMIT {
        return Err("stdin API key exceeds the supported size".into());
    }
    let value = String::from_utf8(bytes).map_err(|_| "stdin API key must be UTF-8")?;
    Ok(value.trim().to_owned())
}

fn print_receipt(environment: Environment, json: bool) -> Result<(), String> {
    if json {
        let receipt = serde_json::json!({
            "schema": "lastdb.recovery-home.v1",
            "ok": true,
            "environment": environment.as_str(),
            "credentials_only": true,
            "source_opened_read_only": true,
            "identity_matches_source": true,
            "api_key_matches_source": true,
            "saved_url_matches_environment": true,
            "cloud_sync_off": true,
            "data_directory_created": false,
            "files": 2,
            "home_mode": "0700",
            "file_mode": "0600"
        });
        serde_json::to_writer(std::io::stdout().lock(), &receipt)
            .map_err(|_| "cannot write recovery-home receipt")?;
        std::io::stdout()
            .write_all(b"\n")
            .map_err(|_| "cannot finish recovery-home receipt")?;
    } else {
        println!("Prepared credentials-only recovery home; Cloud Sync stays Off.");
    }
    Ok(())
}

struct SeedFailure {
    code: &'static str,
    registration_may_have_committed: bool,
    device_id: Option<String>,
}

impl SeedFailure {
    fn before_registration(code: &'static str) -> Self {
        Self {
            code,
            registration_may_have_committed: false,
            device_id: None,
        }
    }

    fn after_registration(code: &'static str, device_id: &str) -> Self {
        Self {
            code,
            registration_may_have_committed: true,
            device_id: Some(device_id.to_owned()),
        }
    }
}

struct SeedRecovered {
    device_id: String,
}

fn seed_registration_command(
    source: Option<&Path>,
    destination: &Path,
    expected_user_hash: &str,
    seed_stdin: bool,
    json: bool,
) -> Result<(), String> {
    let result = recover_dev_seed(source, destination, expected_user_hash, seed_stdin);
    let receipt = match &result {
        Ok(recovered) => serde_json::json!({
            "schema": "lastdb.dev-credentials-recovery.v1", "ok": true, "environment": "dev",
            "user_hash_matches_expected": true, "fresh_device_id": recovered.device_id,
            "credentials_only": true, "cloud_sync_off": true,
            "data_directory_created": false, "files": 2, "home_mode": "0700", "file_mode": "0600",
            "identity_seed_output": false, "api_key_output": false, "local_store_opened": false,
            "cloud_data_published": false, "device_registered": true,
            "remote_key_rotation_policy_verified": false,
            "credentials_home_committed": true,
        }),
        Err(failure) => serde_json::json!({
            "schema": "lastdb.dev-credentials-recovery.v1", "ok": false, "environment": "dev",
            "code": failure.code, "registration_may_have_committed": failure.registration_may_have_committed,
            "fresh_device_id": failure.device_id, "identity_seed_output": false, "api_key_output": false,
            "local_store_opened": false, "cloud_data_published": false,
            "credentials_home_committed": false,
        }),
    };
    if json {
        serde_json::to_writer(std::io::stdout().lock(), &receipt)
            .map_err(|_| "cannot write DEV recovery receipt")?;
        println!();
    } else if result.is_ok() {
        println!("Prepared DEV credentials-only home; one fresh device registered; Cloud Sync stays Off.");
    }
    result.map(|_| ()).map_err(|failure| {
        if failure.registration_may_have_committed {
            format!(
                "{}; remote registration may have committed; do not retry automatically",
                failure.code
            )
        } else {
            failure.code.to_owned()
        }
    })
}

fn recover_dev_seed(
    source: Option<&Path>,
    destination: &Path,
    expected_user_hash: &str,
    seed_stdin: bool,
) -> Result<SeedRecovered, SeedFailure> {
    if source.is_some() || !seed_stdin {
        return Err(SeedFailure::before_registration(
            "explicit_seed_stdin_without_data_dir_required",
        ));
    }
    if expected_user_hash.len() != 32
        || !expected_user_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SeedFailure::before_registration(
            "invalid_expected_user_hash",
        ));
    }
    let seed = read_dev_seed()?;
    let keypair = Ed25519KeyPair::from_secret_key(&seed)
        .map_err(|_| SeedFailure::before_registration("invalid_identity_seed"))?;
    let user_hash = user_hash_from_pubkey(&keypair.public_key_base64())
        .map_err(|_| SeedFailure::before_registration("cannot_derive_account_hash"))?;
    if user_hash != expected_user_hash {
        return Err(SeedFailure::before_registration(
            "identity_account_mismatch",
        ));
    }
    let target = FreshHome::preflight_source_free_dev(destination)
        .map_err(|_| SeedFailure::before_registration("invalid_fresh_private_destination"))?;
    let mut target = target
        .create()
        .map_err(|_| SeedFailure::before_registration("cannot_create_fresh_private_destination"))?;
    target
        .write(IDENTITY_KEY_FILE, &seed)
        .map_err(|_| SeedFailure::before_registration("cannot_write_protected_identity"))?;
    let device_id = uuid::Uuid::new_v4().to_string();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| SeedFailure::before_registration("cannot_create_registration_runtime"))?;
    let registered = runtime
        .block_on(register_dev_recovery_device(&keypair, &device_id))
        .map_err(|_| {
            SeedFailure::after_registration("dev_registration_result_uncertain", &device_id)
        })?;
    if registered.user_hash != user_hash {
        return Err(SeedFailure::after_registration(
            "registered_account_mismatch",
            &device_id,
        ));
    }
    commit_seed_credentials(&mut target, registered.api_key, &device_id)?;
    Ok(SeedRecovered { device_id })
}

fn read_dev_seed() -> Result<[u8; 32], SeedFailure> {
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(SEED_INPUT_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| SeedFailure::before_registration("cannot_read_seed_stdin"))?;
    if bytes.len() as u64 > SEED_INPUT_LIMIT {
        return Err(SeedFailure::before_registration(
            "seed_stdin_exceeds_supported_size",
        ));
    }
    let encoded = std::str::from_utf8(&bytes)
        .map_err(|_| SeedFailure::before_registration("invalid_seed_encoding"))?;
    let seed = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .map_err(|_| SeedFailure::before_registration("invalid_seed_encoding"))?;
    seed.try_into()
        .map_err(|_| SeedFailure::before_registration("invalid_seed_size"))
}

fn commit_seed_credentials(
    target: &mut FreshHome,
    api_key: String,
    device_id: &str,
) -> Result<(), SeedFailure> {
    let bytes = SavedCredentials::new(exemem_api_url_for(Environment::Dev), api_key)
        .to_json_pretty()
        .map_err(|_| {
            SeedFailure::after_registration("cannot_encode_protected_credentials", device_id)
        })?;
    target
        .write(CLOUD_CREDENTIALS_FILE, &bytes)
        .and_then(|()| target.commit())
        .map_err(|_| {
            SeedFailure::after_registration("cannot_commit_protected_credentials", device_id)
        })
}
