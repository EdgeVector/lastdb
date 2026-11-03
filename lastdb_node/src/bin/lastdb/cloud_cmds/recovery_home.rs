//! Protected credential homes: offline transfer or explicit DEV registration.
//! Neither action opens a local store or enables Cloud Sync.

use std::io::{Read, Write};
use std::path::Path;

use folddb_profile::endpoints::{exemem_api_url_for, Environment};
use lastdb_node::cloud::{SavedCredentials, CLOUD_CREDENTIALS_FILE};
use lastdb_node::host::IDENTITY_KEY_FILE;

#[path = "recovery_home/files.rs"]
pub(super) mod files;
#[path = "recovery_home/native_registration.rs"]
mod native_registration;
#[path = "recovery_home/seed_registration.rs"]
mod seed_registration;
use super::CloudCommand;
use files::{open_directory, read_private_file, FreshHome};

const API_KEY_LIMIT: u64 = 4096;

pub(super) fn handles(action: &CloudCommand) -> bool {
    matches!(
        action,
        CloudCommand::PrepareRecoveryHome { .. }
            | CloudCommand::RecoverDevCredentialsHome { .. }
            | CloudCommand::RecoverCredentialsHome { .. }
    )
}

pub(super) fn command(source: Option<&Path>, action: CloudCommand) -> Result<(), String> {
    match action {
        CloudCommand::RecoverCredentialsHome {
            into,
            env,
            expected_user_hash,
            identity_seed_stdin,
            invite_code_file,
            json,
        } => {
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
        CloudCommand::PrepareRecoveryHome {
            into,
            env,
            api_key_stdin,
            json,
        } => {
            if !api_key_stdin {
                return Err("prepare-recovery-home requires --api-key-stdin".into());
            }
            run(source, &into, &env, json)
        }
        CloudCommand::RecoverDevCredentialsHome {
            into,
            expected_user_hash,
            identity_seed_stdin,
            api_key_lastsecrets_slug,
            json,
        } => seed_registration::command(
            source,
            &into,
            &expected_user_hash,
            identity_seed_stdin,
            api_key_lastsecrets_slug.as_deref(),
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
