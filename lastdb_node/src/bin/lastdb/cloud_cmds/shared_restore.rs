//! Explicit owner-side shared restore. Require a registered device and an absent destination.

use super::super::SharedRestoreCommand;
use super::recovery_home::files;
use cloud_backup_client::AuthClient;
use cloud_backup_protocol::Id;
use fold_db::security::Ed25519KeyPair;
use folddb_profile::endpoints::{exemem_api_url_for, Environment};
use lastdb_node::cloud::{SavedCredentials, CLOUD_CREDENTIALS_FILE};
use lastdb_node::shared_restore::{restore_owner, SharedRestore};
use std::path::Path;
use zeroize::Zeroizing;

pub(super) fn run(
    credential_home: Option<&Path>,
    selection: SharedRestoreCommand,
) -> Result<(), String> {
    let home = credential_home
        .filter(|home| home.is_absolute())
        .ok_or("shared restore requires an explicit absolute --data-dir credential home")?;
    if !selection.into.is_absolute() || !selection.recovery_phrase_stdin {
        return Err("shared restore requires absolute --into and --recovery-phrase-stdin".into());
    }
    let database = Id::try_from(selection.database_id)
        .map_err(|_| "shared database ID must be a canonical UUID")?;
    let device = Id::try_from(selection.device_id)
        .map_err(|_| "registered device ID must be a canonical UUID")?;
    let signer = device_signer(&selection.device_key_file)?;
    let environment = match selection.env.as_str() {
        "dev" => Environment::Dev,
        "prod" => Environment::Prod,
        _ => return Err("shared restore needs an explicit cloud environment".into()),
    };
    let auth = protected_auth(home, environment)?;
    let phrase = super::read_recovery_phrase()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot construct shared recovery runtime")?;
    let home = runtime
        .block_on(restore_owner(
            auth,
            device,
            signer,
            &database,
            &selection.into,
            &phrase,
            environment,
        ))
        .map_err(|error| error.to_string())?;
    print(&home, selection.json);
    Ok(())
}

pub(super) fn device_signer(path: &Path) -> Result<Ed25519KeyPair, String> {
    let parent = path.parent().ok_or("missing shared device key directory")?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("invalid shared device key file name")?;
    let directory = files::open_directory(parent)?;
    files::require_owned_directory(&directory)?;
    let seed = Zeroizing::new(files::read_private_file(&directory, name, 32)?);
    if seed.len() != 32 {
        return Err("shared device signing seed must be exactly 32 bytes".into());
    }
    Ed25519KeyPair::from_secret_key(&seed).map_err(|_| "invalid shared device signing key".into())
}

pub(super) fn resume(
    credential_home: Option<&Path>,
    workspace: &Path,
    json: bool,
) -> Result<(), String> {
    if credential_home.is_some() || !workspace.is_absolute() {
        return Err(
            "shared restore resume requires absolute --workspace without --data-dir".into(),
        );
    }
    let home = SharedRestore::resume_activation(workspace).map_err(|error| error.to_string())?;
    print(&home, json);
    Ok(())
}

fn protected_auth(home: &Path, environment: Environment) -> Result<AuthClient, String> {
    let directory = files::open_directory(home)?;
    files::require_owned_directory(&directory)?;
    let bytes = Zeroizing::new(files::read_private_file(
        &directory,
        CLOUD_CREDENTIALS_FILE,
        65_536,
    )?);
    let credentials =
        SavedCredentials::parse(&bytes).map_err(|_| "invalid protected cloud credentials")?;
    let endpoint = exemem_api_url_for(environment);
    if credentials.api_url != endpoint || credentials.api_key.trim().is_empty() {
        return Err("saved credentials differ from the explicit shared cloud endpoint".into());
    }
    Ok(AuthClient::new(
        endpoint.to_owned(),
        credentials.api_key.as_str().to_owned(),
    ))
}

fn print(home: &Path, json: bool) {
    if json {
        let output = serde_json::json!({"activated": true, "destination": home});
        println!("{output}");
    } else {
        println!("Restored shared home at {}", home.display());
    }
}
