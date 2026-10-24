//! Explicit environment, source-free signed registration. Retain unknown intent.
use super::{files::FreshHome, Credentials};
use base64::Engine as _;
use fold_db::security::Ed25519KeyPair;
use folddb_profile::endpoints::{exemem_api_url_for, Environment};
use lastdb_node::cloud::{
    register_native_device, CLOUD_RESUME_REQUIRED_FILE, CLOUD_SYNC_PAUSED_FILE,
};
use lastdb_node::host::IDENTITY_KEY_FILE;
use std::io::Read;
use std::path::Path;
use zeroize::Zeroizing;

#[path = "native_registration/input.rs"]
mod input;

const INTENT: &str = ".native_backup_registration.json";

pub(super) fn command(
    destination: &Path,
    environment: &str,
    expected_account: &str,
    invite_file: Option<&Path>,
    json: bool,
) -> Result<(), String> {
    let environment = Environment::try_from_str(environment)
        .map_err(|_| "credential environment must be dev or prod")?;
    let account = cloud_backup_protocol::AccountId::try_from(expected_account.to_owned())
        .map_err(|_| "invalid expected account")?;
    let seed = seed()?;
    let pair =
        Ed25519KeyPair::from_secret_key(seed.as_ref()).map_err(|_| "invalid stdin account seed")?;
    if lastdb_identity::user_hash_from_pubkey(&pair.public_key_base64())? != account.as_str() {
        return Err("stdin seed does not match the expected account".into());
    }
    let invite = invite_file.map(input::invite).transpose()?;
    input::destination(destination)?;
    let mut target = FreshHome::preflight_empty(destination)?.create()?;
    let device = uuid::Uuid::new_v4().to_string();
    let intent = serde_json::to_vec(&serde_json::json!({
        "version": 1, "account_id": account, "environment": environment.as_str(),
        "device_id": device, "registration_attempted": true,
    }))
    .map_err(|_| "cannot encode registration intent")?;
    target.write(IDENTITY_KEY_FILE, seed.as_ref())?;
    target.write(CLOUD_RESUME_REQUIRED_FILE, b"")?;
    target.write(INTENT, &intent)?;
    // Flush the stable device intent before the first possible remote effect.
    // Later failures retain this directory; reruns cannot register a new device.
    target.commit()?;
    let result = register(
        &pair,
        &device,
        environment,
        invite.as_deref().map(String::as_str),
    );
    let registered = result.map_err(|_| {
        "registration result is uncertain; retain this credentials home and its device intent; do not retry automatically"
    })?;
    if registered.user_hash != account.as_str() {
        return Err("registered account differs; retain the device intent".into());
    }
    let bytes = Zeroizing::new(
        serde_json::to_vec(&Credentials {
            api_url: exemem_api_url_for(environment).to_owned(),
            api_key: registered.api_key,
        })
        .map_err(|_| "cannot encode paused credentials; retain the device intent")?,
    );
    target
        .write(CLOUD_SYNC_PAUSED_FILE, &bytes)
        .and_then(|()| target.commit_registered())
        .map_err(|_| "cannot commit paused credentials; retain the device intent")?;
    if json {
        serde_json::to_writer(
            std::io::stdout().lock(),
            &serde_json::json!({
                "schema": "lastdb.credentials-recovery.v1", "ok": true,
                "environment": environment.as_str(), "account_id": account, "device_id": device,
                "credentials_only": true, "cloud_sync_off": true, "cloud_resume_required": true,
                "data_directory_created": false, "local_store_opened": false,
                "api_key_output": false, "identity_seed_output": false,
            }),
        )
        .map_err(|_| "cannot write credentials-only result")?;
        println!();
    } else {
        println!("Prepared credentials-only home; Cloud Sync stays Off.");
    }
    Ok(())
}

fn register(
    pair: &Ed25519KeyPair,
    device: &str,
    environment: Environment,
    invite: Option<&str>,
) -> Result<lastdb_node::cloud::Registered, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot construct account registration runtime")?;
    runtime.block_on(register_native_device(environment, pair, device, invite))
}

fn seed() -> Result<Zeroizing<[u8; 32]>, String> {
    let mut bytes = Zeroizing::new(Vec::new());
    std::io::stdin()
        .take(129)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read account seed from stdin")?;
    if bytes.len() > 128 {
        return Err("stdin account seed exceeds its byte limit".into());
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| "invalid account seed encoding")?;
    let decoded = Zeroizing::new(
        base64::engine::general_purpose::STANDARD
            .decode(text.trim())
            .map_err(|_| "invalid account seed encoding")?,
    );
    let value: [u8; 32] = decoded
        .as_slice()
        .try_into()
        .map_err(|_| "invalid account seed size")?;
    Ok(Zeroizing::new(value))
}
