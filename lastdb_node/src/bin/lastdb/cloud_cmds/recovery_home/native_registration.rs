//! Explicit environment, source-free signed registration. Retain unknown intent.
use super::files::{open_directory, FreshHome};
use base64::Engine as _;
use fold_db::security::Ed25519KeyPair;
use lastdb_node::cloud::{register_native_device, SavedCredentials, CLOUD_CREDENTIALS_FILE};
use lastdb_node::host::IDENTITY_KEY_FILE;
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use std::fs;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use zeroize::Zeroizing;

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
    if lastdb_node::identity::user_hash_from_pubkey(&pair.public_key_base64())? != account.as_str()
    {
        return Err("stdin seed does not match the expected account".into());
    }
    let invite = invite_file.map(read_invite_file).transpose()?;
    validate_destination(destination)?;
    let mut target = FreshHome::preflight_empty(destination)?.create()?;
    let device = uuid::Uuid::new_v4().to_string();
    let intent = serde_json::to_vec(&serde_json::json!({
        "version": 1, "account_id": account, "environment": environment.as_str(),
        "device_id": device, "registration_attempted": true,
    }))
    .map_err(|_| "cannot encode registration intent")?;
    target.write(IDENTITY_KEY_FILE, seed.as_ref())?;
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
    let bytes = SavedCredentials::new(exemem_api_url_for(environment), registered.api_key)
        .to_json()
        .map_err(|_| "cannot encode paused credentials; retain the device intent")?;
    target
        .write(CLOUD_CREDENTIALS_FILE, &bytes)
        .and_then(|()| target.commit_registered())
        .map_err(|_| "cannot commit paused credentials; retain the device intent")?;
    if json {
        serde_json::to_writer(
            std::io::stdout().lock(),
            &serde_json::json!({
                "schema": "lastdb.credentials-recovery.v1", "ok": true,
                "environment": environment.as_str(), "account_id": account, "device_id": device,
                "credentials_only": true, "cloud_sync_off": true,
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

/// A caller-owned, empty private parent keeps registration data outside stores.
fn validate_destination(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or("credential destination needs a parent")?;
    let directory = open_directory(parent)?;
    let metadata = directory
        .metadata()
        .map_err(|_| "cannot inspect credential parent")?;
    if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o777 != 0o700 {
        return Err("credential parent must be an owner-only directory".into());
    }
    for entry in
        rustix::fs::Dir::read_from(&directory).map_err(|_| "cannot read credential parent")?
    {
        let entry = entry.map_err(|_| "cannot inspect credential parent")?;
        if !matches!(entry.file_name().to_bytes(), b"." | b"..") {
            return Err("credential parent must be empty".into());
        }
    }
    Ok(())
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
        || before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
    {
        return Err("invite file changed or exceeds its limit".into());
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| "invalid invite encoding")?
        .trim();
    if text.is_empty() || !text.is_ascii() || text.bytes().any(|b| b.is_ascii_control()) {
        return Err("invalid invite text".into());
    }
    Ok(Zeroizing::new(text.to_owned()))
}
