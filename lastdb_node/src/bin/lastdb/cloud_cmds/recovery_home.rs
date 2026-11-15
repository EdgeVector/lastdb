//! Protected credential homes: explicit source-free registration.
use super::{FreshHome, RecoverCredentialsArgs};
use base64::Engine as _;
use cloud_backup_protocol::AccountId;
use fold_db::security::Ed25519KeyPair;
use lastdb_node::cloud::{register_native_device, SavedCredentials, CLOUD_CREDENTIALS_FILE};
use lastdb_node::host::{user_hash_from_pubkey, IDENTITY_KEY_FILE};
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use std::io::Read;
use std::path::Path;
use zeroize::Zeroizing;

const INTENT: &str = ".native_backup_registration.json";
const RECOVERY_ERRS: [&str; 4] = [
    "registration result is uncertain; retain this credentials home and its device intent; do not retry automatically",
    "registered account differs; retain the device intent",
    "cannot encode paused credentials; retain the device intent",
    "cannot commit paused credentials; retain the device intent",
];

pub(super) fn recover_credentials_home(
    source: Option<&Path>,
    args: RecoverCredentialsArgs,
) -> Result<(), String> {
    if source.is_some() || !args.identity_seed_stdin {
        return Err("recover-credentials-home requires seed stdin without --data-dir".into());
    }
    let env = Environment::try_from_str(&args.env)
        .map_err(|_| "credential environment must be dev or prod")?;
    let account =
        AccountId::try_from(args.expected_user_hash).map_err(|_| "invalid expected account")?;
    let (seed, pair) = verified_seed_pair(account.as_str())?;
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
    register_into(env, &pair, &device, account.as_str(), &mut target)
        .map_err(|i| RECOVERY_ERRS[i])?;
    if args.json {
        let receipt = serde_json::json!({
            "schema": "lastdb.credentials-recovery.v1", "ok": true,
            "environment": env.as_str(), "account_id": account, "device_id": device,
            "credentials_only": true, "cloud_sync_off": true,
            "data_directory_created": false, "local_store_opened": false,
            "api_key_output": false, "identity_seed_output": false,
        });
        serde_json::to_writer(std::io::stdout().lock(), &receipt)
            .map_err(|_| "cannot write recovery receipt")?;
        println!();
    } else {
        println!("Prepared credentials-only home; Cloud Sync stays Off.");
    }
    Ok(())
}

fn register_into(
    env: Environment,
    pair: &Ed25519KeyPair,
    device: &str,
    expected: &str,
    target: &mut FreshHome,
) -> Result<(), usize> {
    let (api_key, user_hash) = crate::block_on(
        "account registration",
        register_native_device(env, pair, device),
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
