//! DEV-only seed recovery. Never resolve a home or open a local store.

use std::io::Read;
use std::path::Path;

use base64::Engine as _;
use fold_db::security::Ed25519KeyPair;
use folddb_profile::endpoints::{exemem_api_url_for, Environment};
use lastdb_node::cloud::{
    register_dev_recovery_device, CLOUD_RESUME_REQUIRED_FILE, CLOUD_SYNC_PAUSED_FILE,
};
use lastdb_node::host::{user_hash_from_pubkey, IDENTITY_KEY_FILE};

use super::{files::FreshHome, Credentials};

#[path = "lastsecrets.rs"]
mod lastsecrets;

const SEED_INPUT_LIMIT: u64 = 128;

struct Failure {
    code: &'static str,
    registration_may_have_committed: bool,
    device_id: Option<String>,
    lastsecrets: Box<lastsecrets::Receipt>,
}

impl Failure {
    fn before_registration(code: &'static str) -> Self {
        Self {
            code,
            registration_may_have_committed: false,
            device_id: None,
            lastsecrets: Box::default(),
        }
    }

    fn after_registration(code: &'static str, device_id: &str) -> Self {
        Self {
            code,
            registration_may_have_committed: true,
            device_id: Some(device_id.to_owned()),
            lastsecrets: Box::default(),
        }
    }

    fn with_lastsecrets(mut self, receipt: &lastsecrets::Receipt) -> Self {
        self.lastsecrets = Box::new(receipt.clone());
        self
    }
}

struct Recovered {
    device_id: String,
    lastsecrets: lastsecrets::Receipt,
}

pub(super) fn command(
    source: Option<&Path>,
    destination: &Path,
    expected_user_hash: &str,
    seed_stdin: bool,
    api_key_lastsecrets_slug: Option<&str>,
    json: bool,
) -> Result<(), String> {
    let result = recover(
        source,
        destination,
        expected_user_hash,
        seed_stdin,
        api_key_lastsecrets_slug,
    );
    let receipt = match &result {
        Ok(recovered) => serde_json::json!({
            "schema": "lastdb.dev-credentials-recovery.v1", "ok": true, "environment": "dev",
            "user_hash_matches_expected": true, "fresh_device_id": recovered.device_id,
            "credentials_only": true, "cloud_sync_off": true, "cloud_resume_required": true,
            "data_directory_created": false, "files": 3, "home_mode": "0700", "file_mode": "0600",
            "identity_seed_output": false, "api_key_output": false, "local_store_opened": false,
            "cloud_data_published": false, "device_registered": true,
            "remote_key_rotation_policy_verified": false,
            "credentials_home_committed": true,
            "lastsecrets": recovered.lastsecrets,
        }),
        Err(failure) => serde_json::json!({
            "schema": "lastdb.dev-credentials-recovery.v1", "ok": false, "environment": "dev",
            "code": failure.code, "registration_may_have_committed": failure.registration_may_have_committed,
            "fresh_device_id": failure.device_id, "identity_seed_output": false, "api_key_output": false,
            "local_store_opened": false, "cloud_data_published": false,
            "credentials_home_committed": false, "lastsecrets": failure.lastsecrets,
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

fn recover(
    source: Option<&Path>,
    destination: &Path,
    expected_user_hash: &str,
    seed_stdin: bool,
    api_key_lastsecrets_slug: Option<&str>,
) -> Result<Recovered, Failure> {
    if source.is_some() || !seed_stdin {
        return Err(Failure::before_registration(
            "explicit_seed_stdin_without_data_dir_required",
        ));
    }
    if expected_user_hash.len() != 32
        || !expected_user_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Failure::before_registration("invalid_expected_user_hash"));
    }
    let lastsecrets = api_key_lastsecrets_slug
        .map(|slug| lastsecrets::LastSecrets::preflight(slug, expected_user_hash))
        .transpose()
        .map_err(Failure::before_registration)?;
    let pending_receipt = lastsecrets
        .as_ref()
        .map(lastsecrets::LastSecrets::receipt)
        .unwrap_or_default();
    let before_registration =
        |code| Failure::before_registration(code).with_lastsecrets(&pending_receipt);
    let seed = read_seed().map_err(|failure| failure.with_lastsecrets(&pending_receipt))?;
    let keypair = Ed25519KeyPair::from_secret_key(&seed)
        .map_err(|_| before_registration("invalid_identity_seed"))?;
    let user_hash = user_hash_from_pubkey(&keypair.public_key_base64())
        .map_err(|_| before_registration("cannot_derive_account_hash"))?;
    if user_hash != expected_user_hash {
        return Err(before_registration("identity_account_mismatch"));
    }
    let target = FreshHome::preflight_source_free_dev(destination)
        .map_err(|_| before_registration("invalid_fresh_private_destination"))?;
    let mut target = target
        .create()
        .map_err(|_| before_registration("cannot_create_fresh_private_destination"))?;
    target
        .write(IDENTITY_KEY_FILE, &seed)
        .map_err(|_| before_registration("cannot_write_protected_identity"))?;
    let device_id = uuid::Uuid::new_v4().to_string();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| before_registration("cannot_create_registration_runtime"))?;
    let registered = runtime
        .block_on(register_dev_recovery_device(&keypair, &device_id))
        .map_err(|_| {
            Failure::after_registration("dev_registration_result_uncertain", &device_id)
                .with_lastsecrets(&pending_receipt)
        })?;
    if registered.user_hash != user_hash {
        return Err(
            Failure::after_registration("registered_account_mismatch", &device_id)
                .with_lastsecrets(&pending_receipt),
        );
    }
    let secret_receipt = store_registered_key(lastsecrets, &registered.api_key, &device_id)?;
    if let Err(mut failure) = commit_credentials(&mut target, registered.api_key, &device_id) {
        failure.lastsecrets = Box::new(secret_receipt);
        return Err(failure);
    }
    Ok(Recovered {
        device_id,
        lastsecrets: secret_receipt,
    })
}

fn store_registered_key(
    lastsecrets: Option<lastsecrets::LastSecrets>,
    key: &str,
    device_id: &str,
) -> Result<lastsecrets::Receipt, Failure> {
    let Some(lastsecrets) = lastsecrets else {
        return Ok(lastsecrets::Receipt::default());
    };
    lastsecrets.store(key).map_err(|error| {
        let mut failure = Failure::after_registration(error.code, device_id);
        failure.lastsecrets = Box::new(error.receipt);
        failure
    })
}

fn read_seed() -> Result<[u8; 32], Failure> {
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(SEED_INPUT_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Failure::before_registration("cannot_read_seed_stdin"))?;
    if bytes.len() as u64 > SEED_INPUT_LIMIT {
        return Err(Failure::before_registration(
            "seed_stdin_exceeds_supported_size",
        ));
    }
    let encoded = std::str::from_utf8(&bytes)
        .map_err(|_| Failure::before_registration("invalid_seed_encoding"))?;
    let seed = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .map_err(|_| Failure::before_registration("invalid_seed_encoding"))?;
    seed.try_into()
        .map_err(|_| Failure::before_registration("invalid_seed_size"))
}

fn commit_credentials(
    target: &mut FreshHome,
    api_key: String,
    device_id: &str,
) -> Result<(), Failure> {
    let credentials = Credentials {
        api_url: exemem_api_url_for(Environment::Dev).to_owned(),
        api_key,
    };
    let bytes = serde_json::to_vec_pretty(&credentials).map_err(|_| {
        Failure::after_registration("cannot_encode_protected_credentials", device_id)
    })?;
    target
        .write(CLOUD_SYNC_PAUSED_FILE, &bytes)
        .and_then(|()| target.write(CLOUD_RESUME_REQUIRED_FILE, b""))
        .and_then(|()| target.commit())
        .map_err(|_| Failure::after_registration("cannot_commit_protected_credentials", device_id))
}
