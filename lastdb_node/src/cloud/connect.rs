//! Cloud connect flow and identity preparation.

use super::*;

/// Register a copied identity against the compiled DEV endpoint.
///
/// The CLI performs the same preflight before it reads stdin. This function
/// repeats every filesystem check so non-CLI callers cannot weaken the mode.
pub async fn connect_existing_identity_dev(
    home: &Path,
    invite_code: &str,
) -> Result<ExistingIdentityDevConnectReport, String> {
    connect_existing_identity_dev_with_register(home, invite_code, |request| {
        send_signed_register(request)
    })
    .await
}

pub(super) async fn connect_existing_identity_dev_with_register<F, Fut>(
    home: &Path,
    invite_code: &str,
    register: F,
) -> Result<ExistingIdentityDevConnectReport, String>
where
    F: FnOnce(SignedRegisterRequest) -> Fut,
    Fut: Future<Output = Result<Registered, String>>,
{
    connect_existing_identity_dev_with(home, invite_code, register, |path, device_id| {
        PendingOwnerOnlyFile::create(path, device_id.as_bytes(), "fresh DEV device ID")
    })
    .await
}

pub(super) async fn connect_existing_identity_dev_with<F, Fut, W>(
    home: &Path,
    invite_code: &str,
    register: F,
    persist_device: W,
) -> Result<ExistingIdentityDevConnectReport, String>
where
    F: FnOnce(SignedRegisterRequest) -> Fut,
    Fut: Future<Output = Result<Registered, String>>,
    W: FnOnce(&Path, &str) -> Result<PendingOwnerOnlyFile, String>,
{
    let invite_code = invite_code.trim();
    if invite_code.is_empty() {
        return Err("the DEV reuse-existing-identity invite is empty".to_string());
    }

    let canonical_home = preflight_existing_identity_dev(home)?;
    let identity = prepare_connect_identity(
        &canonical_home,
        ConnectIdentityMode::ReuseExistingDev,
        || Err("reuse-existing identity mode attempted to read a recovery phrase".to_string()),
    )?;
    let original_seed = identity.keypair.secret_key_bytes();
    let local_user_hash = host::user_hash_from_pubkey(&identity.keypair.public_key_base64())?;
    let device_id = uuid::Uuid::new_v4().to_string();
    let pending_device = persist_device(
        &canonical_home.join("data").join(DEVICE_ID_FILE),
        &device_id,
    )?;
    let api_url =
        folddb_profile::endpoints::exemem_api_url_for(folddb_profile::endpoints::Environment::Dev);
    let request = signed_register_request(
        api_url,
        &identity.keypair,
        Some(invite_code),
        Some(&device_id),
    )?;
    let registered = register(request).await?;
    if registered.user_hash != local_user_hash {
        return Err(
            "DEV register returned a user_hash that does not match the copied identity".to_string(),
        );
    }

    let checked_home =
        preflight_existing_identity_dev_with_pending_device(&canonical_home, &device_id)?;
    if checked_home != canonical_home
        || validate_existing_identity_file(&checked_home)? != original_seed
    {
        return Err("copied identity changed during DEV registration".to_string());
    }

    write_new_cloud_sync_config(&canonical_home, api_url, &registered.api_key)?;
    pending_device.commit();

    Ok(ExistingIdentityDevConnectReport {
        api_url: api_url.to_string(),
        user_hash: local_user_hash,
    })
}

/// `lastdbd connect`: read the 24-word phrase from stdin, install the account
/// seed as this node's identity, mint a per-device session, and enable sync.
///
/// Refuses to replace an EXISTING, DIFFERENT identity unless `force` — a
/// wrong phrase must not silently orphan data written under the old key.
/// Run while the daemon is stopped (the next boot performs the restore).
pub async fn connect(
    home: &Path,
    api_url: &str,
    invite_code: Option<&str>,
    force: bool,
) -> Result<(), String> {
    let identity = prepare_connect_identity(
        home,
        ConnectIdentityMode::Standard { invite_code, force },
        || {
            eprintln!("Paste the 24-word recovery phrase (input is read from stdin until EOF):");
            let mut words = String::new();
            std::io::stdin()
                .read_to_string(&mut words)
                .map_err(|e| format!("failed to read phrase from stdin: {e}"))?;
            Ok(words)
        },
    )?;
    if let Some(phrase) = &identity.fresh_recovery_phrase {
        eprintln!(
            "Created a new LastDB account identity at {}/identity.key",
            home.display()
        );
        crate::host::emit_fresh_recovery_phrase(phrase);
    }
    let keypair = identity.keypair;

    eprintln!("Registering this device with {api_url} ...");
    let data_dir = home.join("data");
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| format!("failed to create data dir {}: {e}", data_dir.display()))?;
    let device_id = data_dir
        .to_str()
        .map(fold_db::sync::get_or_create_device_id);
    let registered = signed_register(api_url, &keypair, invite_code, device_id.as_deref()).await?;
    write_cloud_sync_config(home, api_url, &registered.api_key)?;

    // A fresh (or force-replaced) identity means the local store has no
    // account data yet — clear the marker so the next boot bootstraps.
    let _ = std::fs::remove_file(home.join(BOOTSTRAP_DONE_FILE));

    let user_hash = host::user_hash_from_pubkey(&keypair.public_key_base64())?;
    debug_assert_eq!(user_hash, registered.user_hash);
    eprintln!("Connected. account user_hash={}", registered.user_hash);
    eprintln!("Start (or restart) the daemon to pull the account's data and begin syncing.");
    Ok(())
}

pub(super) struct ConnectIdentity {
    pub(super) keypair: Ed25519KeyPair,
    pub(super) fresh_recovery_phrase: Option<String>,
}

#[derive(Clone, Copy)]
pub(super) enum ConnectIdentityMode<'a> {
    Standard {
        invite_code: Option<&'a str>,
        force: bool,
    },
    ReuseExistingDev,
}

pub(super) fn prepare_connect_identity<F>(
    home: &Path,
    mode: ConnectIdentityMode<'_>,
    read_recovery_phrase: F,
) -> Result<ConnectIdentity, String>
where
    F: FnOnce() -> Result<String, String>,
{
    if matches!(mode, ConnectIdentityMode::ReuseExistingDev) {
        let seed = lastdb_identity::load_seed(home)?
            .ok_or_else(|| format!("{} is missing", home.join(IDENTITY_KEY_FILE).display()))?;
        let keypair = Ed25519KeyPair::from_secret_key(&seed)
            .map_err(|_| "identity.key is not a valid Ed25519 seed".to_string())?;
        return Ok(ConnectIdentity {
            keypair,
            fresh_recovery_phrase: None,
        });
    }

    let ConnectIdentityMode::Standard { invite_code, force } = mode else {
        unreachable!("reuse-existing mode returned above")
    };
    host::ensure_node_home(home)
        .map_err(|e| format!("failed to create node home {}: {e}", home.display()))?;

    let key_path = home.join(IDENTITY_KEY_FILE);
    if invite_code.is_some() && !key_path.exists() {
        let seed = fresh_identity_seed(home);
        host::write_owner_only(&key_path, &seed)
            .map_err(|e| format!("failed to write {}: {e}", key_path.display()))?;
        let keypair = Ed25519KeyPair::from_secret_key(&seed)
            .map_err(|e| format!("generated seed was not a valid Ed25519 key: {e}"))?;
        let mnemonic = bip39::Mnemonic::from_entropy(&seed)
            .map_err(|e| format!("generated seed could not be encoded as BIP39: {e}"))?;
        return Ok(ConnectIdentity {
            keypair,
            fresh_recovery_phrase: Some(mnemonic.to_string()),
        });
    }

    let words = read_recovery_phrase()?;
    let seed = seed_from_phrase(&words)?;
    let keypair = Ed25519KeyPair::from_secret_key(&seed)
        .map_err(|e| format!("phrase did not yield a valid Ed25519 seed: {e}"))?;

    if key_path.exists() {
        let existing = std::fs::read(&key_path)
            .map_err(|e| format!("failed to read {}: {e}", key_path.display()))?;
        if existing.as_slice() == seed {
            eprintln!("Identity already matches this phrase; leaving identity.key unchanged.");
        } else if force {
            std::fs::remove_file(&key_path)
                .map_err(|e| format!("failed to replace {}: {e}", key_path.display()))?;
            host::write_owner_only(&key_path, &seed)
                .map_err(|e| format!("failed to write {}: {e}", key_path.display()))?;
            eprintln!("Replaced identity.key with the account identity (--force).");
        } else {
            return Err(format!(
                "{} already holds a DIFFERENT identity. Connecting would orphan any data \
                 written under it. Re-run with --force to replace it (the existing local \
                 data dir will no longer decrypt), or use a fresh --data-dir.",
                key_path.display()
            ));
        }
    } else {
        host::write_owner_only(&key_path, &seed)
            .map_err(|e| format!("failed to write {}: {e}", key_path.display()))?;
    }

    Ok(ConnectIdentity {
        keypair,
        fresh_recovery_phrase: None,
    })
}

/// Ensure `identity.key` exists (create a fresh account key if missing).
/// Returns the keypair and whether a new identity was created.
pub(super) fn ensure_identity(
    home: &Path,
    force: bool,
) -> Result<(Ed25519KeyPair, [u8; 32], bool), String> {
    host::ensure_node_home(home)
        .map_err(|e| format!("failed to create node home {}: {e}", home.display()))?;
    let key_path = home.join(IDENTITY_KEY_FILE);
    if key_path.exists() && !force {
        let existing = std::fs::read(&key_path)
            .map_err(|e| format!("failed to read {}: {e}", key_path.display()))?;
        let seed: [u8; 32] = existing
            .as_slice()
            .try_into()
            .map_err(|_| format!("{} is not a 32-byte Ed25519 seed", key_path.display()))?;
        let keypair = Ed25519KeyPair::from_secret_key(&seed)
            .map_err(|e| format!("identity.key is not a valid Ed25519 seed: {e}"))?;
        return Ok((keypair, seed, false));
    }
    // Fresh account: random 32-byte seed (not BIP39 phrase in this path —
    // phrase export is a separate concern; paid dogfood uses the raw seed
    // under identity.key like a new Mini install).
    let seed = fresh_identity_seed(home);
    if key_path.exists() && force {
        let _ = std::fs::remove_file(&key_path);
    }
    host::write_owner_only(&key_path, &seed)
        .map_err(|e| format!("failed to write {}: {e}", key_path.display()))?;
    let keypair = Ed25519KeyPair::from_secret_key(&seed)
        .map_err(|e| format!("generated seed was not a valid Ed25519 key: {e}"))?;
    Ok((keypair, seed, true))
}

pub(super) fn fresh_identity_seed(home: &Path) -> [u8; 32] {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    // Prefer OS randomness via getrandom if available through fold_db;
    // fall back to a unique-enough seed for local dogfood only.
    let mut bytes = [0u8; 32];
    if getrandom_fill(&mut bytes).is_err() {
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        home.hash(&mut h);
        let v = h.finish().to_le_bytes();
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = v[i % 8].wrapping_add(i as u8);
        }
    }
    bytes
}

pub(super) fn getrandom_fill(buf: &mut [u8]) -> Result<(), ()> {
    // std doesn't expose getrandom; use /dev/urandom on unix.
    use std::io::Read;
    let mut f = std::fs::File::open("/dev/urandom").map_err(|_| ())?;
    f.read_exact(buf).map_err(|_| ())
}
