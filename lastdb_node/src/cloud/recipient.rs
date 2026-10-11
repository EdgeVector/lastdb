//! Recipient addresses remain separate from account/device identity and old queues.
use super::*;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use fold_db::sync::auth::{AuthClient, SyncAuth};
use fold_db::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};

mod persistence;

static ENROLLMENT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecipientAddress {
    pub format_version: u32,
    pub account: CloudAccount,
    pub api_url: String,
    pub enrollment_id: String,
    pub recipient_pseudonym: String,
    pub identity_public_key: String,
    pub messaging_public_key: String,
}

#[derive(Serialize, Deserialize)]
struct EnrollmentIntent {
    format_version: u32,
    account: CloudAccount,
    api_url: String,
    enrollment_id: String,
    identity_public_key: String,
    messaging_public_key: String,
    address: Option<RecipientAddress>,
}

/// This runs after signed account registration and before any messaging send.
/// The durable intent precedes the server claim; retries reuse the same UUID.
pub async fn enroll_recipient_session(
    home: &Path,
    api_url: &str,
    api_key: &str,
    keypair: &Ed25519KeyPair,
) -> Result<RecipientAddress, String> {
    let http = fold_db::sync::build_shared_http_client().map_err(|error| error.to_string())?;
    let auth = AuthClient::new(
        Arc::new(http),
        api_url.into(),
        SyncAuth::ApiKey(api_key.into()),
    )
    .without_db_auto_claim();
    ensure_recipient_address(home, api_url, keypair, &auth).await
}

pub async fn ensure_recipient_address(
    home: &Path,
    api_url: &str,
    keypair: &Ed25519KeyPair,
    auth: &AuthClient,
) -> Result<RecipientAddress, String> {
    let _guard = ENROLLMENT.lock().await;
    let account = CloudAccount {
        kind: CloudAccountKind::Personal,
        id: host::user_hash_from_pubkey(&keypair.public_key_base64())?,
    };
    let api_url = api_url.trim_end_matches('/');
    let path = persistence::path(home, api_url, &account)?;
    let mut intent = if let Some(intent) = persistence::read(&path)? {
        intent
    } else {
        let intent = EnrollmentIntent {
            format_version: 1,
            account: account.clone(),
            api_url: api_url.into(),
            enrollment_id: uuid::Uuid::new_v4().to_string(),
            identity_public_key: keypair.public_key_base64(),
            messaging_public_key: messaging_public_key(keypair)?,
            address: None,
        };
        if persistence::create(&path, &intent)? {
            intent
        } else {
            persistence::read(&path)?.ok_or("the concurrent recipient intent is absent")?
        }
    };
    validate_intent(&intent, api_url, &account, keypair)?;
    if let Some(address) = &intent.address {
        return Ok(address.clone());
    }
    let enrolled = auth
        .cloud_recipient_enroll(&account, &intent.enrollment_id)
        .await
        .map_err(|error| error.to_string())?;
    let address = RecipientAddress {
        format_version: 1,
        account,
        api_url: api_url.into(),
        enrollment_id: enrolled.enrollment_id,
        recipient_pseudonym: enrolled.recipient_pseudonym,
        identity_public_key: intent.identity_public_key.clone(),
        messaging_public_key: intent.messaging_public_key.clone(),
    };
    intent.address = Some(address.clone());
    persistence::write(&path, &intent)?;
    Ok(address)
}

fn validate_intent(
    intent: &EnrollmentIntent,
    api_url: &str,
    account: &CloudAccount,
    keypair: &Ed25519KeyPair,
) -> Result<(), String> {
    if intent.format_version != 1
        || intent.account != *account
        || intent.api_url != api_url
        || intent.identity_public_key != keypair.public_key_base64()
        || intent.messaging_public_key != messaging_public_key(keypair)?
        || !uuid::Uuid::parse_str(&intent.enrollment_id)
            .is_ok_and(|uuid| uuid.to_string() == intent.enrollment_id)
    {
        return Err("recipient enrollment belongs to another account or environment".into());
    }
    if let Some(address) = &intent.address {
        if address.format_version != 1
            || address.account != *account
            || address.api_url != api_url
            || address.enrollment_id != intent.enrollment_id
            || address.identity_public_key != intent.identity_public_key
            || address.messaging_public_key != intent.messaging_public_key
            || !uuid::Uuid::parse_str(&address.recipient_pseudonym)
                .is_ok_and(|uuid| uuid.to_string() == address.recipient_pseudonym)
        {
            return Err("saved recipient address differs from its enrollment".into());
        }
    }
    Ok(())
}

fn messaging_public_key(keypair: &Ed25519KeyPair) -> Result<String, String> {
    let hk = Hkdf::<Sha256>::new(None, &keypair.secret_key_bytes());
    let mut bytes = [0_u8; 32];
    hk.expand(b"lastdb:messaging:x25519:v1", &mut bytes)
        .map_err(|_| "recipient messaging key derivation failed")?;
    Ok(B64.encode(PublicKey::from(&StaticSecret::from(bytes)).as_bytes()))
}

pub(crate) fn recipient_api_url(home: &Path) -> Result<String, String> {
    let (active, paused) = cloud_sync_paths(home);
    let path = if active.exists() { active } else { paused };
    let metadata = std::fs::symlink_metadata(&path).map_err(|_| "cloud sync is not configured")?;
    if !metadata.file_type().is_file() || metadata.len() > 64 * 1024 {
        return Err("cloud configuration must be a bounded regular file".into());
    }
    let value: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path).map_err(|_| "cloud configuration cannot be read")?,
    )
    .map_err(|_| "cloud configuration is invalid")?;
    value["api_url"]
        .as_str()
        .filter(|url| !url.is_empty())
        .map(str::to_string)
        .ok_or_else(|| "cloud configuration has no account endpoint".into())
}
