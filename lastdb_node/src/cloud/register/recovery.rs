//! Bounded existing-account registration for the explicit source-free DEV action.

use std::time::Duration;

use folddb_profile::endpoints::{exemem_api_url_for, Environment};

use super::{parse_registered_response, signed_register_request, Ed25519KeyPair, Registered};

const RESPONSE_LIMIT: usize = 65_536;

/// Register exactly one fresh DEV device without an invite or caller endpoint.
///
/// A failed request may still mint a remote key. The caller must not retry
/// automatically or describe local rollback as remote key revocation.
pub async fn register_dev_recovery_device(
    keypair: &Ed25519KeyPair,
    device_id: &str,
) -> Result<Registered, String> {
    register_dev_native_device(keypair, device_id, None).await
}

/// Register one native-backup DEV device through the existing signed auth API.
/// This does not enroll a legacy recipient or change cloud control.
pub async fn register_dev_native_device(
    keypair: &Ed25519KeyPair,
    device_id: &str,
    invite_code: Option<&str>,
) -> Result<Registered, String> {
    register_native_device(Environment::Dev, keypair, device_id, invite_code).await
}

/// Register the existing identity at one compiled environment; change no local intent.
pub async fn register_native_device(
    environment: Environment,
    keypair: &Ed25519KeyPair,
    device_id: &str,
    invite_code: Option<&str>,
) -> Result<Registered, String> {
    let parsed = uuid::Uuid::parse_str(device_id).map_err(|_| "invalid native device ID")?;
    if parsed.get_version() != Some(uuid::Version::Random) {
        return Err("native enrollment requires a random device ID".into());
    }
    let request = signed_register_request(
        exemem_api_url_for(environment),
        keypair,
        invite_code,
        Some(device_id),
    )?;
    let client = fold_db::sync::build_shared_http_client()
        .map_err(|_| "cannot construct native register client")?;
    let mut response = client
        .post(request.url.clone())
        .timeout(Duration::from_secs(30))
        .json(&request.body)
        .send()
        .await
        .map_err(|_| "native register request failed")?;
    if response.status().is_redirection() || response.url() != &request.url {
        return Err("native register redirected".into());
    }
    if !response.status().is_success() {
        return Err(format!(
            "native register HTTP status {}",
            response.status().as_u16()
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "cannot read native register response")?
    {
        if chunk.len() > RESPONSE_LIMIT.saturating_sub(bytes.len()) {
            return Err("native register response exceeds supported size".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| "native register response is not UTF-8")?;
    parse_registered_response(text, &request.expected_user_hash)
}
