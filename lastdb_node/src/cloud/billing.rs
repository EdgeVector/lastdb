//! Checkout, subscription, and auth refresh.

use super::*;

/// Open a Stripe Checkout session for paid cloud-sync signup (no invite).
/// Returns (checkout_url, user_hash).
pub async fn create_paid_checkout(
    api_url: &str,
    keypair: &Ed25519KeyPair,
    success_url: Option<&str>,
    cancel_url: Option<&str>,
) -> Result<(String, String), String> {
    let public_key_hex = fold_db::hex::hex_lower(keypair.public_key_bytes());
    let timestamp = chrono::Utc::now().timestamp();
    let payload = format!("{public_key_hex}:{timestamp}");
    let signature = keypair.sign(payload.as_bytes());
    let signature_b64 = fold_db::security::KeyUtils::signature_to_base64(&signature);

    let body = paid_checkout_body(
        &public_key_hex,
        timestamp,
        &signature_b64,
        success_url,
        cancel_url,
    );

    let url = format!("{api_url}/api/subscription/create-checkout-signup");
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Failed to reach subscription API at {api_url}: {e}"))?;
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Failed to read checkout response: {e}"))?;
    let json: serde_json::Value =
        serde_json::from_str(&text).map_err(|_| format!("Invalid JSON response: {text}"))?;
    if !json
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        let error = json
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown error");
        return Err(format!("create-checkout-signup failed: {error}"));
    }
    let checkout_url = json
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "checkout response missing url".to_string())?
        .to_string();
    let user_hash = json
        .get("user_hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "checkout response missing user_hash".to_string())?
        .to_string();
    Ok((checkout_url, user_hash))
}

pub(super) fn paid_checkout_body(
    public_key_hex: &str,
    timestamp: i64,
    signature_b64: &str,
    success_url: Option<&str>,
    cancel_url: Option<&str>,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "public_key": public_key_hex,
        "timestamp": timestamp,
        "signature": signature_b64,
    });
    if let Some(u) = success_url {
        body["success_url"] = serde_json::Value::String(u.to_string());
    }
    if let Some(u) = cancel_url {
        body["cancel_url"] = serde_json::Value::String(u.to_string());
    }
    body
}

/// Authenticated upgrade checkout for an already-registered account.
pub async fn create_upgrade_checkout(
    api_url: &str,
    api_key: &str,
    success_url: Option<&str>,
    cancel_url: Option<&str>,
) -> Result<String, String> {
    let body = upgrade_checkout_body(success_url, cancel_url);
    let url = format!("{api_url}/api/subscription/create-checkout");
    let resp = reqwest::Client::new()
        .post(&url)
        // CLI mint uses `em_…` API keys (X-API-Key), not session Bearer tokens.
        .header("X-API-Key", api_key)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Failed to reach subscription API: {e}"))?;
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Failed to read checkout response: {e}"))?;
    let json: serde_json::Value =
        serde_json::from_str(&text).map_err(|_| format!("Invalid JSON response: {text}"))?;
    if !json
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        let error = json
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown error");
        return Err(format!("create-checkout failed: {error}"));
    }
    json.get("url")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| "checkout response missing url".to_string())
}

pub(super) fn upgrade_checkout_body(
    success_url: Option<&str>,
    cancel_url: Option<&str>,
) -> serde_json::Value {
    let mut body = serde_json::json!({});
    if let Some(u) = success_url {
        body["success_url"] = serde_json::Value::String(u.to_string());
    }
    if let Some(u) = cancel_url {
        body["cancel_url"] = serde_json::Value::String(u.to_string());
    }
    body
}

/// Fetch subscription status for an authenticated account.
pub async fn subscription_status(
    api_url: &str,
    api_key: &str,
) -> Result<serde_json::Value, String> {
    let url = format!("{api_url}/api/subscription/status");
    let resp = reqwest::Client::new()
        .get(&url)
        .header("X-API-Key", api_key)
        .send()
        .await
        .map_err(|e| format!("Failed to reach subscription status: {e}"))?;
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Failed to read status response: {e}"))?;
    serde_json::from_str(&text).map_err(|_| format!("Invalid JSON response: {text}"))
}

/// Open Stripe Billing Portal so the user can update card / pay invoices.
pub async fn create_portal_session(
    api_url: &str,
    api_key: &str,
    return_url: Option<&str>,
) -> Result<String, String> {
    let mut body = serde_json::json!({});
    if let Some(u) = return_url {
        body["return_url"] = serde_json::Value::String(u.to_string());
    }
    let url = format!("{api_url}/api/subscription/portal");
    let resp = reqwest::Client::new()
        .post(&url)
        .header("X-API-Key", api_key)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Failed to reach billing portal API: {e}"))?;
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Failed to read portal response: {e}"))?;
    let json: serde_json::Value =
        serde_json::from_str(&text).map_err(|_| format!("Invalid JSON response: {text}"))?;
    if !json
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        let error = json
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown error");
        return Err(format!(
            "billing portal failed: {error}\n\
             If you never completed Checkout, run: lastdb cloud upgrade"
        ));
    }
    json.get("url")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| "portal response missing url".to_string())
}

/// Mint an authenticated, short-lived Exemem account page link for the
/// connected LastDB account.
pub async fn account_link(api_url: &str, api_key: &str) -> Result<serde_json::Value, String> {
    let url = format!("{api_url}/api/subscription/account");
    let resp = reqwest::Client::new()
        .post(&url)
        .header("X-API-Key", api_key)
        .json(&serde_json::json!({}))
        .send()
        .await
        .map_err(|e| format!("Failed to reach account link API: {e}"))?;
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Failed to read account link response: {e}"))?;
    let json: serde_json::Value =
        serde_json::from_str(&text).map_err(|_| format!("Invalid JSON response: {text}"))?;
    if !json
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        let error = json
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown error");
        return Err(format!("account link failed: {error}"));
    }
    if json.get("url").and_then(|v| v.as_str()).is_none() {
        return Err("account response missing url".to_string());
    }
    Ok(json)
}
