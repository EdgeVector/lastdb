//! Stripe-hosted payments bound to this home's existing native account.

use std::path::Path;
use std::time::Duration;

use fold_db::security::Ed25519KeyPair;
use folddb_profile::endpoints::{environment_for_exemem_api_url, exemem_api_url_for, Environment};
use lastdb_node::cloud::CLOUD_SYNC_PAUSED_FILE;
use lastdb_node::host::IDENTITY_KEY_FILE;
use serde::Deserialize;
use zeroize::{Zeroize, Zeroizing};

use super::recovery_home::files;
use super::CloudPaymentCommand;

const RESPONSE_LIMIT: usize = 65_536;
const REQUEST_FAILED: &str =
    "Payment request failed. No payment was confirmed; check status before retry.";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Credentials {
    api_url: String,
    api_key: String,
}

impl Drop for Credentials {
    fn drop(&mut self) {
        self.api_key.zeroize();
    }
}

struct Account {
    api_key: Zeroizing<String>,
    user_hash: String,
    environment: Environment,
}

#[derive(Deserialize)]
struct Status {
    ok: bool,
    user_hash: String,
    configured: bool,
    status: String,
    can_checkout: bool,
    can_manage: bool,
}

#[derive(Deserialize)]
struct HostedUrl {
    ok: bool,
    url: String,
}

pub(super) fn command(
    home: Option<&Path>,
    action: CloudPaymentCommand,
    json: bool,
) -> Result<(), String> {
    let home = home.ok_or("payments requires an explicit absolute --data-dir")?;
    let account = account(home)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot construct payment request runtime")?;
    let (status, url) = runtime.block_on(run(&account, action))?;
    print(&account, action, &status, url, json)
}

fn account(home: &Path) -> Result<Account, String> {
    let directory = files::open_directory(home)?;
    files::require_owned_directory(&directory)?;
    let seed = Zeroizing::new(files::read_private_file(&directory, IDENTITY_KEY_FILE, 32)?);
    if seed.len() != 32 {
        return Err("saved native identity must contain exactly 32 bytes".into());
    }
    let pair = Ed25519KeyPair::from_secret_key(&seed)
        .map_err(|_| "cannot read saved native account identity")?;
    let user_hash = lastdb_identity::user_hash_from_pubkey(&pair.public_key_base64())
        .map_err(|_| "cannot derive saved native account identity")?;
    let bytes = Zeroizing::new(files::read_private_file(
        &directory,
        CLOUD_SYNC_PAUSED_FILE,
        RESPONSE_LIMIT as u64,
    )?);
    let mut credentials: Credentials =
        serde_json::from_slice(&bytes).map_err(|_| "invalid saved cloud credentials")?;
    let environment = environment_for_exemem_api_url(&credentials.api_url)
        .filter(|env| credentials.api_url == exemem_api_url_for(*env))
        .ok_or("payments requires a saved compiled Exemem endpoint")?;
    if !credentials.api_key.starts_with("em_")
        || credentials.api_key.len() != 51
        || !credentials.api_key.as_bytes()[3..]
            .iter()
            .all(u8::is_ascii_hexdigit)
    {
        return Err("invalid saved native account API key".into());
    }
    Ok(Account {
        api_key: Zeroizing::new(std::mem::take(&mut credentials.api_key)),
        user_hash,
        environment,
    })
}

async fn run(
    account: &Account,
    action: CloudPaymentCommand,
) -> Result<(Status, Option<String>), String> {
    // trace-egress: propagate (exemem payments API; short-lived CLI client)
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|_| REQUEST_FAILED)?;
    // The server derives this field from the saved API key, never caller input.
    // Check it before a checkout or portal request can create a hosted session.
    let status: Status = request(&client, account, "status", false).await?;
    if !status.ok || status.user_hash != account.user_hash {
        return Err("saved credentials do not authorize this native account".into());
    }
    if !matches!(
        status.status.as_str(),
        "unavailable"
            | "none"
            | "creating"
            | "pending"
            | "checkout_expired"
            | "incomplete"
            | "incomplete_expired"
            | "trialing"
            | "active"
            | "past_due"
            | "canceled"
            | "unpaid"
            | "paused"
            | "deleting"
    ) {
        return Err(REQUEST_FAILED.into());
    }
    let (path, host, allowed) = match action {
        CloudPaymentCommand::Status => return Ok((status, None)),
        CloudPaymentCommand::Checkout => ("checkout", "checkout.stripe.com", status.can_checkout),
        CloudPaymentCommand::Portal => ("portal", "billing.stripe.com", status.can_manage),
    };
    if !status.configured || !allowed {
        return Err("This payment action is unavailable for the native account.".into());
    }
    let hosted: HostedUrl = request(&client, account, path, true).await?;
    let url = reqwest::Url::parse(&hosted.url).map_err(|_| REQUEST_FAILED)?;
    if !hosted.ok
        || hosted.url.len() > 8192
        || url.scheme() != "https"
        || url.host_str() != Some(host)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
    {
        return Err(REQUEST_FAILED.into());
    }
    Ok((status, Some(url.to_string())))
}

async fn request<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    account: &Account,
    action: &str,
    post: bool,
) -> Result<T, String> {
    let url = format!(
        "{}/api/auth/payments/{action}",
        exemem_api_url_for(account.environment)
    );
    let builder = if post {
        client.post(&url).json(&serde_json::json!({}))
    } else {
        client.get(&url)
    };
    let mut response = builder
        .bearer_auth(account.api_key.as_str())
        .send()
        .await
        .map_err(|_| REQUEST_FAILED)?;
    if !response.status().is_success() || response.url().as_str() != url {
        // Provider and service error bodies never enter output or logs.
        return Err(REQUEST_FAILED.into());
    }
    let mut bytes = Zeroizing::new(Vec::new());
    while let Some(chunk) = response.chunk().await.map_err(|_| REQUEST_FAILED)? {
        if chunk.len() > RESPONSE_LIMIT.saturating_sub(bytes.len()) {
            return Err(REQUEST_FAILED.into());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| REQUEST_FAILED.into())
}

fn print(
    account: &Account,
    action: CloudPaymentCommand,
    status: &Status,
    url: Option<String>,
    json: bool,
) -> Result<(), String> {
    if json {
        let action = match action {
            CloudPaymentCommand::Status => "status",
            CloudPaymentCommand::Checkout => "checkout",
            CloudPaymentCommand::Portal => "portal",
        };
        let mut result = serde_json::json!({
            "schema": "lastdb.cloud-payments.v1", "ok": true,
            "account_id": account.user_hash, "environment": account.environment.as_str(),
            "action": action, "configured": status.configured, "status": status.status,
            "can_checkout": status.can_checkout, "can_manage": status.can_manage,
        });
        if let Some(url) = url {
            result["url"] = url.into();
        }
        serde_json::to_writer(std::io::stdout().lock(), &result)
            .map_err(|_| "cannot write payment result")?;
        println!();
    } else if let Some(url) = url {
        println!("{url}");
    } else {
        println!("Payment status: {}", status.status);
    }
    Ok(())
}
