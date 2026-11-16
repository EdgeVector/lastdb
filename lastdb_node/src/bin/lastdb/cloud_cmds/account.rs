//! Pseudonymous Stripe payment commands and shared cloud session client for a LastDB home.
use super::{dev_keys, print_pretty_json, CloudAccountCommand, CloudPaymentCommand};
use cloud_backup_protocol::AccountId;
use lastdb_node::cloud::load_verified_cloud_credentials;
use lastdb_node::cloud_backup_client::{build_shared_http_client, read_bounded_bytes};
use lastdb_node::profile::endpoints::Environment;
use reqwest::{Method, Url};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;
use zeroize::Zeroizing;

const MAX_JSON_BYTES: u64 = 65_536;

pub(super) struct CloudSession {
    pub(super) env: Environment,
    pub(super) api_url: &'static str,
    pub(super) api_key: Zeroizing<String>,
    pub(super) expected_account_id: Option<String>,
}

pub(crate) fn cloud_account_command(
    data_dir: Option<PathBuf>,
    action: CloudAccountCommand,
) -> Result<(), String> {
    let data_dir_explicit = data_dir.is_some();
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let session = load_cloud_session(&home)?;
    match action {
        CloudAccountCommand::Payment(cmd) => run_payment(&home, data_dir_explicit, &session, &cmd),
        CloudAccountCommand::DeveloperKey(cmd) => {
            dev_keys::run_developer_key(&home, &session, &cmd)
        }
    }
}

pub(super) fn load_cloud_session(home: &Path) -> Result<CloudSession, String> {
    let creds = load_verified_cloud_credentials(home)?.ok_or(
        "no saved cloud credentials — run 'lastdb --data-dir <home> backup connect --env prod' first",
    )?;
    if let Some(explicit) = Environment::explicit_from_env().filter(|e| *e != creds.env) {
        return Err(format!(
            "LASTDB_ENV={} conflicts with saved {} cloud credentials in {}",
            explicit.as_str(),
            creds.env.as_str(),
            home.display()
        ));
    }
    Ok(CloudSession {
        env: creds.env,
        api_url: creds.api_url,
        api_key: creds.api_key,
        expected_account_id: creds.expected_account_id,
    })
}

fn run_payment(
    home: &Path,
    data_dir_explicit: bool,
    session: &CloudSession,
    action: &CloudPaymentCommand,
) -> Result<(), String> {
    match *action {
        CloudPaymentCommand::Status { json } => {
            payment_status(home, data_dir_explicit, session, json)
        }
        CloudPaymentCommand::Checkout { json } => payment_hosted_url(
            session,
            "/api/auth/payments/checkout",
            "checkout.stripe.com",
            "payment checkout",
            json,
        ),
        CloudPaymentCommand::Portal { json } => payment_hosted_url(
            session,
            "/api/auth/payments/portal",
            "billing.stripe.com",
            "payment portal",
            json,
        ),
    }
}

fn payment_status(
    home: &Path,
    data_dir_explicit: bool,
    session: &CloudSession,
    json: bool,
) -> Result<(), String> {
    let (status, body) = crate::block_on(
        "payment status",
        exchange_json(session, Method::GET, "/api/auth/payments/status", None),
    )?;
    ensure_ok(status, &body, "payment status")?;
    let user_hash = str_field(&body, "user_hash", "payment status")?;
    AccountId::try_from(user_hash.clone())
        .map_err(|_| "payment status returned an invalid account identifier".to_owned())?;
    if session
        .expected_account_id
        .as_deref()
        .is_some_and(|exp| exp != user_hash)
    {
        return Err("payment status returned a different account than this home's identity".into());
    }
    let configured = bool_field(&body, "configured", "payment status")?;
    let sub_status = str_field(&body, "status", "payment status")?;
    let can_checkout = bool_field(&body, "can_checkout", "payment status")?;
    let can_manage = bool_field(&body, "can_manage", "payment status")?;
    let review_required = bool_field(&body, "review_required", "payment status")?;
    if !configured || sub_status == "active" {
        lastdb_node::log_sync::request_wake(home);
        super::setup::poke_running_daemon(data_dir_explicit, home);
    }
    if json {
        print_pretty_json(&json!({
            "ok": true, "account_id": user_hash, "environment": session.env.as_str(),
            "configured": configured, "status": sub_status, "can_checkout": can_checkout,
            "can_manage": can_manage, "review_required": review_required,
        }));
        return Ok(());
    }
    println!("Account:         {user_hash}");
    println!("Environment:     {}", session.env.as_str());
    println!("Configured:      {configured}");
    println!("Status:          {sub_status}");
    println!("Can checkout:    {can_checkout}");
    println!("Can manage:      {can_manage}");
    println!("Review required: {review_required}");
    Ok(())
}

fn payment_hosted_url(
    session: &CloudSession,
    path: &str,
    expected_host: &str,
    label: &str,
    json: bool,
) -> Result<(), String> {
    let empty = json!({});
    let (status, body) = crate::block_on(
        label,
        exchange_json(session, Method::POST, path, Some(&empty)),
    )?;
    ensure_ok(status, &body, label)?;
    let url = str_field(&body, "url", label)?;
    validate_stripe_url(&url, expected_host)?;
    if json {
        print_pretty_json(&json!({
            "ok": true, "account_id": session.expected_account_id,
            "environment": session.env.as_str(), "url": url,
        }));
    } else {
        println!("{url}");
    }
    Ok(())
}

fn validate_stripe_url(raw: &str, expected_host: &str) -> Result<(), String> {
    if raw.is_empty() || raw.len() > 2048 || raw.chars().any(char::is_control) {
        return Err(format!("invalid hosted {expected_host} URL"));
    }
    let parsed = Url::parse(raw).map_err(|_| format!("invalid hosted {expected_host} URL"))?;
    let valid = parsed.scheme() == "https"
        && parsed.host_str() == Some(expected_host)
        && parsed.port().is_none()
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && !parsed.path().is_empty()
        && parsed.path() != "/";
    valid
        .then_some(())
        .ok_or_else(|| format!("refusing untrusted payment URL outside https://{expected_host}/"))
}

pub(super) async fn exchange_json(
    session: &CloudSession,
    method: Method,
    path: &str,
    body: Option<&Value>,
) -> Result<(u16, Value), String> {
    let full = format!("{}{path}", session.api_url.trim_end_matches('/'));
    let url = Url::parse(&full).map_err(|_| "invalid Exemem API URL")?;
    if url.scheme() != "https" || url.host_str().is_none() {
        return Err("Exemem API URL must use HTTPS".into());
    }
    let mut req = build_shared_http_client()
        .map_err(|_| "cannot construct cloud HTTP client")?
        .request(method, url.clone())
        .timeout(Duration::from_secs(30))
        .bearer_auth(session.api_key.as_str());
    if let Some(payload) = body {
        req = req.json(payload);
    }
    let resp = req.send().await.map_err(|_| "cloud API request failed")?;
    if resp.status().is_redirection() || resp.url() != &url {
        return Err("cloud API request redirected unexpectedly".into());
    }
    let status = resp.status().as_u16();
    let bytes = read_bounded_bytes(resp, MAX_JSON_BYTES)
        .await
        .map_err(|_| "cloud API response exceeded limit or failed")?;
    let parsed: Value =
        serde_json::from_slice(&bytes).map_err(|_| "cloud API response was not valid JSON")?;
    Ok((status, parsed))
}

pub(super) fn ensure_ok(status: u16, body: &Value, label: &str) -> Result<(), String> {
    if (200..300).contains(&status) && body.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(());
    }
    if status == 401 {
        return Err(
            "cloud API key was rejected (HTTP 401) — reconnect this home with 'lastdb backup connect'".into(),
        );
    }
    let msg = body
        .get("error")
        .or_else(|| body.get("message"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|m| !m.is_empty() && m.len() <= 256 && !m.chars().any(char::is_control));
    Err(msg.map_or_else(
        || format!("{label} failed with HTTP status {status}"),
        |detail| format!("{label} failed (HTTP {status}): {detail}"),
    ))
}

pub(super) fn str_field(body: &Value, key: &str, label: &str) -> Result<String, String> {
    body.get(key)
        .and_then(Value::as_str)
        .and_then(|s| {
            let trimmed = s.trim();
            (!trimmed.is_empty() && !trimmed.chars().any(char::is_control))
                .then(|| trimmed.to_owned())
        })
        .ok_or_else(|| format!("{label} response missing valid '{key}'"))
}

pub(super) fn bool_field(body: &Value, key: &str, label: &str) -> Result<bool, String> {
    body.get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("{label} response missing boolean '{key}'"))
}
