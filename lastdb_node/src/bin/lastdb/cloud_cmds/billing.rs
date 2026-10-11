//! Exemem billing and account subcommands of `lastdb cloud`:
//! `setup-paid`, `upgrade`, `fix-billing`, `account`, and `status`.

use super::*;

/// Print where to open a URL: either "Opened <what> ..." or the failure.
fn open_or_print_url(what: &str, url: &str) {
    match open_url(url) {
        Ok(()) => {
            println!("Opened {what} in your browser.");
            println!("If it did not appear, open this URL:");
            println!("{url}");
        }
        Err(e) => {
            println!("Could not open {what} automatically ({e}).");
            println!("Open this URL:");
            println!("{url}");
        }
    }
}

fn pretty_json(value: &serde_json::Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

pub(super) fn setup_paid(
    home: &Path,
    runtime: &tokio::runtime::Runtime,
    env: Option<&str>,
    api_url: Option<String>,
    force: bool,
) -> Result<(), String> {
    let url = resolve_exemem_url(api_url, env)?;
    runtime.block_on(lastdb_node::cloud::setup_paid(home, &url, force))
}

pub(super) fn upgrade(
    home: &Path,
    runtime: &tokio::runtime::Runtime,
    env: Option<&str>,
    api_url: Option<String>,
) -> Result<(), String> {
    let (url, api_key) = load_cloud_creds(home, api_url, env)?;
    let return_urls = checkout_return_urls(&url);
    let checkout = runtime.block_on(lastdb_node::cloud::create_upgrade_checkout(
        &url,
        &api_key,
        Some(return_urls.success.as_str()),
        Some(return_urls.cancel.as_str()),
    ))?;
    open_or_print_url("Stripe Checkout", &checkout);
    println!();
    println!("Checkout will return to:");
    println!("  {}", return_urls.success);
    println!("After payment succeeds, wait ~1 minute, then run:");
    println!("  lastdb cloud status");
    println!("You need plan=paid and access_allowed=true.");
    Ok(())
}

pub(super) fn fix_billing(
    home: &Path,
    runtime: &tokio::runtime::Runtime,
    env: Option<&str>,
    api_url: Option<String>,
) -> Result<(), String> {
    let (url, api_key) = load_cloud_creds(home, api_url, env)?;
    eprintln!("Opening Stripe Billing Portal to fix payment...");
    eprintln!("In the portal: update your card and/or pay any open invoice.");
    let portal = runtime.block_on(lastdb_node::cloud::create_portal_session(
        &url, &api_key, None,
    ))?;
    println!("{portal}");
    println!();
    println!("After you fix billing in the browser, wait ~1 minute, then run:");
    println!("  lastdb cloud status");
    println!("You need plan=paid and access_allowed=true before sync works again.");
    println!();
    println!("If the portal fails (never subscribed), run instead:");
    println!("  lastdb cloud upgrade");
    Ok(())
}

pub(super) fn account(
    home: &Path,
    runtime: &tokio::runtime::Runtime,
    env: Option<&str>,
    api_url: Option<String>,
    json: bool,
    no_open: bool,
) -> Result<(), String> {
    let (url, api_key) = load_cloud_creds(home, api_url, env)?;
    let account = runtime.block_on(lastdb_node::cloud::account_link(&url, &api_key))?;
    if json {
        println!("{}", pretty_json(&account));
        return Ok(());
    }
    let account_url = account
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "account response missing url".to_string())?;
    if no_open {
        println!("Open this URL:");
        println!("{account_url}");
    } else {
        open_or_print_url("Exemem account page", account_url);
    }
    if let Some(status) = account.pointer("/account/status") {
        println!();
        lastdb_node::cloud::print_subscription_status(status);
    }
    Ok(())
}

pub(super) fn status(
    home: &Path,
    runtime: &tokio::runtime::Runtime,
    env: Option<&str>,
    api_url: Option<String>,
) -> Result<(), String> {
    let (url, api_key) = load_cloud_creds(home, api_url, env)?;
    let status = runtime.block_on(lastdb_node::cloud::subscription_status(&url, &api_key))?;
    lastdb_node::cloud::print_subscription_status(&status);
    println!();
    println!("--- raw ---");
    println!("{}", pretty_json(&status));
    Ok(())
}
