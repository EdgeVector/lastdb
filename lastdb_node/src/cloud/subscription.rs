//! Subscription status output, paid setup, and auth refresh.

use super::*;

/// Print human-readable subscription status, with loud fix steps if blocked.
pub fn print_subscription_status(status: &serde_json::Value) {
    let plan = status.get("plan").and_then(|v| v.as_str()).unwrap_or("?");
    let access = status
        .get("access_allowed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let used = status
        .pointer("/storage/used_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let quota = status
        .pointer("/storage/quota_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let files = status
        .pointer("/storage/file_reference_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let db = status
        .pointer("/storage/database_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);

    println!("plan:            {plan}");
    println!("access_allowed:  {access}");
    println!("storage:         used={used}  quota={quota}  (db={db} files={files})");
    if let Some(dbs) = status
        .pointer("/storage/databases")
        .and_then(|v| v.as_array())
    {
        if !dbs.is_empty() {
            println!("databases:");
            for d in dbs {
                let kind = d.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
                let scope = d.get("scope").and_then(|v| v.as_str()).unwrap_or("?");
                let d_used = d
                    .get("used_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let d_db = d
                    .get("database_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let d_files = d
                    .get("file_reference_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let scope_short = if scope.len() > 16 {
                    format!("{}…", &scope[..12])
                } else {
                    scope.to_string()
                };
                println!("  - {kind:8} {scope_short}  used={d_used} (db={d_db} files={d_files})");
            }
        }
    }

    if !access {
        eprintln!();
        eprintln!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        eprintln!("!!  CLOUD SYNC BLOCKED — payment failed or inactive       !!");
        eprintln!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        if let Some(msg) = status.get("access_denied_message").and_then(|v| v.as_str()) {
            eprintln!("{msg}");
        } else if let Some(steps) = status.get("how_to_fix").and_then(|v| v.as_array()) {
            eprintln!("How to fix:");
            for step in steps {
                if let Some(s) = step.as_str() {
                    eprintln!("  • {s}");
                }
            }
        } else {
            eprintln!("Run:  lastdb cloud fix-billing   (update card / pay invoice)");
            eprintln!("  or: lastdb cloud upgrade       (new Checkout)");
            eprintln!("Then: lastdb cloud status        (need plan=paid, access_allowed=true)");
        }
        eprintln!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    }
}

/// Fresh-DB paid cloud setup: ensure identity → Stripe Checkout (sandbox) →
/// wait for payment → register without invite → write `cloud_sync.json`.
///
/// Stripe test card: `4242 4242 4242 4242`, any future expiry, any CVC.
pub async fn setup_paid(home: &Path, api_url: &str, force: bool) -> Result<(), String> {
    let (keypair, _seed, created) = ensure_identity(home, force)?;
    if created {
        eprintln!("Created new identity at {}/identity.key", home.display());
    } else {
        eprintln!("Using existing identity at {}/identity.key", home.display());
    }

    let user_hash = host::user_hash_from_pubkey(&keypair.public_key_base64())?;
    eprintln!("Account user_hash={user_hash}");
    eprintln!("Opening Stripe Checkout for paid cloud sync ($10/mo, 50 GB, test mode) ...");

    let (checkout_url, checkout_hash) = create_paid_checkout(api_url, &keypair, None, None).await?;
    debug_assert_eq!(user_hash, checkout_hash);
    eprintln!();
    eprintln!("Pay here (Stripe sandbox):");
    eprintln!("  {checkout_url}");
    eprintln!();
    eprintln!("After payment succeeds, press Enter to register this device and enable sync.");
    eprintln!("(Webhook needs a few seconds — if register fails, wait and re-run with the same identity.)");
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .map_err(|e| format!("failed to read confirmation: {e}"))?;

    eprintln!("Registering this device with {api_url} (paid entitlement, no invite) ...");
    let data_dir = home.join("data");
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| format!("failed to create data dir {}: {e}", data_dir.display()))?;
    let device_id = data_dir
        .to_str()
        .map(fold_db::sync::get_or_create_device_id);
    // No invite — server accepts when BillingTable.plan == paid for user_hash.
    let registered = signed_register(api_url, &keypair, None, device_id.as_deref()).await?;
    write_cloud_sync_config(home, api_url, &registered.api_key)?;
    let _ = std::fs::remove_file(home.join(BOOTSTRAP_DONE_FILE));

    match subscription_status(api_url, &registered.api_key).await {
        Ok(status) => {
            eprintln!(
                "Subscription status: plan={} access_allowed={} quota_bytes={}",
                status.get("plan").and_then(|v| v.as_str()).unwrap_or("?"),
                status
                    .get("access_allowed")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
                status
                    .pointer("/storage/quota_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
            );
        }
        Err(e) => eprintln!("(status check skipped: {e})"),
    }

    eprintln!(
        "Connected (paid). account user_hash={}",
        registered.user_hash
    );
    eprintln!("Start (or restart) the daemon to begin syncing.");
    Ok(())
}

/// Build the throttleable auth-refresh callback the sync engine calls on a
/// 401: re-register from the identity key (L0 → fresh L1) and persist the
/// new api_key so the next boot uses it. One shared refresh path, and the
/// cache is never wiped as error handling (canonical-auth L1 rule).
pub fn auth_refresh_callback(
    home: std::path::PathBuf,
    api_url: String,
    keypair: Arc<Ed25519KeyPair>,
) -> AuthRefreshCallback {
    Arc::new(move || {
        let home = home.clone();
        let api_url = api_url.clone();
        let keypair = Arc::clone(&keypair);
        Box::pin(async move {
            tracing::info!(target: "lastdbd::cloud", "sync auth expired; re-registering device");
            let device_id = home
                .join("data")
                .to_str()
                .map(fold_db::sync::get_or_create_device_id);
            let registered =
                signed_register(&api_url, &keypair, None, device_id.as_deref()).await?;
            let file_name = if cloud_sync_file_state(&home) == "off" {
                CLOUD_SYNC_PAUSED_FILE
            } else {
                CLOUD_SYNC_CONFIG_FILE
            };
            if let Err(e) =
                write_cloud_sync_config_at(&home, &api_url, &registered.api_key, file_name)
            {
                // Refresh still succeeds for the running engine; only the
                // persisted cache is stale. Log, don't fail the refresh.
                tracing::warn!(target: "lastdbd::cloud", error = %e,
                    "refreshed session but failed to persist new api_key");
            }
            Ok(SyncAuth::ApiKey(registered.api_key))
        })
    })
}
