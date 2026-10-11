//! Authenticated exact-key DELETE tickets stay in memory until explicit intent.

use super::auth::AuthClient;
use super::cloud_purge::protocol::CloudAccount;
use super::error::{SyncError, SyncResult};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const GRANT_LIMIT: usize = 4096;
const LOCAL_PREFIX: &str = "lastdb-delete://";

#[derive(Clone)]
struct Grant {
    auth: AuthClient,
    account: CloudAccount,
    key: String,
    ticket: String,
    deadline: Instant,
}

fn grants() -> &'static Mutex<HashMap<String, Grant>> {
    static GRANTS: OnceLock<Mutex<HashMap<String, Grant>>> = OnceLock::new();
    GRANTS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn is_local_reference(url: &str) -> bool {
    url.starts_with(LOCAL_PREFIX)
}

pub(crate) fn register_response(auth: &AuthClient, response: &Value) -> SyncResult<()> {
    let mut captured = Vec::new();
    for item in response
        .get("urls")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let local = item
            .get("url")
            .and_then(Value::as_str)
            .is_some_and(is_local_reference);
        if !local && item.get("deletion_ticket").is_none() {
            continue;
        }
        let text = |field: &str| {
            item.get(field)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
        };
        let url = text("url").ok_or_else(|| invalid("missing local reference"))?;
        let ticket = text("deletion_ticket").ok_or_else(|| invalid("missing private ticket"))?;
        let key = text("key").ok_or_else(|| invalid("missing exact key"))?;
        let ttl = item
            .get("expires_in_secs")
            .and_then(Value::as_u64)
            .filter(|ttl| *ttl > 0 && *ttl <= 86400)
            .ok_or_else(|| invalid("invalid expiry"))?;
        if item.get("method").and_then(Value::as_str) != Some("DELETE")
            || !is_local_reference(url)
            || url.len() == LOCAL_PREFIX.len()
        {
            return Err(invalid("invalid method or local reference"));
        }
        let account: CloudAccount = serde_json::from_value(
            item.get("account")
                .cloned()
                .ok_or_else(|| invalid("missing account"))?,
        )?;
        if account.id.is_empty() {
            return Err(invalid("empty account"));
        }
        captured.push((
            crate::hex::sha256_hex(url.as_bytes()),
            Grant {
                auth: auth.clone(),
                account,
                key: key.to_string(),
                ticket: ticket.to_string(),
                deadline: Instant::now() + Duration::from_secs(ttl),
            },
        ));
    }
    let mut grants = grants().lock().map_err(|_| invalid("grant lock failed"))?;
    grants.retain(|_, grant| grant.deadline > Instant::now());
    for (identity, grant) in captured {
        if grants.len() >= GRANT_LIMIT && !grants.contains_key(&identity) {
            return Err(invalid("too many pending grants"));
        }
        grants.insert(identity, grant);
    }
    Ok(())
}

pub(crate) async fn complete(url: &str) -> SyncResult<()> {
    let identity = crate::hex::sha256_hex(url.as_bytes());
    let grant = grants()
        .lock()
        .map_err(|_| invalid("grant lock failed"))?
        .get(&identity)
        .filter(|grant| grant.deadline > Instant::now())
        .cloned()
        .ok_or_else(|| invalid("captured grant expired or missing"))?;
    grant
        .auth
        .complete_cloud_delete(&grant.account, &grant.key, &grant.ticket)
        .await?;
    if let Ok(mut grants) = grants().lock() {
        grants.remove(&identity);
    }
    Ok(())
}

fn invalid(detail: &str) -> SyncError {
    SyncError::Storage(format!("cloud delete grant: {detail}"))
}
