//! Multipart grants stay in memory and never enter a backup or a receipt.

use super::auth::AuthClient;
use super::error::{SyncError, SyncResult};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const GRANT_LIMIT: usize = 4096;

struct Grant {
    auth: AuthClient,
    ticket: String,
    account: super::cloud_purge::protocol::CloudAccount,
    deadline: Instant,
}

fn grants() -> &'static Mutex<HashMap<String, Grant>> {
    static GRANTS: OnceLock<Mutex<HashMap<String, Grant>>> = OnceLock::new();
    GRANTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn identity(url: &str) -> String {
    crate::hex::sha256_hex(url.as_bytes())
}

pub(crate) fn is_multipart(url: &str) -> bool {
    reqwest::Url::parse(url).ok().is_some_and(|url| {
        let pairs = url.query_pairs();
        pairs.clone().any(|(key, _)| key == "uploadId")
            && pairs.clone().any(|(key, _)| key == "partNumber")
    })
}

pub(crate) fn register_response(auth: &AuthClient, response: &Value) -> SyncResult<()> {
    let mut urls: Vec<&Value> = response
        .get("urls")
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .unwrap_or_default();
    if response.get("url").is_some_and(Value::is_object) {
        urls.push(&response["url"]);
    }
    if response.get("completion_ticket").is_some() {
        urls.push(response);
    }
    let mut grants = grants()
        .lock()
        .map_err(|_| SyncError::Storage("upload grant lock failed".into()))?;
    let now = Instant::now();
    grants.retain(|_, grant| grant.deadline > now);
    for item in urls {
        let Some(ticket) = item.get("completion_ticket").and_then(Value::as_str) else {
            continue;
        };
        let url = item
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(|| SyncError::Storage("multipart grant lacks URL".into()))?;
        if ticket.is_empty() || !is_multipart(url) {
            return Err(SyncError::Storage(
                "invalid multipart completion grant".into(),
            ));
        }
        let key = identity(url);
        if grants.len() >= GRANT_LIMIT && !grants.contains_key(&key) {
            return Err(SyncError::Storage("too many pending upload grants".into()));
        }
        let ttl = item
            .get("completion_expires_in_secs")
            .or_else(|| item.get("expires_in_secs"))
            .and_then(Value::as_u64)
            .unwrap_or(900)
            .min(86400);
        let account = serde_json::from_value(
            item.get("account")
                .or_else(|| response.get("account"))
                .cloned()
                .ok_or_else(|| SyncError::Storage("multipart grant lacks account".into()))?,
        )?;
        grants.insert(
            key,
            Grant {
                auth: auth.clone(),
                ticket: ticket.to_string(),
                account,
                deadline: now + Duration::from_secs(ttl),
            },
        );
    }
    Ok(())
}

pub(crate) async fn complete(
    url: &str,
    etag: Option<&str>,
    expected_etag: Option<&str>,
    create_only: bool,
) -> SyncResult<bool> {
    if !is_multipart(url) {
        return Ok(true);
    }
    let key = identity(url);
    let (auth, ticket, account, deadline) = {
        let grants = grants()
            .lock()
            .map_err(|_| SyncError::Storage("upload grant lock failed".into()))?;
        let grant = grants
            .get(&key)
            .filter(|grant| grant.deadline > Instant::now())
            .ok_or_else(|| {
                SyncError::Storage("multipart completion grant expired or missing".into())
            })?;
        (
            grant.auth.clone(),
            grant.ticket.clone(),
            grant.account.clone(),
            grant.deadline,
        )
    };
    let etag = etag
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SyncError::Storage("multipart upload lacks part ETag".into()))?;
    let committed = auth
        .complete_cloud_upload(
            &account,
            &ticket,
            etag,
            expected_etag,
            create_only,
            deadline,
        )
        .await?;
    if let Ok(mut grants) = grants().lock() {
        grants.remove(&key);
    }
    Ok(committed)
}
