//! AuthClient HTTP transport: construct, credential refresh, POST.

use super::ops::register_db::DB_HASH_NOT_REGISTERED;
use super::AuthClient;
use super::{
    AuthRefreshCallback, SyncAuth, DEFAULT_AUTH_REQUEST_TIMEOUT, MAX_AUTH_REFRESH_RETRIES,
};
use crate::sync::error::{SyncError, SyncResult};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

impl AuthClient {
    pub fn new(http: Arc<Client>, base_url: String, auth: SyncAuth) -> Self {
        Self {
            http,
            base_url,
            auth: Arc::new(RwLock::new(auth)),
            auth_refresh: None,
            db_hash: None,
            db_claim: Arc::new(tokio::sync::Mutex::new(None)),
            db_auto_claim: true,
            request_timeout: DEFAULT_AUTH_REQUEST_TIMEOUT,
        }
    }

    /// Builder variant: attach a refresh callback that's invoked on a 401.
    pub fn with_auth_refresh(mut self, cb: AuthRefreshCallback) -> Self {
        self.auth_refresh = Some(cb);
        self
    }

    /// Builder variant: override the per-request timeout (default
    /// [`DEFAULT_AUTH_REQUEST_TIMEOUT`]). Primarily for tests that need a short
    /// deadline to exercise the black-hole path without waiting 30s.
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    /// Builder variant: root storage requests at a database hash when the
    /// request does not explicitly choose a share/org/db scope.
    pub fn with_db_hash(mut self, db_hash: Option<String>) -> Self {
        self.db_hash = db_hash;
        self
    }

    /// Refuse the automatic database-root claim recovery for this client.
    ///
    /// Restore uses this mode so a scoped read that receives an unregistered
    /// 403 stays a read-only failure. Normal sync keeps auto-claim enabled.
    pub fn without_db_auto_claim(mut self) -> Self {
        self.db_auto_claim = false;
        self
    }

    /// The database root this client scopes unqualified requests to, if any.
    pub(crate) fn db_hash_scope(&self) -> Option<&str> {
        self.db_hash.as_deref()
    }

    /// Replace the current authentication credential with a fresh one.
    ///
    /// Called after a successful token refresh to update the in-memory credential
    /// so subsequent requests use the new token.
    pub async fn update_auth(&self, new_auth: SyncAuth) {
        *self.auth.write().await = new_auth;
    }

    /// Check if the current auth credential is a bearer token.
    ///
    /// Useful for callers to decide whether a refresh is needed (bearer tokens
    /// expire, API keys do not).
    pub async fn is_bearer_token(&self) -> bool {
        matches!(&*self.auth.read().await, SyncAuth::BearerToken(_))
    }

    pub(super) async fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let auth = self.auth.read().await;
        match &*auth {
            SyncAuth::ApiKey(key) => req.header("X-API-Key", key.clone()),
            SyncAuth::BearerToken(token) => req.header("Authorization", format!("Bearer {token}")),
        }
    }

    pub(super) async fn post(
        &self,
        path: &str,
        mut body: serde_json::Value,
    ) -> SyncResult<serde_json::Value> {
        self.apply_default_db_hash(&mut body);
        let first = self.post_no_default_db_hash(path, body.clone()).await;

        // A scoped request against a database this principal has never claimed
        // comes back 403 — not because the account is banned, but because the
        // storage registry has no row for it. Nothing else clears that: the
        // credential is fine, so the 401 refresh path above is not involved,
        // and every later cycle fails identically. Claim the database once and
        // retry, which is what an operator otherwise does by hand
        // (card lastdb-cloud-auto-register-db-hash-on-enable).
        if !self.is_unregistered_db_hash(&first, &body) {
            return first;
        }
        if !self.db_auto_claim {
            return first;
        }
        if !self.claim_db_once().await {
            return first;
        }
        self.post_no_default_db_hash(path, body).await
    }

    /// True when `result` is the storage service's "this principal has not
    /// registered this database" 403 *for the scope this client can claim*.
    ///
    /// Guards on the body's `db_hash` matching [`Self::db_hash`]: a caller that
    /// explicitly addressed some other database is not one whose access
    /// registering *our* scope would restore, and claiming on its behalf would
    /// be a silent scope change rather than a recovery.
    fn is_unregistered_db_hash(
        &self,
        result: &SyncResult<serde_json::Value>,
        body: &serde_json::Value,
    ) -> bool {
        let Err(SyncError::Banned(msg)) = result else {
            return false;
        };
        if !msg.contains(DB_HASH_NOT_REGISTERED) {
            return false;
        }
        let Some(scope) = self.db_hash.as_deref() else {
            return false;
        };
        body.get("db_hash").and_then(serde_json::Value::as_str) == Some(scope)
    }

    /// Run the owner claim at most once per definitive outcome; return whether
    /// the caller should retry its request.
    ///
    /// Concurrent callers that all raced into the same 403 serialize here: the
    /// first performs the claim while holding the lock, the rest observe its
    /// recorded outcome.
    ///
    /// **Latch rules (won't-undo — transient must not ban for process life):**
    /// - success → `Some(true)` (retry the original scoped request)
    /// - definitive ownership refusal / ban → `Some(false)` so a database owned
    ///   by another principal produces exactly one `register_db` call, not one
    ///   per sync cycle
    /// - network / timeout / 5xx / other transient errors → leave `None` so a
    ///   later cycle retries the claim (first-enable during a blip must not
    ///   disable auto-claim until process restart)
    async fn claim_db_once(&self) -> bool {
        let mut claim = self.db_claim.lock().await;
        if let Some(previous) = *claim {
            return previous;
        }
        match self.register_db().await {
            Ok(registration) => {
                tracing::info!(
                    db_hash = %registration.db_hash,
                    role = %registration.role,
                    "claimed cloud storage database root for this principal"
                );
                *claim = Some(true);
                true
            }
            Err(e) if Self::is_permanent_register_db_refusal(&e) => {
                tracing::warn!(
                    error = %e,
                    "could not claim cloud storage database root; cloud sync stays unavailable \
                     for this database until the claim is resolved"
                );
                *claim = Some(false);
                false
            }
            Err(e) => {
                // Leave latch as None — next unregistered-403 may retry.
                tracing::warn!(
                    error = %e,
                    "transient failure claiming cloud storage database root; will retry on a later request"
                );
                false
            }
        }
    }

    /// Ownership denial and permanent ban are the only errors that must latch
    /// as "do not claim again". Transport/5xx/parse flakes must not.
    fn is_permanent_register_db_refusal(err: &SyncError) -> bool {
        matches!(err, SyncError::Banned(_))
    }

    /// POST without applying the client's default `db_hash`.
    ///
    /// This is intentionally narrow: new Mini writes and ordinary reads should
    /// stay database-rooted, but bootstrap migration needs to probe the legacy
    /// authenticated-principal root when a db_hash root has not been populated
    /// yet.
    pub(super) async fn post_no_default_db_hash(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> SyncResult<serde_json::Value> {
        let first = self.post_once(path, body.clone()).await;
        if !matches!(first, Err(SyncError::Auth(_))) || self.auth_refresh.is_none() {
            return first;
        }

        // On a 401, refresh the credential and retry. Recovers an `AuthClient`
        // whose credential went stale (e.g. the node re-authenticated or
        // bootstrap issued a fresh api_key after this client was built).
        //
        // Retry up to `MAX_AUTH_REFRESH_RETRIES` with a short escalating
        // backoff, not just once: a PEER device sharing this account keeps
        // re-registering (each register deactivates prior keys), so a
        // long-running pull — notably a cloud restore on a large account
        // while another device is actively syncing — can have its freshly
        // minted key deactivated again within the same request window. A
        // single retry loses that race; a bounded backoff loop rides through
        // the peer's intermittent key rotation until a quiet window opens
        // (verified 2026-07-11: a large-account cloud restore was evicted
        // mid-stream by the primary's re-auth churn — card
        // cloud-restore-device-key-deactivated-mid-large-restore). Bounded so
        // a genuinely bad credential still fails fast rather than looping.
        let mut last = first;
        for attempt in 1..=MAX_AUTH_REFRESH_RETRIES {
            self.refresh_auth_once().await?;
            last = self.post_once(path, body.clone()).await;
            if !matches!(last, Err(SyncError::Auth(_))) {
                return last;
            }
            if attempt < MAX_AUTH_REFRESH_RETRIES {
                // Escalating backoff (250ms, 500ms, …) gives a peer's in-flight
                // register time to settle before we mint again and collide anew.
                tokio::time::sleep(Duration::from_millis(250 * u64::from(attempt))).await;
            }
        }
        last
    }

    fn apply_default_db_hash(&self, body: &mut serde_json::Value) {
        let Some(db_hash) = self.db_hash.as_deref() else {
            return;
        };
        let Some(obj) = body.as_object_mut() else {
            return;
        };
        if obj.contains_key("share_prefix")
            || obj.contains_key("org_hash")
            || obj.contains_key("db_hash")
        {
            return;
        }
        obj.insert(
            "db_hash".to_string(),
            serde_json::Value::String(db_hash.to_string()),
        );
    }

    /// Invoke the refresh callback (if any), updating the shared credential so
    /// the retry uses the fresh key. Errors if no callback is set or it fails.
    pub(super) async fn refresh_auth_once(&self) -> SyncResult<()> {
        let cb = self
            .auth_refresh
            .as_ref()
            .expect("refresh_auth_once is only called after auth_refresh is checked");
        tracing::info!("AuthClient auth failed (401); attempting credential refresh");
        let new_auth = cb().await.map_err(|e| {
            tracing::warn!(error = %e, "AuthClient credential refresh failed");
            SyncError::Auth("authentication failed after credential refresh failure".to_string())
        })?;
        self.update_auth(new_auth).await;
        tracing::info!("AuthClient credential refreshed");
        Ok(())
    }

    /// Bound the entire request/response cycle so a black-holed transport
    /// cannot leave the caller — ultimately [`super::SyncEngine::sync`] —
    /// awaiting forever. `reqwest`'s own client-level timeout covers the
    /// request, but wrapping the whole future here also bounds the streamed
    /// response-body reads (`response.text()` / `response.json()`), which is
    /// where a half-open connection stalls. A trip surfaces as
    /// [`SyncError::Network`], which the engine records as `Offline` — the same
    /// classification S3 transfer timeouts get — so state leaves `Syncing`.
    pub(super) async fn post_once(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> SyncResult<serde_json::Value> {
        match tokio::time::timeout(self.request_timeout, self.post_once_inner(path, body)).await {
            Ok(result) => result,
            Err(_) => Err(SyncError::Network(format!(
                "auth Lambda request timed out after {:?}",
                self.request_timeout
            ))),
        }
    }

    pub(super) async fn post_once_inner(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> SyncResult<serde_json::Value> {
        let url = format!("{}{}", self.base_url, path);
        let req = self.http.post(&url).json(&body);
        let req = self.apply_auth(req).await;
        let req = observability::propagation::inject_w3c(req);

        let response = req.send().await.map_err(|e| {
            if e.is_timeout() {
                SyncError::Network(format!("auth Lambda timeout: {e}"))
            } else if e.is_connect() {
                SyncError::Network(format!("auth Lambda unreachable: {e}"))
            } else {
                SyncError::Network(e.to_string())
            }
        })?;

        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            // Surface the Lambda's error body (consistent with the 5xx branch
            // below) instead of an opaque message. A 401 here is almost always
            // a stale/wrong credential or an endpoint pointed at the wrong env;
            // the body (e.g. "invalid api key") is what makes that diagnosable.
            // No credential material is logged — only the server's response.
            let body = response.text().await.unwrap_or_default();
            let detail = if body.is_empty() {
                String::new()
            } else {
                format!(": {body}")
            };
            return Err(SyncError::Auth(format!(
                "authentication failed — re-authenticate (HTTP 401{detail})"
            )));
        }

        if status == reqwest::StatusCode::FORBIDDEN {
            let body = response.text().await.unwrap_or_default();
            let body = forbidden_detail(&body);
            let detail = if body.is_empty() {
                String::new()
            } else {
                format!(": {body}")
            };
            return Err(SyncError::Banned(format!("banned (HTTP 403{detail})")));
        }

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let body = response.text().await.unwrap_or_default();
            let detail = if body.trim().is_empty() {
                "HTTP 429".to_string()
            } else {
                format!("HTTP 429: {}", body.trim())
            };
            return Err(SyncError::QuotaExceeded(detail));
        }

        if status.is_server_error() {
            // A 5xx from the auth Lambda is a SERVER-side transient (cold start,
            // dependency blip, throttle) — NOT a credential problem. Classifying
            // it as `Auth` was wrong twice over: it drove the engine into the
            // credential-refresh path (churning a perfectly valid token), and
            // `Auth` errors back off toward the 1h auth cap, so a brief Lambda
            // 5xx could leave sync unavailable for up to an hour after recovery.
            // Classify as `Network` so it retries on the normal offline backoff
            // and never touches the credential-refresh path.
            let body = response.text().await.unwrap_or_default();
            return Err(SyncError::Network(format!(
                "auth Lambda error: HTTP {status}: {body}"
            )));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| SyncError::Serialization(format!("invalid JSON from auth Lambda: {e}")))?;

        Ok(json)
    }
}

fn forbidden_detail(body: &str) -> String {
    let body = body.trim();
    if body.is_empty() {
        return String::new();
    }

    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return body.to_string();
    };

    let reason = value.get("reason").and_then(serde_json::Value::as_str);
    let status = value
        .get("status")
        .and_then(serde_json::Value::as_i64)
        .or_else(|| value.get("statusCode").and_then(serde_json::Value::as_i64));

    match (reason, status) {
        (Some(reason), Some(status)) => format!("reason={reason} status={status}"),
        (Some(reason), None) => format!("reason={reason}"),
        _ => body.to_string(),
    }
}
