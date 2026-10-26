//! Credential ownership and refresh only; no legacy HTTP action transport.
use super::{AuthClient, SyncAuth, DEFAULT_AUTH_REQUEST_TIMEOUT};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

impl AuthClient {
    pub fn new(base_url: String, auth: SyncAuth) -> Self {
        Self {
            base_url,
            auth: Arc::new(RwLock::new(auth)),
            auth_refresh: None,
            request_timeout: DEFAULT_AUTH_REQUEST_TIMEOUT,
        }
    }
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    pub async fn update_auth(&self, new_auth: SyncAuth) {
        *self.auth.write().await = new_auth;
    }

    pub(super) async fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let auth = self.auth.read().await;
        match &*auth {
            SyncAuth::ApiKey(key) => req.header("X-API-Key", key.clone()),
            SyncAuth::BearerToken(token) => req.header("Authorization", format!("Bearer {token}")),
        }
    }

    pub fn with_auth_refresh_callback(mut self, callback: super::AuthRefreshCallback) -> Self {
        self.auth_refresh = Some(callback);
        self
    }
}
