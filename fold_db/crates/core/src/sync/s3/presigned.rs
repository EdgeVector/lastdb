//! Provider grants and captured local references to cloud service operations.

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct PresignedUrl {
    pub url: String,
    pub method: String,
    pub expires_in_secs: u64,
}

impl std::fmt::Debug for PresignedUrl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PresignedUrl")
            .field("url", &"[private grant]")
            .field("method", &self.method)
            .field("expires_in_secs", &self.expires_in_secs)
            .finish()
    }
}

impl PresignedUrl {
    pub(super) fn provider(url: String, method: String, expires_in_secs: u64) -> Self {
        Self {
            url,
            method,
            expires_in_secs,
        }
    }
}
