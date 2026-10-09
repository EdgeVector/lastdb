//! App schema distribution readiness helpers.
//!
//! Durable app writes resolve to Schema Service catalog identities before
//! storage. Shared apps add publish/attach metadata through the shared-surface
//! route; this module keeps the readiness response shapes used by Mini.

use serde::{Deserialize, Serialize};

/// Request: verify required schema identities exist on Schema Service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyDistributionReadyRequest {
    pub app_id: String,
    /// Identity hashes (or service schema names) that must resolve.
    pub schema_identities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DistributionReadyStatus {
    Present,
    Missing,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionReadyItem {
    pub identity: String,
    pub status: DistributionReadyStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyDistributionReadyResponse {
    pub app_id: String,
    pub items: Vec<DistributionReadyItem>,
    /// True only when every identity is `Present`.
    pub ready: bool,
}

/// Aggregate verify results: ready iff every item is Present and list non-empty.
pub fn distribution_ready(items: &[DistributionReadyItem]) -> bool {
    !items.is_empty()
        && items
            .iter()
            .all(|i| i.status == DistributionReadyStatus::Present)
}
