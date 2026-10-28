use super::types::{ShareRule, ShareSubscription};
use crate::db_operations::DbOperations;
use crate::error::FoldDbError;
use crate::storage::KvStore;
use std::sync::Arc;

const SHARE_RULE_TREE: &str = "share_rules";
const SHARE_SUB_TREE: &str = "share_subscriptions";

async fn namespace(ops: &DbOperations, name: &str) -> Result<Arc<dyn KvStore>, FoldDbError> {
    ops.open_namespace(name).await.map_err(FoldDbError::from)
}

async fn collect_namespace<T: serde::de::DeserializeOwned>(
    ops: &DbOperations,
    namespace_name: &str,
    label: &str,
) -> Result<Vec<T>, FoldDbError> {
    let store = namespace(ops, namespace_name).await?;
    let mut items = Vec::new();
    for (_, value) in store.scan_prefix(b"").await? {
        items.push(serde_json::from_slice(&value)?);
    }
    tracing::debug!(label, count = items.len(), "collected sharing namespace");
    Ok(items)
}

pub async fn list_share_rules_in_ops(ops: &DbOperations) -> Result<Vec<ShareRule>, FoldDbError> {
    collect_namespace(ops, SHARE_RULE_TREE, "share rules").await
}

pub async fn list_share_subscriptions_in_ops(
    ops: &DbOperations,
) -> Result<Vec<ShareSubscription>, FoldDbError> {
    collect_namespace(ops, SHARE_SUB_TREE, "share subscriptions").await
}
