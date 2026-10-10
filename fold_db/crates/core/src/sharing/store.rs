use super::types::{PendingDelivery, ShareRule, ShareSubscription, StagedDeliveryArtifact};
use crate::db_operations::DbOperations;
use crate::error::FoldDbError;
use crate::storage::KvStore;
use std::sync::Arc;

const SHARE_RULE_TREE: &str = "share_rules";
const SHARE_SUB_TREE: &str = "share_subscriptions";
const SHARE_DELIVERY_OUTBOX_TREE: &str = "share_delivery_outbox";

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

pub async fn store_pending_delivery_in_ops(
    ops: &DbOperations,
    delivery: &PendingDelivery,
) -> Result<(), FoldDbError> {
    let store = namespace(ops, SHARE_DELIVERY_OUTBOX_TREE).await?;
    let key = format!("delivery:{}", delivery.delivery_id);
    let value = serde_json::to_vec(delivery)?;
    store.put(key.as_bytes(), value).await?;
    Ok(())
}

pub async fn store_staged_delivery_artifact_in_ops(
    ops: &DbOperations,
    delivery_id: &str,
    artifact: &StagedDeliveryArtifact,
) -> Result<(), FoldDbError> {
    let store = namespace(ops, SHARE_DELIVERY_OUTBOX_TREE).await?;
    let key = format!("delivery_artifact:{delivery_id}");
    let value = serde_json::to_vec(artifact)?;
    store.put(key.as_bytes(), value).await?;
    Ok(())
}

pub async fn list_pending_deliveries_in_ops(
    ops: &DbOperations,
) -> Result<Vec<PendingDelivery>, FoldDbError> {
    let store = namespace(ops, SHARE_DELIVERY_OUTBOX_TREE).await?;
    let mut deliveries = Vec::new();
    let mut skipped = 0u64;
    for (key, value) in store.scan_prefix(b"delivery:").await? {
        if value.is_empty() {
            skipped += 1;
            tracing::warn!(
                key = %String::from_utf8_lossy(&key),
                "pending deliveries: skipping empty value"
            );
            continue;
        }
        match serde_json::from_slice(&value) {
            Ok(delivery) => deliveries.push(delivery),
            Err(e) => {
                skipped += 1;
                tracing::warn!(
                    key = %String::from_utf8_lossy(&key),
                    error = %e,
                    "pending deliveries: skipping corrupt value"
                );
            }
        }
    }
    if skipped > 0 {
        tracing::warn!(
            skipped,
            kept = deliveries.len(),
            "pending deliveries: skipped corrupt/empty rows"
        );
    }
    Ok(deliveries)
}

pub async fn get_pending_delivery_in_ops(
    ops: &DbOperations,
    delivery_id: &str,
) -> Result<Option<PendingDelivery>, FoldDbError> {
    let store = namespace(ops, SHARE_DELIVERY_OUTBOX_TREE).await?;
    let key = format!("delivery:{delivery_id}");
    Ok(match store.get(key.as_bytes()).await? {
        Some(value) => Some(serde_json::from_slice(&value)?),
        None => None,
    })
}

pub async fn get_staged_delivery_artifact_in_ops(
    ops: &DbOperations,
    delivery_id: &str,
) -> Result<Option<StagedDeliveryArtifact>, FoldDbError> {
    let store = namespace(ops, SHARE_DELIVERY_OUTBOX_TREE).await?;
    let key = format!("delivery_artifact:{delivery_id}");
    Ok(match store.get(key.as_bytes()).await? {
        Some(value) => Some(serde_json::from_slice(&value)?),
        None => None,
    })
}

pub async fn remove_pending_delivery_in_ops(
    ops: &DbOperations,
    delivery_id: &str,
) -> Result<(), FoldDbError> {
    let store = namespace(ops, SHARE_DELIVERY_OUTBOX_TREE).await?;
    let key = format!("delivery:{delivery_id}");
    store.delete(key.as_bytes()).await?;
    let artifact_key = format!("delivery_artifact:{delivery_id}");
    store.delete(artifact_key.as_bytes()).await?;
    Ok(())
}
