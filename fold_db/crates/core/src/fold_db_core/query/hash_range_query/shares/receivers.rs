//! Queries select only the locally committed receiver namespace.

use crate::db_operations::DbOperations;
use crate::sync::cloud_share::{read_bindings, sender_hash};
use std::collections::BTreeSet;

pub(super) async fn namespaces(ops: &DbOperations) -> crate::sync::SyncResult<Vec<String>> {
    let subscriptions = crate::sharing::store::list_share_subscriptions_in_ops(ops)
        .await
        .map_err(|_| {
            crate::sync::cloud_share::held("the inbound subscription registry is unavailable")
        })?;
    let senders = subscriptions
        .iter()
        .filter(|subscription| subscription.active)
        .map(|subscription| {
            sender_hash(&subscription.share_prefix)
                .map(str::to_owned)
                .ok_or_else(|| {
                    crate::sync::cloud_share::held("the inbound subscription scope is invalid")
                })
        })
        .collect::<crate::sync::SyncResult<BTreeSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>();
    let bindings = read_bindings(ops.namespaced_store().as_ref(), &senders).await?;
    Ok(senders
        .into_iter()
        .zip(bindings)
        .map(|(sender, binding)| {
            binding.map_or_else(
                || format!("from:{sender}"),
                |binding| binding.storage_prefix,
            )
        })
        .collect())
}
