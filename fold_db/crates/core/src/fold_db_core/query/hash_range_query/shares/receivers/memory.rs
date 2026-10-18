//! Committed receiver controls share the core's captured metadata lifetime.

use crate::db_operations::DbOperations;
use crate::storage::disk_pack::{
    core_memory_get, core_memory_insert, CoreMemorySource, CoreMemoryValue,
};
use crate::sync::cloud_share::{
    held, read_bindings, sender_hash, ReceiverBinding, RECEIVER_BINDINGS_NAMESPACE,
};
use crate::sync::SyncResult;
use std::collections::BTreeSet;
use std::sync::Arc;

static RECEIVER_MEMORY: u8 = 0;
const MEMORY_NAME: &str = "committed_receiver_controls";

/// Only active sender identities and their committed control records remain.
pub(super) struct ReceiverControls {
    pub(super) senders: Vec<String>,
    pub(super) bindings: Vec<Option<ReceiverBinding>>,
}

pub(super) async fn controls(
    ops: &DbOperations,
) -> SyncResult<Arc<CoreMemoryValue<ReceiverControls>>> {
    let runtime = ops.namespaced_store().disk_pack_runtime();
    let owner = std::ptr::from_ref(&RECEIVER_MEMORY) as usize;
    if let Some(controls) = core_memory_get(runtime.clone(), owner, MEMORY_NAME)? {
        return Ok(controls);
    }
    let subscriptions = crate::sharing::store::list_share_subscriptions_in_ops(ops)
        .await
        .map_err(|_| held("the inbound subscription registry is unavailable"))?;
    let senders = subscriptions
        .iter()
        .filter(|subscription| subscription.active)
        .map(|subscription| {
            sender_hash(&subscription.share_prefix)
                .map(str::to_owned)
                .ok_or_else(|| held("the inbound subscription scope is invalid"))
        })
        .collect::<SyncResult<BTreeSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>();
    let bindings = read_bindings(ops.namespaced_store().as_ref(), &senders).await?;
    let wire_bytes = serde_json::to_vec(&(&senders, &bindings))?.len();
    let bytes = wire_bytes
        .saturating_mul(8)
        .saturating_add(senders.len().saturating_mul(1024))
        .saturating_add(1024);
    let sources = vec![
        // Whole-collection dependencies include new subscriptions and bindings
        // that were absent when this complete control state was captured.
        CoreMemorySource::collection("share_subscriptions"),
        CoreMemorySource::collection(RECEIVER_BINDINGS_NAMESPACE),
    ];
    Ok(core_memory_insert(
        runtime,
        owner,
        MEMORY_NAME.to_owned(),
        ReceiverControls { senders, bindings },
        sources,
        bytes,
    )?)
}
