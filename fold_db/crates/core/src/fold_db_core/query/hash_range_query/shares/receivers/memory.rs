//! Committed receiver controls share the core's captured metadata lifetime.

use crate::db_operations::DbOperations;
use crate::storage::disk_pack::{
    core_memory_get, core_memory_insert, CoreMemorySource, CoreMemoryValue,
};
use crate::storage::error::{StorageError, StorageResult};
use std::collections::BTreeSet;
use std::sync::Arc;

static RECEIVER_MEMORY: u8 = 0;
const MEMORY_NAME: &str = "committed_receiver_controls";

/// Only active sender identities remain.
pub(super) struct ReceiverControls {
    pub(super) senders: Vec<String>,
}

pub(super) async fn controls(
    ops: &DbOperations,
) -> StorageResult<Arc<CoreMemoryValue<ReceiverControls>>> {
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
        .collect::<StorageResult<BTreeSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>();
    let wire_bytes: usize = senders.iter().map(|sender| sender.len() + 3).sum();
    let bytes = wire_bytes
        .saturating_mul(8)
        .saturating_add(senders.len().saturating_mul(1024))
        .saturating_add(1024);
    // The whole subscription collection is a dependency, so a new subscription
    // that was absent when this complete control state was captured refreshes it.
    let sources = vec![CoreMemorySource::collection("share_subscriptions")];
    core_memory_insert(
        runtime,
        owner,
        MEMORY_NAME.to_owned(),
        ReceiverControls { senders },
        sources,
        bytes,
    )
}

/// The sender in `share:{sender}:{opaque}`: 32 hex digits and one opaque part.
fn sender_hash(prefix: &str) -> Option<&str> {
    let mut parts = prefix.split(':');
    if parts.next()? != "share" {
        return None;
    }
    let sender = parts.next()?;
    let opaque = parts.next()?;
    if parts.next().is_some()
        || opaque.is_empty()
        || sender.len() != 32
        || !sender.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some(sender)
}

fn held(message: &str) -> StorageError {
    StorageError::BackendError(message.into())
}
