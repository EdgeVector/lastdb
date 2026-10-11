//! Retain each peer's exact live foreign namespace through personal adoption.

use super::*;
use crate::sync::cloud_share::{read_bindings, sender_hash, RECEIVER_BINDINGS_NAMESPACE};

type ReceiverSource = (BTreeSet<String>, Vec<(Vec<u8>, Vec<u8>)>);

pub(super) async fn preserve_frontiers(
    previous: &DbOperations,
    candidate: &DbOperations,
    rows: &[(Vec<u8>, Vec<u8>)],
    digest: &mut sha2::Sha256,
) -> SyncResult<Vec<String>> {
    let bindings = rows
        .iter()
        .map(|(_, bytes)| {
            serde_json::from_slice::<crate::sync::cloud_share::ReceiverBinding>(bytes)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let keys = bindings
        .iter()
        .flat_map(|binding| {
            binding.share_roots.iter().map(|root| {
                if binding.data_generation == 0 {
                    root.clone()
                } else {
                    format!("{root}:receiver:{}", binding.storage_prefix)
                }
            })
        })
        .collect::<Vec<_>>();
    let source = previous
        .open_namespace("sync_foreign_share_frontiers")
        .await
        .map_err(error)?;
    let values = source
        .get_many(keys.iter().map(|key| key.as_bytes().to_vec()).collect())
        .await
        .map_err(error)?;
    if values.len() != keys.len() {
        return Err(error("the retained foreign frontier batch is incomplete"));
    }
    let copied = keys
        .iter()
        .zip(values)
        .filter_map(|(key, value)| value.map(|value| (key.as_bytes().to_vec(), value)))
        .collect::<Vec<_>>();
    let destination = candidate
        .open_namespace("sync_foreign_share_frontiers")
        .await
        .map_err(error)?;
    loop {
        let inherited = destination
            .scan_range_paged(&[], &[0xff], 128)
            .await
            .map_err(error)?;
        if inherited.is_empty() {
            break;
        }
        destination
            .batch_delete(inherited.into_iter().map(|(key, _)| key).collect())
            .await
            .map_err(error)?;
    }
    hash(digest, "sync_foreign_share_frontiers", &copied);
    destination.restore_batch_put(copied).await.map_err(error)?;
    Ok(keys)
}

pub(super) async fn source(
    previous: &DbOperations,
    subscriptions: &[crate::sharing::types::ShareSubscription],
) -> SyncResult<ReceiverSource> {
    let senders = subscriptions
        .iter()
        .map(|subscription| {
            sender_hash(&subscription.share_prefix)
                .map(str::to_owned)
                .ok_or_else(|| error("the prior inbound share scope is invalid"))
        })
        .collect::<SyncResult<BTreeSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>();
    let bindings = read_bindings(previous.namespaced_store().as_ref(), &senders).await?;
    let mut scopes = BTreeSet::new();
    let mut rows = Vec::new();
    for (sender, binding) in senders.into_iter().zip(bindings) {
        let prefix = binding.as_ref().map_or_else(
            || format!("from:{sender}"),
            |binding| binding.storage_prefix.clone(),
        );
        scopes.insert(format!("{prefix}:"));
        if let Some(binding) = binding {
            rows.push((sender.into_bytes(), serde_json::to_vec(&binding)?));
        }
    }
    Ok((scopes, rows))
}

pub(super) async fn preserve(
    candidate: &DbOperations,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    digest: &mut sha2::Sha256,
) -> SyncResult<()> {
    let destination = candidate
        .open_namespace(RECEIVER_BINDINGS_NAMESPACE)
        .await
        .map_err(error)?;
    loop {
        let old = destination
            .scan_range_paged(&[], &[0xff], 128)
            .await
            .map_err(error)?;
        if old.is_empty() {
            break;
        }
        destination
            .batch_delete(old.into_iter().map(|(key, _)| key).collect())
            .await
            .map_err(error)?;
    }
    hash(digest, RECEIVER_BINDINGS_NAMESPACE, &rows);
    destination.restore_batch_put(rows).await.map_err(error)?;
    Ok(())
}
