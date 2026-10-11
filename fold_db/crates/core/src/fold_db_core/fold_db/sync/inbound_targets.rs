//! Inbound targets preserve existing grants and exact receiver bindings.

use super::FoldDB;
use crate::sync::cloud_share::{read_bindings, sender_hash, ShareTargetReadBinding};
use crate::sync::SyncTarget;
use std::collections::BTreeSet;
use std::sync::Arc;

pub(super) async fn targets(db: &FoldDB) -> Result<Vec<SyncTarget>, crate::error::FoldDbError> {
    let subscriptions = crate::sharing::store::list_share_subscriptions_in_ops(db.db_ops()).await?;
    let senders = subscriptions
        .iter()
        .filter(|subscription| subscription.active)
        .filter_map(|subscription| sender_hash(&subscription.share_prefix).map(str::to_owned))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let bindings = read_bindings(db.db_ops().namespaced_store().as_ref(), &senders)
        .await
        .map_err(|error| crate::error::FoldDbError::Database(error.to_string()))?;
    let bindings = senders
        .into_iter()
        .zip(bindings)
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut targets = Vec::new();
    let mut roots = BTreeSet::new();
    for subscription in subscriptions
        .into_iter()
        .filter(|subscription| subscription.active)
    {
        let sender = sender_hash(&subscription.share_prefix).ok_or_else(|| {
            crate::error::FoldDbError::Database("an inbound share scope is invalid".into())
        })?;
        if !roots.insert(subscription.share_prefix.clone()) {
            return Err(crate::error::FoldDbError::Database(
                "duplicate inbound share scope".into(),
            ));
        }
        let key: [u8; 32] = subscription
            .share_e2e_secret
            .as_slice()
            .try_into()
            .map_err(|_| {
                crate::error::FoldDbError::Database(
                    "an inbound share key has invalid length".into(),
                )
            })?;
        let read_binding = bindings.get(sender).and_then(Option::as_ref).map_or_else(
            || ShareTargetReadBinding {
                generation: 0,
                data_generation: 0,
                storage_prefix: format!("from:{sender}"),
                share_grant: subscription.share_grant.clone(),
            },
            |binding| binding.target(subscription.share_grant.clone()),
        );
        targets.push(SyncTarget {
            label: "received share".into(),
            prefix: subscription.share_prefix,
            crypto: Arc::new(crate::crypto::LocalCryptoProvider::from_key(key)),
            read_binding: Some(read_binding),
        });
    }
    Ok(targets)
}
