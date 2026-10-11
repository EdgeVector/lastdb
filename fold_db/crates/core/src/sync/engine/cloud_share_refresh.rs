//! Publish a complete sender union without changing any prior receiver bytes.

use super::*;
use crate::sync::cloud_share::{
    held, read_bindings, sender_hash, ReceiverBinding, ShareReadNotice,
};
use std::collections::BTreeMap;

mod bootstrap;
mod commit;
mod import;
mod stage;
mod tail;

impl SyncEngine {
    pub(super) async fn refresh_foreign_share_targets(
        &self,
        targets: &[SyncTarget],
        outcomes: Vec<SyncResult<Option<ShareReadNotice>>>,
    ) -> SyncResult<Vec<SyncTarget>> {
        let router = self.photograph_mutation_router.lock().await.clone();
        crate::sync::capture::with_mutation_admission(
            router,
            Box::pin(self.refresh_foreign_share_targets_admitted(targets, outcomes)),
        )
        .await
    }

    async fn refresh_foreign_share_targets_admitted(
        &self,
        targets: &[SyncTarget],
        outcomes: Vec<SyncResult<Option<ShareReadNotice>>>,
    ) -> SyncResult<Vec<SyncTarget>> {
        let _refresh = self.cloud_share_refresh_lock.lock().await;
        let mut allowed = Vec::new();
        let mut owners: BTreeMap<String, Vec<(SyncTarget, Option<ShareReadNotice>)>> =
            BTreeMap::new();
        for (target, outcome) in targets.iter().zip(outcomes) {
            if target.read_binding.is_none() {
                outcome?;
                allowed.push(target.clone());
                continue;
            }
            let sender =
                sender_hash(&target.prefix).ok_or_else(|| held("the receiver scope is invalid"))?;
            let notice = if let Ok(Some(notice)) = outcome {
                Some(notice)
            } else {
                tracing::warn!(
                    "a foreign share remains held; its prior receiver data stays in place"
                );
                None
            };
            owners
                .entry(sender.into())
                .or_default()
                .push((target.clone(), notice));
        }
        let senders = owners.keys().cloned().collect::<Vec<_>>();
        let bindings = read_bindings(self.store.as_ref(), &senders).await?;
        for ((sender, mut shares), previous) in owners.into_iter().zip(bindings) {
            shares.sort_by(|a, b| a.0.prefix.cmp(&b.0.prefix));
            if let Ok(binding) = self
                .refresh_share_owner(&sender, &shares, previous.as_ref())
                .await
            {
                for (mut target, _) in shares {
                    let grant = target
                        .read_binding
                        .as_ref()
                        .and_then(|binding| binding.share_grant.clone());
                    target.read_binding = Some(binding.target(grant));
                    allowed.push(target);
                }
            } else {
                tracing::warn!(
                    "a foreign share refresh remains held; its prior receiver data stays in place"
                );
            }
        }
        Ok(allowed)
    }

    async fn refresh_share_owner(
        &self,
        sender: &str,
        shares: &[(SyncTarget, Option<ShareReadNotice>)],
        previous: Option<&ReceiverBinding>,
    ) -> SyncResult<ReceiverBinding> {
        let notice = stage::consistent_notice(shares)?;
        let roots = shares
            .iter()
            .map(|(target, _)| target.prefix.clone())
            .collect::<Vec<_>>();
        let same_seed = previous.is_some_and(|binding| {
            binding.data_generation == notice.data_generation && binding.share_roots == roots
        });
        if let Some(binding) = previous.filter(|binding| {
            same_seed
                && binding.generation == notice.generation
                && binding.operation_id == notice.operation_id
        }) {
            return Ok(binding.clone());
        }
        if same_seed || notice.data_generation == 0 {
            let mut binding = previous.cloned().unwrap_or_else(|| ReceiverBinding {
                format_version: 1,
                sender_hash: sender.into(),
                account: notice.account.clone(),
                generation: 0,
                data_generation: 0,
                operation_id: None,
                storage_prefix: format!("from:{sender}"),
                share_roots: roots.clone(),
                snapshots: BTreeMap::new(),
                writer_frontiers: BTreeMap::new(),
            });
            if notice.data_generation == 0
                && previous.is_some_and(|binding| binding.data_generation > 0)
            {
                return Err(held("the receiver physical generation decreased"));
            }
            binding.generation = notice.generation;
            binding.operation_id.clone_from(&notice.operation_id);
            binding.share_roots = roots;
            commit::publish(self, shares, &binding, previous, &BTreeMap::new()).await?;
            return Ok(binding);
        }
        let staged = stage::download(self, shares, &notice).await?;
        let binding = stage::binding(sender, &notice, &roots, &staged)?;
        import::write(self, &staged, &binding.storage_prefix).await?;
        commit::publish(
            self,
            shares,
            &binding,
            previous,
            &staged
                .iter()
                .map(|item| (item.target.prefix.clone(), item.last_seq))
                .collect(),
        )
        .await?;
        Ok(binding)
    }
}
