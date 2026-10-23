//! Foreign shares retain exact receiver bindings; changed generations stay held.

use super::*;
use crate::sync::cloud_share::{held, read_bindings, sender_hash, ShareReadNotice};
use std::collections::BTreeMap;

impl SyncEngine {
    pub(super) async fn refresh_foreign_share_targets(
        &self,
        targets: &[SyncTarget],
        outcomes: Vec<SyncResult<Option<ShareReadNotice>>>,
    ) -> SyncResult<Vec<SyncTarget>> {
        if targets.len() != outcomes.len() {
            return Err(held("cloud target admission count differs"));
        }
        let mut allowed = Vec::new();
        let mut owners: BTreeMap<String, Vec<(SyncTarget, ShareReadNotice)>> = BTreeMap::new();
        for (target, outcome) in targets.iter().zip(outcomes) {
            let notice = outcome?;
            if target.read_binding.is_none() {
                allowed.push(target.clone());
                continue;
            }
            let sender =
                sender_hash(&target.prefix).ok_or_else(|| held("the receiver scope is invalid"))?;
            let notice =
                notice.ok_or_else(|| held("the foreign share has no current read notice"))?;
            owners
                .entry(sender.into())
                .or_default()
                .push((target.clone(), notice));
        }
        let senders = owners.keys().cloned().collect::<Vec<_>>();
        let bindings = read_bindings(self.store.as_ref(), &senders).await?;
        for ((sender, mut shares), previous) in owners.into_iter().zip(bindings) {
            shares.sort_by(|a, b| a.0.prefix.cmp(&b.0.prefix));
            let Some(binding) = previous else {
                for (target, notice) in shares {
                    let expected_prefix = format!("from:{sender}");
                    if notice.generation != 0
                        || notice.data_generation != 0
                        || notice.operation_id.is_some()
                        || !target.read_binding.as_ref().is_some_and(|binding| {
                            binding.generation == 0
                                && binding.data_generation == 0
                                && binding.storage_prefix == expected_prefix
                        })
                    {
                        return Err(held(
                            "foreign share initialization requires retired cloud adoption",
                        ));
                    }
                    allowed.push(target);
                }
                continue;
            };
            binding.validate()?;
            let roots = shares
                .iter()
                .map(|(target, _)| target.prefix.clone())
                .collect::<Vec<_>>();
            for (mut target, notice) in shares {
                if binding.account != notice.account
                    || binding.generation != notice.generation
                    || binding.data_generation != notice.data_generation
                    || binding.operation_id != notice.operation_id
                    || binding.share_roots != roots
                {
                    return Err(held(
                        "foreign share generation changed; this build cannot adopt its replacement",
                    ));
                }
                let grant = target
                    .read_binding
                    .as_ref()
                    .and_then(|value| value.share_grant.clone());
                target.read_binding = Some(binding.target(grant));
                allowed.push(target);
            }
        }
        Ok(allowed)
    }
}
