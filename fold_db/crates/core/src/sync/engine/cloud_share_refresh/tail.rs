//! Each foreign root advances only its own durable incorporated frontier.

use super::*;
use crate::sync::snapshot_log::Frontier;

impl SyncEngine {
    pub(in crate::sync::engine) async fn peer_cloud_targets_snapshot(&self) -> Vec<SyncTarget> {
        self.targets
            .lock()
            .await
            .iter()
            .filter(|target| target.read_binding.is_none())
            .cloned()
            .collect()
    }

    pub(in crate::sync::engine) async fn download_foreign_share(
        &self,
        target: &SyncTarget,
    ) -> SyncResult<u64> {
        let key = target.cursor_key();
        let frontiers = self
            .store
            .open_namespace("sync_foreign_share_frontiers")
            .await?;
        let mut frontier = if let Some(bytes) = frontiers.get(key.as_bytes()).await? {
            if bytes.len() > 256 * 1024 {
                return Err(held("the foreign frontier exceeds its metadata limit"));
            }
            serde_json::from_slice::<BTreeMap<String, u64>>(&bytes)?
        } else {
            let sender = sender_hash(&target.prefix)
                .ok_or_else(|| held("the foreign frontier scope is invalid"))?;
            let binding = read_bindings(self.store.as_ref(), &[sender.into()])
                .await?
                .pop()
                .flatten()
                .ok_or_else(|| held("the foreign receiver binding is absent"))?;
            if binding.storage_prefix
                != target
                    .read_binding
                    .as_ref()
                    .ok_or_else(|| held("the foreign read binding is absent"))?
                    .storage_prefix
            {
                return Err(held("the foreign seed namespace changed"));
            }
            binding
                .writer_frontiers
                .get(&target.prefix)
                .cloned()
                .unwrap_or_default()
        };
        let seed = Frontier::vector(frontier.clone());
        let segments = self
            .download_mutation_log_segments_above_target(target, &seed)
            .await?;
        let report = self
            .replay_foreign_share_segments(target, &segments, &seed)
            .await?;
        for (writer, through) in report.frontier_after {
            frontier
                .entry(writer)
                .and_modify(|prior| *prior = (*prior).max(through))
                .or_insert(through);
        }
        let bytes = serde_json::to_vec(&frontier)?;
        if bytes.len() > 256 * 1024 {
            return Err(held("the foreign frontier exceeds its metadata limit"));
        }
        frontiers.put(key.as_bytes(), bytes).await?;
        self.store.restore_durability_barrier().await?;
        Ok((report.records_applied as u64).saturating_add(self.download_entries(target).await?))
    }
}
