//! Each foreign root advances only its own durable incorporated frontier.

use super::*;
use crate::sync::cloud_share::{held, read_bindings, sender_hash};
use crate::sync::snapshot_log::Frontier;
use std::collections::BTreeMap;

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
                .flatten();
            let read = target
                .read_binding
                .as_ref()
                .ok_or_else(|| held("the foreign read binding is absent"))?;
            match binding {
                Some(binding) => {
                    binding.validate()?;
                    if binding.storage_prefix != read.storage_prefix
                        || binding.generation != read.generation
                        || binding.data_generation != read.data_generation
                    {
                        return Err(held("the foreign seed generation or namespace changed"));
                    }
                    binding
                        .writer_frontiers
                        .get(&target.prefix)
                        .cloned()
                        .unwrap_or_default()
                }
                None if read.generation == 0
                    && read.data_generation == 0
                    && read.storage_prefix == format!("from:{sender}") =>
                {
                    BTreeMap::new()
                }
                None => {
                    return Err(held(
                        "the foreign receiver requires retired replacement adoption",
                    ))
                }
            }
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

impl SyncEngine {
    pub(in crate::sync::engine) async fn bootstrap_foreign_share(
        &self,
        target: &SyncTarget,
    ) -> SyncResult<BootstrapOutcome> {
        let (targets, _) = self.target_config_snapshot().await;
        let permitted = self.preflight_cloud_peer_targets(&targets).await?;
        let target = permitted
            .iter()
            .find(|allowed| allowed.prefix == target.prefix)
            .ok_or_else(|| held("this foreign share remains held with its prior receiver data"))?;
        let entries_replayed = self.download_with_auth_retry(target).await?;
        let last_seq = self
            .download_cursors
            .lock()
            .await
            .get(&target.cursor_key())
            .copied()
            .unwrap_or(0);
        Ok(BootstrapOutcome {
            last_seq,
            entries_replayed: usize::try_from(entries_replayed)
                .map_err(|_| held("the received share replay count exceeds this device limit"))?,
            schemas_replayed: false,
            embeddings_replayed: false,
        })
    }
}
