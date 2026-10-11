//! Fresh owned outbound share cuts accompany the personal replacement.

use super::*;
use crate::db_operations::DbOperations;
use crate::sync::cloud_purge::RetainedShareSnapshot;
use crate::sync::snapshot::Snapshot;
use std::collections::BTreeMap;

impl SyncEngine {
    pub(super) async fn cloud_purge_owned_share_cuts(
        &self,
        ops: &DbOperations,
        directory: &std::path::Path,
    ) -> SyncResult<Vec<RetainedShareSnapshot>> {
        let source = ops
            .namespaced_store()
            .raw_last_store()
            .ok_or_else(|| error("share source requires LastStore"))?;
        let admission = crate::sync::cloud_purge::memory::Admission::acquire(0).await?;
        let keys: Vec<_> =
            crate::sync::cloud_purge::org_rows::physical_keys(&source, "share_rules", &admission)?
                .keys()
                .map(|key| key.as_bytes().to_vec())
                .collect();
        let rules_store = ops.open_namespace("share_rules").await?;
        let ids = keys
            .iter()
            .map(|key| String::from_utf8(key.clone()).map_err(|_| error("invalid share rule key")))
            .collect::<SyncResult<Vec<_>>>()?;
        let sizes = crate::sync::cloud_purge::memory::raw_lengths(
            &admission,
            0,
            ops.namespaced_store().as_ref(),
            "share_rules",
            &ids,
        )?;
        let mut rules = BTreeMap::new();
        for (id, size) in ids.iter().zip(sizes) {
            admission.check(crate::sync::cloud_purge::memory::read_peak(
                size.ok_or_else(|| error("share rule disappeared"))?,
            )?)?;
            let value = rules_store.get(id.as_bytes()).await?;
            let rule: crate::sharing::types::ShareRule = serde_json::from_slice(
                &value.ok_or_else(|| error("owned share rule disappeared"))?,
            )
            .map_err(|_| error("invalid owned share rule"))?;
            if rule.active && rules.insert(rule.share_prefix.clone(), rule).is_some() {
                return Err(error("duplicate owned share prefix"));
            }
        }
        drop(admission);
        let mut cuts = Vec::new();
        for (prefix, rule) in rules {
            let admission = crate::sync::cloud_purge::memory::Admission::acquire(0).await?;
            let frontier = self.cloud_purge_share_frontier(ops, &prefix).await?;
            let last_seq = frontier.values().copied().max().unwrap_or(0);
            let mut snapshot = Snapshot::create_cloud_purge_share(
                ops.namespaced_store().as_ref(),
                &self.device_id,
                last_seq,
                &prefix,
                &admission,
            )
            .await?;
            snapshot.writer_frontier = frontier.clone();
            super::cloud_purge_share_files::add_share_file_closure(ops, &mut snapshot, &admission)
                .await?;
            admission.check(crate::sync::cloud_purge::memory::snapshot_peak(
                snapshot.cloud_purge_share_bytes()?,
                crate::sync::cloud_purge::memory::row_peak(0)?,
            )?)?;
            let bytes = serde_json::to_vec(&snapshot)
                .map_err(|_| error("share source snapshot encoding failed"))?;
            let digest = crate::hex::sha256_hex(&bytes);
            drop(bytes);
            let key: [u8; 32] = rule
                .share_e2e_secret
                .try_into()
                .map_err(|_| error("owned share encryption key has invalid length"))?;
            let crypto: Arc<dyn crate::crypto::CryptoProvider> =
                Arc::new(crate::crypto::LocalCryptoProvider::from_key(key));
            let ciphertext = snapshot.seal(&crypto).await?;
            cuts.push(RetainedShareSnapshot::retain(
                directory,
                prefix,
                digest,
                &ciphertext,
                frontier,
            )?);
        }
        Ok(cuts)
    }

    async fn cloud_purge_share_frontier(
        &self,
        ops: &DbOperations,
        prefix: &str,
    ) -> SyncResult<BTreeMap<String, u64>> {
        let target = crate::hex::sha256_hex(prefix.as_bytes());
        let mut frontier = self
            .pin_log
            .read_published_f_strict(&target)
            .await
            .map_err(SyncError::Storage)?;
        let store = ops.open_namespace("sync_pin_log").await?;
        let (mut cursor, end) =
            crate::kind_partition::exact_prefix_bounds(&format!("target:{target}:entry:"));
        loop {
            let rows = store
                .scan_range_paged(cursor.as_bytes(), end.as_bytes(), 128)
                .await?;
            if rows.is_empty() {
                break;
            }
            cursor = String::from_utf8(
                rows.last()
                    .ok_or_else(|| error("share frontier page absent"))?
                    .0
                    .clone(),
            )
            .map_err(|_| error("invalid share frontier key"))?;
            cursor.push('\0');
            for (_, value) in rows {
                let record: super::pin_log::PinLogRecord = serde_json::from_slice(&value)
                    .map_err(|_| error("invalid share frontier record"))?;
                if record.target_prefix != prefix || record.target_id != target {
                    return Err(error("share frontier crosses target"));
                }
                frontier
                    .entry(record.writer_id)
                    .and_modify(|through| *through = (*through).max(record.frontier_after))
                    .or_insert(record.frontier_after);
            }
        }
        Ok(frontier)
    }
}

fn error(message: &str) -> SyncError {
    SyncError::Storage(message.into())
}
