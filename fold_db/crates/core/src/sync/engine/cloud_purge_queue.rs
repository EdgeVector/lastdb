//! Target-only detachment of old cloud queues after durable local retention.

use super::*;
use crate::sync::cloud_purge_peer::{AdoptionPlan, RetainedHome};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

mod personal;
mod preservation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudPurgeQueueDetachReceipt {
    pub generation: u64,
    pub target_prefix: String,
    pub target_prefixes: Vec<String>,
    pub outbox_entries: u64,
    pub capture_markers: u64,
    pub pin_rows: u64,
    pub archive_pages: Vec<String>,
}

impl CloudPurgeQueueDetachReceipt {
    pub fn sha256(&self) -> SyncResult<String> {
        Ok(crate::hex::sha256_hex(serde_json::to_vec(self)?))
    }
}

struct DetachContext<'a> {
    plan: &'a AdoptionPlan,
    retained: &'a RetainedHome,
    retired_prefixes: BTreeSet<String>,
    targets: Vec<SyncTarget>,
    partitioner: Option<SyncPartitioner>,
}

impl SyncEngine {
    /// The host holds its serving mutation fence and stops all active target
    /// publishers before this call. Routing still names the old local instance.
    /// Local archives flush before any active queue row changes.
    pub async fn detach_cloud_purge_queues(
        &self,
        plan: &AdoptionPlan,
        retained: &RetainedHome,
        target_prefix: &str,
        replacement_frontier: &BTreeMap<String, u64>,
    ) -> SyncResult<CloudPurgeQueueDetachReceipt> {
        let peer = self.cloud_purge_peer().ok_or_else(|| {
            SyncError::CloudPurgeHeld("cloud peer receipts are unavailable".into())
        })?;
        peer.verify_retained_home(plan, retained)?;
        let _config = self.target_config_lock.lock().await;
        let (targets, partitioner, retired_prefixes) = if plan.account.kind
            == crate::sync::cloud_purge::protocol::CloudAccountKind::Personal
            && target_prefix.is_empty()
        {
            personal::routes(self).await?
        } else {
            (
                self.targets.lock().await.clone(),
                self.partitioner.lock().await.clone(),
                BTreeSet::from([target_prefix.to_string()]),
            )
        };
        let context = DetachContext {
            plan,
            retained,
            targets,
            partitioner,
            retired_prefixes,
        };
        if !context
            .targets
            .iter()
            .any(|target| target.prefix == target_prefix)
        {
            return Err(SyncError::MissingSyncTarget {
                share_prefix: target_prefix.into(),
            });
        }
        let mut receipt = CloudPurgeQueueDetachReceipt {
            generation: plan.generation,
            target_prefix: target_prefix.into(),
            target_prefixes: context.retired_prefixes.iter().cloned().collect(),
            outbox_entries: 0,
            capture_markers: 0,
            pin_rows: 0,
            archive_pages: Vec::new(),
        };
        self.detach_outbox(&context, &mut receipt).await?;
        self.detach_capture_markers(&context, &mut receipt).await?;
        let frontiers = context
            .retired_prefixes
            .iter()
            .map(|prefix| (prefix.clone(), replacement_frontier.clone()))
            .collect();
        receipt.pin_rows = self
            .pin_log
            .detach_cloud_generations(
                &frontiers,
                |rows| {
                    peer.retain_queue_page(plan, retained, PIN_LOG_NAMESPACE, rows)
                        .map_err(|error| error.to_string())
                },
                &mut receipt.archive_pages,
            )
            .await
            .map_err(SyncError::Storage)?;
        // The old allocation floor remains, so a new write cannot reuse a retired seq.
        let cursors = self.cursor_store.open_namespace("sync_cursors").await?;
        let cursor_keys: Vec<_> = context
            .retired_prefixes
            .iter()
            .map(|prefix| format!("cursor:{prefix}").into_bytes())
            .collect();
        let old = cursors.get_many(cursor_keys.clone()).await?;
        let archived: Vec<_> = cursor_keys
            .iter()
            .cloned()
            .zip(old)
            .filter_map(|(key, old)| old.map(|old| (key, old)))
            .collect();
        if !archived.is_empty() {
            self.archive_detach_page(&context, "sync_cursors", &archived, &mut receipt)?;
        }
        cursors
            .batch_put(
                cursor_keys
                    .into_iter()
                    .map(|key| Ok((key, self.encode_cursor(0).map_err(SyncError::Storage)?)))
                    .collect::<SyncResult<Vec<_>>>()?,
            )
            .await?;
        cursors.flush().await?;
        self.download_cursors
            .lock()
            .await
            .retain(|prefix, _| !context.retired_prefixes.contains(prefix));
        let mut pending = self.pending.lock().await;
        let mut remaining = Vec::new();
        for entry in pending.iter() {
            let (_, remainder) = filter_entry(&context, entry)?;
            if let Some(entry) = remainder {
                remaining.push(entry);
            }
        }
        *pending = remaining;
        drop(pending);
        *self.outbox_meta.lock().await = OutboxMeta::default();
        *self.capture_reexport_scan_cursor.lock().await = None;
        self.capture_reexport_presence
            .store(0, std::sync::atomic::Ordering::Release);
        if self.laststore_backup_source.is_some() {
            self.clear_backup_presence_cache_for_fresh_root().await?;
        }
        *self.backup_presence_listed_at.lock().await = None;
        *self.backup_published_tip_identity.lock().await = None;
        Ok(receipt)
    }

    async fn detach_outbox(
        &self,
        context: &DetachContext<'_>,
        receipt: &mut CloudPurgeQueueDetachReceipt,
    ) -> SyncResult<()> {
        let store = self.outbox_store().await.map_err(SyncError::Storage)?;
        let mut cursor = b"entry:".to_vec();
        loop {
            let rows = store.scan_range_paged(&cursor, b"entry;", 128).await?;
            if rows.is_empty() {
                break;
            }
            cursor = after(&rows.last().expect("nonempty page").0);
            let mut archived = Vec::new();
            let mut puts = Vec::new();
            let mut deletes = Vec::new();
            let mut seqs = Vec::new();
            for (key, bytes) in rows {
                let entry: LogEntry = serde_json::from_slice(&bytes)?;
                let (changed, remainder) = filter_entry(context, &entry)?;
                if !changed {
                    continue;
                }
                seqs.push(entry.seq);
                archived.push((key.clone(), bytes));
                match remainder {
                    Some(entry) => puts.push((key, serde_json::to_vec(&entry)?)),
                    None => deletes.push(key),
                }
            }
            if archived.is_empty() {
                continue;
            }
            self.archive_detach_page(context, "sync_outbox", &archived, receipt)?;
            receipt.outbox_entries += archived.len() as u64;
            store.batch_put(puts).await?;
            store.batch_delete(deletes).await?;
            store.flush().await?;
            self.capture_clear_pending_for_seqs(&seqs).await;
        }
        Ok(())
    }

    async fn detach_capture_markers(
        &self,
        context: &DetachContext<'_>,
        receipt: &mut CloudPurgeQueueDetachReceipt,
    ) -> SyncResult<()> {
        let store = self.store.open_namespace("sync_capture_reexport").await?;
        let mut cursor = Vec::new();
        loop {
            let rows = store.scan_range_paged(&cursor, &[0xff], 128).await?;
            if rows.is_empty() {
                break;
            }
            cursor = after(&rows.last().expect("nonempty page").0);
            let mut archived = Vec::new();
            let mut puts = Vec::new();
            let mut deletes = Vec::new();
            for (key, bytes) in rows {
                let mut marker: serde_json::Value = serde_json::from_slice(&bytes)?;
                let op = marker_op(&marker)?;
                let synthetic = LogEntry {
                    seq: 0,
                    timestamp_ms: 0,
                    device_id: self.device_id.clone(),
                    op,
                };
                let (changed, remainder) = filter_entry(context, &synthetic)?;
                if !changed {
                    continue;
                }
                archived.push((key.clone(), bytes));
                match remainder {
                    Some(entry) => {
                        replace_marker_op(&mut marker, entry.op)?;
                        puts.push((key, serde_json::to_vec(&marker)?));
                    }
                    None => deletes.push(key),
                }
            }
            if archived.is_empty() {
                continue;
            }
            self.archive_detach_page(context, "sync_capture_reexport", &archived, receipt)?;
            receipt.capture_markers += archived.len() as u64;
            store.batch_put(puts).await?;
            store.batch_delete(deletes).await?;
            store.flush().await?;
        }
        Ok(())
    }

    fn archive_detach_page(
        &self,
        context: &DetachContext<'_>,
        namespace: &str,
        rows: &[(Vec<u8>, Vec<u8>)],
        receipt: &mut CloudPurgeQueueDetachReceipt,
    ) -> SyncResult<()> {
        let peer = self
            .cloud_purge_peer()
            .ok_or_else(|| SyncError::CloudPurgeHeld("cloud peer disappeared".into()))?;
        receipt.archive_pages.push(peer.retain_queue_page(
            context.plan,
            context.retained,
            namespace,
            rows,
        )?);
        Ok(())
    }
}

fn filter_entry(
    context: &DetachContext<'_>,
    entry: &LogEntry,
) -> SyncResult<(bool, Option<LogEntry>)> {
    let parts = SyncEngine::partition_entry(&context.partitioner, entry, &context.targets)?;
    let changed = parts.iter().any(|(index, _)| {
        context
            .retired_prefixes
            .contains(&context.targets[*index].prefix)
    });
    if !changed {
        return Ok((false, Some(entry.clone())));
    }
    let mut remaining = parts
        .into_iter()
        .filter(|(index, _)| {
            !context
                .retired_prefixes
                .contains(&context.targets[*index].prefix)
        })
        .map(|(_, entry)| entry);
    let Some(mut first) = remaining.next() else {
        return Ok((true, None));
    };
    for entry in remaining {
        merge_op(&mut first.op, entry.op)?;
    }
    Ok((true, Some(first)))
}

fn merge_op(first: &mut LogOp, next: LogOp) -> SyncResult<()> {
    match (first, next) {
        (LogOp::BatchPut { items, .. }, LogOp::BatchPut { items: other, .. })
        | (LogOp::PhysicalDigest { items, .. }, LogOp::PhysicalDigest { items: other, .. }) => {
            items.extend(other);
        }
        (LogOp::BatchDelete { keys, .. }, LogOp::BatchDelete { keys: other, .. }) => {
            keys.extend(other);
        }
        (LogOp::LogicalCommit { changes }, LogOp::LogicalCommit { changes: other }) => {
            changes.extend(other);
        }
        _ => {
            return Err(SyncError::CloudPurgeHeld(
                "an old mixed-target queue entry cannot be detached safely".into(),
            ))
        }
    }
    Ok(())
}

fn marker_op(marker: &serde_json::Value) -> SyncResult<LogOp> {
    if let Some(intent) = marker
        .get("mutation_intent")
        .filter(|value| !value.is_null())
    {
        return Ok(LogOp::MutationIntent {
            mutations: serde_json::from_value(intent.clone())?,
        });
    }
    let keys: Vec<String> =
        serde_json::from_value(marker.get("keys").cloned().unwrap_or(serde_json::json!([])))?;
    Ok(LogOp::PhysicalDigest {
        namespace: marker
            .get("namespace")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .into(),
        items: keys.into_iter().map(|key| (key, String::new())).collect(),
    })
}

fn replace_marker_op(marker: &mut serde_json::Value, op: LogOp) -> SyncResult<()> {
    match op {
        LogOp::MutationIntent { mutations } => {
            marker["mutation_intent"] = serde_json::to_value(mutations)?;
        }
        LogOp::PhysicalDigest { items, .. } => {
            marker["keys"] =
                serde_json::to_value(items.into_iter().map(|(key, _)| key).collect::<Vec<_>>())?;
        }
        _ => {
            return Err(SyncError::CloudPurgeHeld(
                "capture marker changed its queue format".into(),
            ))
        }
    }
    Ok(())
}

fn after(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}
