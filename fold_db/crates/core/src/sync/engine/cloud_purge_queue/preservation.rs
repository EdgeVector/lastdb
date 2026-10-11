//! Preserve unchanged org queues in a stopped personal replacement candidate.

use super::*;
use crate::storage::NamespacedStore;
use crate::sync::cloud_purge_peer::scope_alias;
use sha2::Digest;
use std::collections::BTreeSet;

impl SyncEngine {
    /// Called only on a stopped inactive candidate with old target routing.
    /// The whole old store is already retained. Drop inherited source-device
    /// queues, copy only the peer's unchanged org streams, and keep their
    /// exact writer/frontier identities and global allocation floor.
    pub async fn preserve_cloud_purge_org_queues(
        &self,
        previous_stores: (Arc<dyn NamespacedStore>, Arc<dyn NamespacedStore>),
        plan: &AdoptionPlan,
        retained: &RetainedHome,
        org_targets: &[String],
        inbound_targets: &[String],
        replacement_frontier: &BTreeMap<String, u64>,
    ) -> SyncResult<String> {
        let (previous, previous_cursors) = previous_stores;
        let peer = self
            .cloud_purge_peer()
            .ok_or_else(|| SyncError::CloudPurgeHeld("peer retention is unavailable".into()))?;
        peer.verify_retained_home(plan, retained)?;
        if plan.account.kind != crate::sync::cloud_purge::protocol::CloudAccountKind::Personal {
            return Err(SyncError::CloudPurgeHeld(
                "personal queue preservation requires the personal account".into(),
            ));
        }
        let targets = self.targets.lock().await.clone();
        let partitioner = self.partitioner.lock().await.clone();
        let orgs: BTreeSet<_> = org_targets.iter().cloned().collect();
        let ids: BTreeSet<_> = orgs
            .iter()
            .map(|prefix| crate::hex::sha256_hex(prefix.as_bytes()))
            .collect();
        let mut digest = sha2::Sha256::new();
        // Exact source snapshots must never activate another device's queue.
        for name in ["sync_outbox", "sync_capture_reexport", PIN_LOG_NAMESPACE] {
            clear(self.store.open_namespace(name).await?.as_ref()).await?;
        }
        clear(
            self.cursor_store
                .open_namespace("sync_cursors")
                .await?
                .as_ref(),
        )
        .await?;
        // Retire the adopted account before installing locally retargeted
        // survivor queues. Old routing cannot classify the new local aliases.
        let reset = self
            .detach_cloud_purge_queues(plan, retained, "", replacement_frontier)
            .await?;
        digest.update(reset.sha256()?.as_bytes());
        self.copy_preserved_outbox(&previous, &targets, &partitioner, &orgs, &mut digest)
            .await?;
        self.copy_preserved_markers(&previous, &targets, &partitioner, &orgs, &mut digest)
            .await?;
        let floor = self
            .copy_preserved_pins(&previous, &orgs, &ids, &mut digest)
            .await?
            .max(replacement_frontier.values().copied().max().unwrap_or(0));
        let pins = self.store.open_namespace(PIN_LOG_NAMESPACE).await?;
        let floor_row = (b"appended_f:global".to_vec(), floor.to_be_bytes().to_vec());
        hash_rows(
            &mut digest,
            PIN_LOG_NAMESPACE,
            std::slice::from_ref(&floor_row),
        );
        pins.put(&floor_row.0, floor_row.1).await?;
        pins.flush().await?;
        let prior = previous_cursors.open_namespace("sync_cursors").await?;
        let current = self.cursor_store.open_namespace("sync_cursors").await?;
        let cursor_targets: BTreeSet<_> = orgs.iter().chain(inbound_targets.iter()).collect();
        let keys: Vec<_> = cursor_targets
            .iter()
            .map(|target| format!("cursor:{target}").into_bytes())
            .collect();
        let values = prior.get_many(keys.clone()).await?;
        let rows: Vec<_> = keys
            .into_iter()
            .zip(values)
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect();
        hash_rows(&mut digest, "sync_cursors", &rows);
        current.batch_put(rows).await?;
        current.flush().await?;
        self.store.restore_durability_barrier().await?;
        self.cursor_store.restore_durability_barrier().await?;
        Ok(crate::hex::hex_lower(digest.finalize()))
    }

    async fn copy_preserved_outbox(
        &self,
        previous: &Arc<dyn NamespacedStore>,
        targets: &[SyncTarget],
        partitioner: &Option<SyncPartitioner>,
        orgs: &BTreeSet<String>,
        digest: &mut sha2::Sha256,
    ) -> SyncResult<()> {
        let source = previous.open_namespace("sync_outbox").await?;
        let destination = self.store.open_namespace("sync_outbox").await?;
        let mut cursor = b"entry:".to_vec();
        loop {
            let rows = source.scan_range_paged(&cursor, b"entry;", 128).await?;
            if rows.is_empty() {
                break;
            }
            cursor = after(&rows.last().expect("nonempty page").0);
            let mut copied = Vec::new();
            for (key, bytes) in rows {
                let entry: LogEntry = serde_json::from_slice(&bytes)?;
                if let Some(entry) =
                    preserved_entry(self.store.as_ref(), &entry, targets, partitioner, orgs).await?
                {
                    copied.push((key, serde_json::to_vec(&entry)?));
                }
            }
            hash_rows(digest, "sync_outbox", &copied);
            destination.batch_put(copied).await?;
        }
        destination.flush().await?;
        Ok(())
    }

    async fn copy_preserved_markers(
        &self,
        previous: &Arc<dyn NamespacedStore>,
        targets: &[SyncTarget],
        partitioner: &Option<SyncPartitioner>,
        orgs: &BTreeSet<String>,
        digest: &mut sha2::Sha256,
    ) -> SyncResult<()> {
        let source = previous.open_namespace("sync_capture_reexport").await?;
        let destination = self.store.open_namespace("sync_capture_reexport").await?;
        let mut cursor = Vec::new();
        loop {
            let rows = source.scan_range_paged(&cursor, &[0xff], 128).await?;
            if rows.is_empty() {
                break;
            }
            cursor = after(&rows.last().expect("nonempty page").0);
            let mut copied = Vec::new();
            for (key, bytes) in rows {
                let mut marker: serde_json::Value = serde_json::from_slice(&bytes)?;
                let entry = LogEntry {
                    seq: 0,
                    timestamp_ms: 0,
                    device_id: self.device_id.clone(),
                    op: marker_op(&marker)?,
                };
                if let Some(entry) =
                    preserved_entry(self.store.as_ref(), &entry, targets, partitioner, orgs).await?
                {
                    replace_marker_op(&mut marker, entry.op)?;
                    copied.push((key, serde_json::to_vec(&marker)?));
                }
            }
            hash_rows(digest, "sync_capture_reexport", &copied);
            destination.batch_put(copied).await?;
        }
        destination.flush().await?;
        Ok(())
    }

    async fn copy_preserved_pins(
        &self,
        previous: &Arc<dyn NamespacedStore>,
        orgs: &BTreeSet<String>,
        ids: &BTreeSet<String>,
        digest: &mut sha2::Sha256,
    ) -> SyncResult<u64> {
        let source = previous.open_namespace(PIN_LOG_NAMESPACE).await?;
        let destination = self.store.open_namespace(PIN_LOG_NAMESPACE).await?;
        let mut cursor = Vec::new();
        let mut floor = 0_u64;
        loop {
            let rows = source.scan_range_paged(&cursor, &[0xff], 128).await?;
            if rows.is_empty() {
                break;
            }
            cursor = after(&rows.last().expect("nonempty page").0);
            let mut copied = Vec::new();
            for (key, bytes) in rows {
                let text = std::str::from_utf8(&key)
                    .map_err(|_| SyncError::CloudPurgeHeld("pin queue key is not text".into()))?;
                let value = if text.starts_with("target:") {
                    let mut record: super::super::PinLogRecord = serde_json::from_slice(&bytes)?;
                    floor = floor.max(record.frontier_after);
                    if !orgs.contains(&record.target_prefix) {
                        continue;
                    }
                    scope_alias::rebind_entry(
                        self.store.as_ref(),
                        &record.target_prefix,
                        &mut record.entry,
                    )
                    .await?;
                    serde_json::to_vec(&record)?
                } else if text == "appended_f:global" {
                    let bytes: [u8; 8] = bytes.as_slice().try_into().map_err(|_| {
                        SyncError::CloudPurgeHeld("old allocation floor is invalid".into())
                    })?;
                    floor = floor.max(u64::from_be_bytes(bytes));
                    continue;
                } else if text
                    .strip_prefix("published_f:")
                    .is_some_and(|id| ids.contains(id))
                {
                    bytes
                } else if text.starts_with("capture_marker_receipt:") {
                    let mut receipt: serde_json::Value = serde_json::from_slice(&bytes)?;
                    let mut records = Vec::new();
                    for value in receipt["records"].as_array().ok_or_else(|| {
                        SyncError::CloudPurgeHeld("old capture receipt is invalid".into())
                    })? {
                        let mut record: super::super::PinLogRecord =
                            serde_json::from_value(value.clone())?;
                        if orgs.contains(&record.target_prefix) {
                            scope_alias::rebind_entry(
                                self.store.as_ref(),
                                &record.target_prefix,
                                &mut record.entry,
                            )
                            .await?;
                            records.push(record);
                        }
                    }
                    if records.is_empty() {
                        continue;
                    }
                    if records.len() != 1 {
                        return Err(SyncError::CloudPurgeHeld(
                            "capture receipt contains an unsupported mixed org commit".into(),
                        ));
                    }
                    receipt["op_sha256"] = crate::hex::sha256_hex(serde_json::to_vec(
                        &serde_json::to_value(&records[0].entry.op)?,
                    )?)
                    .into();
                    receipt["records"] = serde_json::to_value(records)?;
                    serde_json::to_vec(&receipt)?
                } else {
                    continue;
                };
                copied.push((key, value));
            }
            hash_rows(digest, PIN_LOG_NAMESPACE, &copied);
            destination.batch_put(copied).await?;
        }
        destination.flush().await?;
        Ok(floor)
    }
}

async fn preserved_entry(
    store: &dyn NamespacedStore,
    entry: &LogEntry,
    targets: &[SyncTarget],
    partitioner: &Option<SyncPartitioner>,
    orgs: &BTreeSet<String>,
) -> SyncResult<Option<LogEntry>> {
    let mut copied = None::<LogEntry>;
    for (index, mut part) in SyncEngine::partition_entry(partitioner, entry, targets)? {
        let target = &targets[index].prefix;
        if !orgs.contains(target) {
            continue;
        }
        scope_alias::rebind_entry(store, target, &mut part).await?;
        if let Some(first) = &mut copied {
            merge_op(&mut first.op, part.op)?;
        } else {
            copied = Some(part);
        }
    }
    Ok(copied)
}

async fn clear(store: &dyn crate::storage::KvStore) -> SyncResult<()> {
    loop {
        let rows = store.scan_range_paged(&[], &[0xff], 128).await?;
        if rows.is_empty() {
            break;
        }
        store
            .batch_delete(rows.into_iter().map(|(key, _)| key).collect())
            .await?;
    }
    store.flush().await?;
    Ok(())
}

fn hash_rows(digest: &mut sha2::Sha256, namespace: &str, rows: &[(Vec<u8>, Vec<u8>)]) {
    digest.update((namespace.len() as u64).to_be_bytes());
    digest.update(namespace.as_bytes());
    for (key, value) in rows {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
}
