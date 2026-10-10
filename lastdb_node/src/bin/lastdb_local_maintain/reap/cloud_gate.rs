//! Strict stopped-home cloud confirmation gate; all reads are read-only.

use super::walk::walk_both;
use super::ReapError;
use crate::home::HomeStore;
use fold_db::storage::traits::KvStore;
use fold_db::sync::engine::{
    audit_pin_log_plane, decode_offline_pin_log_row, offline_pin_log_probe_keys, OfflinePinLogRow,
    PinLogWriterStat, PIN_LOG_NAMESPACE, PIN_LOG_OPERATOR_KEYS_PER_CALL,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct CloudGateSummary {
    pub complete: bool,
    pub journal_present: bool,
    pub cloud_configured: bool,
    pub cloud_paused: bool,
    pub physical_keys: u64,
    pub metadata_probes: usize,
    pub published_maps: BTreeMap<String, BTreeMap<String, u64>>,
    pub published_map_sha256: BTreeMap<String, String>,
    pub entry_rows: u64,
    pub audit_pages: u64,
    pub audit_keys: u64,
    pub unreadable_rows: u64,
    pub personal_pending_rows: u64,
    pub org_pending_rows: u64,
    pub capture_receipts: u64,
    pub capture_reexport_keys: u64,
    pub allocation_floor: Option<u64>,
    pub writers: Vec<PinLogWriterStat>,
}

fn abort(message: impl Into<String>) -> ReapError {
    ReapError::abort("OFFLINE_CLOUD_FRONTIER", message)
}

#[derive(Default)]
struct PhysicalFacts {
    summary: CloudGateSummary,
    required: BTreeMap<(String, String), u64>,
}

impl PhysicalFacts {
    fn record(&mut self, record: &fold_db::sync::engine::PinLogRecord) {
        let frontier = self
            .required
            .entry((record.target_id.clone(), record.writer_id.clone()))
            .or_default();
        *frontier = (*frontier).max(record.frontier_after);
    }

    fn observe(&mut self, key: &[u8], value: &[u8]) -> Result<(), ReapError> {
        self.summary.physical_keys += 1;
        match decode_offline_pin_log_row(key, value).map_err(abort)? {
            OfflinePinLogRow::Entry(record) => {
                self.record(&record);
                self.summary.entry_rows += 1;
            }
            OfflinePinLogRow::Published {
                target_id,
                by_writer,
            } => {
                if self.summary.published_maps.contains_key(&target_id) {
                    return Err(abort("duplicate physical published target"));
                }
                self.summary.published_map_sha256.insert(
                    target_id.clone(),
                    fold_db::hex::hex_lower(Sha256::digest(value)),
                );
                self.summary.published_maps.insert(target_id, by_writer);
            }
            OfflinePinLogRow::AllocationFloor(frontier) => {
                if self.summary.allocation_floor.replace(frontier).is_some() {
                    return Err(abort("duplicate physical allocation floor"));
                }
            }
            OfflinePinLogRow::CaptureReceipt(records) => {
                self.summary.capture_receipts += 1;
                for record in records {
                    self.record(&record);
                }
            }
            OfflinePinLogRow::RestoreFrontier => {}
        }
        Ok(())
    }

    fn require_personal_writers(&self) -> Result<(), ReapError> {
        if (self.summary.cloud_configured
            || self.summary.cloud_paused
            || self.summary.physical_keys > 0)
            && !self
                .summary
                .published_maps
                .get("personal")
                .is_some_and(|map| !map.is_empty())
        {
            return Err(abort(
                "existing cloud state requires a nonempty durable personal writer map",
            ));
        }
        for ((target, writer), frontier) in &self.required {
            if target == "personal"
                && !self
                    .summary
                    .published_maps
                    .get(target)
                    .and_then(|map| map.get(writer))
                    .is_some_and(|published| published >= frontier)
            {
                return Err(abort(format!("personal writer {writer:?} has an unconfirmed durable entry or capture receipt")));
            }
        }
        Ok(())
    }
}

struct KeyedMetadata {
    values: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

async fn keyed_probes(raw: &dyn KvStore, seam: &dyn KvStore) -> Result<KeyedMetadata, ReapError> {
    let keys = offline_pin_log_probe_keys();
    let (raw_values, plain_values) =
        tokio::join!(raw.get_many(keys.clone()), seam.get_many(keys.clone()));
    let raw_values = raw_values.map_err(|error| abort(error.to_string()))?;
    let plain_values = plain_values.map_err(|error| abort(error.to_string()))?;
    if raw_values.len() != keys.len() || plain_values.len() != keys.len() {
        return Err(abort("cloud metadata probe count differs"));
    }
    for (at, key) in keys.iter().enumerate() {
        if raw_values[at].is_some() != plain_values[at].is_some() {
            return Err(abort("cloud metadata is hidden from the decrypted reader"));
        }
        if let Some(value) = &plain_values[at] {
            decode_offline_pin_log_row(key, value).map_err(abort)?;
        }
    }
    Ok(KeyedMetadata {
        values: keys.into_iter().zip(plain_values).collect(),
    })
}

async fn complete_audit(store: &dyn KvStore, facts: &mut PhysicalFacts) -> Result<(), ReapError> {
    let mut cursor: Option<String> = None;
    let mut writers: BTreeMap<(String, String), PinLogWriterStat> = BTreeMap::new();
    loop {
        let page = audit_pin_log_plane(store, PIN_LOG_OPERATOR_KEYS_PER_CALL, cursor.as_deref())
            .await
            .map_err(|error| abort(error.to_string()))?;
        facts.summary.audit_pages += 1;
        facts.summary.audit_keys += page.keys_scanned;
        facts.summary.unreadable_rows += page.unreadable_rows;
        for writer in page.writers {
            let entry = writers
                .entry((writer.target_id.clone(), writer.writer_id.clone()))
                .or_insert_with(|| PinLogWriterStat {
                    target_id: writer.target_id.clone(),
                    writer_id: writer.writer_id.clone(),
                    durable_published_f: writer.durable_published_f,
                    ..Default::default()
                });
            entry.confirmed_rows += writer.confirmed_rows;
            entry.confirmed_bytes += writer.confirmed_bytes;
            entry.pending_rows += writer.pending_rows;
            entry.pending_bytes += writer.pending_bytes;
        }
        if !page.more_remaining {
            break;
        }
        let next = page
            .next_after_key
            .ok_or_else(|| abort("cloud audit page has no resume cursor"))?;
        if cursor.as_ref().is_some_and(|old| old >= &next) || page.keys_scanned == 0 {
            return Err(abort("cloud audit cursor does not advance"));
        }
        cursor = Some(next);
    }
    let audited = writers
        .values()
        .map(|writer| writer.confirmed_rows + writer.pending_rows)
        .sum::<u64>();
    if facts.summary.unreadable_rows != 0
        || audited != facts.summary.entry_rows
        || facts.summary.audit_keys != facts.summary.entry_rows
    {
        return Err(abort(
            "cloud audit differs from the strict physical entry census",
        ));
    }
    for writer in writers.into_values() {
        let durable = facts
            .summary
            .published_maps
            .get(&writer.target_id)
            .and_then(|map| map.get(&writer.writer_id))
            .copied()
            .unwrap_or(0);
        if durable != writer.durable_published_f {
            return Err(abort("cloud published map differs across read-only passes"));
        }
        if writer.target_id == "personal" {
            facts.summary.personal_pending_rows += writer.pending_rows;
        } else {
            facts.summary.org_pending_rows += writer.pending_rows;
        }
        facts.summary.writers.push(writer);
    }
    if facts.summary.personal_pending_rows != 0 {
        return Err(abort("personal cloud entries are not fully confirmed"));
    }
    Ok(())
}

pub(crate) async fn check(home: &Path, opened: &HomeStore) -> Result<CloudGateSummary, ReapError> {
    let present = opened
        .base
        .list_namespaces()
        .await
        .map_err(|error| abort(error.to_string()))?;
    let raw = opened
        .base
        .open_namespace(PIN_LOG_NAMESPACE)
        .await
        .map_err(|error| abort(error.to_string()))?;
    let seam = opened
        .store
        .open_namespace(PIN_LOG_NAMESPACE)
        .await
        .map_err(|error| abort(error.to_string()))?;
    let mut facts = PhysicalFacts::default();
    facts.summary.cloud_configured = home
        .join(lastdb_node::host::CLOUD_SYNC_CONFIG_FILE)
        .try_exists()
        .map_err(|error| abort(error.to_string()))?;
    facts.summary.cloud_paused = home
        .join(lastdb_node::cloud::CLOUD_SYNC_PAUSED_FILE)
        .try_exists()
        .map_err(|error| abort(error.to_string()))?;
    facts.summary.journal_present = present.iter().any(|name| name == PIN_LOG_NAMESPACE);
    let probes = keyed_probes(&*raw, &*seam).await?;
    facts.summary.metadata_probes = probes.values.len();
    let mut physical_probes = BTreeMap::new();
    // Even an absent/inactive journal gets keyed probes and a physical walk.
    // Namespace inventory alone cannot prove absence of durable capture.
    walk_both(
        raw,
        Arc::clone(&seam),
        "OFFLINE_CLOUD_FRONTIER",
        |_, page| {
            for (key, value) in &page.rows {
                if probes.values.contains_key(key)
                    && physical_probes.insert(key.clone(), value.clone()).is_some()
                {
                    return Err(abort("duplicate physical cloud metadata probe"));
                }
                facts.observe(key, value)?;
            }
            Ok(())
        },
    )
    .await?;
    for (key, expected) in probes.values {
        if physical_probes.remove(&key) != expected {
            return Err(abort(
                "keyed cloud metadata differs from its complete physical walk",
            ));
        }
    }
    facts.require_personal_writers()?;
    complete_audit(&*seam, &mut facts).await?;
    let raw = opened
        .base
        .open_namespace("sync_capture_reexport")
        .await
        .map_err(|error| abort(error.to_string()))?;
    let seam = opened
        .store
        .open_namespace("sync_capture_reexport")
        .await
        .map_err(|error| abort(error.to_string()))?;
    walk_both(raw, seam, "OFFLINE_CLOUD_FRONTIER", |_, page| {
        facts.summary.capture_reexport_keys += page.rows.len() as u64;
        if !page.rows.is_empty() {
            return Err(abort("durable capture reexport markers remain"));
        }
        Ok(())
    })
    .await?;
    facts.summary.complete = true;
    Ok(facts.summary)
}
