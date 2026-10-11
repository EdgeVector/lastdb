use super::*;

// ---------------------------------------------------------------------------
// Operator pin-log audit
//
// `sync_pin_log` is the largest plane on the primary (21 GiB / 51% of the store
// at measurement time). This read-only verb makes its confirmed-vs-pending
// split measurable from the durable published-F map (#1511). Reclaim is owned
// by confirmed truncation plus plane compaction; the audit never deletes.
// ---------------------------------------------------------------------------

/// Default entry rows one audit daemon call examines before returning a
/// resume cursor. Matches the per-cycle pin-log scan row budget so a single
/// call cannot monopolise the owner socket on a multi-GiB plane.
pub const PIN_LOG_OPERATOR_KEYS_PER_CALL: usize = 50_000;

/// Per-writer slice of a pin-log audit page.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PinLogWriterStat {
    pub target_id: String,
    pub writer_id: String,
    /// Durable published high-water mark for this writer (0 if absent).
    pub durable_published_f: u64,
    pub confirmed_rows: u64,
    pub confirmed_bytes: u64,
    pub pending_rows: u64,
    pub pending_bytes: u64,
}

/// Bounded, resumable, read-only pin-log plane report.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PinLogPlaneReport {
    pub keys_scanned: u64,
    pub entry_rows: u64,
    pub entry_bytes: u64,
    pub confirmed_rows: u64,
    pub confirmed_bytes: u64,
    pub pending_rows: u64,
    pub pending_bytes: u64,
    pub unreadable_rows: u64,
    /// Distinct targets represented by entry rows on this page.
    pub targets_seen: u64,
    pub writers: Vec<PinLogWriterStat>,
    pub more_remaining: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_key: Option<String>,
}

/// Point-read one target's durable published-F map.
///
/// An absent or undecodable map is treated as empty, so its rows classify as
/// pending. The audit deliberately does not scan the published-F namespace.
pub(super) async fn load_durable_published_f_map(
    store: &dyn crate::storage::traits::KvStore,
    target_id: &str,
) -> Result<BTreeMap<String, u64>, String> {
    let value = store
        .get(&pin_log_published_f_key(target_id))
        .await
        .map_err(|error| format!("read durable pin-log published-F map: {error}"))?;
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    match serde_json::from_slice(&value) {
        Ok(map) => Ok(map),
        Err(error) => {
            tracing::warn!(
                target: "fold_db::sync::pin_log",
                target_id = %target_id,
                error = %error,
                "durable published-F map undecodable during operator audit; treating as empty"
            );
            Ok(BTreeMap::new())
        }
    }
}

pub(super) fn writer_hwm(
    maps: &BTreeMap<String, BTreeMap<String, u64>>,
    target_id: &str,
    writer_id: &str,
) -> u64 {
    maps.get(target_id)
        .and_then(|m| m.get(writer_id).copied())
        .unwrap_or(0)
}

pub(super) fn bump_writer_stat(
    writers: &mut BTreeMap<(String, String), PinLogWriterStat>,
    target_id: &str,
    writer_id: &str,
    hwm: u64,
    confirmed: bool,
    bytes: u64,
) {
    let key = (target_id.to_string(), writer_id.to_string());
    let row = writers.entry(key).or_insert_with(|| PinLogWriterStat {
        target_id: target_id.to_string(),
        writer_id: writer_id.to_string(),
        durable_published_f: hwm,
        ..Default::default()
    });
    row.durable_published_f = row.durable_published_f.max(hwm);
    if confirmed {
        row.confirmed_rows += 1;
        row.confirmed_bytes = row.confirmed_bytes.saturating_add(bytes);
    } else {
        row.pending_rows += 1;
        row.pending_bytes = row.pending_bytes.saturating_add(bytes);
    }
}

/// Bounded, resumable, read-only pin-log plane audit.
///
/// Classification predicate for "confirmed":
/// `record.frontier_after <= durable_published_f[target][writer]`
/// (missing map/writer ⇒ 0 ⇒ every row pending). Durable maps are point-read
/// lazily for only the targets represented on the bounded entry page.
pub async fn audit_pin_log_plane(
    store: &dyn crate::storage::traits::KvStore,
    max_keys: usize,
    after_key: Option<&str>,
) -> Result<PinLogPlaneReport, crate::schema::SchemaError> {
    // lint:fn-size-ok moved verbatim from pin_log.rs; splitting this function is separate work.
    let max_keys = max_keys.clamp(1, 10_000_000);
    let mut published_maps: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();

    let entry_prefix = PIN_LOG_ENTRY_PREFIX.as_bytes();
    let end = prefix_upper_bound(entry_prefix).ok_or_else(|| {
        crate::schema::SchemaError::InvalidData(
            "pin-log entry prefix has no finite range upper bound".to_string(),
        )
    })?;
    let mut cursor = match after_key {
        Some(s) => {
            let bytes = s.as_bytes().to_vec();
            // Resume must stay inside the entry keyspace.
            if !bytes.starts_with(entry_prefix) {
                return Err(crate::schema::SchemaError::InvalidCursor(format!(
                    "after_key must be a pin-log entry key under '{PIN_LOG_ENTRY_PREFIX}'"
                )));
            }
            bytes
        }
        None => entry_prefix.to_vec(),
    };

    let mut report = PinLogPlaneReport::default();
    let mut writers: BTreeMap<(String, String), PinLogWriterStat> = BTreeMap::new();

    let page_size = pin_log_scan_page_size();
    let mut more_remaining = false;
    let mut next_after: Option<String> = None;

    loop {
        let rows = store
            .scan_range_paged(&cursor, &end, page_size)
            .await
            .map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!("scan pin-log entries: {e}"))
            })?;
        if rows.is_empty() {
            break;
        }
        let final_page = rows.len() < page_size;
        let last_idx = rows.len() - 1;
        let mut page_next = None;

        for (idx, (key, value)) in rows.into_iter().enumerate() {
            page_next = Some(key_after(&key));
            report.keys_scanned += 1;
            let key_str = String::from_utf8_lossy(&key);

            // Non-entry keys under the target prefix should not appear, but
            // skip defensively while still charging the bounded key budget.
            if !key_str.contains(":entry:") {
                if report.keys_scanned as usize >= max_keys {
                    more_remaining = true;
                    next_after = page_next
                        .as_ref()
                        .map(|k| String::from_utf8_lossy(k).into_owned());
                    break;
                }
                continue;
            }

            match serde_json::from_slice::<PinLogRecord>(&value) {
                Ok(record) => {
                    let bytes = value.len() as u64;
                    report.entry_rows += 1;
                    report.entry_bytes = report.entry_bytes.saturating_add(bytes);
                    if !published_maps.contains_key(&record.target_id) {
                        let map = load_durable_published_f_map(store, &record.target_id)
                            .await
                            .map_err(crate::schema::SchemaError::InvalidData)?;
                        published_maps.insert(record.target_id.clone(), map);
                    }
                    let hwm = writer_hwm(&published_maps, &record.target_id, &record.writer_id);
                    let confirmed = record.frontier_after <= hwm;
                    if confirmed {
                        report.confirmed_rows += 1;
                        report.confirmed_bytes = report.confirmed_bytes.saturating_add(bytes);
                    } else {
                        report.pending_rows += 1;
                        report.pending_bytes = report.pending_bytes.saturating_add(bytes);
                    }
                    bump_writer_stat(
                        &mut writers,
                        &record.target_id,
                        &record.writer_id,
                        hwm,
                        confirmed,
                        bytes,
                    );
                }
                Err(e) if final_page && idx == last_idx => {
                    tracing::warn!(
                        target: "fold_db::sync::pin_log",
                        key = %key_str,
                        error = %e,
                        "ignoring corrupt trailing pin-log entry during operator walk"
                    );
                    report.unreadable_rows += 1;
                }
                Err(e) => {
                    return Err(crate::schema::SchemaError::InvalidData(format!(
                        "decode pin-log entry {key_str}: {e}"
                    )));
                }
            }

            if report.keys_scanned as usize >= max_keys {
                more_remaining = true;
                next_after = page_next
                    .as_ref()
                    .map(|k| String::from_utf8_lossy(k).into_owned());
                break;
            }
        }

        if more_remaining {
            break;
        }
        if final_page {
            break;
        }
        match page_next {
            Some(next) => cursor = next,
            None => break,
        }
    }

    let mut target_ids: BTreeSet<String> = BTreeSet::new();
    for (tid, _) in writers.keys() {
        target_ids.insert(tid.clone());
    }
    report.targets_seen = target_ids.len() as u64;
    report.writers = writers.into_values().collect();
    report.writers.sort_by(|a, b| {
        a.target_id
            .cmp(&b.target_id)
            .then_with(|| a.writer_id.cmp(&b.writer_id))
    });
    report.more_remaining = more_remaining;
    report.next_after_key = next_after;

    Ok(report)
}
