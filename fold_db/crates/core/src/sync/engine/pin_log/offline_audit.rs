//! Strict read-only row decoding for stopped-home maintenance.

use super::*;

/// A durable row whose identity agrees with the production key builders.
#[derive(Debug)]
pub enum OfflinePinLogRow {
    Entry(PinLogRecord),
    Published {
        target_id: String,
        by_writer: BTreeMap<String, u64>,
    },
    AllocationFloor(u64),
    CaptureReceipt(Vec<PinLogRecord>),
    RestoreFrontier,
}

/// Exact keyed probes supplement a complete physical journal walk.
#[must_use]
pub fn offline_pin_log_probe_keys() -> Vec<Vec<u8>> {
    vec![
        pin_log_published_f_key("personal"),
        PIN_LOG_APPENDED_F_KEY.to_vec(),
        BACKUP_RESTORE_F_KEY.to_vec(),
    ]
}

fn validate_record(record: &PinLogRecord) -> Result<(), String> {
    if record.model_version != PIN_LOG_MODEL_VERSION
        || record.target_id != target_id_for_prefix(&record.target_prefix)
        || record.writer_id.is_empty()
        || record.writer_id != record.entry.device_id
        || record.frontier_after == 0
        || record.frontier_after != record.entry.seq
        || record.timestamp_ms != record.entry.timestamp_ms
    {
        return Err("pin-log entry target/writer/frontier identity is inconsistent".into());
    }
    Ok(())
}

/// Reject unknown formats and malformed metadata rather than infer absence.
pub fn decode_offline_pin_log_row(key: &[u8], value: &[u8]) -> Result<OfflinePinLogRow, String> {
    if key.starts_with(PIN_LOG_ENTRY_PREFIX.as_bytes()) {
        let record: PinLogRecord = serde_json::from_slice(value)
            .map_err(|error| format!("decode pin-log entry: {error}"))?;
        validate_record(&record)?;
        if key != pin_log_entry_key(&record.target_id, record.frontier_after) {
            return Err("pin-log storage key differs from target/frontier".into());
        }
        return Ok(OfflinePinLogRow::Entry(record));
    }
    if let Some(target) = key.strip_prefix(PIN_LOG_PUBLISHED_F_PREFIX.as_bytes()) {
        let target_id = std::str::from_utf8(target).map_err(|error| error.to_string())?;
        if target_id.is_empty() {
            return Err("published target is empty".into());
        }
        let by_writer = strict_writer_map(value)?;
        if by_writer.keys().any(String::is_empty) {
            return Err("published writer is empty".into());
        }
        return Ok(OfflinePinLogRow::Published {
            target_id: target_id.into(),
            by_writer,
        });
    }
    if key == PIN_LOG_APPENDED_F_KEY {
        let bytes: [u8; 8] = value
            .try_into()
            .map_err(|_| "allocation floor is not eight bytes".to_string())?;
        return Ok(OfflinePinLogRow::AllocationFloor(u64::from_be_bytes(bytes)));
    }
    if key.starts_with(CAPTURE_MARKER_RECEIPT_PREFIX.as_bytes()) {
        let receipt: CaptureMarkerAppendReceipt = serde_json::from_slice(value)
            .map_err(|error| format!("decode capture receipt: {error}"))?;
        validate_capture_marker_receipt(&receipt, &receipt.marker_key, &receipt.op_sha256)?;
        if key != capture_marker_receipt_key(&receipt.marker_key) {
            return Err("capture receipt key differs from marker".into());
        }
        for record in &receipt.records {
            validate_record(record)?;
        }
        return Ok(OfflinePinLogRow::CaptureReceipt(receipt.records));
    }
    if key == BACKUP_RESTORE_F_KEY {
        let frontier: super::restore::BackupRestoreFrontier = serde_json::from_slice(value)
            .map_err(|error| format!("decode restore frontier: {error}"))?;
        frontier.validate().map_err(|error| error.to_string())?;
        return Ok(OfflinePinLogRow::RestoreFrontier);
    }
    Err(format!(
        "unsupported durable pin-log key {:?}",
        String::from_utf8_lossy(key)
    ))
}

fn strict_writer_map(value: &[u8]) -> Result<BTreeMap<String, u64>, String> {
    struct UniqueMap;
    impl<'de> serde::de::Visitor<'de> for UniqueMap {
        type Value = BTreeMap<String, u64>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a unique writer frontier map")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut access: A,
        ) -> Result<Self::Value, A::Error> {
            let mut map = BTreeMap::new();
            while let Some((writer, frontier)) = access.next_entry::<String, u64>()? {
                if map.insert(writer, frontier).is_some() {
                    return Err(serde::de::Error::custom("duplicate published writer"));
                }
            }
            Ok(map)
        }
    }
    let mut decoder = serde_json::Deserializer::from_slice(value);
    let map = serde::de::Deserializer::deserialize_map(&mut decoder, UniqueMap)
        .map_err(|error| format!("decode published writer map: {error}"))?;
    decoder.end().map_err(|error| error.to_string())?;
    Ok(map)
}
