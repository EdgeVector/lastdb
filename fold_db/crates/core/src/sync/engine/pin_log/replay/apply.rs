use super::*;

pub(super) struct ValidatedReplayUnit {
    pub(super) records: Vec<PinLogRecord>,
}

/// Check every replay unit's identity, writer stream and record order before
/// any record is applied.
pub(super) fn validate_replay_units(
    ordered: Vec<MutationLogReplayUnit>,
) -> SyncResult<Vec<ValidatedReplayUnit>> {
    let mut validated = Vec::with_capacity(ordered.len());
    for segment in ordered {
        let records = segment.records;
        let last = records.last().ok_or_else(|| {
            SyncError::Storage("mutation-log segment decoded empty during replay".to_string())
        })?;
        let writer_id = last.writer_id.as_str();
        if records.iter().any(|record| record.writer_id != writer_id) {
            return Err(SyncError::Storage(format!(
                "mutation-log segment {} mixes writer streams",
                segment.segment.object_key
            )));
        }
        if segment.segment.writer_id.as_deref() != Some(writer_id)
            || segment.segment.through_id != last.frontier_after
            || segment.segment.object_key != segment.segment.expected_object_key()
        {
            return Err(SyncError::Storage(format!(
                "mutation-log segment identity mismatch for {}",
                segment.segment.object_key
            )));
        }
        let has_typed_identity = segment.segment.schema_name.is_some()
            || segment.segment.utc_nanos.is_some()
            || segment.segment.sequence.is_some();
        if has_typed_identity && !segment.transaction_group {
            let (schema, utc_nanos) = mutation_log_batch_identity(&records)
                .map_err(SyncError::Storage)?
                .ok_or_else(|| {
                    SyncError::Storage(format!(
                        "mutation-log segment typed identity has a legacy payload for {}",
                        segment.segment.object_key
                    ))
                })?;
            if segment.segment.schema_name.as_deref() != Some(schema)
                || segment.segment.utc_nanos != Some(utc_nanos)
                || segment.segment.sequence != Some(last.frontier_after)
            {
                return Err(SyncError::Storage(format!(
                    "mutation-log segment typed identity mismatch for {}",
                    segment.segment.object_key
                )));
            }
        }
        if records
            .windows(2)
            .any(|pair| pair[0].frontier_after >= pair[1].frontier_after)
        {
            return Err(SyncError::Storage(format!(
                "mutation-log segment {} has non-monotonic records",
                segment.segment.object_key
            )));
        }
        validated.push(ValidatedReplayUnit { records });
    }
    Ok(validated)
}
