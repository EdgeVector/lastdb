//! Poison-job classification and quarantine records for the persist lane.

use super::*;

/// Metadata key prefix of persist-lane quarantine records.
///
/// One point key per quarantined envelope:
/// `persist_lane_quarantine:<schema>:<unix_ms>:<commit_id>`.
pub(crate) const PERSIST_LANE_QUARANTINE_PREFIX: &str = "persist_lane_quarantine:";

/// True for an error that a retry of the same envelope cannot change.
///
/// A durable row that does not decode (empty, torn, or a foreign format)
/// fails the same way on every attempt. Transient store, IO, capacity, and
/// turn-wait errors stay on the normal retry path.
pub(crate) fn is_deterministic_decode_error(error: &SchemaError) -> bool {
    let SchemaError::InvalidData(message) = error else {
        return false;
    };
    message.contains("Serialization error:")
        || message.contains("expected value at line")
        || message.contains("EOF while parsing")
}

pub(super) fn lane_job_schema(job: &LanePersistJob) -> &str {
    match job {
        LanePersistJob::Purge { job, .. } => job
            .erasures
            .first()
            .map_or("", |mutation| mutation.schema_name.as_str()),
        LanePersistJob::StorageSlotPurge { job, .. } => job.schema_name.as_str(),
    }
}

/// Short job kind plus target keys for lane log lines.
pub(super) fn describe_lane_job(
    envelope: &crate::resident::PersistEnvelope<LanePersistJob>,
) -> String {
    const MAX_KEYS: usize = 8;
    match &envelope.payload {
        LanePersistJob::Purge { job, .. } => {
            let keys: Vec<String> = job
                .erasures
                .iter()
                .take(MAX_KEYS)
                .map(|mutation| {
                    format!(
                        "hash={} range={}",
                        mutation.key_value.hash.as_deref().unwrap_or("-"),
                        mutation.key_value.range.as_deref().unwrap_or("-")
                    )
                })
                .collect();
            format!(
                "purge job verb={:?} schema={} keys={} [{}]",
                job.verb,
                lane_job_schema(&envelope.payload),
                job.erasures.len(),
                keys.join(", ")
            )
        }
        LanePersistJob::StorageSlotPurge { job, .. } => format!(
            "storage-slot purge job schema={} targets={}",
            job.schema_name,
            job.targets.len()
        ),
    }
}

/// Durable description of a quarantined envelope.
///
/// It identifies the background purge job and its erasure mutations.
pub(super) fn quarantine_record(
    envelope: &crate::resident::PersistEnvelope<LanePersistJob>,
    error: &str,
) -> serde_json::Value {
    let job = match &envelope.payload {
        LanePersistJob::Purge { job, .. } => serde_json::json!({
            "kind": "purge",
            "schema": lane_job_schema(&envelope.payload),
            "verb": format!("{:?}", job.verb),
            "storage_prefix": job.storage_prefix,
            "erasures": job.erasures,
        }),
        LanePersistJob::StorageSlotPurge { job, .. } => serde_json::json!({
            "kind": "storage_slot_purge",
            "schema": job.schema_name,
            "targets": job.targets.len(),
            "durable_complete": job.durable_complete,
        }),
    };
    serde_json::json!({
        "quarantined_at": chrono::Utc::now().to_rfc3339(),
        "commit_id": envelope.commit_id,
        "bytes": envelope.bytes,
        "error": error,
        "description": describe_lane_job(envelope),
        "job": job,
    })
}
