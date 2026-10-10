//! Strict, inert inspection of production candidate and queue metadata.

use super::*;

// Strictness is local to offline admission; the production record codec stays
// unchanged. An audit decision supplies no body or delete authority here.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Candidate {
    atom_uuid: String,
    zero_since_unix_nanos: u64,
    queue_key: String,
    #[serde(default)]
    audit: Option<Audit>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Audit {
    audit_id: String,
    zero_since_unix_nanos: u64,
    audited_at_unix_nanos: u64,
    reachable: bool,
}

pub(in crate::db_operations::atom_store) fn offline_gc_metadata(
    bare_key: &str,
    value: &[u8],
    storage_prefix: Option<&str>,
) -> Result<bool, SchemaError> {
    let candidate_row = bare_key.starts_with(ATOM_GC_CANDIDATE_PREFIX);
    if !candidate_row && !bare_key.starts_with(ATOM_GC_QUEUE_PREFIX) {
        return Ok(false);
    }
    let candidate: Candidate =
        serde_json::from_slice(value).map_err(|_| invalid("invalid atom GC metadata value"))?;
    let expected_queue = atom_gc_queue_key(
        &candidate.atom_uuid,
        candidate.zero_since_unix_nanos,
        storage_prefix,
    );
    let expected_key = if candidate_row {
        atom_gc_candidate_key(&candidate.atom_uuid, storage_prefix)
    } else {
        expected_queue.clone()
    };
    if candidate.atom_uuid.is_empty()
        || candidate.zero_since_unix_nanos == 0
        || candidate.queue_key != expected_queue
        || build_storage_key(storage_prefix, bare_key) != expected_key
    {
        return Err(invalid(
            "atom GC metadata key or epoch differs from its value",
        ));
    }
    if let Some(audit) = candidate.audit {
        let audit = AtomGcAuditResult {
            audit_id: audit.audit_id,
            zero_since_unix_nanos: audit.zero_since_unix_nanos,
            audited_at_unix_nanos: audit.audited_at_unix_nanos,
            reachable: audit.reachable,
        };
        if audit.zero_since_unix_nanos != candidate.zero_since_unix_nanos
            || audit.audited_at_unix_nanos < candidate.zero_since_unix_nanos
        {
            return Err(invalid("atom GC audit epoch differs from its candidate"));
        }
    }
    // A stale queue epoch is valid inert metadata. Production removes it only
    // after comparison with the current candidate; no cross-row guess occurs.
    Ok(true)
}

fn invalid(message: &str) -> SchemaError {
    SchemaError::InvalidData(message.into())
}
