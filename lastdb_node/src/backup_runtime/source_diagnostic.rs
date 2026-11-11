//! Static diagnostics at actual native backup storage-error propagation.
use fold_db::storage::StorageError;

mod run_v2;

pub(super) fn report(error: &StorageError) {
    if let StorageError::NativeProof { stage, cause } = error {
        tracing::warn!(source_class = "native_proof", source_stage = %stage,
            source_reason = %cause, "native backup source refused");
        return;
    }
    let (class, detail) = match error {
        StorageError::NativeProof { .. } => ("native_proof", None),
        StorageError::SerializationError(detail) => ("serialization", Some(detail.as_str())),
        StorageError::BackendError(detail) => ("backend", Some(detail.as_str())),
        StorageError::ReadResourceLimit { detail } => {
            ("read_resource_limit", Some(detail.as_str()))
        }
        StorageError::WriteResourceLimit { detail } => {
            ("write_resource_limit", Some(detail.as_str()))
        }
        StorageError::MutationLimit { detail } => ("mutation_limit", Some(detail.as_str())),
        StorageError::RecordTooLarge { .. } => ("record_too_large", None),
        StorageError::InvalidOperation(detail) => ("invalid_operation", Some(detail.as_str())),
        StorageError::SnapshotCompactionPending | StorageError::PublicationEditPending => return,
        StorageError::PublicationFile => ("publication_file", None),
        StorageError::IoError(_) => ("io", None),
        StorageError::StorageFull { detail } => ("storage_full", Some(detail.as_str())),
        StorageError::ConfigurationError(detail) => ("configuration", Some(detail.as_str())),
        StorageError::EncryptionError(detail) => ("encryption", Some(detail.as_str())),
    };
    let reason = detail.map_or("source_other", static_reason);
    tracing::warn!(
        source_class = class,
        source_reason = reason,
        "native backup source refused"
    );
}

fn static_reason(mut detail: &str) -> &'static str {
    let mut fallback = "source_other";
    // Storage adapters add only these fixed wrappers. Strip at most three;
    // still return only an allowlist entry or a fixed code, never the tail.
    for _ in 0..3 {
        let next = [
            ("corrupt: ", "laststore_corrupt_other"),
            ("config: ", "laststore_config_other"),
        ]
        .into_iter()
        .find_map(|(prefix, code)| detail.strip_prefix(prefix).map(|tail| (tail, code)));
        let Some((tail, code)) = next else {
            break;
        };
        detail = tail;
        fallback = code;
    }
    if let Some(reason) = run_v2::reason(detail) {
        return reason;
    }
    if let Some(reason) = REASONS.iter().copied().find(|allowed| *allowed == detail) {
        return reason;
    }
    if detail.starts_with("unsupported key format: ") {
        return "unsupported_key_format";
    }
    fallback
}

// Return only these source literals, never the caller's detail or a digest of it.
const REASONS: &[&str] = &["record JSON suffix", "decoded record JSON allocation cap"];
