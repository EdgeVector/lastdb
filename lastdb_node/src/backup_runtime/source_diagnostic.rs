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
        StorageError::InvalidOperation(detail) => ("invalid_operation", Some(detail.as_str())),
        StorageError::CatalogMembershipDenied { .. } => ("catalog_membership_denied", None),
        StorageError::IoError(_) => ("io", None),
        StorageError::DownloadFailed(detail) => ("download_failed", Some(detail.as_str())),
        StorageError::UploadFailed(detail) => ("upload_failed", Some(detail.as_str())),
        StorageError::StorageFull { detail } => ("storage_full", Some(detail.as_str())),
        StorageError::ConfigurationError(detail) => ("configuration", Some(detail.as_str())),
        StorageError::LockContention { .. } => ("lock_contention", None),
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
    // These existing wrappers append private context. Emit only a fixed code.
    const WRAPPERS: [(&str, &str); 4] = [
        ("unwrap field key bundle: ", "field_key_unwrap"),
        ("decode field key bundle: ", "field_key_bundle_json"),
        (
            "field key wrap metadata mismatch for ",
            "field_key_wrap_metadata",
        ),
        ("unsupported key format: ", "unsupported_key_format"),
    ];
    WRAPPERS
        .iter()
        .find(|(prefix, _)| detail.starts_with(*prefix))
        .map_or(fallback, |(_, reason)| *reason)
}

// Return only these source literals, never the caller's detail or a digest of it.
const REASONS: &[&str] = &[
    "record JSON suffix",
    "decoded record JSON allocation cap",
    "field key bundle requires non-empty field key and domain",
    "field key bundle wrap is not encrypted",
    "native field key identity exceeds its bound",
    "native field key record exceeds its bound",
    "native field key record format",
];
