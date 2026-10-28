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
        ("unwrap molecule key bundle: ", "molecule_key_unwrap"),
        ("decode molecule key bundle: ", "molecule_key_bundle_json"),
        (
            "molecule key wrap metadata mismatch for ",
            "molecule_key_wrap_metadata",
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
    "account-v2 native CAS requires ENC text",
    "atom JSON suffix",
    "decoded atom JSON allocation cap",
    "duplicate native CAS selector",
    "file dependency atom identity",
    "file dependency directory differs from exact atom pointer",
    "file dependency directory encode",
    "file dependency directory exceeds its bound",
    "file dependency directory format",
    "file dependency exceeds total backup policy",
    "file dependency key format",
    "file dependency pointer digest",
    "file dependency requires canonical digest",
    "file dependency size is missing",
    "file dependency storage digest",
    "generation metadata coordinate",
    "generation metadata delete coordinate",
    "generation metadata identity",
    "generation metadata key",
    "generation metadata molecule",
    "invalid file dependency access",
    "invalid thumbnail dependency",
    "legacy metadata file identity",
    "legacy native CAS requires account encryption",
    "legacy or incomplete file dependency is uncertified",
    "live file metadata lacks certified atom pointer coverage",
    "metadata atom identity",
    "metadata atom identity differs",
    "metadata file identity",
    "metadata file reference count",
    "metadata generation coordinate",
    "metadata generation identity",
    "metadata molecule identity",
    "metadata record format",
    "metadata record key",
    "metadata references an absent atom",
    "metadata tip identity",
    "metadata tip identity exceeds release bound",
    "metadata-only file dependency is uncertified",
    "molecule key bundle requires non-empty molecule and domain",
    "molecule key bundle wrap is not encrypted",
    "native CAS JSON UTF-8",
    "native CAS JSON control byte",
    "native CAS JSON depth",
    "native CAS JSON ended",
    "native CAS JSON escape",
    "native CAS JSON hex",
    "native CAS JSON literal",
    "native CAS JSON member separator",
    "native CAS JSON number",
    "native CAS JSON scalar",
    "native CAS JSON separator",
    "native CAS JSON surrogate",
    "native CAS JSON token",
    "native CAS JSON trailing comma",
    "native CAS JSON value",
    "native CAS account envelope",
    "native CAS blob identity",
    "native CAS file digest",
    "native CAS file identity",
    "native CAS file size differs",
    "native CAS inner output",
    "native CAS per-file encryption suite is uncertified",
    "native CAS per-file envelope",
    "native CAS plaintext identity",
    "native CAS raw source digest",
    "native CAS raw source exceeds backup policy",
    "native CAS raw source identity",
    "native CAS raw source length",
    "native CAS scratch parent",
    "native CAS scratch type",
    "native CAS selector UTF-8",
    "native CAS selector inventory",
    "native CAS size",
    "native atom key encoding",
    "native authorized file consumer copy is unavailable",
    "native file source owner scope needs a read-only grant",
    "native molecule key identity exceeds its bound",
    "native molecule key metadata differs",
    "native molecule key record envelope",
    "native molecule key record exceeds its bound",
    "native molecule key record format",
    "thumbnail dependency identity",
    "unsupported thumbnail dependency kind",
];
