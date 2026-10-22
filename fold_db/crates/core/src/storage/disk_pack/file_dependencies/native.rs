use super::*;
use crate::sharing::delivery_wire::{
    ContentAddressedBlob, FILE_BLOB_CIPHER_SUITE, FILE_BLOB_CIPHER_SUITE_LEGACY,
};
use cloud_backup_protocol::bytes_sha256;
use laststore::disk_pack::home::HomeRead;

/// This digest names the exact outer account-encrypted native record bytes.
/// Native file inventory already protects these bytes; no BlobPart is required.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCasProof {
    pub collection: String,
    pub key: String,
    pub stored_row_sha256: Digest,
    pub stored_row_bytes: u64,
}

/// Verify only the exact key at the caller's retained native root.
/// `None` means absence; corruption and key failure never become absence.
pub struct NativeCasRecord {
    proof: NativeCasProof,
    stored: Vec<u8>,
}

pub fn read_native_cas(
    read: &mut HomeRead,
    dependency: &FileDependency,
) -> StorageResult<Option<NativeCasRecord>> {
    let key =
        super::super::super::laststore::native_routing::encode(dependency.blob_ref().as_bytes());
    let Some(stored) = read
        .get_bounded("cas_blobs", &key, NATIVE_CAS_ROW_MAX_BYTES)
        .map_err(super::super::map)?
    else {
        return Ok(None);
    };
    if stored.len() as u64 > NATIVE_CAS_ROW_MAX_BYTES {
        return Err(invalid(
            "native CAS stored record exceeds release byte limit",
        ));
    }
    let proof = NativeCasProof {
        collection: "cas_blobs".into(),
        key,
        stored_row_sha256: bytes_sha256(&stored),
        stored_row_bytes: stored.len() as u64,
    };
    Ok(Some(NativeCasRecord { proof, stored }))
}

/// The caller drops its native reader and atom JSON before this decode stage.
pub fn verify_native_cas(
    row: NativeCasRecord,
    account_key: &[u8; 32],
    dependency: &FileDependency,
) -> StorageResult<NativeCasProof> {
    let plain = crate::crypto::at_rest::open_at_rest_bounded_owned(
        account_key,
        row.stored,
        NATIVE_CAS_ROW_MAX_BYTES,
        NATIVE_CAS_ROW_MAX_BYTES,
    )
    .map_err(|_| invalid("native CAS account envelope"))?;
    let blob: ContentAddressedBlob =
        serde_json::from_slice(&plain).map_err(|_| invalid("native CAS record format"))?;
    drop(plain);
    if blob.blob_ref != dependency.record.blob_ref
        || blob.content_sha256 != dependency.record.file_hash.as_str()
        || blob.size > FILE_DEPENDENCY_MAX_PLAINTEXT_BYTES
        || blob.size.checked_add(29) != Some(dependency.expected_stored_bytes())
    {
        return Err(invalid("native CAS record differs from file dependency"));
    }
    let size = blob.size;
    let bytes = match blob.local_cipher_suite.as_deref() {
        Some(suite) if compatible_local_suite(suite, dependency.encryption_suite()) => {
            let key = zeroize::Zeroizing::new(
                crate::hex::hex_decode_array::<32>(&dependency.access().dek)
                    .ok_or_else(|| invalid("file dependency key format"))?,
            );
            crate::crypto::at_rest::open_at_rest_bounded_owned(
                &key,
                blob.bytes_b64.into_bytes(),
                NATIVE_CAS_ROW_MAX_BYTES,
                FILE_DEPENDENCY_MAX_PLAINTEXT_BYTES,
            )
            .map_err(|_| invalid("native CAS per-file envelope"))?
        }
        None => {
            return Err(invalid(
                "legacy native CAS without per-file encryption is uncertified",
            ))
        }
        _ => return Err(invalid("native CAS encryption suite")),
    };
    if bytes.len() as u64 != size
        || bytes.len() as u64 > FILE_DEPENDENCY_MAX_PLAINTEXT_BYTES
        || bytes_sha256(&bytes) != dependency.record.file_hash
    {
        return Err(invalid("native CAS plaintext identity"));
    }
    Ok(row.proof)
}

fn compatible_local_suite(local: &str, pointer: &str) -> bool {
    // The existing local constructor uses the convergent label even when it
    // receives a legacy pointer's DEK. That label names the shared at-rest
    // codec, not proof of key derivation. The original pointer DEK must still
    // authenticate the complete bytes and their plaintext digest/size above.
    (local == pointer
        && matches!(
            local,
            FILE_BLOB_CIPHER_SUITE | FILE_BLOB_CIPHER_SUITE_LEGACY
        ))
        || (local == FILE_BLOB_CIPHER_SUITE && pointer == FILE_BLOB_CIPHER_SUITE_LEGACY)
}

/// The exact atom scope also owns its directory record.
pub fn read_directories(
    read: &mut HomeRead,
    scope: Option<&str>,
    atom_id: &str,
    dependencies: &[FileDependency],
    account_key: &[u8; 32],
) -> StorageResult<Vec<FileDependencyRecord>> {
    if dependencies.len() > 3 {
        return Err(invalid("file dependency selector count"));
    }
    let kinds = [
        FileDependencyKind::Main,
        FileDependencyKind::Thumbnail,
        FileDependencyKind::Poster,
    ];
    let keys: Vec<_> = kinds
        .iter()
        .map(|kind| {
            let raw = crate::schema::types::field::build_storage_key(
                scope,
                &directory_key(atom_id, *kind),
            );
            (
                "atoms".into(),
                super::super::super::laststore::native_routing::encode(raw.as_bytes()),
            )
        })
        .collect();
    let rows = read
        .get_many_bounded(&keys, DIRECTORY_MAX_BYTES * 2)
        .map_err(super::super::map)?;
    rows.into_iter()
        .zip(kinds)
        .filter_map(|(row, kind)| {
            let dependency = dependencies
                .iter()
                .find(|dependency| dependency.kind() == kind);
            let row = match row.map_err(super::super::map) {
                Ok(row) => row,
                Err(error) => return Some(Err(error)),
            };
            let (row, dependency) = match (row, dependency) {
                (None, None) => return None,
                (Some(row), Some(dependency)) => (row, dependency),
                _ => {
                    return Some(Err(invalid(
                        "native file dependency directory membership differs",
                    )))
                }
            };
            Some((|| {
                let plain = crate::crypto::at_rest::open_at_rest_bounded_owned(
                    account_key,
                    row,
                    DIRECTORY_MAX_BYTES * 2,
                    DIRECTORY_MAX_BYTES,
                )
                .map_err(|_| invalid("native file dependency directory envelope"))?;
                decode_directory(&plain, dependency)
            })())
        })
        .collect()
}
