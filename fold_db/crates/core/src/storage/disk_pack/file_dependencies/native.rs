use super::*;
use crate::sharing::delivery_wire::{FILE_BLOB_CIPHER_SUITE, FILE_BLOB_CIPHER_SUITE_LEGACY};
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

/// Fixed ASCII fields borrow the account-opened JSON. Unrelated fields are
/// skipped without owned strings or an arbitrary metadata tree. Current CAS
/// writers do not escape these selectors or their base64 envelope field.
#[derive(Deserialize)]
struct NativeCasView<'a> {
    blob_ref: &'a str,
    content_sha256: &'a str,
    bytes_b64: &'a str,
    size: u64,
    local_cipher_suite: Option<&'a str>,
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
    let (size, sealed) = {
        let blob: NativeCasView<'_> =
            serde_json::from_slice(&plain).map_err(|_| invalid("native CAS record format"))?;
        if blob.blob_ref != dependency.record.blob_ref
            || blob.content_sha256 != dependency.record.file_hash.as_str()
            || blob.size > FILE_DEPENDENCY_MAX_PLAINTEXT_BYTES
            || blob.size.checked_add(29) != Some(dependency.expected_stored_bytes())
        {
            return Err(invalid("native CAS record differs from file dependency"));
        }
        if !blob
            .local_cipher_suite
            .is_some_and(|suite| compatible_local_suite(suite, dependency.encryption_suite()))
        {
            return Err(invalid(
                "native CAS per-file encryption suite is uncertified",
            ));
        }
        (blob.size, blob.bytes_b64.as_bytes().to_vec())
    };
    drop(plain);
    let key = zeroize::Zeroizing::new(
        crate::hex::hex_decode_array::<32>(&dependency.access().dek)
            .ok_or_else(|| invalid("file dependency key format"))?,
    );
    let bytes = crate::crypto::at_rest::open_at_rest_bounded_owned(
        &key,
        sealed,
        NATIVE_CAS_ROW_MAX_BYTES,
        FILE_DEPENDENCY_MAX_PLAINTEXT_BYTES,
    )
    .map_err(|_| invalid("native CAS per-file envelope"))?;
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
