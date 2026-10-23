use super::*;
use crate::sharing::delivery_wire::{FILE_BLOB_CIPHER_SUITE, FILE_BLOB_CIPHER_SUITE_LEGACY};
use laststore::disk_pack::home::HomeRead;
use laststore::disk_pack::maintenance::SpacePolicy;
use std::io::Write;
use std::path::Path;

mod json;
mod json_string;
pub(super) mod scratch;
mod verify;

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

/// Copy only the exact held key. Output remains private until this succeeds.
/// The native reader checks the complete record digest without a body-sized Vec.
pub fn read_native_cas(
    read: &mut HomeRead,
    dependency: &FileDependency,
    output: &mut impl Write,
) -> StorageResult<Option<NativeCasProof>> {
    let key =
        super::super::super::laststore::native_routing::encode(dependency.blob_ref().as_bytes());
    let Some(stored) = read
        .copy_body_to("cas_blobs", &key, output)
        .map_err(super::super::map)?
    else {
        return Ok(None);
    };
    let proof = NativeCasProof {
        collection: "cas_blobs".into(),
        key,
        stored_row_sha256: Digest::try_from(crate::hex::hex_lower(stored.body_sha256))
            .map_err(|_| invalid("native CAS digest encoding"))?,
        stored_row_bytes: stored.body_bytes,
    };
    Ok(Some(proof))
}

/// Drop the native reader and atom JSON before this disk-backed decode stage.
/// The caller owns raw_path; this function removes its exact private siblings.
pub fn verify_native_cas(
    proof: NativeCasProof,
    raw_path: &Path,
    account_key: &[u8; 32],
    dependency: &FileDependency,
    space: &SpacePolicy,
) -> StorageResult<NativeCasProof> {
    verify::verify(proof, raw_path, account_key, dependency, space)
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
