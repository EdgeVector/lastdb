use super::*;
use crate::sharing::delivery_wire::FILE_BLOB_CIPHER_SUITE;
use crate::storage::error::{NativeProofCause, NativeProofStage};
use laststore::space::SpacePolicy;
use std::path::Path;

mod json;
mod json_string;
pub(super) mod scratch;
mod verify;

fn invalid(code: &'static str) -> StorageError {
    StorageError::NativeProof {
        stage: NativeProofStage::NativeCas,
        cause: NativeProofCause::Static(code),
    }
}

/// This digest names the exact stored native CAS record.
/// Native file inventory already protects these bytes; no BlobPart is required.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCasProof {
    pub collection: String,
    pub key: String,
    pub stored_row_sha256: Digest,
    pub stored_row_bytes: u64,
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
    local == FILE_BLOB_CIPHER_SUITE && pointer == FILE_BLOB_CIPHER_SUITE
}
