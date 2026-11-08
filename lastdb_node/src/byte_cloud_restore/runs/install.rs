use super::super::{keys::RecoveryDecryption, local, native, NativeFileProof};
use super::super::{RestoreError, RestoreSession, Result};
use fold_db::storage::disk_pack::{byte_backup::ClientRoot, run_restore};

pub(super) fn verify(
    session: &RestoreSession,
    decryption: &RecoveryDecryption,
    root: &ClientRoot,
) -> Result<run_restore::RunRestoreGuard> {
    let data = session.root.join("home/data");
    local::prepare_native_ephemera(&data)?;
    // A personal backup carries no CAS home. Record blobs are fetched from
    // their cloud paths on request, so the restored node starts with an
    // empty local blob store; the proof requires it to stay empty.
    if root.run.cas.is_none() {
        run_restore::install_empty_cas_home(&data, decryption.content_key())
            .map_err(RestoreError::native_proof)?;
    }
    let profile = &root.run;
    let proof = run_restore::verify(&data, profile, &root.native_root, decryption.content_key())
        .map_err(RestoreError::native_proof)?;
    for file in proof.files() {
        let entry: NativeFileProof = local::read_json(
            &session
                .root
                .join("entries")
                .join(native::path_key(&file.path)),
            256 * 1024,
        )?;
        if entry.path != file.path || entry.bytes != file.bytes {
            return Err(RestoreError::Invalid("run exact native inventory"));
        }
        crate::byte_cloud_restore::verify_file(&data.join(&file.path), &entry.sha256, file.bytes)?;
    }
    Ok(proof)
}
