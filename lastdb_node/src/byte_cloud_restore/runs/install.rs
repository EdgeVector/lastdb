use super::super::{local, native, NativeFileProof, NativeInventory, RecoveryKeys};
use super::super::{RestoreError, RestoreReport, RestoreSession, Result, VerifiedNativeContext};
use fold_db::storage::disk_pack::{byte_backup::ClientRoot, run_restore};
use std::fs::{self, File};
use std::os::unix::fs::DirBuilderExt;

pub(super) fn finish(
    session: &RestoreSession,
    keys: &RecoveryKeys,
    root: &ClientRoot,
    context: &VerifiedNativeContext<'_>,
    totals: NativeInventory,
) -> Result<RestoreReport> {
    let data = session.root.join("home/data");
    for name in ["disk-run", "disk-cas"] {
        let home = data.join(name);
        local::directory(&home)?;
        let runs = home.join("runs");
        match fs::DirBuilder::new().mode(0o700).create(&runs) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        local::directory(&runs)?;
        let path = home.join("LOCK");
        let lock = match local::owner_file(&path, true) {
            Ok(lock) => lock,
            Err(RestoreError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                local::owner_file(&path, false)?
            }
            Err(error) => return Err(error),
        };
        if lock.metadata()?.len() != 0 {
            return Err(RestoreError::Invalid("run ephemeral lock"));
        }
        lock.sync_all()?;
        File::open(&home)?.sync_all()?;
    }
    let scratch = session.root.join("run-proof");
    match fs::DirBuilder::new().mode(0o700).create(&scratch) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    local::directory(&scratch)?;
    let profile = root
        .run
        .as_ref()
        .ok_or(RestoreError::Invalid("run profile"))?;
    let proof = run_restore::verify(
        &data,
        profile,
        &root.native_root,
        keys.content_key(),
        &scratch,
    )
    .map_err(|_| RestoreError::Invalid("complete run native proof"))?;
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
        crate::native_cloud_restore::verify_file(
            &data.join(&file.path),
            &entry.sha256,
            file.bytes,
        )?;
    }
    context.finish_run(keys, totals, &proof)
}
