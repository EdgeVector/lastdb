//! One owned raw CAS temporary; neither file size nor cumulative I/O is heap admission.
use super::*;
use lastdb_storage_policy::backup_decode::{
    NATIVE_STREAM_BLOCKS, NATIVE_STREAM_LOOKUP_READ_BYTES, NATIVE_STREAM_PAGES,
};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;

pub(super) fn verify(
    session: &RestoreSession,
    home: &DiskHome,
    key: &[u8; 32],
    dependency: &directory::FileDependency,
) -> Result<bool> {
    let path = session.root.join("native-cas.pending");
    // A process loss can leave any decode stage. Release all owned prefixes
    // before the first new raw byte needs filesystem capacity.
    cleanup(session)?;
    let result = verify_inner(session, home, key, dependency, &path);
    cleanup(session)?;
    result
}

fn cleanup(session: &RestoreSession) -> Result<()> {
    for name in [
        "native-cas.pending",
        "native-cas.json.pending",
        "native-cas.inner.pending",
    ] {
        let path = session.root.join(name);
        match fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_file() => fs::remove_file(&path)?,
            Ok(_) => return Err(RestoreError::Invalid("native file proof temporary")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    File::open(&session.root)?.sync_all()?;
    Ok(())
}

fn verify_inner(
    session: &RestoreSession,
    home: &DiskHome,
    key: &[u8; 32],
    dependency: &directory::FileDependency,
    path: &std::path::Path,
) -> Result<bool> {
    let proof = {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let mut output = CapacityWriter {
            file,
            root: &session.root,
        };
        let mut read = home
            .read(ReadBudget {
                pages: NATIVE_STREAM_PAGES,
                blocks: NATIVE_STREAM_BLOCKS,
                bytes: NATIVE_STREAM_LOOKUP_READ_BYTES,
                keys: 1,
            })
            .map_err(|_| RestoreError::Limit("native file read"))?;
        directory::read_native_cas(&mut read, dependency, &mut output)
            .map_err(|_| RestoreError::Invalid("native file source"))?
    }; // Drop the native reader and raw writer before account/file decode.
    let Some(proof) = proof else {
        return Ok(false);
    };
    directory::verify_native_cas(
        proof,
        path,
        key,
        dependency,
        &laststore::disk_pack::maintenance::SpacePolicy::default(),
    )
    .map_err(|_| RestoreError::Invalid("native file proof"))?;
    Ok(true)
}

struct CapacityWriter<'a> {
    file: File,
    root: &'a std::path::Path,
}
impl Write for CapacityWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        super::super::download::check_capacity(self.root, bytes.len() as u64)
            .map_err(|_| std::io::Error::other("native file proof disk admission"))?;
        self.file.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
