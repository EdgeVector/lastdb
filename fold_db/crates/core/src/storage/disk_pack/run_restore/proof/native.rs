use super::{budget, files, invalid, map, read_budget, Proof, StorageResult};
use laststore::space::SpacePolicy;
use std::fs::{self, File};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

const RAW: &str = "run-native-cas.pending";
const SCRATCH: [&str; 3] = [
    RAW,
    "run-native-cas.json.pending",
    "run-native-cas.inner.pending",
];

pub(super) fn prepare(root: &Path) -> StorageResult<()> {
    let meta = fs::symlink_metadata(root)?;
    if !meta.is_dir()
        || meta.mode() & 0o777 != 0o700
        || meta.uid() != unsafe { libc::geteuid() }
        || root.canonicalize()? != root
    {
        return Err(invalid("run backup proof scratch ownership differs"));
    }
    cleanup(root)
}

pub(super) fn verify(proof: &Proof<'_>, dependency: &files::FileDependency) -> StorageResult<()> {
    cleanup(proof.scratch)?;
    let raw_path = proof.scratch.join(RAW);
    let result = (|| {
        let policy = SpacePolicy::default();
        let mut output = files::NativeCasOutput::create(&raw_path, &policy)?;
        let key =
            crate::storage::disk_pack::native_routing::encode(dependency.blob_ref().as_bytes());
        let copied = {
            let mut read = proof
                .cas
                .fork_bounded(read_budget(budget::NATIVE_STREAM_LOOKUP_READ_BYTES, 1))
                .map_err(map)?;
            read.copy_body_to("cas_blobs", key.as_bytes(), &mut output)
                .map_err(map)?
                .ok_or_else(|| invalid("run backup remote-only file source unsupported"))?
        };
        drop(output.finish()?);
        let row = files::NativeCasProof {
            collection: "cas_blobs".into(),
            key,
            stored_row_bytes: copied.body_bytes,
            stored_row_sha256: cloud_backup_protocol::Digest::try_from(crate::hex::hex_lower(
                copied.body_sha256,
            ))
            .map_err(|_| invalid("run backup native CAS digest format"))?,
        };
        files::verify_native_cas(row, &raw_path, proof.key, dependency, &policy)?;
        Ok(())
    })();
    cleanup(proof.scratch)?;
    result
}

fn cleanup(root: &Path) -> StorageResult<()> {
    for name in SCRATCH {
        let path = root.join(name);
        match fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_file() && meta.uid() == unsafe { libc::geteuid() } => {
                fs::remove_file(&path)?;
            }
            Ok(_) => return Err(invalid("run backup proof scratch type differs")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    File::open(root)?.sync_all()?;
    Ok(())
}
