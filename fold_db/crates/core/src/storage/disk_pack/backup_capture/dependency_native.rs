use super::super::file_dependencies as files;
use super::dependencies::held_read;
use super::dependency_store::DependencyStore;
use super::{ExportInner, StorageResult};

pub(super) fn verify(
    inner: &ExportInner,
    store: &DependencyStore,
    account_key: &[u8; 32],
    dependency: &files::FileDependency,
) -> StorageResult<Option<files::NativeCasProof>> {
    let path = store.root.join("native-cas.pending");
    for path in [
        &path,
        &path.with_extension("json.pending"),
        &path.with_extension("inner.pending"),
    ] {
        store.remove_owned(path)?;
    }
    let result = (|| {
        let mut output = files::NativeCasOutput::create(&path, &inner.runtime.space_policy)?;
        let mut read = held_read(
            inner,
            lastdb_storage_policy::backup_decode::NATIVE_STREAM_LOOKUP_READ_BYTES,
        )?;
        let proof = files::read_native_cas(&mut read, dependency, &mut output)?;
        drop(read);
        drop(output.finish()?);
        proof
            .map(|proof| {
                files::verify_native_cas(
                    proof,
                    &path,
                    account_key,
                    dependency,
                    &inner.runtime.space_policy,
                )
            })
            .transpose()
    })();
    store.remove_owned(&path)?;
    result
}
