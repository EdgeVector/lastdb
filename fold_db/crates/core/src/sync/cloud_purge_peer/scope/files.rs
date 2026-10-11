//! Reachable Org file copies preserve surviving identities after byte admission.

use super::*;

pub(super) async fn copy_reachable_blobs(
    source: &std::sync::Arc<dyn crate::storage::traits::NamespacedStore>,
    destination: &std::sync::Arc<dyn crate::storage::traits::NamespacedStore>,
    digest: &mut sha2::Sha256,
) -> SyncResult<u64> {
    let raw = source
        .raw_last_store()
        .ok_or_else(|| fail("org files require LastStore"))?;
    let admission = crate::sync::cloud_purge::memory::Admission::acquire(0).await?;
    let ids: Vec<_> =
        crate::sync::cloud_purge::org_rows::physical_keys(&raw, "cas_blobs", &admission)?
            .into_keys()
            .collect();
    drop(admission);
    crate::sync::cloud_purge::blob_copy::copy(
        source.as_ref(),
        destination.as_ref(),
        &ids,
        |key, bytes| {
            digest.update(b"cas_blobs");
            digest.update((key.len() as u64).to_be_bytes());
            digest.update(key);
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
        },
    )
    .await
}
