//! Copy stopped scoped main rows after source and destination memory admission.

use super::memory::{self, Admission};
use crate::storage::NamespacedStore;
use crate::sync::{SyncError, SyncResult};

pub(crate) async fn copy<F: FnMut(&[u8], &[u8])>(
    source: &dyn NamespacedStore,
    destination: &dyn NamespacedStore,
    keys: &[String],
    admission: &Admission,
    mut observe: F,
) -> SyncResult<u64> {
    let source_sizes = memory::raw_lengths(admission, 0, source, "main", keys)?;
    let destination_sizes = memory::raw_lengths(admission, 0, destination, "main", keys)?;
    let old = source.open_namespace("main").await?;
    let new = destination.open_namespace("main").await?;
    for ((key, old_size), new_size) in keys.iter().zip(source_sizes).zip(destination_sizes) {
        let old_size = old_size.ok_or_else(|| fail("scoped source metadata disappeared"))?;
        admission.check(memory::checked_sum(&[
            memory::body_peak(old_size)?,
            new_size.map(memory::body_peak).transpose()?.unwrap_or(0),
        ])?)?;
        let value = old
            .get(key.as_bytes())
            .await?
            .ok_or_else(|| fail("scoped source disappeared"))?;
        let current = new.get(key.as_bytes()).await?;
        if current.is_some() != new_size.is_some() {
            return Err(fail("scoped destination changed after admission"));
        }
        if current.as_ref().is_some_and(|current| current != &value) {
            return Err(fail("scoped destination conflicts with surviving data"));
        }
        observe(key.as_bytes(), &value);
        if current.is_none() {
            new.restore_batch_put(vec![(key.as_bytes().to_vec(), value)])
                .await?;
        }
    }
    Ok(keys.len() as u64)
}

fn fail(message: &str) -> SyncError {
    SyncError::CloudPurgeHeld(message.into())
}

pub(crate) async fn delete(
    store: &dyn NamespacedStore,
    namespace: &str,
    keys: &[String],
    admission: &Admission,
) -> SyncResult<()> {
    let sizes = memory::raw_lengths(admission, 0, store, namespace, keys)?;
    let destination = store.open_namespace(namespace).await?;
    for (page, sizes) in keys.chunks(128).zip(sizes.chunks(128)) {
        let peak = sizes.iter().flatten().try_fold(0, |sum, size| {
            memory::checked_sum(&[sum, memory::body_peak(*size)?])
        })?;
        admission.check(peak)?;
        destination
            .batch_delete(page.iter().map(|key| key.as_bytes().to_vec()).collect())
            .await?;
    }
    Ok(())
}
