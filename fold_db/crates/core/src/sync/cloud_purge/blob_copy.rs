//! Stopped adoption copies one exact file after source and collision admission.

use super::memory::{self, Admission};
use crate::storage::NamespacedStore;
use crate::sync::{SyncError, SyncResult};

pub(crate) async fn copy<F: FnMut(&[u8], &[u8])>(
    source: &dyn NamespacedStore,
    destination: &dyn NamespacedStore,
    ids: &[String],
    observe: F,
) -> SyncResult<u64> {
    let admission = Admission::acquire(0).await?;
    copy_admitted(source, destination, ids, &admission, observe).await
}

pub(crate) async fn copy_admitted<F: FnMut(&[u8], &[u8])>(
    source: &dyn NamespacedStore,
    destination: &dyn NamespacedStore,
    ids: &[String],
    admission: &Admission,
    mut observe: F,
) -> SyncResult<u64> {
    let old_sizes = memory::raw_lengths(admission, 0, source, "cas_blobs", ids)?;
    let new_sizes = memory::raw_lengths(admission, 0, destination, "cas_blobs", ids)?;
    let old = source.open_namespace("cas_blobs").await?;
    let new = destination.open_namespace("cas_blobs").await?;
    let mut count = 0;
    for ((id, old_size), new_size) in ids.iter().zip(old_sizes).zip(new_sizes) {
        // An absent inbound cache stays fetchable under its retained grant.
        let Some(old_size) = old_size else { continue };
        let peak = memory::checked_sum(&[
            memory::read_peak(old_size)?,
            new_size.map(memory::read_peak).transpose()?.unwrap_or(0),
        ])?;
        admission.check(peak)?;
        let bytes = old
            .get(id.as_bytes())
            .await?
            .ok_or_else(|| fail("source file disappeared"))?;
        let current = new.get(id.as_bytes()).await?;
        if current.is_some() != new_size.is_some() {
            return Err(fail("destination file changed after size admission"));
        }
        if current.as_ref().is_some_and(|current| current != &bytes) {
            return Err(fail("replacement file conflicts with surviving local data"));
        }
        observe(id.as_bytes(), &bytes);
        if current.is_none() {
            new.restore_batch_put(vec![(id.as_bytes().to_vec(), bytes)])
                .await?;
        }
        count += 1;
    }
    Ok(count)
}

fn fail(message: &str) -> SyncError {
    SyncError::CloudPurgeHeld(message.into())
}
