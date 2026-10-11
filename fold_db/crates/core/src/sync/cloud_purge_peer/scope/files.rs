//! Reachable org file copies preserve all surviving blob identities.

use super::*;

pub(super) async fn copy_reachable_blobs(
    source: &std::sync::Arc<dyn crate::storage::traits::NamespacedStore>,
    destination: &std::sync::Arc<dyn crate::storage::traits::NamespacedStore>,
    digest: &mut sha2::Sha256,
) -> SyncResult<u64> {
    let source = source.open_namespace("cas_blobs").await.map_err(storage)?;
    let destination = destination
        .open_namespace("cas_blobs")
        .await
        .map_err(storage)?;
    let mut cursor = Vec::new();
    let mut count = 0;
    loop {
        let rows = source
            .scan_range_paged(&cursor, &[255], 4)
            .await
            .map_err(storage)?;
        if rows.is_empty() {
            break;
        }
        cursor = rows
            .last()
            .ok_or_else(|| fail("org file page is empty"))?
            .0
            .clone();
        cursor.push(0);
        let existing = destination
            .get_many(rows.iter().map(|(key, _)| key.clone()).collect())
            .await
            .map_err(storage)?;
        if existing.len() != rows.len() {
            return Err(fail("org file collision read is incomplete"));
        }
        let mut new_rows = Vec::new();
        for ((key, bytes), existing) in rows.into_iter().zip(existing) {
            if existing.as_ref().is_some_and(|existing| existing != &bytes) {
                return Err(fail(
                    "org replacement file identity conflicts with surviving local data",
                ));
            }
            digest.update(b"cas_blobs");
            digest.update((key.len() as u64).to_be_bytes());
            digest.update(&key);
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(&bytes);
            if existing.is_none() {
                new_rows.push((key, bytes));
            }
            count += 1;
        }
        if !new_rows.is_empty() {
            destination
                .restore_batch_put(new_rows)
                .await
                .map_err(storage)?;
        }
    }
    Ok(count)
}
