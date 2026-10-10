//! Read the stopped journal through the same key routes as the cloud engine.

use super::walk::Walker;
use super::ReapError;
use fold_db::storage::traits::{KvStore, PhysicalScanPage};
use fold_db::sync::engine::offline_pin_log_restore_frontier_key;
use std::sync::Arc;

type Values = Vec<Option<Vec<u8>>>;

fn abort(message: impl Into<String>) -> ReapError {
    ReapError::abort("OFFLINE_CLOUD_FRONTIER", message)
}

/// Read every key raw and decrypt all but the production raw restore marker.
/// Missing plaintext is an error for every other physically present key.
pub(super) async fn read_many(
    raw: &dyn KvStore,
    seam: &dyn KvStore,
    keys: &[Vec<u8>],
) -> Result<(Values, Values), ReapError> {
    let encrypted_keys: Vec<_> = keys
        .iter()
        .filter(|key| key.as_slice() != offline_pin_log_restore_frontier_key())
        .cloned()
        .collect();
    let (stored, decrypted) = tokio::join!(
        raw.get_many(keys.to_vec()),
        seam.get_many(encrypted_keys.clone())
    );
    let stored = stored.map_err(|error| abort(error.to_string()))?;
    let decrypted = decrypted.map_err(|error| abort(error.to_string()))?;
    if stored.len() != keys.len() || decrypted.len() != encrypted_keys.len() {
        return Err(abort("cloud read batch count differs"));
    }
    let mut decrypted = decrypted.into_iter();
    let mut values = Vec::with_capacity(keys.len());
    for (key, stored_value) in keys.iter().zip(&stored) {
        let value = if key.as_slice() == offline_pin_log_restore_frontier_key() {
            stored_value.clone()
        } else {
            decrypted
                .next()
                .ok_or_else(|| abort("cloud decrypted batch is incomplete"))?
        };
        if value.is_some() != stored_value.is_some() {
            return Err(abort(format!(
                "cloud key {:?} is hidden from the decrypted reader",
                String::from_utf8_lossy(key)
            )));
        }
        values.push(value);
    }
    Ok((stored, values))
}

/// Enumerate all physical keys and require exact point-read agreement.
/// Each page remains bounded by the shared physical walker.
pub(super) async fn walk<F>(
    raw: Arc<dyn KvStore>,
    seam: Arc<dyn KvStore>,
    mut on_page: F,
) -> Result<(), ReapError>
where
    F: FnMut(&PhysicalScanPage) -> Result<(), ReapError>,
{
    let mut walker = Walker::new(Arc::clone(&raw));
    while let Some(mut page) = walker
        .next_page()
        .await
        .map_err(|error| abort(error.to_string()))?
    {
        let keys: Vec<_> = page.rows.iter().map(|(key, _)| key.clone()).collect();
        let (stored, plain) = read_many(&*raw, &*seam, &keys).await?;
        for (((_, physical), stored), plain) in page.rows.iter_mut().zip(stored).zip(plain) {
            if stored.as_ref() != Some(physical) {
                return Err(abort(
                    "cloud physical bytes differ from the exact keyed read",
                ));
            }
            *physical = plain.ok_or_else(|| abort("physical cloud key is absent by key"))?;
        }
        on_page(&page)?;
    }
    Ok(())
}
