//! Complete raw inventory and bounded selected-key raw/decrypted batches.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use fold_db::storage::laststore::offline_physical_collection_pair;
use fold_db::storage::traits::{KvStore, PhysicalScanCursor};

type Selected<'a> = (&'a [u8], &'a [u8], source::Identity);

pub(super) async fn collection(
    name: &str,
    physical: &Arc<laststore::LastStore>,
    crypto: &Arc<dyn fold_db::crypto::CryptoProvider>,
    requested: &BTreeSet<String>,
    found: &mut model::Collected,
) -> Result<(), String> {
    let (raw, seam) =
        offline_physical_collection_pair(Arc::clone(physical), name, Arc::clone(crypto));
    let mut walker = crate::reap::walk::Walker::with_page_rows(Arc::clone(&raw), 16)?;
    let mut hash = Sha256::new();
    while let Some(page) = walker.next_page().await.map_err(err)? {
        let mut selected = Vec::new();
        for (key, value) in &page.rows {
            hash.update((key.len() as u64).to_be_bytes());
            hash.update(key);
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value);
            found.counts.physical_rows += 1;
            if let Some(identity) = source::identity(name, key)? {
                found.counts.supported_blob_rows += 1;
                if requested.contains(&identity.reference) {
                    selected.push((key.as_slice(), value.as_slice(), identity));
                }
            }
        }
        if !selected.is_empty() {
            let handle = page
                .row_handle
                .as_ref()
                .ok_or("selected blob page has no physical handle")?;
            batch(name, (&*raw, &*seam), handle, selected, found).await?;
        }
    }
    found
        .digests
        .insert(name.into(), fold_db::hex::hex_lower(hash.finalize()));
    Ok(())
}

async fn batch(
    name: &str,
    readers: (&dyn KvStore, &dyn KvStore),
    handle: &PhysicalScanCursor,
    rows: Vec<Selected<'_>>,
    found: &mut model::Collected,
) -> Result<(), String> {
    if handle
        .collection
        .as_deref()
        .is_some_and(|collection| collection != name)
    {
        return Err("selected blob physical handle names another collection".into());
    }
    let keys: Vec<_> = rows.iter().map(|row| row.0.to_vec()).collect();
    let (stored, plain) = tokio::join!(readers.0.get_many(keys.clone()), readers.1.get_many(keys));
    let stored = stored.map_err(err)?;
    let plain = plain.map_err(err)?;
    if stored.len() != rows.len() || plain.len() != rows.len() {
        return Err("selected blob batch count differs".into());
    }
    for (((key, raw, identity), stored), plain) in rows.into_iter().zip(stored).zip(plain) {
        if stored.as_deref() != Some(raw) {
            return Err("selected physical blob differs from its raw keyed read".into());
        }
        if !found.keys.insert((name.into(), key.to_vec())) {
            return Err("selected blob key repeats across physical handles".into());
        }
        let plain = plain.ok_or("selected physical blob is hidden from the decrypted reader")?;
        let stored_at = source::date(name, &identity.reference, &plain)?;
        found.counts.selected_copies += 1;
        found.counts.selected_raw_bytes += raw.len() as u64;
        found.counts.selected_scoped_copies += u64::from(!identity.scope.is_empty());
        found.counts.selected_undated_copies += u64::from(stored_at.is_none());
        found.found.insert(identity.reference.clone());
        found.copies.write(&model::Copy {
            collection: name.into(),
            shard: handle.shard,
            group_id: handle.group_id,
            scope: identity.scope,
            key_b64: STANDARD.encode(key),
            blob_ref: identity.reference,
            raw_sha256: super::super::model::digest(raw),
            raw_bytes: raw.len() as u64,
            plain_sha256: super::super::model::digest(&plain),
            stored_at,
        })?;
    }
    Ok(())
}
