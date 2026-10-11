//! Retire one target's old stream only after its local bytes are archived.

use super::*;

impl PinLog {
    pub(crate) async fn detach_cloud_generations<F>(
        &self,
        replacement_frontiers: &BTreeMap<String, BTreeMap<String, u64>>,
        mut archive: F,
        archive_pages: &mut Vec<String>,
    ) -> Result<u64, String>
    where
        F: FnMut(&[(Vec<u8>, Vec<u8>)]) -> Result<String, String>,
    {
        let _append = self.append_lock.lock().await;
        let _published = self.published_f_lock.lock().await;
        let target_ids: BTreeMap<_, _> = replacement_frontiers
            .iter()
            .map(|(prefix, frontier)| (target_id_for_prefix(prefix), frontier))
            .collect();
        let store = self.pin_log_store().await?;
        let prefix = PIN_LOG_ENTRY_PREFIX.as_bytes().to_vec();
        let end = prefix_upper_bound(&prefix).ok_or("cloud target prefix has no upper bound")?;
        let mut cursor = prefix;
        let mut count = 0_u64;
        loop {
            let rows = store
                .scan_range_paged(&cursor, &end, 128)
                .await
                .map_err(|e| e.to_string())?;
            if rows.is_empty() {
                break;
            }
            cursor = key_after(&rows.last().expect("nonempty page").0);
            let rows: Vec<_> = rows
                .into_iter()
                .filter(|(key, _)| {
                    std::str::from_utf8(key)
                        .ok()
                        .and_then(|key| key.strip_prefix(PIN_LOG_ENTRY_PREFIX))
                        .and_then(|key| key.split_once(":entry:"))
                        .is_some_and(|(id, _)| target_ids.contains_key(id))
                })
                .collect();
            if rows.is_empty() {
                continue;
            }
            archive_pages.push(archive(&rows)?);
            count += rows.len() as u64;
            store
                .batch_delete(rows.into_iter().map(|(key, _)| key).collect())
                .await
                .map_err(|e| e.to_string())?;
            store.flush().await.map_err(|e| e.to_string())?;
        }
        // Marker receipts are target-bound; leaving one would let its retry
        // reconstruct a retired row after the replacement is adopted.
        let prefix = CAPTURE_MARKER_RECEIPT_PREFIX.as_bytes();
        let end = prefix_upper_bound(prefix).ok_or("capture receipt prefix has no upper bound")?;
        let mut cursor = prefix.to_vec();
        loop {
            let rows = store
                .scan_range_paged(&cursor, &end, 128)
                .await
                .map_err(|e| e.to_string())?;
            if rows.is_empty() {
                break;
            }
            cursor = key_after(&rows.last().expect("nonempty page").0);
            let mut retired = Vec::new();
            for (key, value) in rows {
                let receipt: CaptureMarkerAppendReceipt =
                    serde_json::from_slice(&value).map_err(|e| e.to_string())?;
                if receipt
                    .records
                    .iter()
                    .any(|record| replacement_frontiers.contains_key(&record.target_prefix))
                {
                    retired.push((key, value));
                }
            }
            if retired.is_empty() {
                continue;
            }
            archive_pages.push(archive(&retired)?);
            store
                .batch_delete(retired.into_iter().map(|(key, _)| key).collect())
                .await
                .map_err(|e| e.to_string())?;
            store.flush().await.map_err(|e| e.to_string())?;
        }
        let published_keys: Vec<_> = target_ids
            .keys()
            .map(|id| pin_log_published_f_key(id))
            .collect();
        let previous = store
            .get_many(published_keys.clone())
            .await
            .map_err(|e| e.to_string())?;
        let archived: Vec<_> = published_keys
            .iter()
            .cloned()
            .zip(previous)
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect();
        if !archived.is_empty() {
            archive_pages.push(archive(&archived)?);
        }
        store
            .batch_put(
                target_ids
                    .iter()
                    .map(|(id, frontier)| {
                        Ok((
                            pin_log_published_f_key(id),
                            serde_json::to_vec(frontier).map_err(|e| e.to_string())?,
                        ))
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            )
            .await
            .map_err(|e| e.to_string())?;
        store.flush().await.map_err(|e| e.to_string())?;
        self.state
            .lock()
            .await
            .retain(|id, _| !target_ids.contains_key(id));
        self.known_pending_prefixes
            .lock()
            .await
            .retain(|prefix| !replacement_frontiers.contains_key(prefix));
        Ok(count)
    }
}
