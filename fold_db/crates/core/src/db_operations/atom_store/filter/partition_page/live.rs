//! Merge live disk keys before the generation/page frontier.

use super::*;

impl AtomStore {
    pub(super) async fn merge_live_tip_rows(
        &self,
        scan: &TipScan<'_>,
        rows: &mut TipRows,
    ) -> Result<bool, SchemaError> {
        let live = self
            .main_store
            .scan_items_in_range_paged(
                &build_storage_key(scan.storage_prefix, scan.start),
                &build_storage_key(scan.storage_prefix, scan.end),
                scan.ask,
            )
            .await
            .map_err(|error| SchemaError::InvalidData(format!("tip page: {error}")))?;
        let live = Self::strip_per_key_storage_prefix(scan.storage_prefix, live);
        let exhausted = live.len() < scan.ask;
        for (key, record) in live {
            if scan.cursor.is_none_or(|cursor| key.as_str() > cursor) {
                rows.entry(key).or_default().0 = Some(record);
            }
        }
        Ok(exhausted)
    }
}
