//! Read the captured write graph before any clean warm record or disk bytes.

use super::super::transaction::{PendingBatch, PendingRecord};
use super::ReadContext;
use std::collections::BTreeMap;
use std::sync::Arc;

impl ReadContext {
    pub(in crate::storage::disk_pack) fn overlay_record(
        &self,
        pending: Option<&Arc<PendingBatch>>,
        collection: &str,
        key: &str,
    ) -> Option<Arc<PendingRecord>> {
        pending
            .and_then(|batch| batch.lookup(collection, key))
            .or_else(|| self.graph.get(collection, key))
    }

    pub(in crate::storage::disk_pack) fn overlay_rows(
        &self,
        pending: Option<&Arc<PendingBatch>>,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<PendingRecord>> {
        let mut rows: BTreeMap<_, _> = self
            .graph
            .range(collection, start, end)
            .into_iter()
            .map(|record| (record.key.clone(), record))
            .collect();
        if let Some(batch) = pending {
            for record in batch.matching(collection, start, end) {
                rows.insert(record.key.clone(), record);
            }
        }
        rows.into_values().collect()
    }
}
