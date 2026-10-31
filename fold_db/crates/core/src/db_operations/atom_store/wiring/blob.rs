//! Blob counter wiring: the blob events, the liveness locks, and wrappers.

use std::collections::HashMap;
use std::sync::Arc;

use super::super::blob_counter::{
    BlobCounterReport, BlobCounterView, BlobCounters, BlobDeltas, BlobEvents, BlobRecountReport,
    DwellCursor, DwellGuard, DwellStep, NetStep,
};
use super::super::ports::{BlobStates, NamespacedStores};
use super::super::AtomStore;
use crate::schema::SchemaError;
use crate::storage::{KvMutation, NamespacedStore};

impl NamespacedStores for AtomStore {
    fn namespaced(&self) -> Option<&Arc<dyn NamespacedStore>> {
        self.namespaced_store.as_ref()
    }
}

impl BlobStates for AtomStore {
    fn blob_events(&self) -> &Arc<BlobEvents> {
        &self.blob_events
    }

    async fn lock_liveness_blobs(
        &self,
        blob_refs: &[String],
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        Self::lock_liveness_blobs(self, blob_refs).await
    }
}

impl AtomStore {
    pub(crate) async fn blob_counter_mutations(
        &self,
        deltas: BlobDeltas,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        BlobCounters::new(self).blob_counter_mutations(deltas).await
    }

    pub(crate) async fn register_blob_upload(&self, blob_ref: &str) -> Result<(), SchemaError> {
        BlobCounters::new(self).register_blob_upload(blob_ref).await
    }

    pub(in crate::db_operations::atom_store) async fn blob_rows_exist(
        &self,
        blobs: &[String],
    ) -> Result<Option<Vec<bool>>, SchemaError> {
        BlobCounters::new(self).blob_rows_exist(blobs).await
    }

    pub(crate) async fn recount_blob_counters(
        &self,
        holders: HashMap<String, u64>,
        exact: bool,
        execute: bool,
        report: &mut BlobRecountReport,
    ) -> Result<(), SchemaError> {
        BlobCounters::new(self)
            .recount_blob_counters(holders, exact, execute, report)
            .await
    }

    pub async fn blob_counter_report_full(&self) -> BlobCounterReport {
        BlobCounters::new(self).blob_counter_report_full().await
    }

    pub async fn blob_counter_views(
        &self,
        blobs: &[String],
    ) -> Result<Vec<BlobCounterView>, SchemaError> {
        BlobCounters::new(self).blob_counter_views(blobs).await
    }

    #[must_use]
    pub fn blob_clock_millis(&self) -> u64 {
        BlobCounters::new(self).blob_clock_millis()
    }

    pub fn begin_blob_dwell_pass(&self) -> Option<DwellGuard> {
        BlobCounters::new(self).begin_blob_dwell_pass()
    }

    pub async fn blob_dwell_begin(&self, cursor: &mut DwellCursor) -> Result<(), SchemaError> {
        BlobCounters::new(self).blob_dwell_begin(cursor).await
    }

    pub async fn blob_dwell_refresh(&self, cursor: &mut DwellCursor) -> Result<(), SchemaError> {
        BlobCounters::new(self).blob_dwell_refresh(cursor).await
    }

    pub async fn blob_dwell_step(
        &self,
        cursor: &mut DwellCursor,
        max: usize,
    ) -> Result<Option<DwellStep>, SchemaError> {
        BlobCounters::new(self).blob_dwell_step(cursor, max).await
    }

    pub async fn blob_dwell_net_step(
        &self,
        cursor: &mut DwellCursor,
    ) -> Result<Option<NetStep>, SchemaError> {
        BlobCounters::new(self).blob_dwell_net_step(cursor).await
    }
}
