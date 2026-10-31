//! Retention janitor wiring: its state, the ports it reads, and wrappers.

use serde::de::DeserializeOwned;
use serde_json::Value;

use super::super::counter::ReleaseRequest;
use super::super::ports::{RetentionStates, StoredValues, TimeQueues};
use super::super::retention::{
    RecordSlot, RepairWindow, Repaired, Retention, RetentionCursor, RetentionState, SlotReleases,
    StepOutcome,
};
use super::super::time_queue::{QueueStats, TimeQueue};
use super::super::AtomStore;
use crate::schema::SchemaError;

impl StoredValues for AtomStore {
    async fn read_stored_values<T>(
        &self,
        keys: &[String],
        failure: &'static str,
    ) -> Result<Vec<Option<T>>, SchemaError>
    where
        T: DeserializeOwned + Send + Sync,
    {
        Self::read_stored_values(self, keys, failure).await
    }
}

impl TimeQueues for AtomStore {
    async fn retire_bucket(&self, key: String, token: Vec<u8>) -> Result<bool, SchemaError> {
        Self::retire_bucket(self, key, token).await
    }

    async fn time_queue_stats(
        &self,
        queue: &TimeQueue,
        cap: u64,
    ) -> Result<QueueStats, SchemaError> {
        Self::time_queue_stats(self, queue, cap).await
    }
}

impl RetentionStates for AtomStore {
    fn retention_state(&self) -> &RetentionState {
        Self::retention_state(self)
    }
}

impl AtomStore {
    pub(crate) async fn repair_record_retention(
        &self,
        scope: Option<&str>,
        records: &[String],
        buckets: &[u64],
        window: &RepairWindow,
    ) -> Result<Repaired, SchemaError> {
        Retention::new(self)
            .repair_record_retention(scope, records, buckets, window)
            .await
    }

    pub(crate) fn retention_rows(
        &self,
        scope: Option<&str>,
        history_key: &str,
        slot: &RecordSlot<'_>,
        conflict: Option<String>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        Retention::new(self).retention_rows(scope, history_key, slot, conflict)
    }

    pub(crate) async fn slot_release_requests(
        &self,
        scope: Option<&str>,
        removals: &[(&str, &[(String, String)])],
        preserve_archives: bool,
    ) -> Result<SlotReleases, SchemaError> {
        Retention::new(self)
            .slot_release_requests(scope, removals, preserve_archives)
            .await
    }

    pub(crate) fn directory_release_requests(
        &self,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Vec<ReleaseRequest> {
        Retention::new(self).directory_release_requests(rows)
    }

    pub(crate) fn retention_cursor(&self) -> RetentionCursor {
        Retention::new(self).retention_cursor()
    }

    pub(crate) async fn refresh_queue(
        &self,
        cursor: &mut RetentionCursor,
        now_nanos: u64,
    ) -> Result<(), SchemaError> {
        Retention::new(self).refresh_queue(cursor, now_nanos).await
    }

    pub(crate) async fn release_step(
        &self,
        cursor: &mut RetentionCursor,
        now_nanos: u64,
    ) -> Result<Option<StepOutcome>, SchemaError> {
        Retention::new(self).release_step(cursor, now_nanos).await
    }

    pub(crate) async fn net_step(
        &self,
        cursor: &mut RetentionCursor,
        now_nanos: u64,
    ) -> Result<Option<StepOutcome>, SchemaError> {
        Retention::new(self).net_step(cursor, now_nanos).await
    }

    pub(crate) async fn drain_step(
        &self,
        cursor: &mut RetentionCursor,
    ) -> Result<Option<StepOutcome>, SchemaError> {
        Retention::new(self).drain_step(cursor).await
    }

    pub(crate) async fn recorded_retention_window(&self) -> Result<Option<u64>, SchemaError> {
        Retention::new(self).recorded_retention_window().await
    }

    pub(crate) async fn record_retention_window(&self) -> Result<(), SchemaError> {
        Retention::new(self).record_retention_window().await
    }
}
