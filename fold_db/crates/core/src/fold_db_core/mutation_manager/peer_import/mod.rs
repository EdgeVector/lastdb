//! Peer import: version 2 record outcomes merge field by field into the
//! stored records, in the native request of the import.

mod admission;
mod records;

use super::MutationManager;
use crate::schema::SchemaError;
use crate::storage::disk_pack::logical_capture::{
    AcceptedRecordOutcome, CaptureEvent, ImportReceipt,
};
use crate::storage::disk_pack::{transaction, PackRuntime};
use std::collections::BTreeMap;
use std::sync::Arc;

fn unsupported(reason: &str) -> SchemaError {
    SchemaError::InvalidData(format!("logical peer import v1 does not support {reason}"))
}

impl MutationManager {
    /// Import one bounded, single-device page without authoring a local event.
    /// Only logical facts travel; the receiver stages its own records.
    pub async fn import_logical_events(
        &self,
        events: Vec<CaptureEvent>,
    ) -> Result<ImportReceipt, SchemaError> {
        crate::sync::capture::with_mutation_admission(self.capture_router(), async {
            let runtime = self
                .db_ops
                .namespaced_store()
                .disk_pack_runtime()
                .filter(|runtime| runtime.logical_capture_enabled())
                .ok_or_else(|| unsupported("a home without native logical capture"))?;
            let pending = self.pending_tasks.begin();
            transaction::request_scope(Some(Arc::clone(&runtime)), async {
                let prepared = Box::pin(runtime.begin_logical_import(events)).await?;
                let events = prepared.events();
                self.require_peer_replay_floor(events).await?;
                let schemas = Box::pin(self.admit_peer_records(events)).await?;
                // No queue wait is legal under the native prepare gate. Observe the
                // authenticated clocks under the short resident lock, then stage a
                // monotonic durable maximum in this same request. Failure can leave
                // a counter gap, as on the ordinary author-clock allocation path.
                self.observe_peer_clocks(events);
                Box::pin(self.stage_peer_records(events, &schemas, &runtime)).await?;
                if events.iter().any(|event| event.outcome_count() > 0) {
                    self.stage_native_author_clock().await?;
                }
                let receipt = Box::pin(runtime.stage_logical_import(prepared)).await?;
                self.reserve_native_core_memory(&runtime)?;
                transaction::retain(&runtime, pending)?;
                Ok::<_, SchemaError>(receipt)
            })
            .await
        })
        .await
    }

    fn observe_peer_clocks(&self, events: &[CaptureEvent]) {
        let mut state = self
            .author_clock_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (written_at, logical_counter) in event_clocks(events) {
            state.observe_remote(written_at, logical_counter);
        }
    }
}

/// The write time and logical counter of each record stamp.
fn event_clocks(events: &[CaptureEvent]) -> impl Iterator<Item = (u64, u64)> + '_ {
    events.iter().flat_map(|event| {
        event
            .records
            .iter()
            .flat_map(AcceptedRecordOutcome::stamps)
            .map(|stamp| (stamp.written_at, stamp.logical_counter))
    })
}
