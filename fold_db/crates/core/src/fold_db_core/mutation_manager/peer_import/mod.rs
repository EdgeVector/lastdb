//! Signed peer field facts use the ordinary counted native storage path.

mod admission;
mod prepare;
mod publication;
mod records;

use super::helpers::ModifiedFieldKeys;
use super::molecules::DeferredPersistJob;
use super::MutationManager;
use crate::schema::SchemaError;
use crate::storage::disk_pack::logical_capture::{
    AcceptedFieldOutcome, AcceptedRecordOutcome, CaptureEvent, ImportReceipt,
};
use crate::storage::disk_pack::{transaction, PackRuntime};
use std::collections::BTreeMap;
use std::sync::Arc;

fn unsupported(reason: &str) -> SchemaError {
    SchemaError::InvalidData(format!("logical peer import v1 does not support {reason}"))
}

impl MutationManager {
    /// Import one bounded, single-device page without authoring a local event.
    /// The receiver rebuilds its own atom references; only logical facts travel.
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
                let prepared = runtime.begin_logical_import(events).await?;
                let events = prepared.events();
                self.require_peer_replay_floor(events).await?;
                // A format 7 home merges record outcomes; another home stages
                // field tips through the counted atom path.
                let records = if runtime.record_home() {
                    Some(self.admit_peer_records(events).await?)
                } else {
                    self.admit_peer_outcomes(events).await?;
                    None
                };
                // No queue wait is legal under the native prepare gate. Observe the
                // authenticated clocks under the short resident lock, then stage a
                // monotonic durable maximum in this same request. Failure can leave
                // a counter gap, as on the ordinary author-clock allocation path.
                self.observe_peer_clocks(events);
                match &records {
                    Some(schemas) => self.stage_peer_records(events, schemas, &runtime).await?,
                    None => {
                        for event in events {
                            self.stage_peer_event(event, &runtime).await?;
                        }
                    }
                }
                if events.iter().any(|event| event.outcome_count() > 0) {
                    self.stage_native_author_clock().await?;
                }
                let receipt = runtime.stage_logical_import(prepared).await?;
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

    async fn stage_peer_event(
        &self,
        event: &CaptureEvent,
        runtime: &Arc<PackRuntime>,
    ) -> Result<(), SchemaError> {
        let mut schemas: BTreeMap<&str, Vec<&AcceptedFieldOutcome>> = BTreeMap::new();
        for outcome in &event.outcomes {
            schemas
                .entry(&outcome.schema_name)
                .or_default()
                .push(outcome);
        }
        // Each event sees the private accepted prefix of every prior event.
        // Do not collapse a page to its final tips: intermediate losers retain
        // their normal conflict facts and counted atom references.
        for (name, outcomes) in schemas {
            let mut job = self
                .prepare_peer_job(name, &outcomes, &event.origin)
                .await?;
            let mut seen = std::collections::HashSet::new();
            let keys: Vec<_> = outcomes
                .iter()
                .map(|fact| fact.key.clone())
                .filter(|key| seen.insert(key.clone()))
                .collect();
            let before = if job.schema.alternate_keys.is_empty() {
                Vec::new()
            } else {
                self.db_ops
                    .atoms()
                    .alternate_fields(&job.schema, &keys, None)
                    .await?
            };
            self.stage_native_job(&mut job).await?;
            if !job.schema.alternate_keys.is_empty() {
                self.db_ops
                    .atoms()
                    .stage_alternate_changes(&job.schema, &keys, &before, None)
                    .await?;
            }
            self.reload_changed_for_prepare(&mut job.schema, &job.modified_fields)
                .await?;
            self.stage_native_paths(&job, runtime).await?;
            self.stage_peer_notifications(&job, &outcomes, runtime)
                .await?;
        }
        Ok(())
    }
}

/// The write time and logical counter of each tip and record stamp.
fn event_clocks(events: &[CaptureEvent]) -> impl Iterator<Item = (u64, u64)> + '_ {
    events.iter().flat_map(|event| {
        let tips = event
            .outcomes
            .iter()
            .map(|fact| (fact.tip.written_at, fact.tip.logical_counter));
        let stamps = event
            .records
            .iter()
            .flat_map(AcceptedRecordOutcome::stamps)
            .map(|stamp| (stamp.written_at, stamp.logical_counter));
        tips.chain(stamps)
    })
}
