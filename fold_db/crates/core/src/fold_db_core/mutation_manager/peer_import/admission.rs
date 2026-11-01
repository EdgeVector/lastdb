//! Every imported stamp observes the home-wide durable replay floor.

use super::*;

impl MutationManager {
    pub(super) async fn require_peer_replay_floor(
        &self,
        events: &[CaptureEvent],
    ) -> Result<(), SchemaError> {
        if events.iter().all(|event| event.outcome_count() == 0) {
            return Ok(());
        }
        let cutoff_nanos = self.db_ops.records().minimum_replay_time().await?;
        if cutoff_nanos > 0
            && super::event_clocks(events).any(|(written_at, _)| written_at <= cutoff_nanos)
        {
            return Err(SchemaError::ReplayBeforeDeleteCutoff { cutoff_nanos });
        }
        Ok(())
    }
}
