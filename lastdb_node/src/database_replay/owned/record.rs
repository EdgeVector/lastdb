//! Resolve authenticated record changes under the native replay transaction.

use fold_db::fold_db_core::mutation_manager::{MutationManager, ReplayStamp};
use fold_db::schema::types::{KeyValue, Mutation, MutationType, SchemaError};

use super::SnapshotEntry;

pub(super) struct RecordReplay<'a> {
    pub(super) entry: &'a SnapshotEntry,
    pub(super) mutation_id: &'a str,
    pub(super) is_local: bool,
    pub(super) schema: String,
    pub(super) key: KeyValue,
    pub(super) stamp: ReplayStamp,
    pub(super) explicit_attachment: bool,
    pub(super) incoming_ready: bool,
    pub(super) prior: Option<(Vec<u8>, Mutation)>,
}

impl RecordReplay<'_> {
    pub(super) async fn attempt(
        &mut self,
        manager: &MutationManager,
        mutation: &Mutation,
        fetch: &mut Option<RecordFetch>,
    ) -> Result<(), SchemaError> {
        let (schema, key) = (&self.schema, &self.key);
        manager.observe_replay_time(self.stamp.ordering_ms).await?;
        if self.is_local
            && manager
                .clear_replay_record_after_schema_drop(schema, self.mutation_id)
                .await?
        {
            return Ok(());
        }
        let pending = manager.pending_replay_record(schema, key).await?;
        let winner = manager.replay_record_winner(schema, key).await?;
        let settles_local = self.is_local && pending.as_deref() == Some(self.mutation_id);
        let loses = winner.as_ref().is_some_and(|winner| winner >= &self.stamp);
        self.guard_attachments(
            manager,
            mutation,
            !self.is_local && pending.is_some(),
            loses,
        )
        .await?;

        if settles_local && loses {
            let winner = winner.as_ref().ok_or_else(needs_preparation)?;
            self.restore_winner(manager, winner, fetch).await?;
        } else if !loses {
            if pending.is_some() && !settles_local {
                let encoded = serde_json::to_vec(self.entry)
                    .map_err(|_| SchemaError::InvalidData("owned entry encoding".into()))?;
                manager
                    .stage_deferred_replay_record(schema, key, Some(encoded))
                    .await?;
            } else if !settles_local {
                // Old queue entries can have no marker. Apply the accepted value
                // even for this writer, since a previously read peer may have changed it.
                if !self.incoming_ready {
                    *fetch = Some(RecordFetch::Incoming);
                    return Err(needs_preparation());
                }
                manager.apply_replay_record(mutation.clone()).await?;
            }
            manager
                .accept_replay_record(schema, key, &self.stamp)
                .await?;
        }
        if settles_local {
            manager
                .stage_deferred_replay_record(&self.schema, &self.key, None)
                .await?;
            manager
                .clear_pending_replay_record(&self.schema, &self.key, self.mutation_id)
                .await?;
        }
        Ok(())
    }

    async fn restore_winner(
        &mut self,
        manager: &MutationManager,
        winner: &ReplayStamp,
        fetch: &mut Option<RecordFetch>,
    ) -> Result<(), SchemaError> {
        let (schema, key) = (&self.schema, &self.key);
        if let Some(saved) = manager.deferred_replay_record(schema, key).await? {
            match self.prior.take() {
                Some((loaded, mutation)) if loaded == saved => {
                    manager.apply_replay_record(mutation).await?;
                }
                _ => {
                    *fetch = Some(RecordFetch::Prior(saved));
                    return Err(needs_preparation());
                }
            }
        } else if !manager.apply_replay_baseline(schema, key, winner).await? {
            return Err(SchemaError::Blocked(
                "owned replay lacks the earlier accepted record".into(),
            ));
        }
        Ok(())
    }

    async fn guard_attachments(
        &self,
        manager: &MutationManager,
        mutation: &Mutation,
        overlaps_pending: bool,
        loses: bool,
    ) -> Result<(), SchemaError> {
        let erases = matches!(
            mutation.mutation_type,
            MutationType::Delete | MutationType::Purge
        );
        let (schema, key) = (&self.schema, &self.key);
        if (overlaps_pending || loses)
            && (self.explicit_attachment
                || (erases && manager.replay_record_has_attachment(schema, key).await?)
                || (overlaps_pending && manager.pending_replay_attachment(schema, key).await?))
        {
            return Err(SchemaError::Blocked(
                "concurrent attachment changes require reconciliation".into(),
            ));
        }
        Ok(())
    }
}

pub(super) enum RecordFetch {
    Incoming,
    Prior(Vec<u8>),
}

fn needs_preparation() -> SchemaError {
    SchemaError::InvalidData("owned replay requires materialization".into())
}
