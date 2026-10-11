//! Materialize local org aliases before encoding the unchanged wire identity.

use super::*;

impl SyncEngine {
    pub(super) async fn generation_upload_entries(
        &self,
        target: &SyncTarget,
        entries: &[LogEntry],
    ) -> SyncResult<Option<Vec<LogEntry>>> {
        let scoped_entries = if target.prefix.is_empty() {
            None
        } else {
            let outcomes =
                futures::stream::iter(entries.iter().cloned().map(|mut entry| async move {
                    crate::sync::cloud_purge_peer::scope_alias::rebind_entry(
                        self.store.as_ref(),
                        &target.prefix,
                        &mut entry,
                    )
                    .await?;
                    if let crate::sync::log::LogOp::MutationIntent { mutations } = &mut entry.op {
                        *mutations = self
                            .materialize_mutation_intent(std::mem::take(mutations))
                            .await
                            .map_err(SyncError::Storage)?;
                    }
                    crate::sync::cloud_purge_peer::scope_alias::wire_entry(
                        self.store.as_ref(),
                        &target.prefix,
                        &mut entry,
                    )
                    .await?;
                    SyncResult::Ok(entry)
                }))
                .buffered(8)
                .collect::<Vec<_>>()
                .await;
            Some(outcomes.into_iter().collect::<SyncResult<Vec<_>>>()?)
        };
        Ok(scoped_entries)
    }
}
