//! Durable inbox and atomic peer replay for explicitly admitted streams.

use super::{invalid, MutationManager, ReplayCandidate};
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction;
use lastdb_model::portable_mutation::{
    MutationEntry, Position, SchemaIdentity, StreamId, StreamProgress,
};
use lastdb_model::portable_replay::{plan_replay, LogRangeId};
use schema_types::PortableSchemaDefinition;
use std::collections::{BTreeMap, BTreeSet};

pub struct ReplayBinding<'a> {
    pub local_schema: &'a str,
    pub definition: &'a PortableSchemaDefinition,
}

impl MutationManager {
    /// Reclaim one bounded batch after shared publish and recovery are qualified.
    /// Each participating stream must already have passed the erased timestamp.
    pub async fn reclaim_portable_deletes(&self) -> Result<u64, SchemaError> {
        transaction::request_scope(self.db_ops.native_runtime(), async {
            let metadata = self.db_ops.metadata();
            let Some(cutoff) = metadata.portable_delete_cutoff().await? else {
                return Ok(0);
            };
            let reclaimed = self
                .db_ops
                .records()
                .stage_delete_reclamation(cutoff)
                .await?;
            if let Some(through) = reclaimed.through_ms {
                metadata.stage_delete_reclaimed(through, cutoff).await?;
            }
            Ok(reclaimed.markers)
        })
        .await
    }

    /// Persist the service-authorized admission of a peer with a safe checkpoint.
    pub async fn admit_portable_peer(
        &self,
        stream: &StreamId,
        first_authored_at_ms: u64,
    ) -> Result<(), SchemaError> {
        transaction::request_scope(
            self.db_ops.native_runtime(),
            self.db_ops
                .metadata()
                .stage_portable_peer(stream, first_authored_at_ms),
        )
        .await
    }

    /// Verify and store a complete authenticated range even when an earlier one is missing.
    pub async fn receive_portable_range(
        &self,
        id: LogRangeId,
        bytes: Vec<u8>,
    ) -> Result<(), SchemaError> {
        transaction::request_scope(
            self.db_ops.native_runtime(),
            self.db_ops.metadata().stage_portable_range(id, bytes),
        )
        .await
    }

    /// Durable applied positions for a bounded set of remote streams.
    /// An unadmitted stream is absent. An admitted idle stream has position zero.
    pub async fn portable_peer_progress(
        &self,
        streams: &BTreeSet<StreamId>,
    ) -> Result<BTreeMap<StreamId, StreamProgress>, SchemaError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(self.db_ops.metadata().portable_peer_progress(streams))
            .await
    }

    /// Get candidates from the current gap-free inbox window before fetching dependencies.
    pub async fn portable_peer_candidates(
        &self,
        streams: &BTreeSet<StreamId>,
    ) -> Result<Vec<MutationEntry>, SchemaError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let window = self.db_ops.metadata().portable_inbox(streams).await?;
                let plan = plan_replay(&window.ranges, &window.progress, window.cutoff)
                    .map_err(|error| replay_error(&error))?;
                Ok(plan
                    .candidates()
                    .iter()
                    .map(|entry| (*entry).clone())
                    .collect())
            })
            .await
    }

    /// Apply the current window and retire fully applied ranges in the same commit.
    pub async fn apply_portable_inbox(
        &self,
        streams: &BTreeSet<StreamId>,
        definitions: &BTreeMap<String, ReplayBinding<'_>>,
        files: BTreeMap<Position, Vec<u8>>,
    ) -> Result<(), SchemaError> {
        transaction::request_scope(self.db_ops.native_runtime(), async {
            let metadata = self.db_ops.metadata();
            let window = metadata.portable_inbox(streams).await?;
            let plan = plan_replay(&window.ranges, &window.progress, window.cutoff)
                .map_err(|error| replay_error(&error))?;
            let bound = metadata.portable_bound_schemas().await?;
            let candidates = plan
                .candidates()
                .iter()
                .map(|entry| {
                    let SchemaIdentity::Shared(id) = &entry.schema else {
                        return Err(invalid("replay candidate is not shared"));
                    };
                    let binding = definitions
                        .get(id)
                        .ok_or_else(|| invalid("portable schema definition missing"))?;
                    if bound.get(id).map(String::as_str) != Some(binding.local_schema) {
                        return Err(invalid(
                            "peer replay schema is outside the bound shared database",
                        ));
                    }
                    Ok(ReplayCandidate {
                        entry,
                        definition: binding.definition,
                        local_schema: binding.local_schema,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.stage_portable_candidates(&candidates, files).await?;
            let progress = plan.progress().clone();
            metadata
                .stage_portable_inbox_progress(window, progress)
                .await
        })
        .await
    }
}

fn replay_error(error: &lastdb_model::portable_replay::ReplayError) -> SchemaError {
    SchemaError::InvalidData(error.to_string())
}
