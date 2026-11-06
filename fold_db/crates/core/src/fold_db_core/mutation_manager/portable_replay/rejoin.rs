//! Preserve device-private state while rebuilding a returning writer's shared roots.

use super::super::portable_journal::schema_fingerprint;
use super::MutationManager;
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::JournalSchema;
use cloud_backup_protocol::shared::{LogHead, WriterStream};
use cloud_backup_protocol::Id;
use lastdb_model::portable_mutation::{StreamId, StreamProgress};
use std::collections::BTreeMap;
use std::sync::Arc;

impl MutationManager {
    /// In the private stage, discard file references derived from the old shared roots.
    /// Run this before replay so new file references survive and retention rebuilds the rest.
    pub async fn reset_portable_rejoin_file_references(&self) -> Result<(), SchemaError> {
        let runtime = self.db_ops.native_runtime();
        runtime.require_staged_shared_rejoin()?;
        let metadata = self.db_ops.metadata();
        let mut after = None;
        loop {
            after = transaction::request_scope(
                Arc::clone(&runtime),
                metadata.stage_reset_rejoin_file_references(after.as_deref()),
            )
            .await?;
            if after.is_none() {
                return Ok(());
            }
        }
    }

    /// Seal complete replay under the original writer at its exact authenticated cloud head.
    /// The caller must keep the original writer stopped and the new home private until cutover.
    pub async fn finish_portable_rejoin(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
        writer: &WriterStream,
        device: &Id,
        expected: Option<&LogHead>,
    ) -> Result<(), SchemaError> {
        let runtime = self.db_ops.native_runtime();
        runtime.require_staged_shared_rejoin()?;
        let metadata = self.db_ops.metadata();
        let mut after = None;
        loop {
            after = transaction::request_scope(
                Arc::clone(&runtime),
                metadata.stage_reset_rejoin_inbox(after.as_deref()),
            )
            .await?;
            if after.is_none() {
                break;
            }
        }
        transaction::request_scope(Arc::clone(&runtime), async {
            let bound = metadata.portable_bound_schemas().await?;
            runtime.require_shared_schema_set(bound.values().map(String::as_str))?;
            let schemas =
                futures::future::try_join_all(bound.into_iter().map(|(id, alias)| async move {
                    let schema = self.load_schema_for_delta(&alias).await?;
                    let local_sha256 = schema_fingerprint(&schema)?;
                    Ok::<_, SchemaError>((alias, JournalSchema { id, local_sha256 }))
                }))
                .await?
                .into_iter()
                .collect();
            metadata
                .stage_portable_rejoin(progress, cutoff, writer, device, expected, &schemas)
                .await
        })
        .await
    }
}
