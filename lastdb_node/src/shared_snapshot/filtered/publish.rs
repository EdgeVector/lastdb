//! Bind the native checkpoint to exact accepted ends and publish as its owner.

use super::{build, FilteredCheckpoint, FilteredCheckpointRequest};
use crate::byte_cloud_restore::{RecoveryKeys, RestoreError, Result};
use crate::shared_snapshot::{publish, SignedSnapshotTransport, SnapshotOutcome};
use cloud_backup_protocol::shared::objects::{SnapshotLogEndsRequest, SnapshotLogPosition};
use cloud_backup_protocol::{DecimalU64, Id};
use fold_db::fold_db_core::MutationManager;
use schema_types::PortableSchemaDefinition;
use std::collections::BTreeMap;
use std::path::Path;

pub struct NativePublishRequest<'a> {
    pub workspace: &'a Path,
    pub snapshot_id: Id,
    pub expires_at_ms: u64,
    pub definitions: &'a BTreeMap<String, PortableSchemaDefinition>,
}

/// Capture the live owner only after its outgoing prefix is accepted. Writes
/// acknowledged after the capture continue to advance the ordinary journal.
pub async fn publish_native(
    writer: &MutationManager,
    request: NativePublishRequest<'_>,
    transport: &SignedSnapshotTransport,
    keys: &RecoveryKeys,
) -> Result<SnapshotOutcome> {
    let mut export = prepare_native(writer, request, transport, keys).await?;
    publish(&mut export, transport, keys.content_key()).await
}

/// Save the exact export so another invocation can resume while the source
/// returns to normal service.
pub async fn prepare_native(
    writer: &MutationManager,
    request: NativePublishRequest<'_>,
    transport: &SignedSnapshotTransport,
    keys: &RecoveryKeys,
) -> Result<FilteredCheckpoint> {
    build::require_time(request.expires_at_ms)?;
    let source = writer
        .capture_portable_checkpoint(request.definitions)
        .await
        .map_err(|_| RestoreError::Invalid("capture shared database root"))?;
    let database_id = source.writer().database_id.clone();
    let positions = source
        .progress()
        .iter()
        .filter(|(_, cursor)| cursor.applied_through != 0)
        .map(|(stream, cursor)| {
            Ok(SnapshotLogPosition {
                source_stream: Id::try_from(stream.as_str().to_owned())?,
                sequence: DecimalU64::new(cursor.applied_through),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let lookup = SnapshotLogEndsRequest {
        database_id: database_id.clone(),
        positions,
    };
    let (prior, covers) = tokio::try_join!(
        transport.current_snapshot(&database_id),
        transport.accepted_log_ends(&lookup),
    )?;
    let accepted = covers
        .into_iter()
        .map(|cover| {
            let stream = cover
                .source_stream
                .as_str()
                .parse()
                .map_err(|_| RestoreError::Invalid("accepted source stream"))?;
            Ok((stream, cover.through))
        })
        .collect::<Result<_>>()?;
    FilteredCheckpoint::prepare(
        request.workspace,
        source,
        FilteredCheckpointRequest {
            snapshot_id: request.snapshot_id,
            replaces: prior.map(|receipt| receipt.snapshot_id),
            expires_at_ms: request.expires_at_ms,
            accepted,
        },
        keys,
    )
    .await
}

pub async fn resume_native(
    workspace: &Path,
    transport: &SignedSnapshotTransport,
    keys: &RecoveryKeys,
) -> Result<SnapshotOutcome> {
    let mut export = FilteredCheckpoint::resume(workspace, keys).await?;
    publish(&mut export, transport, keys.content_key()).await
}
