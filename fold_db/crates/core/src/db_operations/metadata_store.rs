//! Typed metadata operations. The format owns its namespace handle and codec.

use crate::schema::types::MutationAuthorClockState;
use crate::schema::SchemaError;
use crate::storage::format::metadata::{
    AuthoredStamp, CheckpointDirectory, InboxWindow, JournalSchema, Metadata,
};
pub use crate::storage::format::metadata::{OutgoingPortablePage, PortablePeerProgress};
use cloud_backup_protocol::shared::peer::PeerHeads;
use cloud_backup_protocol::shared::{BatchReceipt, LogBatch, WriterStream};
use lastdb_model::portable_mutation::{SchemaIdentity, Stamp, StreamId, StreamProgress};
use lastdb_model::portable_replay::LogRangeId;
use std::collections::{BTreeMap, BTreeSet};

/// Domain access to author clocks; no raw store handle.
#[derive(Clone)]
pub struct MetadataStore {
    format: Metadata,
}

impl MetadataStore {
    pub(crate) async fn portable_checkpoint_directory(
        &self,
    ) -> Result<Option<CheckpointDirectory>, SchemaError> {
        Ok(self.format.portable_checkpoint_directory().await?)
    }

    pub(crate) async fn portable_delete_cutoff(
        &self,
        directory: &PeerHeads,
    ) -> Result<Option<u64>, SchemaError> {
        Ok(self.format.portable_delete_cutoff(directory).await?)
    }

    pub(crate) async fn stage_delete_reclaimed(
        &self,
        through_ms: u64,
        cutoff_ms: u64,
    ) -> Result<(), SchemaError> {
        Ok(self
            .format
            .stage_delete_reclaimed(through_ms, cutoff_ms)
            .await?)
    }

    pub(crate) async fn shared_schema_bound(&self, name: &str) -> Result<bool, SchemaError> {
        Ok(self.format.shared_schema_bound(name).await?)
    }

    pub(crate) async fn stage_portable_peer(
        &self,
        stream: &StreamId,
        first_authored_at_ms: u64,
    ) -> Result<(), SchemaError> {
        Ok(self
            .format
            .stage_portable_peer(stream, first_authored_at_ms)
            .await?)
    }

    pub(crate) async fn stage_portable_range(
        &self,
        id: LogRangeId,
        bytes: Vec<u8>,
    ) -> Result<(), SchemaError> {
        Ok(self.format.stage_portable_range(id, bytes).await?)
    }

    pub(crate) async fn portable_peer_progress(
        &self,
        streams: &BTreeSet<StreamId>,
    ) -> Result<PortablePeerProgress, SchemaError> {
        Ok(self.format.portable_peer_progress(streams).await?)
    }

    pub(crate) async fn portable_bound_schemas(
        &self,
    ) -> Result<BTreeMap<String, String>, SchemaError> {
        Ok(self.format.portable_bound_schemas().await?)
    }

    pub(crate) async fn portable_inbox(
        &self,
        streams: &BTreeSet<StreamId>,
    ) -> Result<InboxWindow, SchemaError> {
        Ok(self.format.portable_inbox(streams).await?)
    }

    pub(crate) async fn stage_portable_inbox_progress(
        &self,
        window: InboxWindow,
        progress: BTreeMap<StreamId, StreamProgress>,
    ) -> Result<(), SchemaError> {
        Ok(self
            .format
            .stage_portable_inbox_progress(window, progress)
            .await?)
    }

    pub(crate) fn new(format: Metadata) -> Self {
        Self { format }
    }

    /// Explicit durability fence. Normal staging joins the existing native
    /// request, whose commit and publication finish before its acknowledgement.
    pub(crate) async fn flush(&self) -> Result<(), SchemaError> {
        self.format.flush().await?;
        Ok(())
    }

    pub(crate) async fn author_clock(
        &self,
        writer: &str,
    ) -> Result<Option<MutationAuthorClockState>, SchemaError> {
        self.format.author_clock(writer).await
    }

    /// Stage a typed state inside the caller's native request. The format
    /// chooses its key and bytes; this method does not commit independently.
    pub(crate) async fn stage_author_clock(
        &self,
        writer: &str,
        state: &MutationAuthorClockState,
    ) -> Result<(), SchemaError> {
        self.format.stage_author_clock(writer, state).await
    }

    pub(crate) async fn stage_local_stamps(
        &self,
        schemas: &[(&str, Option<Stamp>)],
    ) -> Result<Vec<AuthoredStamp>, SchemaError> {
        Ok(self.format.stage_local_stamps(schemas).await?)
    }

    pub(crate) async fn bind_portable_journal(
        &self,
        stream: WriterStream,
        schemas: BTreeMap<String, JournalSchema>,
    ) -> Result<(), SchemaError> {
        Ok(self.format.bind_portable_journal(stream, schemas).await?)
    }

    pub(crate) async fn portable_outgoing(&self) -> Result<OutgoingPortablePage, SchemaError> {
        Ok(self.format.portable_outgoing().await?)
    }

    pub(crate) async fn portable_writer_progress(
        &self,
    ) -> Result<(WriterStream, StreamProgress), SchemaError> {
        Ok(self.format.portable_writer_progress().await?)
    }

    pub(crate) async fn outgoing_schema_name(
        &self,
        id: &SchemaIdentity,
    ) -> Result<String, SchemaError> {
        Ok(self.format.outgoing_schema_name(id).await?)
    }

    pub(crate) async fn stage_outgoing_receipt(
        &self,
        batch: &LogBatch,
        receipt: &BatchReceipt,
    ) -> Result<(), SchemaError> {
        Ok(self.format.stage_outgoing_receipt(batch, receipt).await?)
    }

    pub(crate) async fn stage_portable_restore(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
        fresh_stream: &StreamId,
    ) -> Result<(), SchemaError> {
        Ok(self
            .format
            .stage_portable_restore(progress, cutoff, fresh_stream)
            .await?)
    }

    pub(crate) async fn validate_portable_restore_checkpoint(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
    ) -> Result<(), SchemaError> {
        Ok(self
            .format
            .validate_portable_restore_checkpoint(progress, cutoff)
            .await?)
    }
}
