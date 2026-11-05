//! Typed metadata operations. The format owns its namespace handle and codec.

use crate::schema::types::MutationAuthorClockState;
use crate::schema::SchemaError;
pub use crate::storage::format::metadata::OutgoingPortablePage;
use crate::storage::format::metadata::{AuthoredStamp, JournalSchema, Metadata};
use cloud_backup_protocol::shared::{BatchReceipt, LogBatch, WriterStream};
use lastdb_model::portable_mutation::{SchemaIdentity, Stamp, StreamId, StreamProgress};
use std::collections::BTreeMap;

/// Domain access to author clocks; no raw store handle.
#[derive(Clone)]
pub struct MetadataStore {
    format: Metadata,
}

impl MetadataStore {
    pub(crate) async fn shared_schema_bound(&self, name: &str) -> Result<bool, SchemaError> {
        Ok(self.format.shared_schema_bound(name).await?)
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
