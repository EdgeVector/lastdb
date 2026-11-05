//! Bound main/file capabilities; the record provider never gets raw stores.

use super::{EncodedRecords, RecordChange};
use crate::schema::types::{FileWriteIssuer, Schema};
use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::format::history::{self, EncodedHistory, HistoryChange};
use crate::storage::format::read::ReadStore;
use crate::storage::{KvMutation, KvStore, NamespacedStore, TypedKvStore};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::record_value::StoredRecord;
use std::sync::Arc;

pub(crate) struct RecordFormat {
    pub(super) main_store: Arc<TypedKvStore<dyn KvStore>>,
    file_owner: Arc<dyn NamespacedStore>,
    pub(super) runtime: Arc<PackRuntime>,
    pub(super) content_key: Option<[u8; 32]>,
    pub(super) file_issuer: FileWriteIssuer,
}

impl RecordFormat {
    pub(in crate::storage::format) fn new(
        main: Arc<dyn KvStore>,
        file_owner: Arc<dyn NamespacedStore>,
        runtime: Arc<PackRuntime>,
        content_key: Option<[u8; 32]>,
    ) -> Self {
        Self {
            main_store: Arc::new(TypedKvStore::new(main)),
            file_owner,
            runtime,
            content_key,
            file_issuer: FileWriteIssuer::default(),
        }
    }

    pub(crate) fn reader(&self) -> ReadStore {
        ReadStore::new(Arc::clone(&self.main_store))
    }

    pub(crate) fn runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.main_store.inner().flush().await
    }

    pub(crate) async fn stage_records(
        &self,
        schema: &Schema,
        changes: Vec<RecordChange>,
        history: Vec<HistoryChange>,
        resolve_codec: impl FnMut(&str) -> Result<FieldKeyCodec, SchemaError>,
    ) -> Result<(), SchemaError> {
        self.require_scope()?;
        let alternates = super::alternates::encode(schema, &changes, resolve_codec)?;
        let mut encoded: EncodedRecords = super::encode(changes, alternates)?;
        // File-free results always delete their owned path idempotently.
        // Retained references need the actual record preimage in one batch;
        // a provider cannot suppress cleanup or authorize a new reference.
        let files = self
            .encode_files(std::mem::take(&mut encoded.files))
            .await?;
        let written = encoded
            .stage(
                self.main_store.inner().as_ref(),
                &self.runtime,
                history::encode(history)?,
            )
            .await?;
        self.stage_files(files).await?;
        self.runtime.stage_written_records(written);
        Ok(())
    }

    async fn encode_files(
        &self,
        changes: Vec<super::files::FileChange>,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        if changes.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<_> = changes
            .iter()
            .filter(|change| change.after.file.is_some())
            .map(|change| change.key.as_str().as_bytes().to_vec())
            .collect();
        let expected = keys.len();
        let stored = self.main_store.inner().get_many(keys).await?;
        if stored.len() != expected {
            return Err(SchemaError::InvalidData(
                "incomplete file record read".into(),
            ));
        }
        let mut stored = stored.into_iter();
        let mut rows = Vec::with_capacity(changes.len());
        for change in changes {
            let bytes = if change.after.file.is_some() {
                stored
                    .next()
                    .ok_or_else(|| SchemaError::InvalidData("incomplete file record read".into()))?
            } else {
                None
            };
            let before = bytes
                .map(|bytes| StoredRecord::decode(&bytes))
                .transpose()
                .map_err(super::record_value_error)?;
            rows.extend(super::files::encode(
                change,
                before.as_ref(),
                &self.file_issuer,
                self.content_key.as_ref(),
            )?);
        }
        Ok(rows)
    }

    pub(crate) async fn stage_history(&self, encoded: EncodedHistory) -> Result<(), SchemaError> {
        encoded
            .stage(self.main_store.inner().as_ref(), &self.runtime)
            .await
    }

    pub(crate) async fn retire_history_bucket(
        &self,
        key: String,
        token: Vec<u8>,
    ) -> Result<bool, SchemaError> {
        history::retire_bucket(self.main_store.inner().as_ref(), &self.runtime, key, token).await
    }

    pub(super) fn require_scope(&self) -> Result<(), SchemaError> {
        if !transaction::publication::active(&self.runtime) {
            return Err(SchemaError::InvalidData(
                "record format writes require a complete native write scope".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn file_store(&self) -> StorageResult<Arc<dyn KvStore>> {
        self.file_owner
            .open_namespace(super::super::CAS_COLLECTION)
            .await
    }

    // Only child encoders in this format can supply these raw file rows.
    pub(super) async fn stage_files(&self, rows: Vec<KvMutation>) -> Result<(), SchemaError> {
        if rows.is_empty() {
            return Ok(());
        }
        self.require_scope()?;
        self.file_store().await?.batch_mutate(rows).await?;
        Ok(())
    }
}
