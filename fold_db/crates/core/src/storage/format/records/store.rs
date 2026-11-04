//! Bound main/file capabilities; the record provider never gets raw stores.

use super::{AlternateChange, EncodedRecords, RecordChange};
use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::format::history::{self, EncodedHistory, HistoryChange};
use crate::storage::format::read::ReadStore;
use crate::storage::{KvMutation, KvStore, NamespacedStore, TypedKvStore};
use std::sync::Arc;

pub(crate) struct RecordFormat {
    pub(super) main_store: Arc<TypedKvStore<dyn KvStore>>,
    file_owner: Arc<dyn NamespacedStore>,
    pub(super) runtime: Arc<PackRuntime>,
    pub(super) content_key: Option<[u8; 32]>,
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
        changes: Vec<RecordChange>,
        alternates: Vec<AlternateChange>,
        history: Vec<HistoryChange>,
    ) -> Result<(), SchemaError> {
        let encoded: EncodedRecords =
            super::encode(changes, alternates, self.content_key.as_ref())?;
        let effects = encoded
            .stage(
                self.main_store.inner().as_ref(),
                &self.runtime,
                history::encode(history)?,
            )
            .await?;
        self.stage_files(effects.files).await?;
        self.runtime.stage_written_records(effects.written);
        Ok(())
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
