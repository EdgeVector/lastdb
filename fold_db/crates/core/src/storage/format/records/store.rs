//! Bound main/file capabilities; the record provider never gets raw stores.

use super::{EncodedRecords, RecordChange};
use crate::schema::types::{FileWriteIssuer, Schema};
use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::format::metadata::Metadata;
use crate::storage::format::read::ReadStore;
use crate::storage::{KvMutation, KvStore, NamespacedStore, TypedKvStore};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::record_value::StoredRecord;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) struct RecordFormat {
    pub(super) main_store: Arc<TypedKvStore<dyn KvStore>>,
    file_owner: Arc<dyn NamespacedStore>,
    pub(super) runtime: Arc<PackRuntime>,
    pub(super) content_key: Option<[u8; 32]>,
    pub(super) file_issuer: FileWriteIssuer,
    pub(super) metadata: Metadata,
}

impl RecordFormat {
    pub(in crate::storage::format) fn new(
        main: Arc<dyn KvStore>,
        file_owner: Arc<dyn NamespacedStore>,
        runtime: Arc<PackRuntime>,
        content_key: Option<[u8; 32]>,
        metadata: Metadata,
    ) -> Self {
        Self {
            main_store: Arc::new(TypedKvStore::new(main)),
            file_owner,
            runtime,
            content_key,
            file_issuer: FileWriteIssuer::default(),
            metadata,
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
        resolve_codec: impl FnMut(&str) -> Result<FieldKeyCodec, SchemaError>,
    ) -> Result<(), SchemaError> {
        self.require_scope()?;
        if schema.file_required
            && changes
                .iter()
                .any(|change| change.after.is_live() && change.after.file.is_none())
        {
            return Err(SchemaError::InvalidData(
                "this record requires a file".into(),
            ));
        }
        for change in &changes {
            self.runtime.require_shared_record_route(
                change.key.as_str(),
                !matches!(change.portable, super::PortableEffect::Local),
            )?;
        }
        let entries = super::outgoing::entries(schema, &changes)?;
        let alternates = super::alternates::encode(schema, &changes, resolve_codec)?;
        let mut encoded: EncodedRecords = super::encode(changes, alternates)?;
        // Resolve the prior file version from the actual record preimage.
        // A provider cannot suppress cleanup or authorize a new reference.
        let (files, local_refs) = self
            .encode_files(std::mem::take(&mut encoded.files))
            .await?;
        let written = encoded
            .stage(self.main_store.inner().as_ref(), &self.runtime)
            .await?;
        self.stage_files(files).await?;
        self.metadata.stage_outgoing(entries).await?;
        self.runtime.stage_written_records(written);
        self.schedule_local_file_refs(local_refs)?;
        Ok(())
    }

    /// Record local paths only after this request commits. A failed request
    /// leaves the catalog unchanged. A catalog error does not fail the node:
    /// the put route writes the same entry again after commit.
    fn schedule_local_file_refs(
        &self,
        plan: crate::storage::local_file_ref::SidecarPlan,
    ) -> Result<(), SchemaError> {
        if plan.is_empty() {
            return Ok(());
        }
        let runtime = Arc::clone(&self.runtime);
        crate::storage::disk_pack::transaction::after_commit(&self.runtime, async move {
            let result = crate::storage::disk_pack::run_blocking(move || {
                crate::storage::local_file_ref::apply(&runtime, plan)
            })
            .await;
            if let Err(error) = result {
                tracing::error!(
                    %error,
                    "local file reference catalog update failed after commit"
                );
            }
            Ok(())
        })
        .map_err(SchemaError::from)
    }

    async fn encode_files(
        &self,
        changes: Vec<super::files::FileChange>,
    ) -> Result<(Vec<KvMutation>, crate::storage::local_file_ref::SidecarPlan), SchemaError> {
        if changes.is_empty() {
            return Ok((
                Vec::new(),
                crate::storage::local_file_ref::SidecarPlan::default(),
            ));
        }
        let keys: Vec<_> = changes
            .iter()
            .map(|change| change.key.as_str().as_bytes().to_vec())
            .collect();
        let expected = keys.len();
        let stored = self.main_store.inner().get_many(keys).await?;
        if stored.len() != expected {
            return Err(SchemaError::InvalidData(
                "incomplete file record read".into(),
            ));
        }
        let mut rows = Vec::with_capacity(changes.len());
        let mut references = BTreeMap::new();
        let mut local_refs = crate::storage::local_file_ref::SidecarPlan::default();
        for (change, bytes) in changes.into_iter().zip(stored) {
            let before = bytes
                .map(|bytes| StoredRecord::decode(&bytes))
                .transpose()
                .map_err(super::record_value_error)?;
            change.references(before.as_ref(), &mut references)?;
            let (encoded, plan) = super::files::encode(
                change,
                before.as_ref(),
                &self.file_issuer,
                self.content_key.as_ref(),
            )?;
            rows.extend(encoded);
            local_refs.forget.extend(plan.forget);
            local_refs.remember.extend(plan.remember);
        }
        let retired = self.metadata.stage_file_references(references).await?;
        rows.extend(retired.into_iter().map(KvMutation::delete));
        Ok((rows, local_refs))
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
        let store = self.file_store().await?;
        let mut puts = Vec::new();
        let mut deletes = Vec::new();
        for row in rows {
            match row {
                KvMutation::Put { .. } => puts.push(row),
                KvMutation::Delete { key } => deletes.push(key),
            }
        }
        if !puts.is_empty() {
            store.batch_mutate(puts).await?;
        }
        if !deletes.is_empty() {
            self.runtime.stage_record_file_deletes(deletes)?;
        }
        Ok(())
    }
}
