//! Bound main/file capabilities and record-owned file version encoding.

use super::{EncodedRecords, RecordAddress, RecordChange};
use crate::schema::types::{FileWrite, FileWriteIssuer, Schema};
use crate::schema::SchemaError;
use crate::storage::current_keys::families::{record_file_path, record_file_uploaded_path};
use crate::storage::disk_pack::{transaction, PackKvStore, PackNamespacedStore, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::format::metadata::Metadata;
use crate::storage::format::read::ReadStore;
use crate::storage::local_file_ref::{Remember, SidecarPlan};
use crate::storage::KvMutation;
use lastdb_model::record_value::StoredRecord;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(in crate::storage::format::records) struct FileChange {
    pub(in crate::storage::format::records) key: RecordAddress,
    pub(in crate::storage::format::records) after: Arc<StoredRecord>,
    pub(in crate::storage::format::records) write: Option<FileWrite>,
}

fn encode_file_change(
    change: FileChange,
    before: Option<&StoredRecord>,
    issuer: &FileWriteIssuer,
    content_key: &[u8; 32],
    references: &mut BTreeMap<String, bool>,
) -> Result<(Vec<KvMutation>, SidecarPlan), SchemaError> {
    let FileChange { key, after, write } = change;
    let previous = before.and_then(|record| record.file_version.as_ref());
    let next = after.file_version.as_ref();
    if previous == next {
        if before.and_then(|record| record.file.as_ref()) != after.file.as_ref() {
            return Err(SchemaError::InvalidData(
                "a retained file version has a different marker".into(),
            ));
        }
        return Ok((Vec::new(), SidecarPlan::default()));
    }
    let mut rows = Vec::new();
    let mut plan = SidecarPlan::default();
    if let Some(version) = previous {
        let path = record_file_path(key.as_str(), version)?;
        references.insert(path.clone(), false);
        plan.forget.push(path);
    }
    if let Some(version) = next {
        let path = record_file_path(key.as_str(), version)?;
        references.insert(path.clone(), true);
        let marker = after
            .file
            .as_ref()
            .ok_or_else(|| {
                SchemaError::InvalidData("a file row requires its record marker".into())
            })?
            .clone();
        match write {
            Some(FileWrite::Put(file)) => {
                if file.marker() != &marker {
                    return Err(SchemaError::InvalidData(
                        "file payload differs from its record marker".into(),
                    ));
                }
                let row = issuer.admit(file).ok_or_else(|| {
                    SchemaError::InvalidData("file payload belongs to another format writer".into())
                })?;
                super::file_crypto::require_row(content_key, &marker, &row)?;
                rows.push(KvMutation::put(path.into_bytes(), row));
            }
            Some(FileWrite::LocalPath(local)) => {
                if local.marker() != &marker {
                    return Err(SchemaError::InvalidData(
                        "local file reference differs from its record marker".into(),
                    ));
                }
                let local_path = issuer.admit_local(&local).ok_or_else(|| {
                    SchemaError::InvalidData(
                        "local file reference belongs to another format writer".into(),
                    )
                })?;
                plan.remember.push(Remember {
                    cas_key: path,
                    path: local_path.to_path_buf(),
                    sha256: marker.sha256.clone(),
                    size_bytes: marker.size_bytes,
                });
            }
            Some(FileWrite::Clear) | None => {
                return Err(SchemaError::InvalidData(
                    "a selected file requires its sealed payload or a local reference".into(),
                ));
            }
        }
    }
    Ok((rows, plan))
}

pub(crate) struct RecordFormat {
    pub(in crate::storage::format::records) main_store: PackKvStore,
    file_owner: PackNamespacedStore,
    pub(in crate::storage::format::records) runtime: Arc<PackRuntime>,
    pub(in crate::storage::format::records) content_key: [u8; 32],
    pub(in crate::storage::format::records) file_issuer: FileWriteIssuer,
    pub(in crate::storage::format::records) metadata: Metadata,
}

impl RecordFormat {
    pub(in crate::storage::format) fn new(
        main_store: PackKvStore,
        file_owner: PackNamespacedStore,
        runtime: Arc<PackRuntime>,
        content_key: [u8; 32],
        metadata: Metadata,
    ) -> Self {
        Self {
            main_store,
            file_owner,
            runtime,
            content_key,
            file_issuer: FileWriteIssuer::default(),
            metadata,
        }
    }

    pub(crate) fn reader(&self) -> ReadStore {
        ReadStore::new(self.main_store.clone())
    }

    pub(crate) fn runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.main_store.flush().await
    }

    pub(crate) async fn stage_records(
        &self,
        schema: &Schema,
        changes: Vec<RecordChange>,
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
        let alternates = super::encode_alternates(schema, &changes, &self.content_key)?;
        let mut encoded: EncodedRecords = super::encode(changes, alternates)?;
        let (files, local_refs) = self
            .encode_files(std::mem::take(&mut encoded.files))
            .await?;
        let written = encoded.stage(&self.main_store, &self.runtime).await?;
        self.stage_files(files).await?;
        self.runtime.stage_written_records(written);
        if !local_refs.is_empty() {
            let runtime = Arc::clone(&self.runtime);
            transaction::after_commit(&self.runtime, async move {
                if let Err(error) = crate::storage::disk_pack::run_blocking(move || {
                    crate::storage::local_file_ref::apply(&runtime, local_refs)
                })
                .await
                {
                    tracing::error!(
                        %error,
                        "local file reference catalog update failed after commit"
                    );
                }
                Ok(())
            })
            .map_err(SchemaError::from)?;
        }
        Ok(())
    }

    async fn encode_files(
        &self,
        changes: Vec<FileChange>,
    ) -> Result<(Vec<KvMutation>, SidecarPlan), SchemaError> {
        if changes.is_empty() {
            return Ok((Vec::new(), SidecarPlan::default()));
        }
        let keys: Vec<_> = changes
            .iter()
            .map(|change| change.key.as_str().as_bytes().to_vec())
            .collect();
        let expected = keys.len();
        let stored = self.main_store.get_many(keys).await?;
        if stored.len() != expected {
            return Err(SchemaError::InvalidData(
                "incomplete file record read".into(),
            ));
        }
        let mut rows = Vec::with_capacity(changes.len());
        let mut references = BTreeMap::new();
        let mut local_refs = SidecarPlan::default();
        for (change, bytes) in changes.into_iter().zip(stored) {
            let before = bytes
                .map(|bytes| StoredRecord::decode(&bytes))
                .transpose()
                .map_err(super::record_value_error)?;
            let (encoded, plan) = encode_file_change(
                change,
                before.as_ref(),
                &self.file_issuer,
                &self.content_key,
                &mut references,
            )?;
            rows.extend(encoded);
            local_refs.forget.extend(plan.forget);
            local_refs.remember.extend(plan.remember);
        }
        let retired = self.metadata.stage_file_references(references).await?;
        rows.extend(retired.into_iter().map(KvMutation::delete));
        Ok((rows, local_refs))
    }

    pub(in crate::storage::format::records) fn require_scope(&self) -> Result<(), SchemaError> {
        if !transaction::publication::active(&self.runtime) {
            return Err(SchemaError::InvalidData(
                "record format writes require a complete native write scope".into(),
            ));
        }
        Ok(())
    }

    pub(in crate::storage::format::records) async fn file_store(
        &self,
    ) -> StorageResult<PackKvStore> {
        self.file_owner
            .open_namespace(super::super::CAS_COLLECTION)
            .await
    }

    pub(in crate::storage::format::records) async fn stage_files(
        &self,
        rows: Vec<KvMutation>,
    ) -> Result<(), SchemaError> {
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
            let markers = deletes
                .iter()
                .map(|key| record_file_uploaded_path(key.as_slice()))
                .collect::<Result<Vec<_>, _>>()?;
            let present = self.runtime.markers_present(markers.clone()).await?;
            deletes.extend(
                markers
                    .into_iter()
                    .zip(present)
                    .filter_map(|(key, present)| present.then_some(key)),
            );
            self.runtime.stage_record_file_deletes(deletes)?;
        }
        Ok(())
    }
}
