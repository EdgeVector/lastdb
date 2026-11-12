//! Bound main/file capabilities, record-owned file versions, and schema release.

use super::{EncodedFile, RecordChange, RecordDeleteLedgerEntry, ReleaseFamily, ReleaseKind};
use crate::schema::types::{FileWrite, FileWriteIssuer, LocalFileReference, Schema};
use crate::schema::SchemaError;
use crate::storage::current_keys::families::{record_file_path, record_file_uploaded_path};
use crate::storage::disk_pack::{run_blocking, transaction, PackKvStore, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::format::{
    metadata::Metadata, read::ReadStore, DELETE_AUDIT_FAMILY, MAIN_NAMESPACE, SCHEMA_RELEASE_ROWS,
};
use crate::storage::local_file_ref::{self, SidecarPlan};
use crate::storage::{KvMutation, StorageError};
use lastdb_model::kind_partition::anchored;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::StoredRecord;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

fn encode_file_change(
    (key, after, write): EncodedFile,
    before: Option<&StoredRecord>,
    issuer: &FileWriteIssuer,
    content_key: &[u8; 32],
    rows: &mut Vec<KvMutation>,
    refs: &mut BTreeMap<String, bool>,
    plan: &mut SidecarPlan,
) -> Result<(), SchemaError> {
    let invalid = |m: &str| SchemaError::InvalidData(m.into());
    let (prev, next) = (
        before.and_then(|r| r.file_version.as_ref()),
        after.file_version.as_ref(),
    );
    if prev == next {
        return (before.and_then(|r| r.file.as_ref()) == after.file.as_ref())
            .then_some(())
            .ok_or_else(|| invalid("a retained file version has a different marker"));
    }
    if let Some(version) = prev {
        let path = record_file_path(key.as_str(), version)?;
        refs.insert(path.clone(), false);
        plan.forget.push(path);
    }
    let Some(version) = next else { return Ok(()) };
    let path = record_file_path(key.as_str(), version)?;
    refs.insert(path.clone(), true);
    let marker = after
        .file
        .as_ref()
        .ok_or_else(|| invalid("a file row requires its record marker"))?;
    let invalid = |m: &str| SchemaError::InvalidData(m.into());
    match write {
        Some(FileWrite::Put(f)) if f.marker() == marker => {
            let row = issuer
                .admit(f)
                .ok_or_else(|| invalid("file payload belongs to another format writer"))?;
            super::file_crypto::require_row(content_key, marker, &row)?;
            rows.push(KvMutation::put(path.into_bytes(), row));
            Ok(())
        }
        Some(FileWrite::Put(_)) => Err(invalid("file payload differs from its record marker")),
        Some(FileWrite::LocalPath(l)) if l.marker() == marker => {
            let p = issuer
                .admit_local(&l)
                .ok_or_else(|| invalid("local file reference belongs to another format writer"))?;
            plan.remember.push((
                path,
                p.to_path_buf(),
                marker.sha256.clone(),
                marker.size_bytes,
            ));
            Ok(())
        }
        Some(FileWrite::LocalPath(_)) => Err(invalid(
            "local file reference differs from its record marker",
        )),
        Some(FileWrite::Clear) | None => Err(invalid(
            "a selected file requires its sealed payload or a local reference",
        )),
    }
}

pub(crate) struct RecordFormat {
    pub(super) main_store: PackKvStore,
    pub(super) runtime: Arc<PackRuntime>,
    pub(super) content_key: [u8; 32],
    pub(super) file_issuer: FileWriteIssuer,
    pub(super) metadata: Metadata,
}

impl RecordFormat {
    pub(in crate::storage::format) fn new(
        main_store: PackKvStore,
        runtime: Arc<PackRuntime>,
        content_key: [u8; 32],
        metadata: Metadata,
    ) -> Self {
        Self {
            main_store,
            runtime,
            content_key,
            file_issuer: FileWriteIssuer::default(),
            metadata,
        }
    }

    pub(crate) fn reader(&self) -> ReadStore {
        self.main_store.clone()
    }
    pub(crate) fn runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }
    pub(crate) fn content_key(&self) -> &[u8; 32] {
        &self.content_key
    }
    pub(crate) fn admit_local<'a>(&self, file: &'a LocalFileReference) -> Option<&'a Path> {
        self.file_issuer.admit_local(file)
    }
    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.main_store.flush().await
    }

    pub(crate) async fn record_delete_audit(
        &self,
        entry: RecordDeleteLedgerEntry,
    ) -> Result<(), SchemaError> {
        self.require_scope()?;
        let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0).max(0);
        let key = anchored(
            DELETE_AUDIT_FAMILY,
            &format!("{ts:020}:{}", uuid::Uuid::new_v4()),
        );
        let bytes = serde_json::to_vec(&entry)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        self.main_store
            .put(key.as_bytes(), bytes)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("stage erase audit: {e}")))
    }

    pub(crate) async fn release_schema_batch(
        &self,
        keys: Vec<Vec<u8>>,
        family: &ReleaseFamily,
    ) -> Result<u64, SchemaError> {
        self.require_scope()?;
        if keys.len() > SCHEMA_RELEASE_ROWS {
            return Err(SchemaError::InvalidData(
                "schema release exceeds its row bound".into(),
            ));
        }
        for key in &keys {
            if !key.starts_with(family.prefix.as_bytes()) {
                return Err(SchemaError::InvalidData(
                    "schema release key has a different owner or family".into(),
                ));
            }
            crate::storage::current_keys::validate(MAIN_NAMESPACE, key)?;
        }
        let stored = self.main_store.get_many(keys.clone()).await?;
        if stored.len() != keys.len() {
            return Err(SchemaError::InvalidData(
                "schema release read batch is incomplete".into(),
            ));
        }
        let mut deletes = Vec::new();
        let mut refs = BTreeMap::new();
        for (k, v) in keys.into_iter().zip(stored) {
            let Some(v) = v else { continue };
            if matches!(family.kind, ReleaseKind::Records) {
                let key = std::str::from_utf8(&k).map_err(|e| {
                    SchemaError::InvalidData(format!("a dropped row key is not UTF-8: {e}"))
                })?;
                if key.starts_with(RECORD_PREFIX) {
                    let rec = StoredRecord::decode(&v).map_err(|e| {
                        SchemaError::InvalidData(format!(
                            "dropped record row {key} is unreadable: {e}"
                        ))
                    })?;
                    if let Some(ver) = rec.file_version {
                        refs.insert(record_file_path(key, &ver)?, false);
                    }
                }
            }
            deletes.push(KvMutation::delete(k));
        }
        if deletes.is_empty() {
            return Ok(0);
        }
        let count = deletes.len() as u64;
        self.main_store.batch_mutate(deletes).await?;
        let retired = self.metadata.stage_file_references(refs).await?;
        self.stage_files(Vec::new(), retired).await?;
        Ok(count)
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
                .any(|c| c.after.is_live() && c.after.file.is_none())
        {
            return Err(SchemaError::InvalidData(
                "this record requires a file".into(),
            ));
        }
        let alternates = super::encode_alternates(schema, &changes, &self.content_key)?;
        let (main, files, written) = super::encode(changes, alternates)?;
        let (puts, deletes, local_refs) = self.encode_files(files).await?;
        self.main_store.batch_mutate(main).await?;
        self.stage_files(puts, deletes).await?;
        self.runtime.stage_written_records(written);
        if !local_refs.is_empty() {
            let rt = Arc::clone(&self.runtime);
            transaction::after_commit(&self.runtime, async move {
                if let Err(error) =
                    run_blocking(move || local_file_ref::apply(&rt, local_refs)).await
                {
                    tracing::error!(%error, "local file reference catalog update failed after commit");
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    async fn encode_files(
        &self,
        changes: Vec<EncodedFile>,
    ) -> Result<(Vec<KvMutation>, Vec<Vec<u8>>, SidecarPlan), SchemaError> {
        if changes.is_empty() {
            return Ok((Vec::new(), Vec::new(), SidecarPlan::default()));
        }
        let keys = changes
            .iter()
            .map(|(k, _, _)| k.as_str().as_bytes().to_vec())
            .collect();
        let stored = self.main_store.get_many(keys).await?;
        if stored.len() != changes.len() {
            return Err(SchemaError::InvalidData(
                "incomplete file record read".into(),
            ));
        }
        let (mut puts, mut refs, mut local_refs) =
            (Vec::new(), BTreeMap::new(), SidecarPlan::default());
        for (change, bytes) in changes.into_iter().zip(stored) {
            let before = bytes
                .map(|b| StoredRecord::decode(&b))
                .transpose()
                .map_err(super::record_value_error)?;
            encode_file_change(
                change,
                before.as_ref(),
                &self.file_issuer,
                &self.content_key,
                &mut puts,
                &mut refs,
                &mut local_refs,
            )?;
        }
        let retired = self.metadata.stage_file_references(refs).await?;
        Ok((puts, retired, local_refs))
    }

    pub(super) fn require_scope(&self) -> Result<(), SchemaError> {
        transaction::PendingBatch::current(&self.runtime)
            .is_some()
            .then_some(())
            .ok_or_else(|| {
                SchemaError::InvalidData(
                    "record format writes require a complete native write scope".into(),
                )
            })
    }

    pub(super) async fn stage_files(
        &self,
        puts: Vec<KvMutation>,
        mut deletes: Vec<Vec<u8>>,
    ) -> Result<(), SchemaError> {
        if puts.is_empty() && deletes.is_empty() {
            return Ok(());
        }
        self.require_scope()?;
        let file_store = self.runtime.open_cas().await?;
        if !puts.is_empty() {
            file_store.batch_mutate(puts).await?;
        }
        if !deletes.is_empty() {
            let markers = deletes
                .iter()
                .map(|k| record_file_uploaded_path(k.as_slice()))
                .collect::<Result<Vec<_>, _>>()?;
            let present = file_store.get_many(markers.clone()).await?;
            if present.len() != markers.len() {
                return Err(StorageError::InvalidOperation(
                    "incomplete marker presence read".into(),
                )
                .into());
            }
            deletes.extend(
                markers
                    .into_iter()
                    .zip(present)
                    .filter_map(|(k, p)| p.map(|_| k)),
            );
            self.runtime.stage_record_file_deletes(deletes)?;
        }
        Ok(())
    }
}
