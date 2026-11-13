//! Bound main/file capabilities, record-owned file versions, and schema release.

use super::super::metadata::seal_row;
use super::{
    applied_deletes, decrypt_file_row, inline_files, require_row, EncodedFile, RecordAddress,
    RecordChange, RecordDeleteLedgerEntry, ReleaseFamily, ReleaseKind,
};
use crate::error::FoldDbError;
use crate::schema::types::{FileWrite, FileWriteIssuer, LocalFileReference, Schema};
use crate::schema::SchemaError;
use crate::storage::current_keys::families::record_file_path;
use crate::storage::disk_pack::{run_blocking, transaction, PackKvStore, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::format::{
    metadata::Metadata, read::ReadStore, DELETE_AUDIT_FAMILY, MAIN_NAMESPACE, SCHEMA_RELEASE_ROWS,
};
use crate::storage::local_file_ref::{self, SidecarPlan};
use crate::storage::{KvMutation, StorageError};
use lastdb_model::kind_partition::anchored;
use lastdb_model::portable_mutation::{Position, Stamp};
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

type StagedFiles = (Vec<KvMutation>, BTreeMap<String, bool>, SidecarPlan);

fn inv(msg: impl Into<String>) -> SchemaError {
    SchemaError::InvalidData(msg.into())
}

fn encode_file_change(
    (key, after, write): EncodedFile,
    before: Option<&StoredRecord>,
    issuer: &FileWriteIssuer,
    content_key: &[u8; 32],
    staged: &mut StagedFiles,
) -> Result<(), SchemaError> {
    let (prev, next) = (
        before.and_then(|r| r.file_version.as_ref()),
        after.file_version.as_ref(),
    );
    if prev == next {
        return (before.and_then(|r| r.file.as_ref()) == after.file.as_ref())
            .then_some(())
            .ok_or_else(|| inv("a retained file version has a different marker"));
    }
    if let Some(version) = prev {
        let path = record_file_path(key.as_str(), version)?;
        staged.1.insert(path.clone(), false);
        staged.2 .1.push(path);
    }
    let Some(version) = next else { return Ok(()) };
    let path = record_file_path(key.as_str(), version)?;
    staged.1.insert(path.clone(), true);
    let marker = after
        .file
        .as_ref()
        .ok_or_else(|| inv("a file row requires its record marker"))?;
    match write {
        Some(FileWrite::Put(f)) if f.marker() == marker => {
            let row = issuer
                .admit(f)
                .ok_or_else(|| inv("file payload belongs to another format writer"))?;
            require_row(content_key, marker, &row)?;
            staged.0.push(KvMutation::put(path.into_bytes(), row));
            Ok(())
        }
        Some(FileWrite::Put(_)) => Err(inv("file payload differs from its record marker")),
        Some(FileWrite::LocalPath(l)) if l.marker() == marker => {
            let p = issuer
                .admit_local(&l)
                .ok_or_else(|| inv("local file reference belongs to another format writer"))?;
            staged.2 .0.push((
                path,
                p.to_path_buf(),
                marker.sha256.clone(),
                marker.size_bytes,
            ));
            Ok(())
        }
        Some(FileWrite::LocalPath(_)) => {
            Err(inv("local file reference differs from its record marker"))
        }
        Some(FileWrite::Clear) | None => Err(inv(
            "a selected file requires its sealed payload or a local reference",
        )),
    }
}

pub(crate) struct RecordFormat(
    PackKvStore,
    Arc<PackRuntime>,
    [u8; 32],
    FileWriteIssuer,
    Metadata,
);

impl RecordFormat {
    pub(in crate::storage::format) fn new(
        main: PackKvStore,
        rt: Arc<PackRuntime>,
        key: [u8; 32],
        meta: Metadata,
    ) -> Self {
        Self(main, rt, key, FileWriteIssuer::default(), meta)
    }

    pub(crate) fn reader(&self) -> ReadStore {
        self.0.clone()
    }
    pub(crate) fn runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.1)
    }
    pub(crate) fn content_key(&self) -> &[u8; 32] {
        &self.2
    }
    pub(crate) fn admit_local<'a>(&self, file: &'a LocalFileReference) -> Option<&'a Path> {
        self.3.admit_local(file)
    }
    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.1.flush().await
    }

    pub(crate) async fn applied_deletes(
        &self,
        keys: &[&RecordAddress],
    ) -> StorageResult<Vec<Option<Stamp>>> {
        let req = keys
            .iter()
            .map(|k| format!("ad:{}", k.as_str()).into_bytes())
            .collect();
        let rows = self.0.get_many(req).await?;
        if rows.len() != keys.len() {
            return Err(applied_deletes::inv("incomplete applied-delete read"));
        }
        rows.iter()
            .map(|r| r.as_deref().map(applied_deletes::decode).transpose())
            .collect()
    }

    pub(crate) async fn open_inline_file(
        &self,
        k: &str,
        m: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let Some(r) = self.0.get(inline_files::address(k).as_bytes()).await? else {
            return Ok(None);
        };
        inline_files::decode_for(&r, m)?
            .bytes()
            .map(Some)
            .map_err(FoldDbError::Database)
    }

    pub(crate) fn seal_record_file(
        &self,
        plain: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        let (sha256, row) = seal_row(&self.2, plain)?;
        let m = FileMarker {
            name,
            media_type,
            size_bytes: plain.len() as u64,
            sha256,
        };
        Ok(self.3.issue(m, row))
    }

    pub(crate) async fn reference_local_file(
        &self,
        p: &Path,
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        let (rt, path) = (Arc::clone(&self.1), p.to_path_buf());
        let (file_path, fname, size_bytes, sha256) =
            run_blocking(move || local_file_ref::describe(&rt, &path)).await?;
        let m = FileMarker {
            name: name.or(fname),
            media_type,
            size_bytes,
            sha256,
        };
        m.has_valid_hash()
            .then(|| self.3.issue_local(m, file_path))
            .ok_or_else(|| FoldDbError::Schema(inv("local file hash is not 64 hex digits")))
    }

    pub(crate) async fn open_record_file(
        &self,
        k: &str,
        v: &Position,
        m: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let path = record_file_path(k, v)?;
        let cas = self.1.open_cas().await?.get(path.as_bytes()).await;
        if let Ok(Some(row)) = cas {
            return decrypt_file_row(&self.2, m, &row).map(Some);
        }
        if let Some(bytes) = local_file_ref::read_referenced(&self.1, &path, m).await? {
            return Ok(Some(bytes));
        }
        Ok(cas.map(|_| None)?)
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
        self.0
            .batch_mutate(vec![KvMutation::put(key, bytes)])
            .await
            .map_err(|e| inv(format!("stage erase audit: {e}")))
    }

    pub(crate) async fn release_schema_batch(
        &self,
        keys: Vec<Vec<u8>>,
        family: &ReleaseFamily,
    ) -> Result<u64, SchemaError> {
        self.require_scope()?;
        if keys.len() > SCHEMA_RELEASE_ROWS {
            return Err(inv("schema release exceeds its row bound"));
        }
        for key in &keys {
            if !key.starts_with(family.0.as_bytes()) {
                return Err(inv("schema release key has a different owner or family"));
            }
            crate::storage::current_keys::validate(MAIN_NAMESPACE, key)?;
        }
        let stored = self.0.get_many(keys.clone()).await?;
        if stored.len() != keys.len() {
            return Err(inv("schema release read batch is incomplete"));
        }
        let (mut deletes, mut refs) = (Vec::new(), BTreeMap::new());
        for (k, v) in keys.into_iter().zip(stored) {
            let Some(v) = v else { continue };
            if matches!(family.1, ReleaseKind::Records) {
                let key = std::str::from_utf8(&k)
                    .map_err(|e| inv(format!("a dropped row key is not UTF-8: {e}")))?;
                if key.starts_with(RECORD_PREFIX) {
                    let rec = StoredRecord::decode(&v)
                        .map_err(|e| inv(format!("dropped record row {key} is unreadable: {e}")))?;
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
        self.0.batch_mutate(deletes).await?;
        let retired = self.4.stage_file_references(refs).await?;
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
            return Err(inv("this record requires a file"));
        }
        let (main, files, written) = super::encode_batch(schema, changes, &self.2)?;
        let (puts, deletes, refs) = self.encode_files(files).await?;
        self.0.batch_mutate(main).await?;
        self.stage_files(puts, deletes).await?;
        self.1.stage_written_records(written);
        if !refs.is_empty() {
            let rt = Arc::clone(&self.1);
            transaction::after_commit(&self.1, async move {
                if let Err(error) = run_blocking(move || local_file_ref::apply(&rt, refs)).await {
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
        let stored = self
            .0
            .get_many(
                changes
                    .iter()
                    .map(|c| c.0.as_str().as_bytes().to_vec())
                    .collect(),
            )
            .await?;
        if stored.len() != changes.len() {
            return Err(inv("incomplete file record read"));
        }
        let mut s = StagedFiles::default();
        for (change, bytes) in changes.into_iter().zip(stored) {
            let before = bytes
                .as_deref()
                .map(StoredRecord::decode)
                .transpose()
                .map_err(|e| super::record_value_error(&e))?;
            encode_file_change(change, before.as_ref(), &self.3, &self.2, &mut s)?;
        }
        Ok((s.0, self.4.stage_file_references(s.1).await?, s.2))
    }

    fn require_scope(&self) -> Result<(), SchemaError> {
        transaction::PendingBatch::current(&self.1)
            .map(|_| ())
            .ok_or_else(|| inv("record format writes require a complete native write scope"))
    }

    async fn stage_files(
        &self,
        puts: Vec<KvMutation>,
        del: Vec<Vec<u8>>,
    ) -> Result<(), SchemaError> {
        if puts.is_empty() && del.is_empty() {
            return Ok(());
        }
        self.require_scope()?;
        if !puts.is_empty() {
            self.1.open_cas().await?.batch_mutate(puts).await?;
        }
        if !del.is_empty() {
            self.1.stage_record_file_deletes(&del)?;
        }
        Ok(())
    }
}
