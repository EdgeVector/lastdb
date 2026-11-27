//! Format-driven record access, file sealing/reading, staging, and schema release.

mod encode;

pub(crate) use super::metadata::decrypt_file_row;
pub(crate) use super::schemas::{applied_deletes, release_families, ReleaseFamily, ReleaseKind};
pub(crate) use super::{alternate_bounds, AlternateAddress, RecordAddress};
pub use super::{AlternateEntry, RecordDeleteLedgerEntry, MAX_ALTERNATE_ENTRIES};

use super::metadata::{encode_files, seal_row, Metadata};
use super::{
    read::ReadStore, CAS_COLLECTION, DELETE_AUDIT_FAMILY, MAIN_NAMESPACE, SCHEMA_RELEASE_ROWS,
};
use crate::error::FoldDbError;
use crate::schema::types::{
    FileWrite, FileWriteIssuer, LocalFileReference, MutationType, Schema, SchemaError,
};
use crate::storage::current_keys::families::record_file_path;
use crate::storage::disk_pack::{run_blocking, transaction, PackKvStore, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::{local_file_ref, KvMutation, StorageError};
use lastdb_model::kind_partition::anchored;
use lastdb_model::portable_mutation::{Position, Stamp};
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{FileMarker, InlineWrite, StoredRecord};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

fn inv(msg: impl Into<String>) -> SchemaError {
    SchemaError::InvalidData(msg.into())
}

fn marker(
    name: Option<String>,
    media_type: Option<String>,
    size_bytes: u64,
    sha256: String,
) -> FileMarker {
    FileMarker {
        name,
        media_type,
        size_bytes,
        sha256,
    }
}

pub(crate) struct RecordChange {
    pub key: RecordAddress,
    pub before: Option<Arc<StoredRecord>>,
    pub after: StoredRecord,
    pub file_write: Option<FileWrite>,
    pub inline_write: Option<InlineWrite>,
}

pub(crate) struct RecordFormat {
    pub(in crate::storage) main: PackKvStore,
    pub(in crate::storage) runtime: Arc<PackRuntime>,
    pub(in crate::storage) content_key: [u8; 32],
    pub(in crate::storage) issuer: FileWriteIssuer,
}

impl RecordFormat {
    fn cas(&self) -> PackKvStore {
        self.runtime.open_store(CAS_COLLECTION)
    }

    pub(crate) fn reader(&self) -> ReadStore {
        self.main.clone()
    }

    pub(crate) fn runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) fn content_key(&self) -> &[u8; 32] {
        &self.content_key
    }

    pub(crate) fn admit_local<'a>(&self, file: &'a LocalFileReference) -> Option<&'a Path> {
        self.issuer.admit_local(file)
    }

    pub(crate) async fn applied_deletes(
        &self,
        keys: &[&RecordAddress],
    ) -> StorageResult<Vec<Option<Stamp>>> {
        let requests = keys
            .iter()
            .map(|key| format!("ad:{}", key.as_str()).into_bytes())
            .collect();
        let rows = self.main.get_many(requests).await?;
        if rows.len() != keys.len() {
            return Err(applied_deletes::inv("incomplete applied-delete read"));
        }
        rows.iter()
            .map(|row| row.as_deref().map(applied_deletes::decode).transpose())
            .collect()
    }

    pub(crate) fn seal_record_file(
        &self,
        plain: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        let (sha256, row) = seal_row(&self.content_key, plain)?;
        let file_marker = marker(name, media_type, plain.len() as u64, sha256);
        Ok(self.issuer.issue(file_marker, row))
    }

    pub(crate) async fn reference_local_file(
        &self,
        path: &Path,
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        let (runtime, owned_path) = (Arc::clone(&self.runtime), path.to_path_buf());
        let (canonical, file_name, size, sha) =
            run_blocking(move || local_file_ref::describe(&runtime, &owned_path)).await?;
        let file_marker = marker(name.or(file_name), media_type, size, sha);
        file_marker
            .has_valid_hash()
            .then(|| self.issuer.issue_local(file_marker, canonical))
            .ok_or_else(|| FoldDbError::Schema(inv("local file hash is not 64 hex digits")))
    }

    /// Bytes this node sealed for the file. A local reference is not opened.
    pub(crate) async fn open_owned_record_file(
        &self,
        key: &str,
        version: &Position,
        file_marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let path = record_file_path(key, version)?;
        let Some(row) = self.cas().get(path.as_bytes()).await? else {
            return Ok(None);
        };
        decrypt_file_row(&self.content_key, file_marker, &row).map(Some)
    }

    pub(crate) async fn record_delete_audit(
        &self,
        entry: RecordDeleteLedgerEntry,
    ) -> Result<(), SchemaError> {
        self.require_scope()?;
        let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0).max(0);
        let id = format!("{ts:020}:{}", uuid::Uuid::new_v4());
        let key = anchored(DELETE_AUDIT_FAMILY, &id);
        let bytes = serde_json::to_vec(&entry)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        self.main
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
            if !key.starts_with(family.prefix.as_bytes()) {
                return Err(inv("schema release key has a different owner or family"));
            }
            crate::storage::current_keys::validate(MAIN_NAMESPACE, key)?;
        }
        let stored = self.main.get_many(keys.clone()).await?;
        if stored.len() != keys.len() {
            return Err(inv("schema release read batch is incomplete"));
        }
        let batch = collect_released_rows(keys, stored, family.kind)?;
        if batch.deletes.is_empty() {
            return Ok(0);
        }
        let count = batch.deletes.len() as u64;
        self.main.batch_mutate(batch.deletes).await?;
        Metadata::clear_released_record_replay(&self.runtime, &batch.released).await?;
        if !batch.retired.is_empty() {
            let removals: Vec<String> = batch.retired.into_iter().collect();
            let retired_bytes: Vec<Vec<u8>> =
                removals.iter().map(|s| s.as_bytes().to_vec()).collect();
            self.runtime.stage_record_file_deletes(&retired_bytes)?;
            after_commit_sidecar(
                &self.runtime,
                local_file_ref::SidecarPlan {
                    additions: Vec::new(),
                    removals,
                },
            )?;
        }
        Ok(count)
    }

    pub(crate) async fn stage_records(
        &self,
        schema: &Schema,
        changes: Vec<RecordChange>,
        mutation_types: &[MutationType],
    ) -> Result<(), SchemaError> {
        self.require_scope()?;
        let missing = changes
            .iter()
            .any(|c| c.after.is_live() && c.after.file.is_none());
        if schema.file_required && missing {
            return Err(inv("this record requires a file"));
        }
        let (main, files, written) =
            encode::encode_batch(schema, changes, mutation_types, &self.content_key)?;
        let (puts, deletes, refs) = encode_files(&self.issuer, &self.content_key, files)?;
        self.main.batch_mutate(main).await?;
        if !puts.is_empty() {
            self.cas().batch_mutate(puts).await?;
        }
        if !deletes.is_empty() {
            self.runtime.stage_record_file_deletes(&deletes)?;
        }
        self.runtime.stage_written_records(written);
        after_commit_sidecar(&self.runtime, refs)
    }

    fn require_scope(&self) -> Result<(), SchemaError> {
        transaction::PendingBatch::current(&self.runtime)
            .map(|_| ())
            .ok_or_else(|| inv("record format writes require a complete native write scope"))
    }
}

#[derive(Default)]
struct ReleasedBatch {
    deletes: Vec<KvMutation>,
    retired: BTreeSet<String>,
    released: Vec<String>,
}

fn after_commit_sidecar(
    runtime: &Arc<PackRuntime>,
    refs: local_file_ref::SidecarPlan,
) -> Result<(), SchemaError> {
    if refs.is_empty() {
        return Ok(());
    }
    let rt = Arc::clone(runtime);
    transaction::after_commit(runtime, async move {
        run_blocking(move || local_file_ref::apply(&rt, refs))
            .await
            .inspect_err(|error| {
                tracing::error!(%error, "local file reference catalog update failed after commit");
            })
    })
    .map_err(SchemaError::from)
}

fn collect_released_rows(
    keys: Vec<Vec<u8>>,
    stored: Vec<Option<Vec<u8>>>,
    kind: ReleaseKind,
) -> Result<ReleasedBatch, SchemaError> {
    let mut out = ReleasedBatch::default();
    for (key_bytes, value) in keys.into_iter().zip(stored) {
        let Some(value) = value else { continue };
        match kind {
            ReleaseKind::Records => {
                let key =
                    std::str::from_utf8(&key_bytes).map_err(|e| inv(format!("bad key: {e}")))?;
                if key.starts_with(RECORD_PREFIX) {
                    let record =
                        StoredRecord::decode(&value).map_err(|e| inv(format!("bad {key}: {e}")))?;
                    if let Some(version) = record.file_version {
                        out.retired.insert(record_file_path(key, &version)?);
                    }
                    out.released.push(key.to_owned());
                }
            }
            ReleaseKind::AppliedDeletes => {
                let key =
                    std::str::from_utf8(&key_bytes).map_err(|e| inv(format!("bad key: {e}")))?;
                if let Some(rec) = key
                    .strip_prefix("ad:")
                    .filter(|r| r.starts_with(RECORD_PREFIX))
                {
                    out.released.push(rec.to_owned());
                }
            }
            ReleaseKind::Alternates | ReleaseKind::InlineFiles => {}
        }
        out.deletes.push(KvMutation::delete(key_bytes));
    }
    Ok(out)
}
