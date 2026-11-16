//! Format-driven record access, batch/alternate encoding, file sealing/reading, and schema release.

use super::inline_files;
pub(crate) use super::metadata::{decrypt_file_row, require_row};
pub(crate) use super::schemas::{applied_deletes, release_families, ReleaseFamily, ReleaseKind};
pub(crate) use super::{alternate_bounds, AlternateAddress, RecordAddress};
pub use super::{AlternateEntry, RecordDeleteLedgerEntry, MAX_ALTERNATE_ENTRIES};

use super::metadata::{encode_files, record_value_error, seal_row, FileChange};
use super::{
    read::ReadStore, CAS_COLLECTION, DELETE_AUDIT_FAMILY, MAIN_NAMESPACE, SCHEMA_RELEASE_ROWS,
};
use crate::error::FoldDbError;
use crate::schema::types::{FileWrite, FileWriteIssuer, LocalFileReference, Schema, SchemaError};
use crate::storage::current_keys::families::record_file_path;
use crate::storage::disk_pack::{
    record_warm::WrittenRecord, run_blocking, transaction, PackKvStore, PackRuntime,
};
use crate::storage::error::StorageResult;
use crate::storage::{local_file_ref, KvMutation, StorageError};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::alternate_field_key_id;
use lastdb_model::kind_partition::anchored;
use lastdb_model::portable_mutation::{Position, Stamp};
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{FileMarker, InlineWrite, StoredRecord};
use std::collections::{BTreeSet, HashSet};
use std::path::Path;
use std::sync::Arc;

type EncodedBatch = (Vec<KvMutation>, Vec<FileChange>, Vec<WrittenRecord>);

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
    async fn cas(&self) -> StorageResult<PackKvStore> {
        self.runtime.open_store(CAS_COLLECTION).await
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

    pub(crate) async fn open_inline_file(
        &self,
        key: &str,
        file_marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let Some(row) = self.main.get(inline_files::address(key).as_bytes()).await? else {
            return Ok(None);
        };
        inline_files::decode_for(&row, file_marker)?
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

    pub(crate) async fn open_record_file(
        &self,
        key: &str,
        version: &Position,
        file_marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let path = record_file_path(key, version)?;
        let cas = self.cas().await?.get(path.as_bytes()).await;
        if let Ok(Some(row)) = cas {
            return decrypt_file_row(&self.content_key, file_marker, &row).map(Some);
        }
        if let Some(bytes) =
            local_file_ref::read_referenced(&self.runtime, &path, file_marker).await?
        {
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
        let (mut deletes, mut retired) = (Vec::new(), BTreeSet::new());
        for (key_bytes, value) in keys.into_iter().zip(stored) {
            let Some(value) = value else { continue };
            if matches!(family.kind, ReleaseKind::Records) {
                let key =
                    std::str::from_utf8(&key_bytes).map_err(|e| inv(format!("bad key: {e}")))?;
                if key.starts_with(RECORD_PREFIX) {
                    let record =
                        StoredRecord::decode(&value).map_err(|e| inv(format!("bad {key}: {e}")))?;
                    if let Some(version) = record.file_version {
                        retired.insert(record_file_path(key, &version)?);
                    }
                }
            }
            deletes.push(KvMutation::delete(key_bytes));
        }
        if deletes.is_empty() {
            return Ok(0);
        }
        let count = deletes.len() as u64;
        self.main.batch_mutate(deletes).await?;
        let retired: Vec<Vec<u8>> = retired.into_iter().map(String::into_bytes).collect();
        if !retired.is_empty() {
            self.runtime.stage_record_file_deletes(&retired)?;
        }
        Ok(count)
    }

    pub(crate) async fn stage_records(
        &self,
        schema: &Schema,
        changes: Vec<RecordChange>,
    ) -> Result<(), SchemaError> {
        self.require_scope()?;
        let missing = changes
            .iter()
            .any(|c| c.after.is_live() && c.after.file.is_none());
        if schema.file_required && missing {
            return Err(inv("this record requires a file"));
        }
        let (main, files, written) = encode_batch(schema, changes, &self.content_key)?;
        let (puts, deletes, refs) = encode_files(&self.issuer, &self.content_key, files)?;
        self.main.batch_mutate(main).await?;
        if !puts.is_empty() {
            self.cas().await?.batch_mutate(puts).await?;
        }
        if !deletes.is_empty() {
            self.runtime.stage_record_file_deletes(&deletes)?;
        }
        self.runtime.stage_written_records(written);
        if !refs.is_empty() {
            let runtime = Arc::clone(&self.runtime);
            transaction::after_commit(&self.runtime, async move {
                if let Err(error) =
                    run_blocking(move || local_file_ref::apply(&runtime, refs)).await
                {
                    tracing::error!(%error, "local file reference catalog update failed after commit");
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    fn require_scope(&self) -> Result<(), SchemaError> {
        transaction::PendingBatch::current(&self.runtime)
            .map(|_| ())
            .ok_or_else(|| inv("record format writes require a complete native write scope"))
    }
}

fn encode_alternates(
    schema: &Schema,
    changes: &[RecordChange],
    node_key: &[u8; 32],
) -> Result<Vec<KvMutation>, SchemaError> {
    let defs = schema.require_record_keys()?;
    if defs.alternates.is_empty() {
        return Ok(Vec::new());
    }
    let mut pointers = Vec::new();
    for change in changes {
        let before = change.before.as_ref().filter(|record| record.is_live());
        let after = change.after.is_live().then_some(&change.after);
        for (name, def) in &defs.alternates {
            let old = before.and_then(|record| record.alternates.get(name)?.as_ref());
            let new = after.and_then(|record| record.alternates.get(name)?.as_ref());
            if old == new {
                continue;
            }
            let field_key = alternate_field_key_id(schema, name);
            let codec = FieldKeyCodec::from_node_key(
                node_key,
                &field_key,
                def.range_profile().map_err(inv)?,
            );
            let address = |value| {
                AlternateAddress::new(&codec, &field_key, value, &change.key).map(|a| a.encoded)
            };
            if let Some(old) = old {
                pointers.push(KvMutation::delete(address(old)?));
            }
            if let Some(new) = new {
                let entry = AlternateEntry {
                    primary: change.key.key.clone(),
                    matched: new.clone(),
                };
                let bytes =
                    serde_json::to_vec(&entry).map_err(|_| inv("encode alternate entry"))?;
                pointers.push(KvMutation::put(address(new)?, bytes));
            }
            if pointers.len() > MAX_ALTERNATE_ENTRIES {
                return Err(inv("alternate change batch exceeds 4096 entries"));
            }
        }
    }
    Ok(pointers)
}

fn encode_batch(
    schema: &Schema,
    changes: Vec<RecordChange>,
    node_key: &[u8; 32],
) -> Result<EncodedBatch, SchemaError> {
    let alternates = encode_alternates(schema, &changes, node_key)?;
    let count = changes.len();
    let mut seen = HashSet::with_capacity(count);
    if !changes
        .iter()
        .all(|change| seen.insert(change.key.as_str()))
    {
        return Err(inv("a record batch repeats a record address"));
    }
    let mut main = Vec::with_capacity(count + alternates.len());
    let (mut files, mut written) = (Vec::with_capacity(count), Vec::with_capacity(count));
    for change in changes {
        if change.after.is_live() && change.after.key != change.key.key {
            return Err(inv("record payload differs from its bound primary address"));
        }
        let before = change.before.filter(|record| record.is_live());
        let after = Arc::new(change.after);
        files.push(FileChange {
            address: change.key.clone(),
            before,
            after: Arc::clone(&after),
            write: change.file_write,
        });
        let encoded_key = change.key.encoded;
        if after.is_live() {
            match &change.inline_write {
                Some(InlineWrite::Put(file))
                    if after.inline_file.as_ref() == Some(&file.marker) =>
                {
                    main.push(inline_files::put(&encoded_key, file)?);
                }
                Some(InlineWrite::Clear) if after.inline_file.is_none() => {
                    main.push(inline_files::delete(&encoded_key));
                }
                None => {}
                Some(_) => return Err(inv("the inline file write differs from the record marker")),
            }
            let value = after
                .encode_uncapped()
                .map_err(|e| record_value_error(&e))?;
            written.push(WrittenRecord::new(&encoded_key, after, &value));
            main.push(KvMutation::put(encoded_key, value));
        } else {
            let delete_marker = applied_deletes::encode(&encoded_key, &after.stamp)?;
            main.extend([
                inline_files::delete(&encoded_key),
                delete_marker,
                KvMutation::delete(encoded_key),
            ]);
        }
    }
    main.extend(alternates);
    Ok((main, files, written))
}
