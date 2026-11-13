//! Format-driven record encoding, home binding, bound capabilities, and schema release.

pub(in crate::storage) use super::inline_files;
pub(crate) use super::metadata::{decrypt_file_row, require_row};
pub(crate) use super::schemas::{applied_deletes, release_families, ReleaseFamily, ReleaseKind};
pub(crate) use super::{alternate_bounds, AlternateAddress, RecordAddress};
pub use super::{AlternateEntry, RecordDeleteLedgerEntry, MAX_ALTERNATE_ENTRIES};

use super::metadata::{record_value_error, seal_row, EncodedFile, Metadata};
use super::{
    read::ReadStore, CAS_COLLECTION, DELETE_AUDIT_FAMILY, MAIN_NAMESPACE, SCHEMA_RELEASE_ROWS,
};
use crate::error::FoldDbError;
use crate::schema::types::{FileWrite, FileWriteIssuer, LocalFileReference, Schema};
use crate::schema::SchemaError;
use crate::storage::current_keys::families::record_file_path;
use crate::storage::disk_pack::record_warm::WrittenRecord;
use crate::storage::disk_pack::{run_blocking, transaction, PackKvStore, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::{local_file_ref, KvMutation, StorageError};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::alternate_field_key_id;
use lastdb_model::kind_partition::anchored;
use lastdb_model::portable_mutation::{Position, Stamp};
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{FileMarker, InlineWrite, StoredRecord};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;

pub(crate) mod bind {
    use super::{FileWriteIssuer, Metadata, RecordFormat};
    use crate::storage::disk_pack::{transaction, PackNamespacedStore, PackRuntime};
    use crate::storage::error::{StorageError, StorageResult};
    use crate::storage::format::{
        content_key_proof, control, schemas::SchemaFormat, CURRENT_KEY_FORMAT_KEY,
        DELETE_REPLAY_CUTOFF_KEY, MAIN_NAMESPACE, METADATA_NAMESPACE, SCHEMAS_NAMESPACE,
        SCHEMA_STATES_NAMESPACE,
    };
    use std::sync::Arc;

    pub(crate) struct BoundFormat {
        pub(crate) runtime: Arc<PackRuntime>,
        pub(crate) schemas: SchemaFormat,
        pub(crate) records: RecordFormat,
        pub(crate) metadata: Metadata,
    }

    pub(crate) async fn open(s: PackNamespacedStore, key: [u8; 32]) -> StorageResult<BoundFormat> {
        let runtime = Arc::clone(&s.runtime);
        let (main, meta_kv, s_kv, st_kv) = tokio::try_join!(
            s.open_namespace(MAIN_NAMESPACE),
            s.open_namespace(METADATA_NAMESPACE),
            s.open_namespace(SCHEMAS_NAMESPACE),
            s.open_namespace(SCHEMA_STATES_NAMESPACE),
        )?;
        let schemas = SchemaFormat::new(s_kv, st_kv, Arc::clone(&runtime));
        let metadata = Metadata::new(meta_kv.clone(), Arc::clone(&runtime));
        transaction::scope(Arc::clone(&runtime), async {
            let [a, b] = [CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY];
            let rows = main
                .get_many(vec![a.as_bytes().to_vec(), b.as_bytes().to_vec()])
                .await?;
            let [fmt, cutoff]: [_; 2] = rows
                .try_into()
                .map_err(|_| control::invalid("incomplete fence read"))?;
            control::validate_frozen_replay_floor(fmt.as_deref(), cutoff.as_deref())?;
            let msg = "content key proof is missing; this program opens only a home it created";
            let stored = meta_kv.get(content_key_proof::KEY).await?;
            let stored = stored.ok_or_else(|| StorageError::EncryptionError(msg.into()))?;
            content_key_proof::verify(&key, &stored)
        })
        .await?;
        let iss = FileWriteIssuer::default();
        let records = RecordFormat(main, Arc::clone(&runtime), key, iss, metadata.clone());
        runtime.finish_fresh_initialization().await?;
        Ok(BoundFormat {
            runtime,
            schemas,
            records,
            metadata,
        })
    }
}

type SchemaOps = Result<Vec<KvMutation>, SchemaError>;
type EncodedBatch = (Vec<KvMutation>, Vec<EncodedFile>, Vec<WrittenRecord>);
type BatchRes = Result<EncodedBatch, SchemaError>;
type Rt = Arc<PackRuntime>;
type OptBytes = Result<Option<Vec<u8>>, FoldDbError>;
type FileRes = Result<FileWrite, FoldDbError>;
type OptStr = Option<String>;

fn inv(msg: impl Into<String>) -> SchemaError {
    SchemaError::InvalidData(msg.into())
}

fn marker(name: OptStr, media_type: OptStr, size_bytes: u64, sha256: String) -> FileMarker {
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

fn encode_alternates(s: &Schema, changes: &[RecordChange], nkey: &[u8; 32]) -> SchemaOps {
    let defs = s.require_record_keys()?;
    if defs.alternates.is_empty() {
        return Ok(Vec::new());
    }
    let mut ptrs = Vec::new();
    for c in changes {
        let before = c.before.as_ref().filter(|r| r.is_live());
        let after = c.after.is_live().then_some(&c.after);
        for (name, def) in &defs.alternates {
            let old = before.and_then(|r| r.alternates.get(name)?.as_ref());
            let new = after.and_then(|r| r.alternates.get(name)?.as_ref());
            if old == new {
                continue;
            }
            let fk = alternate_field_key_id(s, name);
            let codec = FieldKeyCodec::from_node_key(nkey, &fk, def.range_profile().map_err(inv)?);
            let addr = |v| AlternateAddress::new(&codec, &fk, v, &c.key).map(|a| a.0);
            if let Some(old) = old {
                ptrs.push(KvMutation::delete(addr(old)?));
            }
            if let Some(new) = new {
                let (primary, matched) = (c.key.1.clone(), new.clone());
                let entry = AlternateEntry { primary, matched };
                let b = serde_json::to_vec(&entry).map_err(|_| inv("encode alternate entry"))?;
                ptrs.push(KvMutation::put(addr(new)?, b));
            }
            if ptrs.len() > MAX_ALTERNATE_ENTRIES {
                return Err(inv("alternate change batch exceeds 4096 entries"));
            }
        }
    }
    Ok(ptrs)
}

fn encode_batch(s: &Schema, changes: Vec<RecordChange>, nkey: &[u8; 32]) -> BatchRes {
    let alternates = encode_alternates(s, &changes, nkey)?;
    let n = changes.len();
    let mut seen = HashSet::with_capacity(n);
    if !changes.iter().all(|c| seen.insert(c.key.as_str())) {
        return Err(inv("a record batch repeats a record address"));
    }
    let mut main = Vec::with_capacity(n + alternates.len());
    let (mut files, mut written) = (Vec::with_capacity(n), Vec::with_capacity(n));
    for c in changes {
        if c.after.is_live() && c.after.key != c.key.1 {
            return Err(inv("record payload differs from its bound primary address"));
        }
        let after = Arc::new(c.after);
        files.push((c.key.clone(), Arc::clone(&after), c.file_write));
        if after.is_live() {
            match &c.inline_write {
                Some(InlineWrite::Put(f)) if after.inline_file.as_ref() == Some(&f.marker) => {
                    main.push(inline_files::put(&c.key.0, f)?);
                }
                Some(InlineWrite::Clear) if after.inline_file.is_none() => {
                    main.push(inline_files::delete(&c.key.0));
                }
                None => {}
                Some(_) => return Err(inv("the inline file write differs from the record marker")),
            }
            let val = after
                .encode_uncapped()
                .map_err(|e| record_value_error(&e))?;
            written.push(WrittenRecord::new(&c.key.0, after, &val));
            main.push(KvMutation::put(c.key.0, val));
        } else {
            let del = applied_deletes::encode(&c.key.0, &after.stamp)?;
            main.extend([
                inline_files::delete(&c.key.0),
                del,
                KvMutation::delete(c.key.0),
            ]);
        }
    }
    main.extend(alternates);
    Ok((main, files, written))
}

pub(crate) struct RecordFormat(PackKvStore, Rt, [u8; 32], FileWriteIssuer, Metadata);

impl RecordFormat {
    async fn cas(&self) -> StorageResult<PackKvStore> {
        self.1.open_store(CAS_COLLECTION).await
    }
    pub(crate) fn reader(&self) -> ReadStore {
        self.0.clone()
    }
    pub(crate) fn runtime(&self) -> Rt {
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
    pub(crate) async fn open_inline_file(&self, k: &str, m: &FileMarker) -> OptBytes {
        let Some(r) = self.0.get(inline_files::address(k).as_bytes()).await? else {
            return Ok(None);
        };
        inline_files::decode_for(&r, m)?
            .bytes()
            .map(Some)
            .map_err(FoldDbError::Database)
    }
    pub(crate) fn seal_record_file(&self, plain: &[u8], name: OptStr, mt: OptStr) -> FileRes {
        let (sha256, row) = seal_row(&self.2, plain)?;
        let m = marker(name, mt, plain.len() as u64, sha256);
        Ok(self.3.issue(m, row))
    }
    pub(crate) async fn reference_local_file(&self, p: &Path, name: OptStr, mt: OptStr) -> FileRes {
        let (rt, path) = (Arc::clone(&self.1), p.to_path_buf());
        let (fp, fnm, sz, sha) = run_blocking(move || local_file_ref::describe(&rt, &path)).await?;
        let m = marker(name.or(fnm), mt, sz, sha);
        m.has_valid_hash()
            .then(|| self.3.issue_local(m, fp))
            .ok_or_else(|| FoldDbError::Schema(inv("local file hash is not 64 hex digits")))
    }
    pub(crate) async fn open_record_file(&self, k: &str, v: &Position, m: &FileMarker) -> OptBytes {
        let path = record_file_path(k, v)?;
        let cas = self.cas().await?.get(path.as_bytes()).await;
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
        let id = format!("{ts:020}:{}", uuid::Uuid::new_v4());
        let key = anchored(DELETE_AUDIT_FAMILY, &id);
        let bytes = serde_json::to_vec(&entry)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        let op = vec![KvMutation::put(key, bytes)];
        self.0
            .batch_mutate(op)
            .await
            .map_err(|e| inv(format!("stage erase audit: {e}")))
    }
    pub(crate) async fn release_schema_batch(
        &self,
        keys: Vec<Vec<u8>>,
        fam: &ReleaseFamily,
    ) -> Result<u64, SchemaError> {
        self.require_scope()?;
        if keys.len() > SCHEMA_RELEASE_ROWS {
            return Err(inv("schema release exceeds its row bound"));
        }
        for key in &keys {
            if !key.starts_with(fam.0.as_bytes()) {
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
            if matches!(fam.1, ReleaseKind::Records) {
                let key = std::str::from_utf8(&k).map_err(|e| inv(format!("bad key: {e}")))?;
                if key.starts_with(RECORD_PREFIX) {
                    let rec =
                        StoredRecord::decode(&v).map_err(|e| inv(format!("bad {key}: {e}")))?;
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
        if !retired.is_empty() {
            self.1.stage_record_file_deletes(&retired)?;
        }
        Ok(count)
    }
    pub(crate) async fn stage_records(
        &self,
        s: &Schema,
        changes: Vec<RecordChange>,
    ) -> Result<(), SchemaError> {
        self.require_scope()?;
        let missing = changes
            .iter()
            .any(|c| c.after.is_live() && c.after.file.is_none());
        if s.file_required && missing {
            return Err(inv("this record requires a file"));
        }
        let (main, files, written) = encode_batch(s, changes, &self.2)?;
        let enc = self
            .4
            .encode_files(&self.0, &self.3, &self.2, files)
            .await?;
        let (puts, deletes, refs) = enc;
        self.0.batch_mutate(main).await?;
        if !puts.is_empty() {
            self.cas().await?.batch_mutate(puts).await?;
        }
        if !deletes.is_empty() {
            self.1.stage_record_file_deletes(&deletes)?;
        }
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
    fn require_scope(&self) -> Result<(), SchemaError> {
        transaction::PendingBatch::current(&self.1)
            .map(|_| ())
            .ok_or_else(|| inv("record format writes require a complete native write scope"))
    }
}
