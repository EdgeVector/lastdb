//! Record writes, warm-set point reads, file delegation, schema release, and delete ledger.

use super::{RangeWalk, RecordStore};
use crate::error::FoldDbError;
use crate::schema::types::{FileWrite, KeyValue, Schema, SchemaError};
use crate::storage::format::records::{
    self as format, release_families, RecordAddress, ReleaseFamily, ReleaseKind,
};
use chrono::Utc;
use cloud_backup_protocol::v2::blobs::BlobPath;
use futures::FutureExt;
use lastdb_model::kind_partition::exact_prefix_bounds;
use lastdb_model::portable_mutation::{Position, Stamp};
use lastdb_model::record_space::record_space_uuid;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const READ_CHUNK: usize = 10_000;

pub const LEDGER_VERB_PURGE: &str = "purge";
pub const LEDGER_VERB_DELETE: &str = "delete";

pub(crate) use format::RecordChange;
pub use format::RecordDeleteLedgerEntry;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SchemaRelease {
    pub(crate) records: u64,
    pub(crate) alternates: u64,
}

impl RecordDeleteLedgerEntry {
    pub fn erasure(verb: &str, caller: &str, schema: &str, described_key: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(schema.as_bytes());
        hasher.update([0u8]);
        hasher.update(described_key.as_bytes());
        Self {
            verb: verb.to_string(),
            at: Utc::now().to_rfc3339(),
            caller: caller.to_string(),
            schema: schema.to_string(),
            key_fingerprint: format!("{:x}", hasher.finalize()),
            records_erased: 0,
            file_records_erased: 0,
        }
    }
}

impl RecordStore {
    pub(crate) async fn record_record_delete_audit(
        &self,
        entry: RecordDeleteLedgerEntry,
    ) -> Result<(), SchemaError> {
        self.format.record_delete_audit(entry).await
    }

    pub async fn list_record_delete_ledger(
        &self,
        limit: usize,
    ) -> Result<Vec<RecordDeleteLedgerEntry>, SchemaError> {
        let prefix =
            lastdb_model::kind_partition::anchored(crate::storage::format::DELETE_AUDIT_FAMILY, "");
        let mut rows = self
            .reader()
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("delete ledger scan: {e}")))?;
        rows.sort_by(|a, b| a.0.cmp(&b.0));

        let mut out = Vec::new();
        for (k, v) in rows {
            let entry = serde_json::from_slice::<RecordDeleteLedgerEntry>(&v).map_err(|e| {
                SchemaError::InvalidData(format!(
                    "delete ledger row {} is unreadable: {e}",
                    String::from_utf8_lossy(&k)
                ))
            })?;
            out.push(entry);
            if limit > 0 && out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    pub(crate) async fn release_schema_rows(
        &self,
        schema: &Schema,
    ) -> Result<SchemaRelease, SchemaError> {
        let mut out = SchemaRelease::default();
        for family in release_families(schema)? {
            let count = self.release_family(&family).await?;
            match family.kind() {
                ReleaseKind::Records => out.records += count,
                ReleaseKind::Alternates => out.alternates += count,
                ReleaseKind::AppliedDeletes | ReleaseKind::InlineFiles => {}
            }
        }
        Ok(out)
    }

    async fn release_family(&self, family: &ReleaseFamily) -> Result<u64, SchemaError> {
        let (start, end) = exact_prefix_bounds(family.prefix());
        let mut walk = RangeWalk::new(
            start.as_bytes(),
            end.as_bytes(),
            crate::storage::format::SCHEMA_RELEASE_ROWS,
        );
        let mut released = 0;
        while !walk.done() {
            let step = self.walk_step(&mut walk).await?;
            if step.rows.is_empty() {
                continue;
            }
            let keys = step.rows.into_iter().map(|(key, _)| key).collect();
            released += self
                .in_write_scope(self.format.release_schema_batch(keys, family))
                .await?;
        }
        Ok(released)
    }

    pub(crate) async fn stage_completed_file_references(
        &self,
        files: Vec<(RecordAddress, Position)>,
        through: u64,
    ) -> Result<(), SchemaError> {
        self.format
            .stage_completed_file_references(files, through)
            .await
    }

    pub(crate) async fn file_retention_complete(&self) -> Result<bool, SchemaError> {
        self.format.file_retention_complete().await
    }

    pub(crate) async fn stage_file_retention_step(&self) -> Result<bool, SchemaError> {
        self.format.stage_file_retention_step().await
    }

    pub(crate) async fn stage_delete_reclamation(
        &self,
        cutoff_ms: u64,
    ) -> Result<format::ReclaimedDeletes, SchemaError> {
        Ok(self.format.stage_delete_reclamation(cutoff_ms).await?)
    }

    pub(crate) async fn read_applied_deletes(
        &self,
        keys: &[&RecordAddress],
    ) -> Result<Vec<Option<Stamp>>, SchemaError> {
        Ok(self.format.applied_deletes(keys).await?)
    }

    pub(crate) fn record_key(
        &self,
        schema: &Schema,
        key: &KeyValue,
    ) -> Result<String, SchemaError> {
        self.record_address(schema, key)
            .map(RecordAddress::into_string)
    }

    pub(crate) fn record_address(
        &self,
        schema: &Schema,
        key: &KeyValue,
    ) -> Result<RecordAddress, SchemaError> {
        let space = record_space_uuid(&schema.name);
        RecordAddress::new(&self.primary_codec(schema)?, &space, key)
    }

    pub(crate) async fn read_records<K: AsRef<str> + Sync>(
        &self,
        keys: &[K],
    ) -> Result<Vec<Option<Arc<StoredRecord>>>, SchemaError> {
        self.record_read_scope(self.read_records_scoped(keys).boxed())
            .await
    }

    async fn read_records_scoped<K: AsRef<str> + Sync>(
        &self,
        keys: &[K],
    ) -> Result<Vec<Option<Arc<StoredRecord>>>, SchemaError> {
        let runtime = self.runtime.clone();
        let mut warm = runtime
            .warm_records(keys)
            .map_err(|error| SchemaError::InvalidData(format!("read records: {error}")))?;
        let mut out = match &mut warm {
            Some(read) => read.take_found(),
            None => vec![None; keys.len()],
        };
        let misses: Vec<usize> = (0..keys.len()).filter(|at| out[*at].is_none()).collect();
        let mut admitted = Vec::new();
        for chunk in misses.chunks(READ_CHUNK) {
            let rows = self
                .main_store
                .get_many(
                    chunk
                        .iter()
                        .map(|at| keys[*at].as_ref().as_bytes().to_vec())
                        .collect(),
                )
                .await
                .map_err(|error| SchemaError::InvalidData(format!("read records: {error}")))?;
            if rows.len() != chunk.len() {
                return Err(SchemaError::InvalidData(
                    "the record read batch is incomplete".into(),
                ));
            }
            for (at, row) in chunk.iter().zip(rows) {
                let Some(bytes) = row else { continue };
                let record = Arc::new(StoredRecord::decode(&bytes).map_err(|error| {
                    SchemaError::InvalidData(format!("a stored record: {error}"))
                })?);
                if warm.is_some() {
                    admitted.push((*at, Arc::clone(&record), bytes.len()));
                }
                out[*at] = Some(record);
            }
        }
        if let Some(read) = &warm {
            runtime.admit_read_records(read, admitted);
        }
        Ok(out)
    }

    pub(crate) async fn stage_records(
        &self,
        schema: &Schema,
        changes: Vec<RecordChange>,
    ) -> Result<(), SchemaError> {
        self.format.stage_records(schema, changes).await
    }

    pub(crate) async fn portable_file(
        &self,
        key: &RecordAddress,
        version: &Position,
        marker: &FileMarker,
    ) -> Result<Vec<u8>, SchemaError> {
        self.format.portable_file(key, version, marker).await
    }

    pub(crate) fn admit_portable_file(
        &self,
        marker: FileMarker,
        sealed: Vec<u8>,
    ) -> Result<FileWrite, SchemaError> {
        self.format.admit_portable_file(marker, sealed)
    }

    pub(crate) fn seal_record_file(
        &self,
        plaintext: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        self.format.seal_record_file(plaintext, name, media_type)
    }

    pub(crate) async fn open_inline_file(
        &self,
        record_key: &str,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        self.format.open_inline_file(record_key, marker).await
    }

    pub(crate) fn dedicated_shared_home(&self) -> bool {
        self.format.dedicated_shared_home()
    }

    pub(crate) fn record_blob_path(
        &self,
        record_key: &str,
        version: &Position,
    ) -> Result<BlobPath, FoldDbError> {
        self.format.record_blob_path(record_key, version)
    }

    pub(crate) async fn install_record_file(
        &self,
        record_key: &str,
        version: &Position,
        marker: &FileMarker,
        row: Vec<u8>,
    ) -> Result<Vec<u8>, FoldDbError> {
        self.format
            .install_record_file(record_key, version, marker, row)
            .await
    }

    pub(crate) async fn reference_local_file(
        &self,
        path: &std::path::Path,
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        self.format
            .reference_local_file(path, name, media_type)
            .await
    }

    pub(crate) async fn open_record_file(
        &self,
        record_key: &str,
        version: &Position,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        self.format
            .open_record_file(record_key, version, marker)
            .await
    }
}
