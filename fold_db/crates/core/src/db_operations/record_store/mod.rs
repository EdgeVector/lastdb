//! Record domain store: codecs, paged hash spans, warm-set reads, alternate reads, and delete ledger.

mod accepted;
mod reads;

pub(crate) use reads::{KeyedRecord, RangeWalk};

use crate::error::FoldDbError;
use crate::schema::types::{
    field::{FilterUtils, HashRangeFilter},
    key_comparison::KeyComparisonContext,
    FileWrite, KeyValue, Schema, SchemaError,
};
use crate::storage::disk_pack::{transaction, PackReadScope, PackRuntime};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::format::{
    control::{validate_frozen_replay_floor, DELETE_REPLAY_CUTOFF_KEY as CUTOFF_KEY},
    read::ReadStore,
    records::{
        self as format, alternate_bounds, AlternateAddress, AlternateEntry, RecordAddress,
        RecordFormat, MAX_ALTERNATE_ENTRIES,
    },
};
use futures::{future::BoxFuture, FutureExt};
use lastdb_model::{
    field_key_codec::FieldKeyCodec,
    field_key_id::alternate_field_key_id,
    record_space::{record_hash_prefix, record_space_uuid},
    record_value::StoredRecord,
};
use std::future::Future;
use std::sync::Arc;

pub use crate::db_operations::WriterWatch;
pub(crate) use format::RecordChange;
pub use format::RecordDeleteLedgerEntry;

#[derive(Clone, Copy)]
pub(crate) enum RecordBound<'a> {
    All,
    Prefix(&'a str),
    Between(&'a str, &'a str),
}

pub(crate) struct RecordSpan {
    pub(super) hash: String,
    pub(super) codec: FieldKeyCodec,
    pub(super) base: String,
    pub(super) start: String,
    pub(super) end: String,
}

pub(crate) struct SpanKey {
    pub(crate) range: String,
    pub(crate) key: KeyValue,
    pub(crate) physical: String,
    pub(crate) record: StoredRecord,
}

pub(crate) struct SpanPage {
    pub(crate) keys: Vec<SpanKey>,
    pub(crate) next: Option<String>,
}

impl RecordSpan {
    pub(crate) fn storage_range<'a>(&self, physical: &'a str) -> &'a str {
        physical.get(self.base.len()..).unwrap_or_default()
    }
}

#[derive(Clone)]
pub struct RecordStore {
    main_store: ReadStore,
    pub(crate) format: Arc<RecordFormat>,
    runtime: Arc<PackRuntime>,
    serving_replay_cutoff: Arc<std::sync::OnceLock<u64>>,
    node_key: [u8; 32],
}

impl RecordStore {
    pub(crate) fn new(format: RecordFormat, node_key: [u8; 32]) -> Self {
        Self {
            main_store: format.reader(),
            runtime: format.runtime(),
            format: Arc::new(format),
            serving_replay_cutoff: Arc::default(),
            node_key,
        }
    }

    pub fn key_comparison_context(
        &self,
        schema: &Schema,
    ) -> Result<Arc<KeyComparisonContext>, SchemaError> {
        Ok(Arc::new(KeyComparisonContext::for_schema(schema)?))
    }

    pub(crate) fn primary_codec(&self, schema: &Schema) -> Result<FieldKeyCodec, SchemaError> {
        let p = schema
            .require_record_keys()?
            .primary
            .range_profile()
            .map_err(invalid)?;
        Ok(FieldKeyCodec::from_node_key(
            &self.node_key,
            &record_space_uuid(&schema.name),
            p,
        ))
    }

    pub(crate) fn reader(&self) -> &ReadStore {
        &self.main_store
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
        self.format.flush().await
    }

    pub(crate) fn in_write_scope<'a, T: Send + 'a>(
        &'a self,
        work: impl Future<Output = Result<T, SchemaError>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, SchemaError>> {
        transaction::scope(self.runtime.clone(), work).boxed()
    }

    pub(super) async fn record_read_scope<T>(
        &self,
        future: impl Future<Output = Result<T, SchemaError>>,
    ) -> Result<T, SchemaError> {
        PackReadScope::for_runtime(&self.runtime)
            .await?
            .run_result(future)
            .await
    }

    pub(crate) async fn minimum_replay_time(&self) -> StorageResult<u64> {
        if let Some(&cutoff) = self.serving_replay_cutoff.get() {
            return Ok(cutoff);
        }
        self.read_minimum_replay_time().await
    }

    pub(crate) async fn freeze_replay_cutoff(&self) -> StorageResult<()> {
        self.runtime.begin_serving();
        let cutoff = self.read_minimum_replay_time().await?;
        if self.serving_replay_cutoff.set(cutoff).is_err()
            && self.serving_replay_cutoff.get() != Some(&cutoff)
        {
            return Err(StorageError::InvalidOperation(
                "delete replay cutoff: serving replay floor changed after startup".into(),
            ));
        }
        Ok(())
    }

    async fn read_minimum_replay_time(&self) -> StorageResult<u64> {
        let keys = [crate::storage::format::CURRENT_KEY_FORMAT_KEY, CUTOFF_KEY]
            .into_iter()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        let values = self.main_store.get_many(keys).await?;
        let [first, second] = values.as_slice() else {
            return Err(StorageError::InvalidOperation(
                "delete replay cutoff: incomplete fence read".into(),
            ));
        };
        validate_frozen_replay_floor(first.as_deref(), second.as_deref())
    }

    pub(crate) fn record_span(
        &self,
        schema: &Schema,
        hash: &str,
        bound: RecordBound<'_>,
        from_range: Option<&str>,
    ) -> Result<Option<RecordSpan>, SchemaError> {
        let space = record_space_uuid(&schema.name);
        let codec = self.primary_codec(schema)?;
        let base = record_hash_prefix(&space, &codec.storage_hash(&space, hash).map_err(invalid)?);
        let stored = |r: &str| {
            codec
                .storage_range(&space, r)
                .map(|s| format!("{base}{s}"))
                .map_err(invalid)
        };
        let (mut start, end) = match bound {
            RecordBound::All => (base.clone(), FilterUtils::create_prefix_end(&base)?),
            RecordBound::Prefix(prefix) => {
                codec.require_text_range_prefix().map_err(invalid)?;
                let s = stored(prefix)?;
                let e = FilterUtils::create_prefix_end(&s)?;
                (s, e)
            }
            RecordBound::Between(s, e) => (stored(s)?, stored(e)?),
        };
        if let Some(from) = from_range.and_then(|r| stored(r).ok()) {
            start = start.max(from);
        }
        Ok((start < end).then(|| RecordSpan {
            hash: hash.to_owned(),
            codec,
            base,
            start,
            end,
        }))
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
        RecordAddress::new(
            &self.primary_codec(schema)?,
            &record_space_uuid(&schema.name),
            key,
        )
    }

    pub(crate) async fn select_alternate_entries(
        &self,
        schema: &Schema,
        name: &str,
        filter: &HashRangeFilter,
        after: Option<&AlternateEntry>,
        limit: usize,
    ) -> Result<Vec<AlternateEntry>, SchemaError> {
        if limit > MAX_ALTERNATE_ENTRIES {
            return Err(invalid("alternate page exceeds 4096 entries"));
        }
        let def = schema
            .require_record_keys()?
            .alternates
            .get(name)
            .ok_or_else(|| invalid("alternate key is not declared"))?;
        let id = alternate_field_key_id(schema, name);
        let codec = FieldKeyCodec::from_node_key(
            &self.node_key,
            &id,
            def.range_profile().map_err(invalid)?,
        );
        let storage_key = |k: &KeyValue, primary: &KeyValue| {
            AlternateAddress::new(&codec, &id, k, &self.record_address(schema, primary)?)
                .map(AlternateAddress::into_string)
        };
        let (mut start, end) = alternate_bounds(&codec, &id, filter)?;
        if let Some(after) = after {
            let cursor = storage_key(&after.matched, &after.primary)?;
            if cursor < start || cursor >= end {
                return Err(invalid(
                    "alternate cursor is outside the requested key range",
                ));
            }
            start = format!("{cursor}\0");
        }
        self.reader()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), limit)
            .await?
            .into_iter()
            .map(|(k, v)| {
                let e: AlternateEntry =
                    serde_json::from_slice(&v).map_err(|_| invalid("invalid alternate entry"))?;
                (storage_key(&e.matched, &e.primary)?.as_bytes() == k)
                    .then_some(e)
                    .ok_or_else(|| {
                        invalid("alternate entry coordinates differ from their storage key")
                    })
            })
            .collect()
    }

    pub(crate) async fn read_records<K: AsRef<str> + Sync>(
        &self,
        keys: &[K],
    ) -> Result<Vec<Option<Arc<StoredRecord>>>, SchemaError> {
        self.record_read_scope(
            async move {
                let mut warm = self
                    .runtime
                    .warm_records(keys)
                    .map_err(|e| invalid(format!("read records: {e}")))?;
                let mut out = match &mut warm {
                    Some(r) => r.take_found(),
                    None => vec![None; keys.len()],
                };
                let misses: Vec<usize> = (0..keys.len()).filter(|&at| out[at].is_none()).collect();
                let mut admitted = Vec::new();
                for chunk in misses.chunks(10_000) {
                    let req = chunk
                        .iter()
                        .map(|&at| keys[at].as_ref().as_bytes().to_vec())
                        .collect();
                    let rows = self
                        .main_store
                        .get_many(req)
                        .await
                        .map_err(|e| invalid(format!("read records: {e}")))?;
                    if rows.len() != chunk.len() {
                        return Err(invalid("the record read batch is incomplete"));
                    }
                    for (&at, row) in chunk.iter().zip(rows) {
                        let Some(bytes) = row else { continue };
                        let record = Arc::new(reads::decode(&bytes)?);
                        if warm.is_some() {
                            admitted.push((at, Arc::clone(&record), bytes.len()));
                        }
                        out[at] = Some(record);
                    }
                }
                if let Some(read) = &warm {
                    self.runtime.admit_read_records(read, admitted);
                }
                Ok(out)
            }
            .boxed(),
        )
        .await
    }

    pub fn seal_record_file(
        &self,
        plaintext: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        self.format.seal_record_file(plaintext, name, media_type)
    }

    pub async fn reference_local_file(
        &self,
        path: &std::path::Path,
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        self.format
            .reference_local_file(path, name, media_type)
            .await
    }
}

pub(super) fn invalid(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(error.to_string())
}
