//! Record domain store: key codecs, paged hash spans, write/read scopes, replay fence, and writer watch.

mod accepted;
mod reads;
mod writes;

pub(crate) use reads::{KeyedRecord, RangeWalk};
pub(crate) use writes::RecordChange;
pub use writes::{RecordDeleteLedgerEntry, LEDGER_VERB_DELETE, LEDGER_VERB_PURGE};

use crate::schema::types::field::FilterUtils;
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::{KeyValue, Schema, SchemaError};
use crate::storage::disk_pack::transaction::blocked::write_scopes;
use crate::storage::disk_pack::PackRuntime;
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::format::control::{
    validate_frozen_replay_floor, DELETE_REPLAY_CUTOFF_KEY as CUTOFF_KEY,
};
use crate::storage::format::{read::ReadStore, records::RecordFormat};
use futures::{future::BoxFuture, FutureExt};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::record_space::{record_hash_prefix, record_space_uuid};
use lastdb_model::record_value::StoredRecord;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const QUIET_SECS_ENV: &str = "LASTDB_MAINTENANCE_QUIET_SECS";
pub const DEFAULT_QUIET_SECS: u64 = 60;

/// Tracks other tasks' write scopes so background maintenance yields when writers are active.
#[derive(Debug)]
pub struct WriterWatch {
    mark: u64,
    last_other: Option<Instant>,
    quiet: Duration,
}

impl Default for WriterWatch {
    fn default() -> Self {
        Self::new()
    }
}

impl WriterWatch {
    pub async fn measure<F: std::future::Future>(work: F) -> (F::Output, Duration, u64) {
        let (output, cost) = crate::storage::disk_pack::transaction::blocked::measure(work).await;
        (output, cost.blocked, cost.scopes)
    }

    #[must_use]
    pub fn new() -> Self {
        Self {
            mark: write_scopes(),
            last_other: None,
            quiet: Duration::from_secs(crate::env_flag::var_or(QUIET_SECS_ENV, DEFAULT_QUIET_SECS)),
        }
    }

    pub fn writer_present(&mut self, own: u64) -> bool {
        let now = write_scopes();
        let others = now.saturating_sub(self.mark).saturating_sub(own);
        self.mark = now;
        if others > 0 {
            self.last_other = Some(Instant::now());
        }
        others > 0 || self.last_other.is_some_and(|at| at.elapsed() < self.quiet)
    }

    pub fn rest_may_end(&mut self) -> bool {
        !self.writer_present(0)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RecordBound<'a> {
    All,
    Prefix(&'a str),
    Between(&'a str, &'a str),
}

pub(crate) struct RecordSpan {
    hash: String,
    codec: FieldKeyCodec,
    base: String,
    start: String,
    end: String,
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

/// Record store for a home: one record at each primary key in the `main` namespace.
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
        let profile = schema
            .require_record_keys()?
            .primary
            .range_profile()
            .map_err(SchemaError::InvalidData)?;
        Ok(FieldKeyCodec::from_node_key(
            &self.node_key,
            &record_space_uuid(&schema.name),
            profile,
        ))
    }

    pub(crate) fn alternate_codec(
        &self,
        schema: &Schema,
        name: &str,
    ) -> Result<FieldKeyCodec, SchemaError> {
        let profile = schema
            .require_record_keys()?
            .alternates
            .get(name)
            .ok_or_else(|| SchemaError::InvalidData("alternate key is not declared".into()))?
            .range_profile()
            .map_err(SchemaError::InvalidData)?;
        let id = lastdb_model::field_key_id::alternate_field_key_id(schema, name);
        Ok(FieldKeyCodec::from_node_key(&self.node_key, &id, profile))
    }

    pub(crate) fn reader(&self) -> &ReadStore {
        &self.main_store
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
        self.format.flush().await
    }

    pub(crate) fn in_write_scope<'a, T: Send + 'a>(
        &'a self,
        work: impl std::future::Future<Output = Result<T, SchemaError>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, SchemaError>> {
        crate::storage::disk_pack::transaction::scope(self.runtime.clone(), work).boxed()
    }

    pub(super) async fn record_read_scope<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, SchemaError>>,
    ) -> Result<T, SchemaError> {
        crate::storage::disk_pack::PackReadScope::for_runtime(&self.runtime)
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
        let stored = |range: &str| {
            codec
                .storage_range(&space, range)
                .map(|r| format!("{base}{r}"))
                .map_err(invalid)
        };
        let (mut start, end) = match bound {
            RecordBound::All => (base.clone(), FilterUtils::create_prefix_end(&base)?),
            RecordBound::Prefix(prefix) => {
                codec.require_text_range_prefix().map_err(invalid)?;
                let start = stored(prefix)?;
                let end = FilterUtils::create_prefix_end(&start)?;
                (start, end)
            }
            RecordBound::Between(start, end) => (stored(start)?, stored(end)?),
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

    pub(crate) async fn record_span_page(
        &self,
        span: &RecordSpan,
        after: Option<&str>,
        limit: usize,
    ) -> Result<SpanPage, SchemaError> {
        let start = after.map_or_else(|| span.start.clone(), |c| format!("{c}\0"));
        let page = self
            .main_store
            .scan_range_yielding(start.as_bytes(), span.end.as_bytes(), limit, 2)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read record keys: {e}")))?;
        let text =
            |k: Vec<u8>| String::from_utf8(k).map_err(|_| invalid("a record key is not text"));
        let keys = page
            .rows
            .into_iter()
            .map(|(physical, value)| {
                let physical = text(physical)?;
                Ok(SpanKey {
                    key: self.span_key(span, &physical)?,
                    range: physical[span.base.len()..].to_owned(),
                    physical,
                    record: reads::decode(&value)?,
                })
            })
            .collect::<Result<_, SchemaError>>()?;
        Ok(SpanPage {
            keys,
            next: page.resume.map(text).transpose()?,
        })
    }

    pub(crate) fn span_key(
        &self,
        span: &RecordSpan,
        physical: &str,
    ) -> Result<KeyValue, SchemaError> {
        let range = physical
            .get(span.base.len()..)
            .ok_or_else(|| invalid("a record key has no range"))?;
        Ok(KeyValue::new(
            Some(span.hash.clone()),
            Some(span.codec.api_range(range).map_err(invalid)?),
        ))
    }
}

fn invalid(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(error.to_string())
}
