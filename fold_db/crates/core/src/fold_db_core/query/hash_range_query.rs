//! HashRange Query Processor
//!
//! A format 7 home keeps one record at each primary key. A query reads the
//! records that its key selector names (`records.rs`), applies the
//! conditions, the order and the window to whole records, then returns the
//! selected fields of each record.
//!
//! Share namespaces: while the node receives data from other users, the read
//! merges each active `from:{sender_hash}` scope with the own scope by key.
//! The own record wins on a collision (`records/received.rs`).

use crate::db_operations::ChangeFeedEvent;
use crate::db_operations::DbOperations;
use crate::schema::types::field::{FieldValue, HashRangeFilter, KeyWindow};
use crate::schema::types::key_value::KeyValue;
use crate::schema::{Schema, SchemaError};
use futures::FutureExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

mod condition_rules;
mod count;
mod field_rules;
mod filter;
mod predicates;
mod records;
mod redacted_filter;
mod shares;
mod watch;

pub use condition_rules::FieldQueryOptions;
pub(crate) use shares::preload_receiver_controls;
pub use watch::*;

/// Log-safe `Option<HashRangeFilter>`: `None`, or the filter's
/// [`redacted_filter::RedactedHashRangeFilter`] view.
///
/// Exists so the query log lines keep the `Some(..)`/`None` shape they always
/// had while the key material inside goes through `redact_id!`. Printing the
/// filter with `{:?}` here used to write the user's own keys — brain slugs,
/// card slugs, search terms — verbatim into a plaintext log file, on a node
/// whose database is encrypted at rest. The format-time deny-list cannot catch
/// that: it matches on *field names*, and anything interpolated into a tracing
/// macro's message is one opaque `message` field by the time it is seen.
struct OptRedactedFilter<'a>(&'a Option<HashRangeFilter>);

impl std::fmt::Display for OptRedactedFilter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(filter) => write!(
                f,
                "Some({})",
                redacted_filter::RedactedHashRangeFilter(filter)
            ),
            None => f.write_str("None"),
        }
    }
}

/// A [`HashRangeFilter::Page`] spanning every record — `offset 0`,
/// `limit usize::MAX`. Used by [`HashRangeQueryProcessor::count_rows`] and
/// the condition pass to read every candidate record. An unfiltered (`None`)
/// read would instead return one page of the default size
/// (`DEFAULT_UNFILTERED_PAGE_LIMIT`) and under-count any larger schema.
fn full_span_page() -> HashRangeFilter {
    HashRangeFilter::Page {
        offset: 0,
        limit: usize::MAX,
    }
}

/// The candidate rows of the condition pass: the condition fields of each
/// record, and the record keys in page order.
struct CokeyRows {
    fields: HashMap<String, HashMap<KeyValue, FieldValue>>,
    page_keys: Vec<KeyValue>,
}

/// Processor for record queries of a format 7 home.
pub struct HashRangeQueryProcessor {
    db_ops: Arc<DbOperations>,
}

impl HashRangeQueryProcessor {
    /// Create a new HashRange query processor.
    pub fn new(db_ops: Arc<DbOperations>) -> Self {
        Self { db_ops }
    }

    fn require_supported_query(
        schema: &Schema,
        filter: Option<&HashRangeFilter>,
    ) -> Result<(), SchemaError> {
        crate::schema::require_logical_layout(&schema.schema_type)?;
        crate::schema::require_logical_filter(filter)
    }

    async fn current_query_scope<T>(
        &self,
        comparison: Arc<crate::schema::types::key_comparison::KeyComparisonContext>,
        future: impl std::future::Future<Output = Result<T, SchemaError>>,
    ) -> Result<T, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        comparison.run(scope.run_result(future)).await
    }

    /// Start a live watch for one HashRange partition.
    ///
    /// The watcher receives only committed create, update, and delete events
    /// for `schema` and `hash`. The change-feed sequence is the commit order;
    /// unrelated schemas, hashes, and ranges stay out of the stream.
    pub fn watch(
        &self,
        schema: impl Into<String>,
        hash: impl Into<String>,
        bounds: Option<HashRangeWatchBounds>,
    ) -> HashRangeWatch {
        HashRangeWatch {
            schema: schema.into(),
            hash: hash.into(),
            bounds: bounds.unwrap_or_default(),
            receiver: self.db_ops.change_feed().subscribe(),
            db_ops: Arc::clone(&self.db_ops),
            comparison: None,
        }
    }

    /// Alias for callers that name the operation after its query shape.
    pub fn watch_hash_range(
        &self,
        schema: impl Into<String>,
        hash: impl Into<String>,
        bounds: Option<HashRangeWatchBounds>,
    ) -> HashRangeWatch {
        self.watch(schema, hash, bounds)
    }

    /// Start a watch without range bounds.
    pub fn watch_partition(
        &self,
        schema: impl Into<String>,
        hash: impl Into<String>,
    ) -> HashRangeWatch {
        self.watch(schema, hash, None)
    }

    /// Start a watch with inclusive-start/exclusive-end range bounds.
    pub fn watch_range(
        &self,
        schema: impl Into<String>,
        hash: impl Into<String>,
        start: Option<String>,
        end: Option<String>,
    ) -> HashRangeWatch {
        self.watch(schema, hash, Some(HashRangeWatchBounds::new(start, end)))
    }
}
