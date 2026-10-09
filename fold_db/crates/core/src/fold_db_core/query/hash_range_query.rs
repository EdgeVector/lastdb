//! HashRange Query Processor
//!
//! **Single co-key plan for every multi-field (and single-field) query:**
//! 1. Primary field establishes the live key set under the request filter
//!    (and optional share namespaces / `as_of`).
//! 2. Secondary fields resolve only those keys (batch slot load today;
//!    full hydrate + rewind when `as_of` is set).
//! 3. Secondary atom bodies are fetched in one `get_many` (zipped by index
//!    so fields that share keys do not clobber each other).
//!
//! There is no separate "legacy per-field loop" path.
//!
//! Share namespaces: the processor also scans `from:{sender_hash}:` for each
//! active `ShareSubscription` so data received from other users surfaces
//! alongside the caller's own data. Personal keys win on collision; shared
//! rows get `writer_pubkey` stamped with the sender when unset.

use crate::db_operations::ChangeFeedEvent;
use crate::db_operations::DbOperations;
use crate::schema::types::field::{FieldValue, HashRangeFilter, KeyWindow};
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::operations::{FieldPredicate, QueryOrderBy, SortOrder};
use crate::schema::{Schema, SchemaError};
use chrono::{DateTime, Utc};
use futures::{stream, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

use super::formatter::records_from_field_map;

mod cokey;
mod count;
mod fields;
mod filter;
mod paging;
mod predicates;
mod resolve;
mod secondary;
mod shares;
mod watch;

use predicates::*;
pub use watch::*;

/// Production defaults to serial field reads. A value above one opts in to
/// bounded overlap for current-head queries only.
fn secondary_field_concurrency(value: Option<&str>, request: Option<usize>) -> usize {
    request
        .or_else(|| value.and_then(|value| value.parse::<usize>().ok()))
        .filter(|value| *value > 0)
        .unwrap_or(1)
        .min(8)
}

/// Log-safe `Option<HashRangeFilter>`: `None`, or the filter's
/// [`HashRangeFilter::redacted`] view.
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
            Some(filter) => write!(f, "Some({})", filter.redacted()),
            None => f.write_str("None"),
        }
    }
}

/// A [`HashRangeFilter::Page`] spanning the entire field — `offset 0`,
/// `limit usize::MAX`. Used by [`HashRangeQueryProcessor::count_rows`] to
/// materialize every key so the live (non-tombstone) row count is exact. An
/// unfiltered (`None`) resolve would instead apply the field's default page
/// cap (`DEFAULT_UNFILTERED_PAGE_LIMIT`, historically 100) and silently
/// under-count any schema with more than that many rows.
fn full_span_page() -> HashRangeFilter {
    HashRangeFilter::Page {
        offset: 0,
        limit: usize::MAX,
    }
}

/// Where a primary key's data lives: personal (`None`) or an org/share storage
/// prefix (`Some`).
type KeySource = Option<String>;

/// Secondary pending atom resolve:
/// `(field, key, uuid, meta, writer, atom prefix, partition_hint)`.
type SecondaryPending = (
    String,
    KeyValue,
    String,
    Option<crate::atom::KeyMetadata>,
    Option<String>,
    Option<String>,
    Option<crate::atom::AtomPartition>,
);

/// A resolved page. Every `KeyValue` in here is **API-form**: the rename in
/// [`HashRangeQueryProcessor::rename_page_to_api_keys`] happens once, on the
/// primary, before anything else sees the page — so callers, secondaries and
/// the emitted response all agree on one key space.
struct CokeyRows {
    fields: HashMap<String, HashMap<KeyValue, FieldValue>>,
    page_keys: Vec<KeyValue>,
    key_sources: HashMap<KeyValue, KeySource>,
}

/// Processor for HashRange schema queries using field resolution
pub struct HashRangeQueryProcessor {
    db_ops: Arc<DbOperations>,
}

impl HashRangeQueryProcessor {
    /// Create a new HashRange query processor.
    pub fn new(db_ops: Arc<DbOperations>) -> Self {
        Self { db_ops }
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
