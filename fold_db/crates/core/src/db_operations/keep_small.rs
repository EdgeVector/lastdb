//! Incremental keep-small meters: live budget and per-schema churn.
//!
//! These are write-path totals (point read of a small in-process projection),
//! not another store walk. `lastdb db inventory` is the wrong daily tool.

use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
};

mod planes;
mod snapshot_rows;
mod types;

pub use snapshot_rows::*;
pub use types::*;

impl KeepSmallSnapshot {
    /// Split per-schema maps into shards and leave a small header behind.
    ///
    /// `only_schemas = None` emits a shard for every schema (repair).
    /// `Some(set)` emits only those names, so a persist of one dirty schema
    /// does not rewrite the rest.
    #[must_use]
    pub fn into_header_and_shards(
        mut self,
        only_schemas: Option<&HashSet<String>>,
    ) -> (Self, Vec<(String, KeepSmallSchemaShard)>) {
        let emit_all = only_schemas.is_none();
        let emit = |name: &str| emit_all || only_schemas.is_some_and(|set| set.contains(name));

        let mut buckets: HashMap<String, KeepSmallSchemaShard> = HashMap::new();
        fn take_bucket<'a>(
            buckets: &'a mut HashMap<String, KeepSmallSchemaShard>,
            name: &str,
        ) -> &'a mut KeepSmallSchemaShard {
            buckets.entry(name.to_string()).or_default()
        }

        for (name, meter) in self.schemas.drain() {
            if emit(&name) {
                take_bucket(&mut buckets, &name).schema = Some(meter);
            }
        }
        for (name, domain) in std::mem::take(&mut self.trust.schemas) {
            if emit(&name) {
                take_bucket(&mut buckets, &name).trust = Some(domain);
            }
        }
        if let Some(set) = only_schemas {
            for name in set {
                buckets.entry(name.clone()).or_default();
            }
        }

        let mut shards: Vec<(String, KeepSmallSchemaShard)> = buckets
            .into_iter()
            .map(|(name, shard)| (keep_small_schema_shard_key(&name), shard))
            .collect();
        shards.sort_by(|a, b| a.0.cmp(&b.0));
        (self, shards)
    }

    /// Fold one shard back into a full in-memory snapshot.
    pub fn merge_shard(&mut self, shard: KeepSmallSchemaShard) {
        let schema_name = shard.schema.as_ref().map(|meter| meter.schema_name.clone());
        if let Some(meter) = shard.schema {
            self.schemas.insert(meter.schema_name.clone(), meter);
        }
        if let (Some(name), Some(trust)) = (schema_name, shard.trust) {
            self.trust.schemas.insert(name, trust);
        }
    }
}

/// The LastStore collection that holds [`KEEP_SMALL_SNAPSHOT_KEY`].
///
/// Node-local, rebuildable bookkeeping: capture-skipped, backup-excluded,
/// compact-allowlisted and swept by the residual self-compactor.
pub const KEEP_SMALL_SNAPSHOT_COLLECTION: &str = "keep_small";

/// In-process incremental projection. Point-read cheap; updated on the write path.
#[derive(Debug)]
pub struct KeepSmallMeters {
    schemas: Mutex<HashMap<String, SchemaMeter>>,
    // No per-key state. A per-key map here held one entry for each key of
    // the home, outside the warm set limit (measured 2026-10-20: about 2.6 KB
    // for each new record).
    /// Set when boot looked for the durable snapshot and did not find it.
    ///
    /// A miss is not the same as a measured zero. On a home that already holds
    /// data, every total below is then a floor of zero for the pre-existing
    /// corpus, and organic writes never close that gap — they only meter what
    /// they themselves write. Readers must be able to say "never measured"
    /// rather than print zero as an exact answer.
    ///
    /// Sticky for the process: a later write makes the totals nonzero without
    /// making them complete, so clearing this on the first write would restore
    /// exactly the false-exact reading it exists to prevent. Only
    /// [`Self::import`] clears it.
    hydrate_missed: AtomicBool,
    /// Set when boot hydrated a snapshot not written by a shutdown flush
    /// (`clean_stop == Some(false)`): metered writes landed after the last
    /// debounced persist and the process stopped without the shutdown flush
    /// (2026-09-21 design — the debounce accepts that staleness, the report
    /// must not call it exact). Cleared only by [`Self::mark_empty_home_absent`].
    stale_after_unclean_stop: AtomicBool,
    trust: Mutex<MeterTrustPayload>,
    /// Schemas whose durable shard is behind the live projection.
    dirty_schemas: Mutex<HashSet<String>>,
}

impl Default for KeepSmallMeters {
    fn default() -> Self {
        Self {
            schemas: Mutex::new(HashMap::new()),
            hydrate_missed: AtomicBool::new(false),
            stale_after_unclean_stop: AtomicBool::new(false),
            trust: Mutex::new(MeterTrustPayload::trusted()),
            dirty_schemas: Mutex::new(HashSet::new()),
        }
    }
}

mod schemas;
mod snapshot_io;
mod trust;
