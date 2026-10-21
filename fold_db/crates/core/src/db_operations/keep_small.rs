//! Incremental keep-small meters: live budget, per-schema churn, atom histogram.
//!
//! These are write-path totals (point read of a small in-process projection),
//! not another store walk. `lastdb db inventory` is the wrong daily tool.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
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
    /// `only_schemas = None` emits a shard for every schema (repair, first
    /// split of a legacy whole-map row). `Some(set)` emits only those names,
    /// so a persist of one dirty schema does not rewrite the rest.
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
        for (molecule, counter) in self.molecules.drain() {
            let owner = schema_owner_for_molecule(&molecule, &self.molecule_schema);
            if emit(&owner) {
                take_bucket(&mut buckets, &owner)
                    .molecules
                    .insert(molecule, counter);
            }
        }
        let molecule_schema = std::mem::take(&mut self.molecule_schema);
        for (molecule, schema) in molecule_schema {
            let owner = if schema.is_empty() {
                KEEP_SMALL_UNATTRIBUTED_SCHEMA.to_string()
            } else {
                schema.clone()
            };
            if emit(&owner) {
                take_bucket(&mut buckets, &owner)
                    .molecule_schema
                    .insert(molecule, schema);
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
        self.sharded = true;
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
        self.molecules.extend(shard.molecules);
        self.molecule_schema.extend(shard.molecule_schema);
    }
}

/// The LastStore collection that holds [`KEEP_SMALL_SNAPSHOT_KEY`].
///
/// Its own plane since 2026-09-21, when the snapshot's former home
/// (`metadata`, one hash group, no automatic compaction) reached 39 GB of
/// superseded copies and looped the primary. Node-local, rebuildable
/// bookkeeping: capture-skipped, backup-excluded, compact-allowlisted and
/// swept by the residual self-compactor. The key name is unchanged so the
/// capture-skip exact-key contract for the legacy row still applies.
pub const KEEP_SMALL_SNAPSHOT_COLLECTION: &str = "keep_small";

type PendingAtomContribution = (String, u64, u64, bool, bool);

/// In-process incremental projection. Point-read cheap; updated on the write path.
#[derive(Debug)]
pub struct KeepSmallMeters {
    atom_bytes: AtomicU64,
    tip_bytes: AtomicU64,
    bookkeeping_bytes: AtomicU64,
    atom_count: AtomicU64,
    tip_count: AtomicU64,
    schemas: Mutex<HashMap<String, SchemaMeter>>,
    molecules: Mutex<HashMap<String, MoleculeStorageCounter>>,
    molecule_counters_complete: AtomicBool,
    /// Atom uuid → (schema, atom bytes, blob bytes, complete, body put credited).
    pending_atoms: Mutex<HashMap<String, PendingAtomContribution>>,
    // No per-key state. The tip planner reads the old tip of every changed key
    // and the size of its atom (from the atom counter) inside the write gate,
    // and passes them in (`TipCommit::previous`). A per-key map here held one
    // entry for each tip key of the home, outside the warm set limit
    // (measured 2026-10-20: about 2.6 KB for each new record).
    /// molecule uuid → owning schema, learned from tip puts.
    ///
    /// A molecule belongs to exactly one field of one schema, so its tip and
    /// bookkeeping rows belong to that schema too. This map is the only thing
    /// that carries that fact to the meters, because the tip/bookkeeping keys
    /// themselves hold no schema name.
    ///
    /// It is deliberately **not** part of [`KeepSmallSnapshot`]: it holds one
    /// entry per written molecule, and persisting it would turn a small
    /// point-read projection into a per-molecule table. After a restart the
    /// first tip put re-learns the binding, so attribution resumes on the next
    /// write instead of being carried across process lifetimes.
    molecule_schema: Mutex<HashMap<String, String>>,
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
    /// (`clean_stop != Some(true)`): metered writes landed after the last
    /// debounced persist and the process stopped without the shutdown flush
    /// (2026-09-21 design — the debounce accepts that staleness, the report
    /// must not call it exact). Cleared only by
    /// [`Self::install_molecule_counter_bootstrap`], the same repair a
    /// hydrate miss takes.
    stale_after_unclean_stop: AtomicBool,
    trust: Mutex<MeterTrustPayload>,
    /// Schemas whose durable shard is behind the live projection.
    dirty_schemas: Mutex<HashSet<String>>,
}

impl Default for KeepSmallMeters {
    fn default() -> Self {
        Self {
            atom_bytes: AtomicU64::new(0),
            tip_bytes: AtomicU64::new(0),
            bookkeeping_bytes: AtomicU64::new(0),
            atom_count: AtomicU64::new(0),
            tip_count: AtomicU64::new(0),
            schemas: Mutex::new(HashMap::new()),
            molecules: Mutex::new(HashMap::new()),
            molecule_counters_complete: AtomicBool::new(true),
            pending_atoms: Mutex::new(HashMap::new()),
            molecule_schema: Mutex::new(HashMap::new()),
            hydrate_missed: AtomicBool::new(false),
            stale_after_unclean_stop: AtomicBool::new(false),
            trust: Mutex::new(MeterTrustPayload::trusted()),
            dirty_schemas: Mutex::new(HashSet::new()),
        }
    }
}

mod atoms;
mod molecules;
mod schemas;
mod snapshot_io;
mod tips;
mod trust;
