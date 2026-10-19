//! CAS lock keys and purge statistics.

use std::collections::HashMap;
use std::sync::Arc;

use crate::schema::types::{KeyValue, Mutation};

use super::MutationManager;

impl MutationManager {
    /// The per-key CAS lock identity for a mutation: `{schema}\u{1f}{key}`.
    /// The schema name and the lossless key encoding are joined by an ASCII
    /// unit separator so distinct `(schema, key)` pairs can never collide.
    pub(super) fn cas_lock_key(schema_name: &str, key_value: &KeyValue) -> String {
        // ASCII unit separator (U+001F) can't appear in a schema name, so the
        // join is unambiguous.
        format!("{schema_name}\u{1f}{}", key_value.to_storage_key())
    }

    /// Take the per-key CAS lock for every CAS mutation in the batch and return
    /// the held guards (they release when the returned vec drops — i.e. when
    /// the write function returns). Keys are locked in a deterministic sorted
    /// order so two batches touching an overlapping key set can never deadlock.
    /// Batches with neither a CAS expectation nor a `must_exist` update
    /// acquire nothing and pay only two cheap predicate checks per mutation.
    pub(super) async fn acquire_cas_locks(
        &self,
        mutations: &[Mutation],
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        // Collect the distinct lock keys this batch needs, then sort them.
        // `must_exist` updates lock too: they are a check-then-set on the same
        // key state, so without the lock a concurrent delete between the
        // presence read and the write reopens the phantom-row hole the flag
        // exists to close.
        let mut lock_keys: Vec<String> = mutations
            .iter()
            .filter(|m| m.expected.is_some() || Self::is_must_exist_update(m))
            .map(|m| Self::cas_lock_key(&m.schema_name, &m.key_value))
            .collect();
        if lock_keys.is_empty() {
            return Vec::new();
        }
        lock_keys.sort_unstable();
        lock_keys.dedup();

        // Resolve each key to a shared `Arc<Mutex>` under the map guard, then
        // await the lock OUTSIDE the map guard (a std::sync::Mutex must not be
        // held across an await).
        let mut guards = Vec::with_capacity(lock_keys.len());
        for lock_key in lock_keys {
            let mutex = {
                let mut map = self.cas_locks.lock().expect("cas_locks poisoned");
                Arc::clone(map.entry(lock_key).or_default())
            };
            guards.push(mutex.lock_owned().await);
        }
        guards
    }

    /// Record one completed erase pass against `schema_name`.
    pub(super) fn record_purge(&self, schema_name: &str, records_purged: u64) {
        self.purge_stats
            .record(schema_name, records_purged, crate::clock::unix_millis());
    }

    pub(super) fn record_purge_path(
        &self,
        schema_name: &str,
        target_slots: u64,
        atoms_deleted: u64,
    ) {
        self.purge_stats
            .record_path(schema_name, target_slots, atoms_deleted);
    }

    /// Snapshot per-schema purge accounting for the status/ops surfaces.
    /// Cumulative since process start, so consumers delta it themselves —
    /// matching how the request-ops aggregates are consumed.
    #[must_use]
    pub fn purge_stats_snapshot(&self) -> HashMap<String, super::PurgeStats> {
        self.purge_stats.snapshot()
    }

    /// One schema's cumulative purge totals.
    /// All-zero means no purge has run on this schema since process start.
    #[must_use]
    pub fn purge_stats_for_schema(&self, schema_name: &str) -> super::PurgeStats {
        self.purge_stats.for_schema(schema_name)
    }
}
