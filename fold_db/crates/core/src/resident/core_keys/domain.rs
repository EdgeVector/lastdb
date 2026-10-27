//! Domain allocation, neutral membership deltas, and readiness lifetime.

use super::{
    CompactRangeKey, CoreKeyBudget, CoreKeyDomain, CoreKeyDomainStats, CoreKeyError,
    CoreKeyIdentity, CoreKeyLease, CoreKeyPhase, DomainState, KeyVisibility, MoleculeKeys,
    RangeKeyEncoding,
};
use std::sync::{Arc, Mutex};

// Conservative high-water accounting covers node slots, pointers, allocator
// overhead, and slack at B-tree occupancy boundaries. The domain reserve also
// covers transient root/node splits. Hash charges cover hash-map resize peaks.
const DOMAIN_OVERHEAD: usize = 4096;
const TREE_ENTRY_OVERHEAD: usize = 256;
const HASH_OVERHEAD: usize = 1024;

impl CoreKeyDomain {
    pub(crate) fn new_loading(
        identity: CoreKeyIdentity,
        range_encoding: RangeKeyEncoding,
        range_profile: Option<schema_types::molecule_range_profile::MoleculeRangeProfile>,
        budget: Arc<CoreKeyBudget>,
    ) -> Result<Arc<Self>, CoreKeyError> {
        let bytes = identity
            .allocation_bytes()
            .checked_add(DOMAIN_OVERHEAD)
            .ok_or(CoreKeyError::Capacity)?;
        let charge = budget.reserve_temporary(bytes)?;
        Ok(Arc::new(Self {
            identity,
            range_encoding,
            range_profile,
            budget,
            state: Mutex::new(DomainState {
                phase: CoreKeyPhase::Loading,
                reason: None,
                keys: MoleculeKeys::default(),
                charge,
                live_key_bytes: 0,
                key_bytes_high_water: 0,
                tree_entries_high_water: 0,
            }),
        }))
    }

    pub(crate) fn identity(&self) -> &CoreKeyIdentity {
        &self.identity
    }

    pub(crate) fn range_encoding(&self) -> RangeKeyEncoding {
        self.range_encoding
    }

    pub(crate) fn accepts_stored_range(&self, range: &str) -> bool {
        self.range_profile
            .is_none_or(|profile| profile.decode_value(range).is_ok())
    }

    /// The caller owns the common publication lock for accepted write deltas.
    /// A failed allocation clears readiness before that write can be visible.
    pub(crate) fn insert_batch(
        &self,
        rows: &[(String, String, KeyVisibility)],
    ) -> Result<usize, CoreKeyError> {
        let mut state = self.state.lock().map_err(|_| CoreKeyError::Poisoned)?;
        accepts_changes(state.phase)?;
        let mut inserted = 0;
        for (hash, range, visibility) in rows {
            match self.insert_one(&mut state, hash, range, *visibility) {
                Ok(new) => inserted += usize::from(new),
                Err(error) => {
                    state.phase = CoreKeyPhase::DiskOnly;
                    state.reason = Some("key_capacity");
                    return Err(error);
                }
            }
        }
        Ok(inserted)
    }

    fn insert_one(
        &self,
        state: &mut DomainState,
        hash: &str,
        range: &str,
        visibility: KeyVisibility,
    ) -> Result<bool, CoreKeyError> {
        let allocation = CompactRangeKey::allocation_bound(range, self.range_encoding);
        let scratch = allocation.checked_mul(2).ok_or(CoreKeyError::Capacity)?;
        let _temporary = self.budget.reserve_temporary(scratch)?;
        let key = CompactRangeKey::from_storage(range, self.range_encoding);
        if !state.keys.contains(hash, &key) {
            let next_key_bytes = state
                .live_key_bytes
                .checked_add(allocation)
                .ok_or(CoreKeyError::Capacity)?;
            let next_entries = state
                .keys
                .len()
                .checked_add(1)
                .ok_or(CoreKeyError::Capacity)?;
            let hash_charge = if state.keys.contains_hash(hash) {
                0
            } else {
                hash.len().saturating_add(HASH_OVERHEAD)
            };
            // Keep separate high-water marks for tree slots and allocated key
            // bytes. A delete releases the key bytes but may leave tree space;
            // later inserts reuse each allowance instead of charging forever.
            let bytes = next_entries
                .saturating_sub(state.tree_entries_high_water)
                .checked_mul(TREE_ENTRY_OVERHEAD)
                .and_then(|value| {
                    value.checked_add(next_key_bytes.saturating_sub(state.key_bytes_high_water))
                })
                .and_then(|value| value.checked_add(hash_charge))
                .ok_or(CoreKeyError::Capacity)?;
            state.charge.grow(bytes)?;
            state.live_key_bytes = next_key_bytes;
            state.key_bytes_high_water = state.key_bytes_high_water.max(next_key_bytes);
            state.tree_entries_high_water = state.tree_entries_high_water.max(next_entries);
        }
        Ok(state.keys.insert(hash, key, visibility))
    }

    pub(crate) fn remove(&self, hash: &str, range: &str) -> Result<bool, CoreKeyError> {
        let mut state = self.state.lock().map_err(|_| CoreKeyError::Poisoned)?;
        accepts_changes(state.phase)?;
        let allocation = CompactRangeKey::allocation_bound(range, self.range_encoding);
        let temporary = self.budget.reserve_temporary(allocation.saturating_mul(2));
        let _temporary = match temporary {
            Ok(temporary) => temporary,
            Err(error) => {
                state.phase = CoreKeyPhase::DiskOnly;
                state.reason = Some("key_capacity");
                return Err(error);
            }
        };
        let key = CompactRangeKey::from_storage(range, self.range_encoding);
        let removed = state.keys.remove(hash, &key);
        if removed {
            state.live_key_bytes -= allocation;
        }
        // Keep both high-water allowances and retained hash allocations. Only
        // complete domain retirement releases them, including old read leases.
        Ok(removed)
    }

    /// Call only after source exhaustion and the final publication validation.
    pub(crate) fn mark_ready(&self) -> Result<(), CoreKeyError> {
        let mut state = self.state.lock().map_err(|_| CoreKeyError::Poisoned)?;
        if state.phase != CoreKeyPhase::Loading {
            return Err(CoreKeyError::State);
        }
        state.phase = CoreKeyPhase::Ready;
        Ok(())
    }

    /// The caller captures this lease under the same common read boundary.
    pub(crate) fn lease(self: &Arc<Self>) -> Result<Option<CoreKeyLease>, CoreKeyError> {
        let state = self.state.lock().map_err(|_| CoreKeyError::Poisoned)?;
        Ok((state.phase == CoreKeyPhase::Ready).then(|| CoreKeyLease {
            domain: Arc::clone(self),
        }))
    }

    pub(crate) fn retire(&self, reason: &'static str) -> Result<(), CoreKeyError> {
        self.set_unavailable(CoreKeyPhase::Retired, reason)
    }

    pub(crate) fn set_unavailable(
        &self,
        phase: CoreKeyPhase,
        reason: &'static str,
    ) -> Result<(), CoreKeyError> {
        if matches!(phase, CoreKeyPhase::Ready | CoreKeyPhase::Loading) {
            return Err(CoreKeyError::State);
        }
        let mut state = self.state.lock().map_err(|_| CoreKeyError::Poisoned)?;
        state.phase = phase;
        state.reason = Some(reason);
        Ok(())
    }

    pub(crate) fn stats(&self) -> Result<CoreKeyDomainStats, CoreKeyError> {
        let state = self.state.lock().map_err(|_| CoreKeyError::Poisoned)?;
        let phase = match state.phase {
            CoreKeyPhase::Loading => "loading",
            CoreKeyPhase::Ready => "ready",
            CoreKeyPhase::DiskOnly => "disk_only",
            CoreKeyPhase::Retired => "retired",
        };
        Ok(CoreKeyDomainStats {
            phase,
            reason: state.reason,
            keys: state.keys.len(),
            hashes: state.keys.hash_count(),
            charged_bytes: state.charge.bytes(),
        })
    }
}

fn accepts_changes(phase: CoreKeyPhase) -> Result<(), CoreKeyError> {
    if matches!(phase, CoreKeyPhase::Loading | CoreKeyPhase::Ready) {
        Ok(())
    } else {
        Err(CoreKeyError::State)
    }
}
