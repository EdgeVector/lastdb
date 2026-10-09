//! Atom-level meter updates: puts, replaces, deletes and pending-atom bookkeeping.

use super::*;

impl KeepSmallMeters {
    /// Record a newly stored atom (logical serialized size).
    ///
    /// Increments live atom bytes, atom count, and today's append counter.
    pub fn record_atom_put(&self, schema: &str, atom_uuid: &str, logical_bytes: u64) {
        self.record_atom_put_with_blob(schema, atom_uuid, logical_bytes, 0, true);
    }

    /// Record a stored atom and the logical blob bytes named by its pointer.
    pub fn record_atom_put_with_blob(
        &self,
        schema: &str,
        atom_uuid: &str,
        logical_bytes: u64,
        blob_reference_bytes: u64,
        contribution_complete: bool,
    ) {
        self.atom_bytes.fetch_add(logical_bytes, Ordering::Relaxed);
        self.atom_count.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut pending) = self.pending_atoms.lock() {
            pending.insert(
                atom_uuid.to_string(),
                (
                    schema.to_string(),
                    logical_bytes,
                    blob_reference_bytes,
                    contribution_complete,
                ),
            );
        }
        if !contribution_complete {
            self.molecule_counters_complete
                .store(false, Ordering::Relaxed);
        }
        self.with_schema(schema, |meter| {
            meter.live_bytes = meter.live_bytes.saturating_add(logical_bytes);
            meter.atom_count = meter.atom_count.saturating_add(1);
            Self::add_appended(meter, logical_bytes);
        });
    }

    /// After [`Self::record_atom_put`] of a replacement body, subtract the
    /// *superseded* live size so live becomes `previous - old + new`.
    ///
    /// The new atom is already in live (and in today's append). Subtracting
    /// the old size (not the new one) is what lets a growing rewrite raise
    /// live and still show high churn (`appended / live`).
    pub fn replace_live_atom(&self, new_uuid: &str, superseded_bytes: u64) {
        let Some(schema) = self.pending_atom_schema(new_uuid) else {
            return;
        };
        self.sub_atom_bytes(superseded_bytes);
        let count = self.atom_count.load(Ordering::Relaxed);
        if count > 0 {
            self.atom_count.fetch_sub(1, Ordering::Relaxed);
        }
        self.with_schema(&schema, |meter| {
            meter.live_bytes = meter.live_bytes.saturating_sub(superseded_bytes);
            meter.atom_count = meter.atom_count.saturating_sub(1);
        });
    }

    pub(super) fn sub_atom_bytes(&self, bytes: u64) {
        let cur = self.atom_bytes.load(Ordering::Relaxed);
        self.atom_bytes.fetch_sub(bytes.min(cur), Ordering::Relaxed);
    }

    /// Look up a just-stored atom's logical size without consuming it.
    #[must_use]
    pub fn pending_atom_bytes(&self, atom_uuid: &str) -> Option<u64> {
        self.pending_atoms
            .lock()
            .ok()
            .and_then(|m| m.get(atom_uuid).map(|(_, b, _, _)| *b))
    }

    /// Owning schema of a just-stored atom, without consuming it.
    ///
    /// The tip-accounting path uses this to bind a molecule to its schema.
    #[must_use]
    pub fn pending_atom_schema(&self, atom_uuid: &str) -> Option<String> {
        self.pending_atoms
            .lock()
            .ok()
            .and_then(|m| m.get(atom_uuid).map(|(schema, _, _, _)| schema.clone()))
    }

    #[must_use]
    pub fn pending_atom_contribution(&self, atom_uuid: &str) -> Option<(u64, u64, bool)> {
        self.pending_atoms.lock().ok().and_then(|m| {
            m.get(atom_uuid)
                .map(|(_, atom_bytes, blob_bytes, complete)| (*atom_bytes, *blob_bytes, *complete))
        })
    }

    /// Known logical contribution for an atom that is already a current tip.
    #[must_use]
    pub fn known_atom_contribution(&self, atom_uuid: &str) -> Option<(u64, u64)> {
        if let Some((atom, blob, complete)) = self.pending_atom_contribution(atom_uuid) {
            return complete.then_some((atom, blob));
        }
        self.last_tip_atom.lock().ok().and_then(|map| {
            map.values()
                .find(|source| source.atom_uuid == atom_uuid)
                .map(|source| (source.atom_value_bytes, source.blob_reference_bytes))
        })
    }

    /// Decrement live atom bytes when a **live-head** atom is deleted.
    ///
    /// Only call this for the current tip body. A superseded atom already
    /// left the live gauge on [`Self::replace_live_atom`]; charging it again
    /// would under-report.
    pub fn record_atom_delete(&self, schema: &str, logical_bytes: u64) {
        self.sub_atom_bytes(logical_bytes);
        let count = self.atom_count.load(Ordering::Relaxed);
        if count > 0 {
            self.atom_count.fetch_sub(1, Ordering::Relaxed);
        }
        self.with_schema(schema, |meter| {
            meter.live_bytes = meter.live_bytes.saturating_sub(logical_bytes);
            meter.atom_count = meter.atom_count.saturating_sub(1);
        });
    }

    /// Replay aggregate hard-erase debits after the snapshot's checkpoint.
    /// The aggregate has no subject keys, so molecule source detail is not
    /// recoverable from it. Mark trust incomplete until a full repair runs.
    pub fn replay_hard_erase_debit(&self, schema: &str, debit: KeepSmallHardEraseDebit) {
        self.sub_atom_bytes(debit.atom_bytes);
        let _ = self
            .atom_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(debit.atom_count))
            });
        let _ = self
            .tip_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(debit.tip_bytes))
            });
        let _ = self
            .tip_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(debit.tip_count))
            });
        self.with_schema(schema, |meter| {
            meter.live_bytes = meter.live_bytes.saturating_sub(debit.atom_bytes);
            meter.atom_count = meter.atom_count.saturating_sub(debit.atom_count);
            meter.tip_bytes = meter.tip_bytes.saturating_sub(debit.tip_bytes);
        });
        self.mark_incomplete("hard_erase_debit_replayed_without_sources");
    }

    /// Drop a just-stored atom's pending size so a later replace cannot
    /// subtract it after the body is gone.
    pub fn forget_pending_atom(&self, atom_uuid: &str) {
        let _ = self.take_pending(atom_uuid);
    }

    /// Logical size of a live-head atom still in the in-process maps.
    ///
    /// Empty after hydrate: the durable snapshot is totals, not per-uuid.
    #[must_use]
    pub fn live_atom_bytes(&self, atom_uuid: &str) -> Option<u64> {
        if let Some(bytes) = self.pending_atom_bytes(atom_uuid) {
            return Some(bytes);
        }
        self.last_tip_atom.lock().ok().and_then(|map| {
            map.values()
                .find(|source| source.atom_uuid == atom_uuid)
                .map(|source| source.atom_value_bytes)
        })
    }
}
