//! Tip and bookkeeping row accounting, last-written-size tracking.

use super::*;

impl KeepSmallMeters {
    /// Remove a known tip without wrapping an incomplete aggregate below zero.
    pub(crate) fn record_tip_delete(&self, schema: Option<&str>, bytes: u64) {
        let before = self
            .tip_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(bytes))
            })
            .unwrap_or(0);
        let count = self
            .tip_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .unwrap_or(0);
        if before < bytes || count == 0 {
            self.mark_incomplete("erase_tip_aggregate_underflow");
        }
        if let Some(schema) = schema {
            self.with_schema(schema, |meter| {
                meter.tip_bytes = meter.tip_bytes.saturating_sub(bytes);
            });
        } else {
            self.mark_schema_dirty(None);
        }
    }

    pub(super) fn take_pending(&self, atom_uuid: &str) -> Option<PendingAtomContribution> {
        self.pending_atoms
            .lock()
            .ok()
            .and_then(|mut m| m.remove(atom_uuid))
    }

    /// Record a tip put. `previous_bytes` is the superseded tip value length.
    ///
    /// `schema` is the owning schema when the caller could resolve one. The
    /// per-schema counter moves by exactly the same delta as the global one,
    /// so the per-schema sum can never claim bytes the global total does not
    /// hold. An unresolved put still lands in the global total — it just stays
    /// structurally unattributed in the storage report.
    pub fn record_tip_put(&self, schema: Option<&str>, new_bytes: u64, previous_bytes: u64) {
        if new_bytes >= previous_bytes {
            self.tip_bytes
                .fetch_add(new_bytes - previous_bytes, Ordering::Relaxed);
        } else {
            self.tip_bytes
                .fetch_sub(previous_bytes - new_bytes, Ordering::Relaxed);
        }
        if previous_bytes == 0 {
            self.tip_count.fetch_add(1, Ordering::Relaxed);
        } else if new_bytes == 0 {
            let count = self.tip_count.load(Ordering::Relaxed);
            if count > 0 {
                self.tip_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
        if let Some(schema) = schema {
            self.with_schema(schema, |meter| {
                meter.tip_bytes = apply_delta(meter.tip_bytes, new_bytes, previous_bytes);
            });
        } else {
            self.mark_schema_dirty(None);
        }
    }

    /// Record bookkeeping bytes (order-log / moc / index rows) appended.
    ///
    /// Same schema rule as [`Self::record_tip_put`].
    pub fn record_bookkeeping_put(
        &self,
        schema: Option<&str>,
        new_bytes: u64,
        previous_bytes: u64,
    ) {
        if new_bytes >= previous_bytes {
            self.bookkeeping_bytes
                .fetch_add(new_bytes - previous_bytes, Ordering::Relaxed);
        } else {
            self.bookkeeping_bytes
                .fetch_sub(previous_bytes - new_bytes, Ordering::Relaxed);
        }
        if let Some(schema) = schema {
            self.with_schema(schema, |meter| {
                meter.bookkeeping_bytes =
                    apply_delta(meter.bookkeeping_bytes, new_bytes, previous_bytes);
            });
        } else {
            self.mark_schema_dirty(None);
        }
    }

    /// Mark the owning schema dirty after a storage key left the store.
    pub fn forget_key(&self, key: &str) {
        let owner = self
            .molecule_schema
            .lock()
            .ok()
            .map(|map| schema_owner_for_store_key(key, &map));
        self.mark_schema_dirty(owner.as_deref());
    }
}
