//! Tip and bookkeeping row accounting, last-written-size tracking.

use super::*;

impl KeepSmallMeters {
    /// Last accounted value length for this store key (0 if unseen).
    #[must_use]
    pub fn last_key_bytes(&self, key: &str) -> u64 {
        self.last_key_bytes
            .lock()
            .ok()
            .and_then(|m| m.get(key).copied())
            .unwrap_or(0)
    }

    pub fn remember_key_bytes(&self, key: &str, bytes: u64) {
        if let Ok(mut map) = self.last_key_bytes.lock() {
            map.insert(key.to_string(), bytes);
        }
    }

    /// Last accounted atom on this tip key, if any.
    #[must_use]
    pub fn last_tip_atom(&self, tip_key: &str) -> Option<MoleculeTipCounterSource> {
        self.last_tip_atom
            .lock()
            .ok()
            .and_then(|m| m.get(tip_key).cloned())
    }

    pub fn remember_tip_atom(
        &self,
        tip_key: &str,
        atom_uuid: &str,
        logical_bytes: u64,
        blob_reference_bytes: u64,
    ) {
        if let Ok(mut map) = self.last_tip_atom.lock() {
            map.insert(
                tip_key.to_string(),
                MoleculeTipCounterSource {
                    atom_uuid: atom_uuid.to_string(),
                    atom_value_bytes: logical_bytes,
                    blob_reference_bytes,
                },
            );
        }
    }

    pub(super) fn take_pending(&self, atom_uuid: &str) -> Option<(String, u64, u64, bool)> {
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

    /// Drop in-flight maps for a storage key that no longer exists.
    pub fn forget_key(&self, key: &str) {
        let owner = self
            .molecule_schema
            .lock()
            .ok()
            .map(|map| schema_owner_for_store_key(key, &map));
        if let Ok(mut map) = self.last_key_bytes.lock() {
            map.remove(key);
        }
        if let Ok(mut map) = self.last_tip_atom.lock() {
            map.remove(key);
        }
        self.mark_schema_dirty(owner.as_deref());
    }
}
