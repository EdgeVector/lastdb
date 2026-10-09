//! Per-molecule storage counters and tip/bookkeeping accounting.

use super::*;

impl KeepSmallMeters {
    /// Bind a molecule to the schema that owns it.
    pub fn remember_molecule_schema(&self, molecule_uuid: &str, schema: &str) {
        if molecule_uuid.is_empty() || schema.is_empty() {
            return;
        }
        if let Ok(mut map) = self.molecule_schema.lock() {
            map.insert(molecule_uuid.to_string(), schema.to_string());
        }
        self.mark_schema_dirty(Some(schema));
    }

    /// Schema that owns `molecule_uuid`, if a tip put has bound it.
    #[must_use]
    pub fn molecule_schema(&self, molecule_uuid: &str) -> Option<String> {
        self.molecule_schema
            .lock()
            .ok()
            .and_then(|m| m.get(molecule_uuid).cloned())
    }

    /// Record one current-tip transition for its molecule.
    ///
    /// `old_atom_bytes` is available on the normal resident mutation path.
    /// A restarted process that cannot recover that old contribution marks the
    /// projection incomplete rather than subtracting an invented value.
    pub fn record_molecule_tip_put(
        &self,
        molecule_uuid: &str,
        new_atom_bytes: u64,
        new_blob_bytes: u64,
        old_source: Option<&MoleculeTipCounterSource>,
        new_tip_bytes: u64,
        old_tip_bytes: u64,
    ) {
        if molecule_uuid.is_empty() {
            return;
        }
        let Ok(mut counters) = self.molecules.lock() else {
            self.molecule_counters_complete
                .store(false, Ordering::Relaxed);
            return;
        };
        let counter = counters
            .entry(molecule_uuid.to_string())
            .or_insert_with(|| MoleculeStorageCounter {
                molecule_uuid: molecule_uuid.to_string(),
                ..MoleculeStorageCounter::default()
            });
        if old_tip_bytes == 0 {
            counter.active_slot_count = counter.active_slot_count.saturating_add(1);
        }
        if old_tip_bytes > 0 && old_source.is_none() {
            self.molecule_counters_complete
                .store(false, Ordering::Relaxed);
        }
        if let Some(old) = old_source {
            counter.active_atom_value_bytes = counter
                .active_atom_value_bytes
                .saturating_sub(old.atom_value_bytes);
            counter.active_blob_reference_bytes = counter
                .active_blob_reference_bytes
                .saturating_sub(old.blob_reference_bytes);
        }
        counter.active_atom_value_bytes = counter
            .active_atom_value_bytes
            .saturating_add(new_atom_bytes);
        counter.active_blob_reference_bytes = counter
            .active_blob_reference_bytes
            .saturating_add(new_blob_bytes);
        counter.tip_index_bytes =
            apply_delta(counter.tip_index_bytes, new_tip_bytes, old_tip_bytes);
        counter.counter_epoch = counter.counter_epoch.saturating_add(1);
        drop(counters);
        self.mark_schema_dirty(self.molecule_schema(molecule_uuid).as_deref());
    }

    /// Attribute one molecule-local structural row.  Tip rows use
    /// [`Self::record_molecule_tip_put`]; headers and order/index rows land
    /// here.  This remains exact for the bytes the write path serializes.
    pub fn record_molecule_bookkeeping_put(
        &self,
        molecule_uuid: &str,
        new_bytes: u64,
        old_bytes: u64,
    ) {
        if molecule_uuid.is_empty() {
            return;
        }
        let Ok(mut counters) = self.molecules.lock() else {
            self.molecule_counters_complete
                .store(false, Ordering::Relaxed);
            return;
        };
        let counter = counters
            .entry(molecule_uuid.to_string())
            .or_insert_with(|| MoleculeStorageCounter {
                molecule_uuid: molecule_uuid.to_string(),
                ..MoleculeStorageCounter::default()
            });
        counter.molecule_metadata_bytes =
            apply_delta(counter.molecule_metadata_bytes, new_bytes, old_bytes);
        counter.counter_epoch = counter.counter_epoch.saturating_add(1);
        drop(counters);
        self.mark_schema_dirty(self.molecule_schema(molecule_uuid).as_deref());
    }

    /// Remove one current tip contribution after its source row is durably
    /// absent. Missing source state fails closed.
    pub fn record_molecule_tip_delete(&self, molecule_uuid: &str, tip_key: &str, tip_bytes: u64) {
        let source = self.last_tip_atom(tip_key);
        let Ok(mut counters) = self.molecules.lock() else {
            self.mark_molecule_counters_incomplete();
            return;
        };
        let Some(counter) = counters.get_mut(molecule_uuid) else {
            self.mark_molecule_counters_incomplete();
            return;
        };
        let Some(source) = source else {
            self.mark_molecule_counters_incomplete();
            return;
        };
        counter.active_slot_count = counter.active_slot_count.saturating_sub(1);
        counter.active_atom_value_bytes = counter
            .active_atom_value_bytes
            .saturating_sub(source.atom_value_bytes);
        counter.active_blob_reference_bytes = counter
            .active_blob_reference_bytes
            .saturating_sub(source.blob_reference_bytes);
        counter.tip_index_bytes = counter.tip_index_bytes.saturating_sub(tip_bytes);
        counter.counter_epoch = counter.counter_epoch.saturating_add(1);
        drop(counters);
        self.mark_schema_dirty(self.molecule_schema(molecule_uuid).as_deref());
    }

    #[must_use]
    pub fn molecule_counter(&self, molecule_uuid: &str) -> Option<MoleculeStorageCounter> {
        self.molecules
            .lock()
            .ok()
            .and_then(|counters| counters.get(molecule_uuid).cloned())
    }

    /// Seed a declared but empty molecule with an exact zero counter.
    pub fn ensure_molecule_counter(&self, molecule_uuid: &str, schema: &str) {
        if molecule_uuid.is_empty() {
            return;
        }
        if let Ok(mut counters) = self.molecules.lock() {
            counters
                .entry(molecule_uuid.to_string())
                .or_insert_with(|| MoleculeStorageCounter {
                    molecule_uuid: molecule_uuid.to_string(),
                    ..MoleculeStorageCounter::default()
                });
        }
        self.remember_molecule_schema(molecule_uuid, schema);
    }

    #[must_use]
    pub fn molecule_counters_complete(&self) -> bool {
        let trust_complete = self.trust.lock().is_ok_and(|trust| {
            trust.molecules.state.is_complete()
                || (trust.molecules.state == MeterTrustState::Absent
                    && self.snapshot() == LiveBudgetTotals::default()
                    && self.schemas.lock().is_ok_and(|schemas| schemas.is_empty()))
        });
        self.molecule_counters_complete.load(Ordering::Relaxed) && trust_complete
    }

    pub fn mark_molecule_counters_incomplete(&self) {
        self.molecule_counters_complete
            .store(false, Ordering::Relaxed);
    }
}
