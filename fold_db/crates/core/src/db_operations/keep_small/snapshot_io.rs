//! Snapshot, export, import and repaired-snapshot install.

use super::*;

impl KeepSmallMeters {
    /// Snapshot for durable persist.
    #[must_use]
    pub fn export(&self) -> KeepSmallSnapshot {
        let molecules = self.molecules.lock().map(|m| m.clone()).unwrap_or_default();
        // The snapshot holds no per-key copy of the counter sources.
        let source_state_complete = self.molecule_counters_complete();
        KeepSmallSnapshot {
            version: KEEP_SMALL_SNAPSHOT_VERSION,
            schemas: self.schemas.lock().map(|m| m.clone()).unwrap_or_default(),
            molecules,
            molecule_counters_complete: source_state_complete,
            molecule_counter_sources_complete: source_state_complete,
            molecule_schema: self
                .molecule_schema
                .lock()
                .map(|m| m.clone())
                .unwrap_or_default(),
            // Stamped by the persist path: only the shutdown flush writes
            // `Some(true)`.
            clean_stop: None,
            trust: self.trust.lock().map_or_else(
                |_| MeterTrustPayload::incomplete("trust_lock_poisoned"),
                |trust| trust.clone(),
            ),
        }
    }

    /// Hydrate from a durable snapshot (pending in-flight map stays empty).
    /// Fails closed: marks incomplete if any lock operation fails, never silently partial.
    pub fn import(&self, snap: KeepSmallSnapshot) -> Result<(), String> {
        let mut trust = self.trust.lock().map_err(|e| {
            let err = format!("trust lock: {e}");
            self.mark_incomplete(&err);
            err
        })?;
        *trust = snap.trust.clone();
        drop(trust);

        let mut schemas = self.schemas.lock().map_err(|e| {
            let err = format!("schemas lock: {e}");
            self.mark_incomplete(&err);
            err
        })?;
        *schemas = snap.schemas;
        drop(schemas);

        let mut molecules = self.molecules.lock().map_err(|e| {
            let err = format!("molecules lock: {e}");
            self.mark_incomplete(&err);
            err
        })?;
        *molecules = snap.molecules;
        drop(molecules);

        let mut molecule_schema = self.molecule_schema.lock().map_err(|e| {
            let err = format!("molecule_schema lock: {e}");
            self.mark_incomplete(&err);
            err
        })?;
        *molecule_schema = snap.molecule_schema;
        drop(molecule_schema);

        self.molecule_counters_complete.store(
            snap.molecule_counters_complete && snap.molecule_counter_sources_complete,
            Ordering::Relaxed,
        );

        self.hydrate_missed.store(false, Ordering::Relaxed);
        Ok(())
    }
}
