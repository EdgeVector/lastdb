//! Snapshot, export, import and repaired-snapshot install.

use super::*;

impl KeepSmallMeters {
    /// Snapshot for durable persist.
    #[must_use]
    pub fn export(&self) -> KeepSmallSnapshot {
        let field_keys = self
            .field_keys
            .lock()
            .map(|m| m.clone())
            .unwrap_or_default();
        // The snapshot holds no per-key copy of the counter sources.
        let source_state_complete = self.field_key_counters_complete();
        KeepSmallSnapshot {
            version: KEEP_SMALL_SNAPSHOT_VERSION,
            schemas: self.schemas.lock().map(|m| m.clone()).unwrap_or_default(),
            field_keys,
            field_key_counters_complete: source_state_complete,
            field_key_counter_sources_complete: source_state_complete,
            field_key_schema: self
                .field_key_schema
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

        let mut field_keys = self.field_keys.lock().map_err(|e| {
            let err = format!("field keys lock: {e}");
            self.mark_incomplete(&err);
            err
        })?;
        *field_keys = snap.field_keys;
        drop(field_keys);

        let mut field_key_schema = self.field_key_schema.lock().map_err(|e| {
            let err = format!("field_key_schema lock: {e}");
            self.mark_incomplete(&err);
            err
        })?;
        *field_key_schema = snap.field_key_schema;
        drop(field_key_schema);

        self.field_key_counters_complete.store(
            snap.field_key_counters_complete && snap.field_key_counter_sources_complete,
            Ordering::Relaxed,
        );

        self.hydrate_missed.store(false, Ordering::Relaxed);
        Ok(())
    }
}
