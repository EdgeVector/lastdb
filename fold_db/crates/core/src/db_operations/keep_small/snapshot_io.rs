//! Snapshot, export, import and repaired-snapshot install.

use super::*;

impl KeepSmallMeters {
    pub(crate) fn largest_schema_name_bytes(&self) -> Result<usize, String> {
        Ok(self
            .molecule_schema
            .lock()
            .map_err(|_| "meter owner lock poisoned")?
            .values()
            .map(String::len)
            .max()
            .unwrap_or(0))
    }

    /// Bound native erase preparation before it clones aggregate maps. The
    /// quote charges 512 bytes per allocated slot plus dynamic strings. At most
    /// eight working copies of this 8 MiB quote fit in the 64 MiB allowance.
    pub(crate) fn check_erase_copy_budget(&self) -> Result<(), String> {
        let mut bytes = 4096usize;
        let mut charge = |entries: usize, strings: &[&str]| {
            bytes = bytes.saturating_add(entries.saturating_mul(512));
            for value in strings {
                bytes = bytes.saturating_add(value.len().saturating_mul(8));
            }
        };
        {
            let schemas = self
                .schemas
                .lock()
                .map_err(|_| "meter schema lock poisoned")?;
            charge(schemas.capacity(), &[]);
            for (key, value) in schemas.iter() {
                charge(
                    0,
                    &[
                        key,
                        &value.schema_name,
                        value.display_name.as_deref().unwrap_or(""),
                        &value.appended_day,
                    ],
                );
            }
        }
        {
            let molecules = self
                .molecules
                .lock()
                .map_err(|_| "meter molecule lock poisoned")?;
            charge(molecules.capacity(), &[]);
            for (key, value) in molecules.iter() {
                charge(0, &[key, &value.molecule_uuid]);
            }
        }
        {
            let owners = self
                .molecule_schema
                .lock()
                .map_err(|_| "meter owner lock poisoned")?;
            charge(owners.capacity(), &[]);
            for (key, value) in owners.iter() {
                charge(0, &[key, value]);
            }
        }
        {
            let dirty = self
                .dirty_schemas
                .lock()
                .map_err(|_| "meter dirty schema lock poisoned")?;
            charge(dirty.capacity(), &[]);
            for name in dirty.iter() {
                charge(0, &[name]);
            }
        }
        let trust = self.trust.lock().map_err(|_| "meter trust lock poisoned")?;
        charge(
            2,
            &[
                trust.global.cause.as_deref().unwrap_or(""),
                trust.molecules.cause.as_deref().unwrap_or(""),
            ],
        );
        for (key, value) in &trust.schemas {
            charge(1, &[key, value.cause.as_deref().unwrap_or("")]);
        }
        if bytes > 8 * 1024 * 1024 {
            return Err("erase aggregate preparation exceeds its 8 MiB copy quote (64 MiB working allowance)".into());
        }
        Ok(())
    }

    #[must_use]
    pub fn snapshot(&self) -> LiveBudgetTotals {
        LiveBudgetTotals {
            atom_bytes: self.atom_bytes.load(Ordering::Relaxed),
            tip_bytes: self.tip_bytes.load(Ordering::Relaxed),
            bookkeeping_bytes: self.bookkeeping_bytes.load(Ordering::Relaxed),
            atom_count: self.atom_count.load(Ordering::Relaxed),
            tip_count: self.tip_count.load(Ordering::Relaxed),
        }
    }

    /// Snapshot for durable persist.
    #[must_use]
    pub fn export(&self) -> KeepSmallSnapshot {
        let molecules = self.molecules.lock().map(|m| m.clone()).unwrap_or_default();
        // The source state of a tip is its durable row and its atom counter,
        // which the write path reads; the snapshot holds no per-key copy.
        let source_state_complete = self.molecule_counters_complete();
        KeepSmallSnapshot {
            version: KEEP_SMALL_SNAPSHOT_VERSION,
            totals: self.snapshot(),
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
                |_| MeterTrustPayload {
                    version: KEEP_SMALL_TRUST_VERSION,
                    global: MeterDomainTrust::incomplete("trust_lock_poisoned"),
                    schemas: BTreeMap::new(),
                    molecules: MeterDomainTrust::incomplete("trust_lock_poisoned"),
                },
                |trust| trust.clone(),
            ),
            sharded: false,
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

        self.atom_bytes
            .store(snap.totals.atom_bytes, Ordering::Relaxed);
        self.tip_bytes
            .store(snap.totals.tip_bytes, Ordering::Relaxed);
        self.bookkeeping_bytes
            .store(snap.totals.bookkeeping_bytes, Ordering::Relaxed);
        self.atom_count
            .store(snap.totals.atom_count, Ordering::Relaxed);
        self.tip_count
            .store(snap.totals.tip_count, Ordering::Relaxed);

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

    /// Publish a complete candidate only after its durable snapshot is safe.
    /// Fails closed: marks incomplete if import fails, never silently partial.
    pub fn install_repaired_snapshot(&self, snapshot: KeepSmallSnapshot) -> Result<(), String> {
        self.import(snapshot).inspect_err(|e| {
            self.mark_incomplete(e);
        })?;
        self.stale_after_unclean_stop
            .store(false, Ordering::Relaxed);
        self.molecule_counters_complete
            .store(true, Ordering::Relaxed);
        Ok(())
    }
}
