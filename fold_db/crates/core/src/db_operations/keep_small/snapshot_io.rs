//! Snapshot, export, import and repaired-snapshot install.

use super::*;

impl KeepSmallMeters {
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
        let molecule_tip_sources = self
            .last_tip_atom
            .lock()
            .map(|m| m.clone())
            .unwrap_or_default();
        let expected_sources = molecules
            .values()
            .map(|counter| counter.active_slot_count)
            .sum::<u64>();
        let source_state_complete = self.molecule_counters_complete()
            && expected_sources == molecule_tip_sources.len() as u64;
        KeepSmallSnapshot {
            totals: self.snapshot(),
            schemas: self.schemas.lock().map(|m| m.clone()).unwrap_or_default(),
            molecules,
            molecule_counters_complete: source_state_complete,
            molecule_counter_sources_complete: source_state_complete,
            molecule_tip_sources,
            molecule_key_bytes: self
                .last_key_bytes
                .lock()
                .map(|m| m.clone())
                .unwrap_or_default(),
            molecule_schema: self
                .molecule_schema
                .lock()
                .map(|m| m.clone())
                .unwrap_or_default(),
            pending_protein_folds: self.pending_protein_folds(),
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
            hard_erase_totals: KeepSmallHardEraseTotals::default(),
            hard_erase_journal_checkpoint_seq: 0,
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

        let mut last_tip_atom = self.last_tip_atom.lock().map_err(|e| {
            let err = format!("last_tip_atom lock: {e}");
            self.mark_incomplete(&err);
            err
        })?;
        *last_tip_atom = snap.molecule_tip_sources;
        drop(last_tip_atom);

        let mut last_key_bytes = self.last_key_bytes.lock().map_err(|e| {
            let err = format!("last_key_bytes lock: {e}");
            self.mark_incomplete(&err);
            err
        })?;
        *last_key_bytes = snap.molecule_key_bytes;
        drop(last_key_bytes);

        let mut molecule_schema = self.molecule_schema.lock().map_err(|e| {
            let err = format!("molecule_schema lock: {e}");
            self.mark_incomplete(&err);
            err
        })?;
        *molecule_schema = snap.molecule_schema;
        drop(molecule_schema);

        self.pending_protein_folds
            .store(snap.pending_protein_folds, Ordering::Relaxed);
        self.molecule_counters_complete.store(
            snap.molecule_counters_complete && snap.molecule_counter_sources_complete,
            Ordering::Relaxed,
        );

        self.hydrate_missed.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Replace all molecule counter state after an isolated-copy scan.
    /// Fails closed: marks incomplete if any lock operation fails, never partial bootstrap.
    pub fn install_molecule_counter_bootstrap(
        &self,
        counters: HashMap<String, MoleculeStorageCounter>,
        tip_sources: HashMap<String, MoleculeTipCounterSource>,
        key_bytes: HashMap<String, u64>,
        molecule_schema: HashMap<String, String>,
    ) -> Result<(), String> {
        let mut molecules = self
            .molecules
            .lock()
            .map_err(|e| format!("molecules lock: {e}"))?;
        *molecules = counters;
        drop(molecules);

        let mut last_tip_atom = self
            .last_tip_atom
            .lock()
            .map_err(|e| format!("last_tip_atom lock: {e}"))?;
        *last_tip_atom = tip_sources;
        drop(last_tip_atom);

        let mut last_key_bytes = self
            .last_key_bytes
            .lock()
            .map_err(|e| format!("last_key_bytes lock: {e}"))?;
        *last_key_bytes = key_bytes;
        drop(last_key_bytes);

        let mut molecule_schema_map = self
            .molecule_schema
            .lock()
            .map_err(|e| format!("molecule_schema lock: {e}"))?;
        *molecule_schema_map = molecule_schema;
        drop(molecule_schema_map);

        let mut trust = self.trust.lock().map_err(|e| format!("trust lock: {e}"))?;
        trust.global = MeterDomainTrust::incomplete("molecule_only_bootstrap");
        for domain in trust.schemas.values_mut() {
            *domain = MeterDomainTrust::incomplete("molecule_only_bootstrap");
        }
        trust.molecules = MeterDomainTrust {
            state: MeterTrustState::Reconciled,
            cause: None,
        };
        drop(trust);

        self.molecule_counters_complete
            .store(true, Ordering::Relaxed);
        self.mark_all_schemas_dirty();
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
