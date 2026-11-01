//! Per-molecule storage counters and their owning schemas.

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

    /// Schema that owns `molecule_uuid`, if a schema install has bound it.
    #[must_use]
    pub fn molecule_schema(&self, molecule_uuid: &str) -> Option<String> {
        self.molecule_schema
            .lock()
            .ok()
            .and_then(|m| m.get(molecule_uuid).cloned())
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
                    && self.schemas.lock().is_ok_and(|schemas| schemas.is_empty()))
        });
        self.molecule_counters_complete.load(Ordering::Relaxed) && trust_complete
    }
}
