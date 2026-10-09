//! Persist dirty resident ladder → LastStore / AtomStore (T1).
//!
//! **Fidelity:** writes the [`PersistPlan`] projection without rewriting
//! content. Rehydrate loaders rebuild the same resident objects.
//!
//! **Tips (won't-undo):** tip puts MUST go through
//! [`AtomStore::store_molecules_changed_keys_batch`] (or the single-molecule
//! sibling). SampleN reads the `mk:` tips. A tip with no `mord:` row is not
//! a defect.

use async_trait::async_trait;
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::graph::RehydrateFlightCell;
use super::types::{PersistPlan, ResidentAtom, ResidentTip};
use super::ResidentGraph;
use crate::atom::molecule_key_codec::hash_range_record_key;
use crate::atom::MoleculeHashRange;
use crate::db_operations::atom_store::{ChangedKey, MoleculeData, PerKeyRecord};
use crate::db_operations::AtomStore;
use crate::schema::types::SchemaError;

struct FlightLeaderGuard<'a> {
    graph: &'a ResidentGraph,
    flight_key: String,
    cell: Arc<RehydrateFlightCell>,
    armed: bool,
}

impl Drop for FlightLeaderGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.cell.complete(Err(SchemaError::InvalidData(
                "rehydrate flight aborted".into(),
            )));
            self.graph
                .clear_rehydrate_flight(&self.flight_key, &self.cell);
        }
    }
}

/// Sink that materializes a [`PersistPlan`] into durable storage.
#[async_trait]
pub trait PersistSink: Send + Sync {
    async fn write_plan(&self, plan: &PersistPlan) -> Result<(), SchemaError>;
}

/// Load a tip or atom from durable storage. File bytes are not a graph input.
#[async_trait]
pub trait RehydrateSource: Send + Sync {
    async fn load_tip(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) -> Result<Option<ResidentTip>, SchemaError>;

    async fn load_atom(&self, atom_uuid: &str) -> Result<Option<ResidentAtom>, SchemaError>;
}

/// AtomStore-backed sink: tips as `mk:…` / [`PerKeyRecord`], atoms via
/// `batch_store_atoms`. CAS files remain outside the resident persist plan.
pub struct AtomStorePersistSink {
    store: AtomStore,
}

impl AtomStorePersistSink {
    pub fn new(store: AtomStore) -> Self {
        Self { store }
    }
}

impl AtomStorePersistSink {
    async fn write_plan_inner(&self, plan: &PersistPlan) -> Result<(), SchemaError> {
        // Atoms first so tips never point at missing bodies mid-batch.
        if !plan.atoms.is_empty() {
            let mut atoms = Vec::with_capacity(plan.atoms.len());
            for ra in &plan.atoms {
                let mut meta = None;
                if let Some(ref blob_ref) = ra.file_blob_ref {
                    let mut m = std::collections::HashMap::new();
                    m.insert("file_blob_ref".to_string(), blob_ref.clone());
                    meta = Some(m);
                }
                let atom =
                    AtomStore::create_atom(&ra.source_schema_name, ra.content.clone(), None, meta)?;
                if atom.uuid() != ra.uuid {
                    return Err(SchemaError::InvalidData(format!(
                        "resident atom uuid {} != content-addressed {} (fidelity)",
                        ra.uuid,
                        atom.uuid()
                    )));
                }
                // Located write: under acks-on-resident this sink is the
                // canonical atom writer, so honoring the carried partition is
                // what keeps post-flip bodies prefixed (+ locator) instead of
                // silently degrading to flat. An absent/invalid prefix places
                // flat, exactly as before.
                let partition = ra
                    .partition_prefix
                    .as_deref()
                    .and_then(crate::atom::AtomPartition::from_prefix);
                atoms.push((atom, partition));
            }
            self.store.batch_store_atoms_located(atoms, None).await?;
        }

        // Tips: group by molecule and persist through the O(changed) path.
        // SampleN reads these `mk:` tips. A missing `mord:` row is not a defect.
        if !plan.tips.is_empty() {
            let mut by_molecule: HashMap<String, Vec<&ResidentTip>> = HashMap::new();
            for tip in &plan.tips {
                by_molecule
                    .entry(tip.molecule_uuid.clone())
                    .or_default()
                    .push(tip);
            }
            let mut batch: Vec<(String, MoleculeData, HashSet<ChangedKey>)> =
                Vec::with_capacity(by_molecule.len());
            for (molecule_uuid, tips) in by_molecule {
                let mut changed = HashSet::with_capacity(tips.len());
                for tip in &tips {
                    changed.insert(ChangedKey::hash_range(tip.hash.clone(), tip.range.clone()));
                }
                // Same load-then-mutate shape as protein/mutation writers: keep
                // version/header continuity when the molecule already exists;
                // seed a TAIL-marked empty molecule for brand-new identities.
                let mut mol = match self
                    .store
                    .load_molecule_for_write(&molecule_uuid, None, &changed)
                    .await?
                {
                    Some(m) => m,
                    None => MoleculeHashRange::from_write_records(
                        molecule_uuid.clone(),
                        0,
                        Utc::now(),
                        Vec::new(),
                    ),
                };
                for tip in &tips {
                    // Preserve the author's complete winner tuple. Re-stamping
                    // this retry with receipt time can make an old dirty-plan
                    // snapshot defeat a newer durable tip.
                    mol.set_atom_uuid_from_values_imported_with_author(
                        tip.hash.clone(),
                        tip.range.clone(),
                        tip.atom_uuid.clone(),
                        tip.device_id.clone(),
                        tip.writer_pubkey.clone(),
                        String::new(),
                        0,
                        Some(tip.written_at),
                        tip.logical_counter,
                        tip.mutation_uuid.clone(),
                    );
                    if let Some(meta) = &tip.key_metadata {
                        mol.set_key_metadata(tip.hash.clone(), tip.range.clone(), meta.clone());
                    }
                }
                batch.push((molecule_uuid, mol, changed));
            }
            self.store
                .store_molecules_changed_keys_batch(&batch, None)
                .await?;
        }

        // Schemas: optional; catalog still owned by SchemaCore in production.
        // PersistPlan may carry them for round-trip tests without SchemaCore.
        let _ = &plan.schemas;

        Ok(())
    }
}

#[async_trait]
impl PersistSink for AtomStorePersistSink {
    async fn write_plan(&self, plan: &PersistPlan) -> Result<(), SchemaError> {
        #[cfg(feature = "cloud-sync")]
        {
            crate::sync::capture::with_capture_suppressed(self.write_plan_inner(plan)).await
        }
        #[cfg(not(feature = "cloud-sync"))]
        {
            self.write_plan_inner(plan).await
        }
    }
}

#[async_trait]
impl RehydrateSource for AtomStorePersistSink {
    async fn load_tip(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) -> Result<Option<ResidentTip>, SchemaError> {
        let key = hash_range_record_key(molecule_uuid, hash, range);
        let rec: Option<PerKeyRecord> = self
            .store
            .raw()
            .get_item(&key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("load tip {key}: {e}")))?;
        Ok(rec.map(|r| ResidentTip {
            molecule_uuid: molecule_uuid.to_string(),
            hash: hash.to_string(),
            range: range.to_string(),
            atom_uuid: r.entry.atom_uuid.clone(),
            written_at: r.entry.written_at,
            logical_counter: r.entry.logical_counter,
            device_id: r.entry.lww_device().to_string(),
            mutation_uuid: r.entry.mutation_uuid,
            key_metadata: r.meta.clone(),
            writer_pubkey: r.entry.writer_pubkey.clone(),
        }))
    }

    async fn load_atom(&self, atom_uuid: &str) -> Result<Option<ResidentAtom>, SchemaError> {
        let atom = self.store.get_atom_by_uuid(atom_uuid, None).await?;
        Ok(atom.as_ref().map(ResidentAtom::from_atom))
    }
}

impl ResidentGraph {
    /// Rehydrate the tip and atom. A file reference is never followed.
    ///
    /// Concurrent cold readers of one slot share a single disk read. The
    /// flight map mutex is released before that read. A disk value installs
    /// only when the slot's resident and durable revisions stay unchanged.
    pub async fn rehydrate_field_from(
        &self,
        source: &dyn RehydrateSource,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) -> Result<Option<ResidentTip>, SchemaError> {
        if let Some(tip) = self.resident_field_ready(molecule_uuid, hash, range) {
            return Ok(Some(tip));
        }

        let (flight_key, cell, is_leader, observed_revisions) =
            self.join_rehydrate_flight_with_snapshot(molecule_uuid, hash, range);
        if is_leader {
            let observed_revisions =
                observed_revisions.expect("a new rehydrate flight captures its slot revisions");
            let mut guard = FlightLeaderGuard {
                graph: self,
                flight_key: flight_key.clone(),
                cell: Arc::clone(&cell),
                armed: true,
            };
            let outcome = async {
                if let Some(tip) = self.resident_field_ready(molecule_uuid, hash, range) {
                    return Ok(Some(tip));
                }
                self.rehydrate_field_from_uncached(
                    source,
                    molecule_uuid,
                    hash,
                    range,
                    observed_revisions,
                )
                .await
            }
            .await;
            cell.complete(outcome.clone());
            guard.armed = false;
            self.clear_rehydrate_flight(&flight_key, &cell);
            return outcome;
        }

        let waited = cell.wait().await;
        if let Some(tip) = self.resident_field_ready(molecule_uuid, hash, range) {
            return Ok(Some(tip));
        }
        waited
    }

    async fn rehydrate_field_from_uncached(
        &self,
        source: &dyn RehydrateSource,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        observed_revisions: (u64, u64),
    ) -> Result<Option<ResidentTip>, SchemaError> {
        let loaded_tip = source.load_tip(molecule_uuid, hash, range).await?;
        let Some(tip) = loaded_tip else {
            let current = self.slot_control(molecule_uuid, hash, range);
            if (current.resident_revision, current.durable_revision) != observed_revisions {
                self.metrics().record_stale_rehydrate_rejected();
                return Ok(self.resident_field_ready(molecule_uuid, hash, range));
            }
            return Ok(None);
        };
        let atom = source.load_atom(&tip.atom_uuid).await?;
        // A field read stops at the atom. Its file reference and access
        // metadata are the query result; CAS bytes belong to explicit file
        // access and must never be hydrated into the resident query graph.

        let current = self.slot_control(molecule_uuid, hash, range);
        if (current.resident_revision, current.durable_revision) != observed_revisions {
            self.metrics().record_stale_rehydrate_rejected();
            return Ok(self.resident_field_ready(molecule_uuid, hash, range));
        }

        let Some(tip_out) = self.rehydrate_tip_at(tip, Some(observed_revisions)) else {
            return Ok(None);
        };
        if let Some(atom) = atom {
            self.rehydrate_atom(atom);
        }
        Ok(Some(tip_out.value))
    }
}
