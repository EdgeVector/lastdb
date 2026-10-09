//! Isolated-copy rebuild of the molecule and blob liveness planes, plus the molecule-scoped row measurement it relies on.

use super::super::core::DbOperations;
use super::molecule_measure::measure_molecule_counter;
use super::schema_molecule_ref_edges;
use crate::db_operations::atom_store::MoleculeRefEdge;
use crate::db_operations::atom_store::{BlobRefCompleteness, MoleculeRefCompleteness};
use crate::db_operations::{
    KeepSmallSnapshot, LiveBudgetTotals, MeterDomainTrust, MeterTrustPayload, MeterTrustState,
    SchemaMeter,
};
use crate::protein::{Protein, PROTEIN_RECORD_PREFIX};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Exact isolated-copy bootstrap proof for molecule and blob liveness planes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LivenessBootstrapReport {
    pub molecule_refs: MoleculeRefCompleteness,
    pub blob_refs: BlobRefCompleteness,
    pub schema_counters: SchemaCounterBootstrapReport,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaCounterBootstrapReport {
    pub complete: bool,
    pub molecules: u64,
    pub active_slots: u64,
    pub logical_value_bytes: u64,
    pub structure_bytes: u64,
}

impl DbOperations {
    /// Rebuild `mref:v1` and `bref:v1` from canonical rows on an isolated copy.
    /// Completeness markers land last. A read or decode failure leaves the
    /// corresponding reclaim gate closed.
    // lint:fn-size-ok verbatim move from schema_operations.rs; splitting this function is separate work
    pub async fn bootstrap_liveness_edges_on_isolated_copy(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<LivenessBootstrapReport, SchemaError> {
        let hard_erase_epoch = self.atoms().hard_erase_mutation_epoch();
        self.atoms()
            .keep_small()
            .mark_incomplete("repair_in_progress");
        // Strict: an unreadable catalog row fails the repair. The tolerant
        // boot reader would skip it and the candidate would claim Reconciled
        // over a partial catalog.
        let schemas = match self.schemas().get_all_schemas_strict().await {
            Ok(schemas) => schemas,
            Err(error) => {
                self.atoms()
                    .keep_small()
                    .mark_incomplete("repair_catalog_unreadable");
                return Err(error);
            }
        };
        let mut molecule_edges = Vec::new();
        let mut molecule_schema = HashMap::new();
        let mut schema_fields = 0u64;
        for (schema_name, schema) in &schemas {
            let edges = schema_molecule_ref_edges(schema_name, schema);
            schema_fields = schema_fields.saturating_add(edges.len() as u64);
            for edge in &edges {
                molecule_schema
                    .entry(edge.molecule_uuid.clone())
                    .or_insert_with(|| schema.name.clone());
            }
            molecule_edges.extend(edges);
        }

        let protein_prefix = build_storage_key(storage_prefix, PROTEIN_RECORD_PREFIX);
        let proteins = self
            .atoms()
            .raw()
            .scan_items_with_prefix::<Protein>(&protein_prefix)
            .await
            .map_err(|error| {
                self.atoms()
                    .keep_small()
                    .mark_incomplete("repair_decode_error");
                SchemaError::InvalidData(format!(
                    "scan proteins for molecule reference bootstrap: {error}"
                ))
            })?;
        let mut protein_members = 0u64;
        for (_, protein) in proteins {
            for member in protein.members {
                molecule_edges.push(MoleculeRefEdge::protein_member(
                    &protein.uuid,
                    &member.molecule_uuid,
                    &member.hash_field,
                    member.range_field.as_deref(),
                ));
                protein_members = protein_members.saturating_add(1);
            }
        }

        let molecule_refs = self
            .atoms()
            .bootstrap_molecule_ref_edges_on_isolated_copy(
                &molecule_edges,
                schema_fields,
                protein_members,
                storage_prefix,
            )
            .await
            .inspect_err(|_| {
                self.atoms()
                    .keep_small()
                    .mark_incomplete("repair_audit_mismatch");
            })?;
        let blob_refs = self
            .atoms()
            .bootstrap_blob_ref_edges_from_atoms_on_isolated_copy(storage_prefix)
            .await
            .inspect_err(|_| {
                self.atoms()
                    .keep_small()
                    .mark_incomplete("repair_decode_error");
            })?;
        let mut counters = HashMap::new();
        let mut tip_sources = HashMap::new();
        let mut key_bytes = HashMap::new();
        let mut schema_counter_report = SchemaCounterBootstrapReport {
            complete: false,
            molecules: 0,
            active_slots: 0,
            logical_value_bytes: 0,
            structure_bytes: 0,
        };
        let mut molecule_ids: Vec<String> = molecule_schema.keys().cloned().collect();
        molecule_ids.sort();
        for molecule_uuid in molecule_ids {
            let (counter, sources, sizes) =
                measure_molecule_counter(self.atoms(), &molecule_uuid, storage_prefix)
                    .await
                    .inspect_err(|error| {
                        let cause = if error.to_string().contains("missing atom") {
                            "repair_missing_atom"
                        } else if error.to_string().contains("blob reference") {
                            "repair_unresolved_reference"
                        } else {
                            "repair_decode_error"
                        };
                        self.atoms().keep_small().mark_incomplete(cause);
                    })?;
            schema_counter_report.molecules = schema_counter_report.molecules.saturating_add(1);
            schema_counter_report.active_slots = schema_counter_report
                .active_slots
                .saturating_add(counter.active_slot_count);
            schema_counter_report.logical_value_bytes = schema_counter_report
                .logical_value_bytes
                .saturating_add(counter.logical_value_bytes());
            schema_counter_report.structure_bytes = schema_counter_report
                .structure_bytes
                .saturating_add(counter.structure_bytes());
            counters.insert(molecule_uuid, counter);
            tip_sources.extend(sources);
            key_bytes.extend(sizes);
        }

        // The atom walk is the independent global/schema oracle. The molecule
        // walk above remains the source for tip and structural counters.
        let mut schema_names: Vec<String> = schemas.keys().cloned().collect();
        schema_names.sort();
        let breakdown = self
            .atoms()
            .storage_breakdown(&[], storage_prefix)
            .await
            .inspect_err(|_| {
                self.atoms()
                    .keep_small()
                    .mark_incomplete("repair_decode_error");
            })?;
        let mut schema_rows = HashMap::new();
        for row in &breakdown.per_schema {
            schema_rows.insert(row.schema_name.clone(), (row.bytes, row.atom_count));
        }
        let mut schema_tips: HashMap<String, (u64, u64)> = HashMap::new();
        let mut tip_bytes = 0u64;
        let mut bookkeeping_bytes = 0u64;
        let mut tip_count = 0u64;
        for counter in counters.values() {
            tip_bytes = tip_bytes.saturating_add(counter.tip_index_bytes);
            bookkeeping_bytes = bookkeeping_bytes.saturating_add(counter.structure_bytes());
            tip_count = tip_count.saturating_add(counter.active_slot_count);
            if let Some(schema) = molecule_schema.get(&counter.molecule_uuid) {
                let entry = schema_tips.entry(schema.clone()).or_default();
                entry.0 = entry.0.saturating_add(counter.tip_index_bytes);
                entry.1 = entry.1.saturating_add(counter.structure_bytes());
            }
        }
        let mut schema_meters = HashMap::new();
        let mut schema_trust = std::collections::BTreeMap::new();
        for schema in &schema_names {
            let (atom_bytes, atom_count) = schema_rows.get(schema).copied().unwrap_or_default();
            let (schema_tip_bytes, schema_bookkeeping_bytes) =
                schema_tips.get(schema).copied().unwrap_or_default();
            schema_meters.insert(
                schema.clone(),
                SchemaMeter {
                    schema_name: schema.clone(),
                    display_name: None,
                    live_bytes: atom_bytes,
                    atom_count,
                    // A repair establishes a new measured origin. Historical
                    // churn is not reconstructed from the candidate itself.
                    appended_bytes: atom_bytes,
                    appended_day: crate::db_operations::keep_small::utc_day(chrono::Utc::now()),
                    tip_bytes: schema_tip_bytes,
                    bookkeeping_bytes: schema_bookkeeping_bytes,
                },
            );
            schema_trust.insert(schema.clone(), MeterDomainTrust::trusted());
        }
        for schema in schema_rows
            .keys()
            .filter(|schema| !schemas.contains_key(*schema))
        {
            let (atom_bytes, atom_count) = schema_rows.get(schema).copied().unwrap_or_default();
            schema_meters.insert(
                schema.clone(),
                SchemaMeter {
                    schema_name: schema.clone(),
                    display_name: None,
                    live_bytes: atom_bytes,
                    atom_count,
                    appended_bytes: atom_bytes,
                    appended_day: crate::db_operations::keep_small::utc_day(chrono::Utc::now()),
                    tip_bytes: 0,
                    bookkeeping_bytes: 0,
                },
            );
            schema_trust.insert(
                schema.clone(),
                MeterDomainTrust::incomplete("schema_binding_unresolved"),
            );
        }
        let trust = MeterTrustPayload {
            version: crate::db_operations::KEEP_SMALL_TRUST_VERSION,
            global: MeterDomainTrust {
                state: MeterTrustState::Reconciled,
                cause: None,
            },
            schemas: schema_trust,
            molecules: MeterDomainTrust {
                state: MeterTrustState::Reconciled,
                cause: None,
            },
        };
        let candidate = KeepSmallSnapshot {
            totals: LiveBudgetTotals {
                atom_bytes: breakdown.total_logical_bytes,
                tip_bytes,
                bookkeeping_bytes,
                atom_count: schema_rows.values().map(|(_, count)| *count).sum(),
                tip_count,
            },
            schemas: schema_meters,
            molecules: counters,
            molecule_counters_complete: true,
            molecule_counter_sources_complete: true,
            molecule_tip_sources: tip_sources,
            molecule_key_bytes: key_bytes,
            molecule_schema: molecule_schema.clone(),
            pending_protein_folds: self.atoms().keep_small().pending_protein_folds(),
            clean_stop: None,
            trust,
            hard_erase_totals: crate::db_operations::KeepSmallHardEraseTotals::default(),
            hard_erase_journal_checkpoint_seq: 0,
            sharded: false,
        };
        // Trust becomes visible only after the complete candidate reaches the
        // durable meter plane. The commit holds the persist lock across put,
        // flush, and install, so no dirty flush can land the old projection
        // over the candidate. An error keeps the live projection incomplete
        // and leaves deletion gates closed.
        self.atoms()
            .commit_repaired_keep_small_snapshot(candidate, hard_erase_epoch)
            .await?;
        schema_counter_report.complete = true;
        Ok(LivenessBootstrapReport {
            molecule_refs,
            blob_refs,
            schema_counters: schema_counter_report,
        })
    }
}
