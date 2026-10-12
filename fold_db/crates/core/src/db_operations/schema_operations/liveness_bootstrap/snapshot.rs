//! Reconciled keep-small snapshot assembly for the isolated-copy liveness bootstrap.

use super::*;

/// Molecule-walk measurements that feed the repaired keep-small snapshot.
pub(super) struct MeasuredMolecules {
    pub(super) counters: HashMap<String, MoleculeStorageCounter>,
    pub(super) tip_sources: HashMap<String, MoleculeTipCounterSource>,
    pub(super) key_bytes: HashMap<String, u64>,
    pub(super) molecule_schema: HashMap<String, String>,
}

/// Assemble the fully reconciled keep-small candidate from the atom oracle and the molecule walk.
pub(super) fn build_repaired_snapshot(
    schemas: &HashMap<String, Schema>,
    schema_rows: &HashMap<String, (u64, u64)>,
    atom_bytes: u64,
    measured: MeasuredMolecules,
    pending_protein_folds: u64,
) -> KeepSmallSnapshot {
    let MeasuredMolecules {
        counters,
        tip_sources,
        key_bytes,
        molecule_schema,
    } = measured;
    let tip_totals = aggregate_tip_totals(&counters, &molecule_schema);
    let (schema_meters, schema_trust) =
        build_schema_meters(schemas, schema_rows, &tip_totals.schema_tips);
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
    KeepSmallSnapshot {
        totals: LiveBudgetTotals {
            atom_bytes,
            tip_bytes: tip_totals.tip_bytes,
            bookkeeping_bytes: tip_totals.bookkeeping_bytes,
            atom_count: schema_rows.values().map(|(_, count)| *count).sum(),
            tip_count: tip_totals.tip_count,
        },
        schemas: schema_meters,
        molecules: counters,
        molecule_counters_complete: true,
        molecule_counter_sources_complete: true,
        molecule_tip_sources: tip_sources,
        molecule_key_bytes: key_bytes,
        molecule_schema,
        pending_protein_folds,
        clean_stop: None,
        trust,
        hard_erase_totals: crate::db_operations::KeepSmallHardEraseTotals::default(),
        hard_erase_journal_checkpoint_seq: 0,
        sharded: false,
    }
}
