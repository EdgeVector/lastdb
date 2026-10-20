//! Molecule counter totals for `lastdb db inventory`.
//!
//! The walk reads every molecule header (`mh:{M}`) of one scope in key order,
//! and every `bindings` record (`mref:v1:n:{target}`) and binding record
//! (`mref:v1:e:...`) of the same scope. It joins the counters and reports the
//! totals, so an operator can compare the counters with the records they count.
//! A walk over a whole scope is a maintenance verb. The planner never reads it.

use super::*;
use crate::atom::molecule_key_codec::{molecule_uuid_from_storage_key, MH_PREFIX};
use crate::db_operations::atom_store::{counts_of_header, MoleculeBindings, RangeWalk};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// How many droppable molecule ids the report names.
const DROPPABLE_SAMPLE: usize = 20;

/// What one inventory walk found about the molecule counters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MoleculeCounterTotals {
    /// Molecule headers read. One for each molecule that lives in the scope.
    pub headers: u64,
    /// Sum of `slots` over the headers.
    pub slots_total: u64,
    /// Sum of `records` over the headers.
    pub records_total: u64,
    /// `bindings` records read (molecules with at least one binding).
    pub bound_molecules: u64,
    /// Sum of `bindings` over the records.
    pub bindings_total: u64,
    /// Binding records read. Equal to `bindings_total` on a healthy scope.
    pub binding_records: u64,
    /// Headers with a counter above zero.
    pub live: u64,
    /// Headers whose three counters are zero, and that are not `suspect`: the
    /// molecules a drain may remove.
    pub droppable: u64,
    /// Headers and `bindings` records marked `suspect`.
    pub suspect: u64,
    /// Headers without counters.
    pub counters_unset: u64,
    /// The first droppable molecules, by uuid.
    #[serde(default)]
    pub droppable_sample: Vec<String>,
}

impl AtomStore {
    /// Walk the molecule headers, the `bindings` records and the binding records
    /// of one scope, and join their counters.
    pub(super) async fn inventory_molecule_counters(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<MoleculeCounterTotals, SchemaError> {
        let mut totals = MoleculeCounterTotals::default();
        let mut bound: HashMap<String, MoleculeBindings> = HashMap::new();
        let binding_prefix = build_storage_key(storage_prefix, "mref:v1:n:");
        self.fold_inventory_rows(&binding_prefix, "molecule bindings", |key, value| {
            let Ok(record) = serde_json::from_slice::<MoleculeBindings>(value) else {
                totals.suspect += 1;
                return;
            };
            let target = key.rsplit(':').next().unwrap_or_default().to_string();
            totals.bound_molecules += 1;
            totals.bindings_total = totals.bindings_total.saturating_add(record.bindings);
            if record.suspect {
                totals.suspect += 1;
            }
            bound.insert(target, record);
        })
        .await?;
        let record_prefix = build_storage_key(storage_prefix, "mref:v1:e:");
        let mut records = 0u64;
        self.fold_inventory_rows(&record_prefix, "molecule binding records", |_, _| {
            records += 1;
        })
        .await?;
        totals.binding_records = records;
        let header_prefix = build_storage_key(storage_prefix, MH_PREFIX);
        self.fold_inventory_rows(&header_prefix, "molecule headers", |key, value| {
            let Some(molecule) = molecule_uuid_from_storage_key(key) else {
                return;
            };
            fold_header(&mut totals, &bound, molecule, value);
        })
        .await?;
        Ok(totals)
    }
}

/// Fold one header into the totals.
fn fold_header(
    totals: &mut MoleculeCounterTotals,
    bound: &HashMap<String, MoleculeBindings>,
    molecule: &str,
    value: &[u8],
) {
    totals.headers += 1;
    let header = serde_json::from_slice::<serde_json::Value>(value).ok();
    let target = crate::hex::hex_lower(Sha256::digest(molecule.as_bytes()));
    let view = crate::db_operations::atom_store::molecule_view(
        molecule,
        header.as_ref(),
        bound.get(&target),
    );
    let counts = header.as_ref().and_then(counts_of_header);
    if counts.is_none() {
        totals.counters_unset += 1;
    }
    if counts.is_some_and(|counts| counts.suspect) {
        totals.suspect += 1;
    }
    totals.slots_total = totals.slots_total.saturating_add(view.slots);
    totals.records_total = totals.records_total.saturating_add(view.records);
    match view.state.as_str() {
        "live" => totals.live += 1,
        "droppable" => {
            totals.droppable += 1;
            if totals.droppable_sample.len() < DROPPABLE_SAMPLE {
                totals.droppable_sample.push(molecule.to_string());
            }
        }
        _ => {}
    }
}

impl AtomStore {
    /// Call `fold` for every row under `prefix`. Every read is bounded by the
    /// read budget of one call (see [`RangeWalk`]), also in a range that holds
    /// many deleted keys, such as the binding records after many unbinds.
    async fn fold_inventory_rows(
        &self,
        prefix: &str,
        what: &str,
        mut fold: impl FnMut(&str, &[u8]),
    ) -> Result<(), SchemaError> {
        let end = FilterUtils::create_prefix_end(prefix)?;
        let mut walk = RangeWalk::new(prefix.as_bytes(), end.as_bytes(), INVENTORY_PAGE_ROWS_MIN);
        while !walk.done() {
            let rows = self
                .walk_step(&mut walk)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("scan {what}: {e}")))?
                .rows;
            let mut page_bytes = 0u64;
            for (key, value) in &rows {
                page_bytes += (key.len() + value.len()) as u64;
                if let Ok(key) = std::str::from_utf8(key) {
                    fold(key, value);
                }
            }
            if !rows.is_empty() {
                walk.set_limit(inventory_next_page_rows(page_bytes, rows.len()));
            }
        }
        Ok(())
    }
}
