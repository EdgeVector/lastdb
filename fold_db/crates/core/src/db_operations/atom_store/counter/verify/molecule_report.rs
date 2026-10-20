//! The molecule fault counts of a verify report. They are part of
//! [`super::report::Mismatches`] and serialize flat next to the atom counts.

use super::report::Kind;
use serde::{Deserialize, Serialize};

/// Faults of the molecule phases, by kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoleculeMismatches {
    /// A header `slots` count differs from the tips that the first phase
    /// counted for the molecule.
    #[serde(default)]
    pub molecule_slots_above_tips: u64,
    #[serde(default)]
    pub molecule_slots_below_tips: u64,
    /// A header `records` count differs from the conflict records counted.
    #[serde(default)]
    pub molecule_records_above_records: u64,
    #[serde(default)]
    pub molecule_records_below_records: u64,
    /// A molecule has tips and no header.
    #[serde(default)]
    pub tips_without_header: u64,
    /// A header carries no counters.
    #[serde(default)]
    pub molecule_counters_unset: u64,
    /// A header has the `suspect` flag.
    #[serde(default)]
    pub suspect_molecule: u64,
    #[serde(default)]
    pub unreadable_header: u64,
    /// A `bindings` counter differs from the number of binding records.
    #[serde(default)]
    pub bindings_counter_mismatch: u64,
    /// A binding record has no `bindings` counter.
    #[serde(default)]
    pub binding_record_without_counter: u64,
    /// A `bindings` counter holds zero and is not `suspect`.
    #[serde(default)]
    pub zero_bindings_counter: u64,
    #[serde(default)]
    pub suspect_bindings: u64,
    #[serde(default)]
    pub unreadable_bindings: u64,
}

impl MoleculeMismatches {
    pub(super) fn slot(&mut self, kind: Kind) -> &mut u64 {
        match kind {
            Kind::SlotsAboveTips => &mut self.molecule_slots_above_tips,
            Kind::SlotsBelowTips => &mut self.molecule_slots_below_tips,
            Kind::RecordsAboveRecords => &mut self.molecule_records_above_records,
            Kind::RecordsBelowRecords => &mut self.molecule_records_below_records,
            Kind::TipsWithoutHeader => &mut self.tips_without_header,
            Kind::MoleculeCountersUnset => &mut self.molecule_counters_unset,
            Kind::SuspectMolecule => &mut self.suspect_molecule,
            Kind::UnreadableHeader => &mut self.unreadable_header,
            Kind::BindingsMismatch => &mut self.bindings_counter_mismatch,
            Kind::BindingRecordWithoutCounter => &mut self.binding_record_without_counter,
            Kind::ZeroBindingsCounter => &mut self.zero_bindings_counter,
            Kind::SuspectBindings => &mut self.suspect_bindings,
            Kind::UnreadableBindings => &mut self.unreadable_bindings,
            other => unreachable!("{} is not a molecule fault kind", other.name()),
        }
    }

    /// The molecule counter faults.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.molecule_slots_above_tips
            + self.molecule_slots_below_tips
            + self.molecule_records_above_records
            + self.molecule_records_below_records
            + self.tips_without_header
            + self.molecule_counters_unset
            + self.suspect_molecule
            + self.unreadable_header
            + self.bindings_counter_mismatch
            + self.binding_record_without_counter
            + self.zero_bindings_counter
            + self.suspect_bindings
            + self.unreadable_bindings
    }
}
