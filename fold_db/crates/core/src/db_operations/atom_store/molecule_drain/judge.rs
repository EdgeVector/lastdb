//! The verdict on one molecule: may the drain remove its keys or its family?
//!
//! The input is what the drain read inside the write gate: the header, the
//! `bindings` counter, and whether any binding record exists. The output says
//! whether nothing names the molecule and every counter can be trusted. This file
//! reads nothing and writes nothing, so a reviewer can check the rule without the
//! store code.
//!
//! The rule is stricter than the `droppable` state of the read verb. The counter
//! and the records must agree: a binding record with no counter, a counter that
//! is `suspect`, and a header without counters are faults. A fault is never a
//! drain. A molecule that a counter or a record names is not dead.

use crate::db_operations::atom_store::counter::{counts_of_header, MoleculeCounts};
use crate::db_operations::atom_store::MoleculeBindings;
use serde_json::Value;

/// What the drain reads about one molecule in one scope.
#[derive(Debug, Default)]
pub(crate) struct MoleculeReads {
    /// The header `mh:{M}`, if it exists.
    pub header: Option<Value>,
    /// The `bindings` counter record, if it exists.
    pub bindings: Option<MoleculeBindings>,
    /// At least one binding record `mref:v1:e:{target}\0...` exists.
    pub binding_records: bool,
}

/// Why a counter cannot authorize a removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Refusal {
    /// The header counters carry `suspect`.
    HeaderSuspect,
    /// The `bindings` counter carries `suspect`.
    BindingsSuspect,
    /// The header exists and carries no counters.
    CountersUnset,
    /// A binding record exists and the `bindings` counter is zero or absent.
    BindingRecordWithoutCounter,
    /// The header counts tip records (`slots`), and the key walk finds none. The
    /// counter is above the rows.
    SlotsWithoutTips,
}

impl Refusal {
    /// The name of the refusal in the status and in the log.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::HeaderSuspect => "header_suspect",
            Self::BindingsSuspect => "bindings_suspect",
            Self::CountersUnset => "counters_unset",
            Self::BindingRecordWithoutCounter => "binding_record_without_counter",
            Self::SlotsWithoutTips => "slots_without_tips",
        }
    }
}

/// The verdict on one molecule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Nothing names the molecule and every counter is trusted. `slots` and
    /// `records` are what the header says. The keys of the molecule may go. The
    /// family may go when both are zero.
    Dead {
        slots: u64,
        records: u64,
    },
    Bound,
    /// A counter or a record is in a state that no removal may rely on.
    Refused(Refusal),
    /// No header, no counter and no record. There is nothing to remove.
    Absent,
}

/// Decide from the reads of one write gate hold.
#[must_use]
pub(crate) fn judge(reads: &MoleculeReads) -> Verdict {
    if reads.bindings.as_ref().is_some_and(|record| record.suspect) {
        return Verdict::Refused(Refusal::BindingsSuspect);
    }
    if reads
        .bindings
        .as_ref()
        .is_some_and(|record| record.bindings > 0)
    {
        return Verdict::Bound;
    }
    if reads.binding_records {
        return Verdict::Refused(Refusal::BindingRecordWithoutCounter);
    }
    let Some(header) = reads.header.as_ref() else {
        return Verdict::Absent;
    };
    match counts_of_header(header) {
        None => Verdict::Refused(Refusal::CountersUnset),
        Some(counts) if counts.suspect => Verdict::Refused(Refusal::HeaderSuspect),
        Some(MoleculeCounts { slots, records, .. }) => Verdict::Dead { slots, records },
    }
}
