//! The verifier: reports faults in the atom counters. It never changes one.
//!
//! `verify_refs_step` reads the tip records, the conflict records, and the body
//! and counter records of atoms, in pages with a cursor, and compares them. It
//! writes nothing: no counter, no body, no tip. It keeps its result in memory
//! only. It takes no write gate, so it never stalls a writer. The only effect
//! on a writer is the read load of a page and a one-line note in the planner
//! while a run reads (see [`Watch`]).
//!
//! # What a page proves
//!
//! One page is one captured read scope. The rows, the counters and the bodies
//! of the page are read at one committed root. A committed root holds whole
//! batches, so I1 to I4 hold at it:
//!
//! - a tip or record whose atom has no body is a fault,
//! - a counter below the tips and records of the page is a fault (the other
//!   holders of the atom only raise the true count),
//! - a body without a counter, a counter without a body, a counter at zero and
//!   a `suspect` counter are faults.
//!
//! No concurrent writer can explain any of these. The checks have no false
//! positive.
//!
//! # What only the whole pass shows
//!
//! A counter above its holders (a leak) needs the total of tips and records for
//! the atom over all pages. The pages are read at different roots, so a writer
//! can add or remove a holder between two pages. The planner tells the
//! [`Watch`] which atoms it changed while a run reads. A difference for an atom
//! no writer touched is a fault (`counter_above_tips`, or `counter_below_tips`
//! for a count split over pages). A difference for a touched atom is counted as
//! `unconfirmed_count_mismatch`: a candidate. To settle candidates, run the
//! verifier again when the writers are quiet, or repair with `recount` on a
//! stopped node.
//!
//! # The molecule counters
//!
//! Three more phases follow the atoms. They check the counters of molecules
//! (`docs/lastdb-molecule-counters.md`) and write nothing.
//!
//! - Headers: each header (`mh:{M}`) carries `slots` and `records`. The first two
//!   phases counted the tips and the conflict records of every molecule, so a
//!   header that differs is a fault, or a candidate when a writer changed the
//!   molecule during the pass (the planner notes the molecule in the [`Watch`]).
//!   A header with no counters, a `suspect` header, and a molecule that has tips
//!   and no header are faults.
//! - Bindings: each `bindings` counter is compared with the binding records of
//!   its molecule. One page reads both at one root, and a bind or an unbind
//!   writes both in one batch, so a difference is always a fault.
//! - Binding records: each molecule that has a binding record has a counter.
//!
//! # Memory
//!
//! The totals hold 16 bytes of digest and 4 bytes of count per referenced atom
//! and per molecule, at most `max_atoms` (1,000,000 by default). Beyond that the verifier does not
//! compare counters with totals, and it says so in the report. A run that no
//! step touched for ten minutes is dropped.

mod blob_pages;
mod blob_report;
mod blobs;
mod check;
mod molecule_judge;
mod molecule_pages;
mod molecule_report;
mod molecule_run;
mod pages;
mod queue;
mod report;
mod run;
mod state;
mod totals;

pub use report::{VerifyReport, VerifyRequest, VerifySummary};
pub(crate) use state::VerifyState;
