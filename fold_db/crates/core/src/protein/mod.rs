//! Protein — UUID'd molecule set that stays coherent by construction.
//!
//! A **protein** binds member molecules so they share one atom set. Membership
//! is bi-directional: the protein lists members, and each member stores the
//! protein UUID. A write on any member updates that member's tip + the shared
//! atom, and prepares the sibling members' tips in the same write request.
//!
//! Design: `design-lastdb-protein-molecule-set`, `concepts-lastdb-canonical-model`.

mod keys;
mod ops;
mod types;

pub use keys::{member_backref_key, protein_record_key};
pub(crate) use keys::{MEMBER_BACKREF_PREFIX, PROTEIN_RECORD_PREFIX};
pub use types::{Protein, ProteinMember, PROTEIN_SCHEMA_MARKER};
