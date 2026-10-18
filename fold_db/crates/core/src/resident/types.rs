//! Stable kinds for the shared process counters.

use serde::{Deserialize, Serialize};

/// Kind of object in the resident graph.
///
/// Covers schema → field → molecule → atom, plus protein metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResidentKind {
    Schema,
    /// Molecule index residency (header + tip slots for touched keys).
    /// Alias name: not a disposable request-local FieldVariant.
    Molecule,
    /// Back-compat alias for tip-focused call sites (same metric bucket as Molecule).
    MoleculeTip,
    Atom,
    /// Legacy metric bucket. CAS bytes no longer enter the resident graph.
    FileBlob,
    Protein,
}

impl ResidentKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Schema => RESIDENT_KIND_SCHEMA,
            Self::Molecule | Self::MoleculeTip => RESIDENT_KIND_MOLECULE,
            Self::Atom => RESIDENT_KIND_ATOM,
            Self::FileBlob => RESIDENT_KIND_FILE_BLOB,
            Self::Protein => RESIDENT_KIND_PROTEIN,
        }
    }

    /// Metric / dirty identity (MoleculeTip collapses into Molecule).
    pub const fn canonical(self) -> Self {
        match self {
            Self::MoleculeTip => Self::Molecule,
            other => other,
        }
    }
}

pub const RESIDENT_KIND_SCHEMA: &str = "schema";
pub const RESIDENT_KIND_MOLECULE: &str = "molecule";
pub const RESIDENT_KIND_ATOM: &str = "atom";
pub const RESIDENT_KIND_FILE_BLOB: &str = "file_blob";
pub const RESIDENT_KIND_PROTEIN: &str = "protein";
