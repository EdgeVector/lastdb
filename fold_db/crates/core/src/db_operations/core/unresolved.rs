//! Compatibility reports for clients that read the former skipped-body gauges.
//! Selected-tip reads now require every body and return an error on a miss.

use super::DbOperations;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnresolvedDamage {
    pub edges: u64,
    pub rows: u64,
    pub capped: bool,
}

impl DbOperations {
    /// Native reads never skip a selected body, so this compatibility gauge is zero.
    #[must_use]
    pub fn unresolved_atom_skip_count(&self) -> u64 {
        0
    }

    /// Preserve the former damage-report shape without dormant identity storage.
    #[must_use]
    pub fn unresolved_atom_distinct(&self) -> UnresolvedDamage {
        UnresolvedDamage {
            edges: 0,
            rows: 0,
            capped: false,
        }
    }
}
