//! Schema install, resolve and eviction for the resident graph.

use super::*;

impl ResidentGraph {
    pub fn has_schema(&self, name: &str) -> bool {
        self.schemas
            .read()
            .expect("schemas lock")
            .contains_key(name)
    }

    pub fn schema_count(&self) -> usize {
        self.schemas.read().expect("schemas lock").len()
    }

    // ── Tips / atoms ────────────────────────────────────────────
}
