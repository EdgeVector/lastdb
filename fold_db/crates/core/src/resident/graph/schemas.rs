//! Schema install, resolve and eviction for the resident graph.

use super::*;

impl ResidentGraph {
    pub fn install_schema(&self, schema: Schema) {
        let name = schema.name.clone();
        self.schemas
            .write()
            .expect("schemas lock")
            .insert(name.clone(), schema);
        self.charge_and_enforce(DirtyKey::Schema(name), APPROX_SCHEMA_BYTES);
    }

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
