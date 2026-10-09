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

    pub fn apply_schema(&self, schema: Schema) {
        // Dirty BEFORE install: the install's budget-enforcement pass must
        // already see this entry as dirty, or it could evict the write it is
        // in the middle of applying.
        self.mark_dirty(DirtyKey::Schema(schema.name.clone()));
        self.install_schema(schema);
    }

    pub fn has_schema(&self, name: &str) -> bool {
        self.schemas
            .read()
            .expect("schemas lock")
            .contains_key(name)
    }

    pub fn is_schema_dirty(&self, name: &str) -> bool {
        self.is_dirty(&DirtyKey::Schema(name.to_string()))
    }

    pub fn resolve_schema(
        &self,
        name: &str,
        loader: &dyn SchemaLoader,
    ) -> Result<Option<ResolveOutcome<Schema>>, String> {
        {
            let map = self.schemas.read().expect("schemas lock");
            if let Some(schema) = map.get(name) {
                self.metrics.record_hit(ResidentKind::Schema);
                let outcome = ResolveOutcome::hit(schema.clone());
                drop(map);
                self.note_touch(&DirtyKey::Schema(name.to_string()));
                return Ok(Some(outcome));
            }
        }
        let Some(schema) = loader.load_schema(name)? else {
            return Ok(None);
        };
        self.install_schema(schema.clone());
        self.metrics.record_rehydrate(ResidentKind::Schema);
        Ok(Some(ResolveOutcome::rehydrated(schema)))
    }

    pub fn try_evict_schema(&self, name: &str) -> Result<bool, String> {
        let key = DirtyKey::Schema(name.to_string());
        if self.is_dirty(&key) {
            return Err(format!(
                "refuse to evict dirty schema '{name}' until persist acks"
            ));
        }
        let removed = self
            .schemas
            .write()
            .expect("schemas lock")
            .remove(name)
            .is_some();
        if removed {
            self.discharge(&key);
        }
        Ok(removed)
    }

    pub fn schema_count(&self) -> usize {
        self.schemas.read().expect("schemas lock").len()
    }

    // ── Tips / atoms ────────────────────────────────────────────
}
