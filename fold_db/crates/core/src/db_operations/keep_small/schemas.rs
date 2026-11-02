//! Per-schema meters, churn report, and dirty-shard tracking.

use super::*;

impl KeepSmallMeters {
    /// Take the schema names whose shards a persist must rewrite.
    #[must_use]
    pub fn take_dirty_schemas(&self) -> HashSet<String> {
        self.dirty_schemas
            .lock()
            .map(|mut dirty| std::mem::take(&mut *dirty))
            .unwrap_or_default()
    }

    /// Mark every live schema dirty so the next persist rewrites every shard.
    pub fn mark_all_schemas_dirty(&self) {
        let names: Vec<String> = self
            .schemas
            .lock()
            .map(|map| map.keys().cloned().collect())
            .unwrap_or_default();
        if let Ok(mut dirty) = self.dirty_schemas.lock() {
            dirty.extend(names);
        }
    }

    pub fn clear_dirty_schemas(&self) {
        if let Ok(mut dirty) = self.dirty_schemas.lock() {
            dirty.clear();
        }
    }

    pub fn apply_display_names(&self, names: &HashMap<String, String>) {
        let Ok(mut map) = self.schemas.lock() else {
            return;
        };
        for meter in map.values_mut() {
            if let Some(label) = names.get(&meter.schema_name) {
                let label = label.trim();
                if !label.is_empty() && label != meter.schema_name {
                    meter.display_name = Some(label.to_string());
                }
            }
        }
    }
}
