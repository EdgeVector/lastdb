//! Per-schema meters, churn report, dirty-shard tracking and pending protein folds.

use super::*;

impl KeepSmallMeters {
    pub fn set_pending_protein_folds(&self, count: u64) {
        self.pending_protein_folds.store(count, Ordering::Relaxed);
    }

    pub fn add_pending_protein_folds(&self, count: u64) {
        self.pending_protein_folds
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn complete_pending_protein_fold(&self) {
        let current = self.pending_protein_folds.load(Ordering::Relaxed);
        if current > 0 {
            self.pending_protein_folds.fetch_sub(1, Ordering::Relaxed);
        }
    }

    #[must_use]
    pub fn pending_protein_folds(&self) -> u64 {
        self.pending_protein_folds.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn schema_meters(&self) -> Vec<SchemaMeter> {
        let mut rows: Vec<SchemaMeter> = self
            .schemas
            .lock()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        rows.sort_by(|a, b| {
            b.churn_ratio()
                .partial_cmp(&a.churn_ratio())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.live_bytes.cmp(&a.live_bytes))
                .then_with(|| a.schema_name.cmp(&b.schema_name))
        });
        rows
    }

    #[must_use]
    pub fn churn_report(&self) -> ChurnReport {
        let day = utc_day(Utc::now());
        let per_schema: Vec<SchemaChurnRow> = self
            .schema_meters()
            .into_iter()
            .filter(|m| m.live_bytes > 0 || m.appended_bytes > 0)
            .map(|m| SchemaChurnRow {
                churn_ratio: m.churn_ratio(),
                schema_name: m.schema_name,
                display_name: m.display_name,
                live_bytes: m.live_bytes,
                appended_bytes: m.appended_bytes,
            })
            .collect();
        ChurnReport {
            measured_at: Utc::now(),
            heavy: false,
            day,
            per_schema,
        }
    }

    pub(super) fn add_appended(meter: &mut SchemaMeter, bytes: u64) {
        let today = utc_day(Utc::now());
        if meter.appended_day != today {
            meter.appended_day = today;
            meter.appended_bytes = 0;
        }
        meter.appended_bytes = meter.appended_bytes.saturating_add(bytes);
    }

    pub(super) fn mark_schema_dirty(&self, schema: Option<&str>) {
        let name = schema
            .filter(|s| !s.is_empty())
            .unwrap_or(KEEP_SMALL_UNATTRIBUTED_SCHEMA);
        if let Ok(mut dirty) = self.dirty_schemas.lock() {
            dirty.insert(name.to_string());
        }
    }

    /// Take the schema names whose shards a persist must rewrite.
    #[must_use]
    pub fn take_dirty_schemas(&self) -> HashSet<String> {
        self.dirty_schemas
            .lock()
            .map(|mut dirty| std::mem::take(&mut *dirty))
            .unwrap_or_default()
    }

    /// Mark every live schema dirty so the next persist splits a legacy
    /// whole-map snapshot into shards.
    pub fn mark_all_schemas_dirty(&self) {
        let names: Vec<String> = self
            .schemas
            .lock()
            .map(|map| map.keys().cloned().collect())
            .unwrap_or_default();
        if let Ok(mut dirty) = self.dirty_schemas.lock() {
            dirty.extend(names);
            dirty.insert(KEEP_SMALL_UNATTRIBUTED_SCHEMA.to_string());
        }
    }

    pub fn clear_dirty_schemas(&self) {
        if let Ok(mut dirty) = self.dirty_schemas.lock() {
            dirty.clear();
        }
    }

    pub(super) fn with_schema(&self, schema: &str, f: impl FnOnce(&mut SchemaMeter)) {
        let Ok(mut map) = self.schemas.lock() else {
            return;
        };
        let meter = map
            .entry(schema.to_string())
            .or_insert_with(|| SchemaMeter {
                schema_name: schema.to_string(),
                display_name: None,
                live_bytes: 0,
                atom_count: 0,
                appended_bytes: 0,
                appended_day: utc_day(Utc::now()),
                tip_bytes: 0,
                bookkeeping_bytes: 0,
            });
        f(meter);
        drop(map);
        self.mark_schema_dirty(Some(schema));
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
