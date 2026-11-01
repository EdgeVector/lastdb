//! per-field-key storage counters and their owning schemas.

use super::*;

impl KeepSmallMeters {
    /// Bind a field key to the schema that owns it.
    pub fn remember_field_key_schema(&self, field_key_id: &str, schema: &str) {
        if field_key_id.is_empty() || schema.is_empty() {
            return;
        }
        if let Ok(mut map) = self.field_key_schema.lock() {
            map.insert(field_key_id.to_string(), schema.to_string());
        }
        self.mark_schema_dirty(Some(schema));
    }

    /// Schema that owns `field_key_id`, if a schema install has bound it.
    #[must_use]
    pub fn field_key_schema(&self, field_key_id: &str) -> Option<String> {
        self.field_key_schema
            .lock()
            .ok()
            .and_then(|m| m.get(field_key_id).cloned())
    }

    #[must_use]
    pub fn field_key_counter(&self, field_key_id: &str) -> Option<FieldKeyStorageCounter> {
        self.field_keys
            .lock()
            .ok()
            .and_then(|counters| counters.get(field_key_id).cloned())
    }

    /// Seed a declared but empty field key with an exact zero counter.
    pub fn ensure_field_key_counter(&self, field_key_id: &str, schema: &str) {
        if field_key_id.is_empty() {
            return;
        }
        if let Ok(mut counters) = self.field_keys.lock() {
            counters
                .entry(field_key_id.to_string())
                .or_insert_with(|| FieldKeyStorageCounter {
                    field_key_id: field_key_id.to_string(),
                    ..FieldKeyStorageCounter::default()
                });
        }
        self.remember_field_key_schema(field_key_id, schema);
    }

    #[must_use]
    pub fn field_key_counters_complete(&self) -> bool {
        let trust_complete = self.trust.lock().is_ok_and(|trust| {
            trust.field_keys.state.is_complete()
                || (trust.field_keys.state == MeterTrustState::Absent
                    && self.schemas.lock().is_ok_and(|schemas| schemas.is_empty()))
        });
        self.field_key_counters_complete.load(Ordering::Relaxed) && trust_complete
    }
}
