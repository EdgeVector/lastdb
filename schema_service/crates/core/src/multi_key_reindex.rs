//! Background tip regeneration for same-product multi-key schemas.
//!
//! Field mappers share **payload** molecules across keyed siblings. Partition
//! tips under a new/repaired key layout must still be built by walking source
//! records and emitting keyed membership rows — async/reindex job class
//! (preference-schema-expand-same-product-different-keys).
//!
//! # Shipped entry point
//!
//! [`KeyedMembershipIndex::reindex`] is the operator Mini / fkanban heal /
//! background workers call: plan tips from source records, persist them into
//! a partition-queryable index. Tests and callers query via
//! [`KeyedMembershipIndex::query_partition`] — not by re-planning.

use schema_types::KeyConfig;
use std::collections::HashMap;

/// One source-of-truth record (e.g. a Card) used to regenerate membership tips.
#[derive(Debug, Clone)]
pub struct SourceRecord {
    pub fields: HashMap<String, String>,
}

/// One regenerated tip under the target key layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegeneratedTip {
    /// Partition key (HashKey / HashRange hash component).
    pub hash_key: String,
    /// Range key when the target is HashRange; None for Hash-only.
    pub range_key: Option<String>,
    /// Thin row fields to dual-write (shared mapped fields + key fields).
    pub fields: HashMap<String, String>,
}

/// Plan tip regeneration for a target key layout over existing source records.
///
/// Pure planner used by [`KeyedMembershipIndex::reindex`]. Prefer the index
/// operator when you need queryable tips after rebuild.
///
/// Skips sources missing the partition field or (when required) the range field.
///
/// # Errors
/// Returns `Err` when the target key config has no hash_field.
pub fn plan_keyed_tip_regeneration(
    sources: &[SourceRecord],
    target_key: &KeyConfig,
    copy_fields: &[String],
) -> Result<Vec<RegeneratedTip>, String> {
    let hash_field = target_key
        .hash_field
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "target key has no hash_field".to_string())?;
    let range_field = target_key
        .range_field
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let mut out = Vec::new();
    for src in sources {
        let Some(hash_key) = src.fields.get(hash_field).map(|s| s.trim().to_string()) else {
            continue;
        };
        if hash_key.is_empty() {
            continue;
        }
        let range_key = if let Some(rf) = range_field {
            let Some(rk) = src.fields.get(rf).map(|s| s.trim().to_string()) else {
                continue;
            };
            if rk.is_empty() {
                continue;
            }
            Some(rk)
        } else {
            None
        };

        let mut fields = HashMap::new();
        fields.insert(hash_field.to_string(), hash_key.clone());
        if let (Some(rf), Some(rk)) = (range_field, range_key.as_ref()) {
            fields.insert(rf.to_string(), rk.clone());
        }
        for f in copy_fields {
            if let Some(v) = src.fields.get(f) {
                fields.entry(f.clone()).or_insert_with(|| v.clone());
            }
        }
        out.push(RegeneratedTip {
            hash_key,
            range_key,
            fields,
        });
    }
    Ok(out)
}
