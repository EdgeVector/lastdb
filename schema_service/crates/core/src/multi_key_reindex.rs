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

/// Queryable membership tip store for a single key layout.
///
/// This is the **shipped reindex operator surface**: run [`Self::reindex`]
/// (or [`Self::reindex_into`] for an existing store) then
/// [`Self::query_partition`] under the new key. Background workers and
/// heal paths dual-write the same tip rows to Mini; this type is the
/// in-process contract and test fixture for that job class.
#[derive(Debug, Clone, Default)]
pub struct KeyedMembershipIndex {
    /// Target key layout this index materializes.
    target_key: Option<KeyConfig>,
    /// All tips in insertion order.
    tips: Vec<RegeneratedTip>,
    /// Partition hash_key → indices into `tips`.
    by_partition: HashMap<String, Vec<usize>>,
}

impl KeyedMembershipIndex {
    /// Empty index (no tips yet).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// **Reindex entry point:** plan tips from source records under
    /// `target_key`, replace this store's contents, return self.
    ///
    /// After this returns, [`Self::query_partition`] for each partition
    /// present on the sources is non-empty (for records that had the
    /// partition field).
    pub fn reindex(
        sources: &[SourceRecord],
        target_key: &KeyConfig,
        copy_fields: &[String],
    ) -> Result<Self, String> {
        let mut idx = Self::new();
        idx.reindex_into(sources, target_key, copy_fields)?;
        Ok(idx)
    }

    /// Rebuild this index from sources (clears prior tips first).
    pub fn reindex_into(
        &mut self,
        sources: &[SourceRecord],
        target_key: &KeyConfig,
        copy_fields: &[String],
    ) -> Result<usize, String> {
        let planned = plan_keyed_tip_regeneration(sources, target_key, copy_fields)?;
        self.tips.clear();
        self.by_partition.clear();
        self.target_key = Some(target_key.clone());
        for tip in planned {
            let part = tip.hash_key.clone();
            let i = self.tips.len();
            self.tips.push(tip);
            self.by_partition.entry(part).or_default().push(i);
        }
        Ok(self.tips.len())
    }

    /// Query membership tips under a partition key (HashRange hash component).
    #[must_use]
    pub fn query_partition(&self, hash_key: &str) -> Vec<&RegeneratedTip> {
        self.by_partition
            .get(hash_key)
            .map(|idxs| idxs.iter().filter_map(|&i| self.tips.get(i)).collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.tips.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tips.is_empty()
    }

    #[must_use]
    pub fn target_key(&self) -> Option<&KeyConfig> {
        self.target_key.as_ref()
    }
}
