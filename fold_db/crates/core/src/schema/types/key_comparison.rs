//! Explicit fallible key comparison, canonical range/filter codecs, and cursor bindings.

use lastdb_model::schema::types::field::{HashRangeFilter, KeyWindow};
use lastdb_model::schema::types::schema::DeclarativeSchemaType;
use lastdb_model::schema::types::{FieldValueType, KeyValue, Schema, SchemaError};
use schema_types::field_range_profile::FieldRangeProfile;
pub use schema_types::key_comparison::RangeKeyComparison;
use sha2::{Digest, Sha256};
use std::{cmp::Ordering, future::Future, sync::Arc};

tokio::task_local! {
    static COMPARISON: Arc<KeyComparisonContext>;
}

#[derive(Debug, Clone)]
pub struct KeyComparisonContext {
    schema_name: String,
    identity_hash: Option<String>,
    owner_app_id: Option<String>,
    schema_type: DeclarativeSchemaType,
    record_keys: schema_types::record_keys::RecordKeys,
    identity: String,
    range: RangeKeyComparison,
    profile: FieldRangeProfile,
}

impl KeyComparisonContext {
    pub fn for_schema(schema: &Schema) -> Result<Self, SchemaError> {
        let keys = schema.require_record_keys()?;
        let rt = keys
            .primary
            .range_type
            .clone()
            .unwrap_or(FieldValueType::String);
        let range = RangeKeyComparison::typed(rt).map_err(SchemaError::InvalidData)?;
        let profile = keys
            .primary
            .range_profile()
            .map_err(SchemaError::InvalidData)?;
        let bytes = serde_json::to_vec(&(
            "lastdb-key-comparison-v2",
            &schema.name,
            &schema.identity_hash,
            &schema.owner_app_id,
            &schema.schema_type,
            keys,
            &range,
        ))
        .map_err(|_| SchemaError::InvalidData("cannot encode key comparison context".into()))?;
        Ok(Self {
            schema_name: schema.name.clone(),
            identity_hash: schema.identity_hash.clone(),
            owner_app_id: schema.owner_app_id.clone(),
            schema_type: schema.schema_type.clone(),
            record_keys: keys.clone(),
            identity: format!("kc1:{:x}", Sha256::digest(bytes)),
            range,
            profile,
        })
    }

    pub(crate) fn current_for_schema(schema: &Schema) -> Option<Arc<Self>> {
        COMPARISON
            .try_with(|ctx| ctx.matches_schema(schema).then(|| Arc::clone(ctx)))
            .ok()
            .flatten()
    }

    fn matches_schema(&self, schema: &Schema) -> bool {
        self.schema_name == schema.name
            && self.identity_hash == schema.identity_hash
            && self.owner_app_id == schema.owner_app_id
            && self.schema_type == schema.schema_type
            && schema.record_keys.as_ref() == Some(&self.record_keys)
    }

    pub(crate) fn verify_active(&self) -> Result<(), SchemaError> {
        if let Ok(same) = COMPARISON.try_with(|active| active.identity == self.identity) {
            if !same {
                return Err(SchemaError::InvalidData(
                    "key comparison context changed during the read".into(),
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn range_comparison(&self) -> &RangeKeyComparison {
        &self.range
    }

    pub fn cursor_binding(&self, request: &impl serde::Serialize) -> Result<String, SchemaError> {
        let bytes = serde_json::to_vec(&(&self.identity, request)).map_err(|_| {
            SchemaError::InvalidData("cannot encode cursor comparison context".into())
        })?;
        Ok(format!("qc1:{:x}", Sha256::digest(bytes)))
    }

    pub async fn run<T, E: From<SchemaError>>(
        self: Arc<Self>,
        future: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        if let Ok(active) = COMPARISON.try_with(Arc::clone) {
            if active.identity != self.identity {
                return Err(SchemaError::InvalidData(
                    "key comparison context changed during the read".into(),
                )
                .into());
            }
            return future.await;
        }
        COMPARISON.scope(self, future).await
    }

    #[must_use]
    pub fn capture_current_range_comparison() -> Option<RangeKeyComparison> {
        COMPARISON.try_with(|ctx| ctx.range.clone()).ok()
    }

    pub fn cmp_with_context(
        &self,
        left: &KeyValue,
        right: &KeyValue,
    ) -> Result<Ordering, SchemaError> {
        let l = left.range.as_deref().unwrap_or("");
        let r = right.range.as_deref().unwrap_or("");
        Ok(self
            .range
            .compare(l, r)
            .map_err(SchemaError::InvalidData)?
            .then_with(|| {
                left.hash
                    .as_deref()
                    .unwrap_or("")
                    .cmp(right.hash.as_deref().unwrap_or(""))
            }))
    }

    pub fn sort_by_key_context<T>(
        &self,
        rows: &mut [T],
        key_of: impl Fn(&T) -> &KeyValue,
    ) -> Result<(), SchemaError> {
        if rows.len() <= 1 {
            return Ok(());
        }
        for row in &*rows {
            self.range
                .validate_value(key_of(row).range.as_deref().unwrap_or(""))
                .map_err(SchemaError::InvalidData)?;
        }
        rows.sort_by(|a, b| {
            key_of(a).cmp_page_order_with(key_of(b), |l, r| {
                self.range.compare(l, r).unwrap_or_else(|_| l.cmp(r))
            })
        });
        Ok(())
    }

    fn compare_current_ranges(l: &str, r: &str) -> Ordering {
        COMPARISON
            .try_with(|ctx| {
                ctx.range.compare(l, r).unwrap_or_else(|_| {
                    ctx.range
                        .validate_value(l)
                        .is_ok()
                        .cmp(&ctx.range.validate_value(r).is_ok())
                        .then_with(|| l.cmp(r))
                })
            })
            .unwrap_or_else(|_| l.cmp(r))
    }

    pub fn canonical_key(&self, mut key: KeyValue) -> Result<KeyValue, SchemaError> {
        canonical_key(self.profile, &mut key)?;
        Ok(key)
    }

    pub fn canonical_filter(
        &self,
        mut filter: Option<HashRangeFilter>,
    ) -> Result<Option<HashRangeFilter>, SchemaError> {
        use HashRangeFilter::*;
        match filter.as_mut() {
            Some(HashRangeKey { range, .. }) => *range = canonical_range(self.profile, range)?,
            Some(HashRangeKeys(keys)) => {
                for (_, r) in keys {
                    *r = canonical_range(self.profile, r)?;
                }
            }
            Some(HashRangeRange { start, end, .. }) => {
                *start = canonical_range(self.profile, start)?;
                *end = canonical_range(self.profile, end)?;
            }
            Some(HashRangePrefix { .. }) => {
                self.profile
                    .require_text_prefix()
                    .map_err(SchemaError::InvalidData)?;
            }
            None | Some(HashKey(_)) => {}
        }
        Ok(filter)
    }

    pub fn canonical_window(
        &self,
        window: Option<KeyWindow>,
    ) -> Result<Option<KeyWindow>, SchemaError> {
        match window {
            Some(KeyWindow::After { mut after, limit }) => {
                canonical_key(self.profile, &mut after)?;
                Ok(Some(KeyWindow::after(after, limit)))
            }
            other => Ok(other),
        }
    }
}

pub trait KeyValuePageOrder {
    fn cmp_page_order(&self, other: &Self) -> Ordering;
}

impl KeyValuePageOrder for KeyValue {
    fn cmp_page_order(&self, other: &Self) -> Ordering {
        self.cmp_page_order_with(other, KeyComparisonContext::compare_current_ranges)
    }
}

pub(crate) fn canonical_range(
    profile: FieldRangeProfile,
    value: &str,
) -> Result<String, SchemaError> {
    profile
        .encode_value(value)
        .and_then(|stored| profile.decode_value(&stored))
        .map_err(SchemaError::InvalidData)
}

pub(crate) fn canonical_key(
    profile: FieldRangeProfile,
    key: &mut KeyValue,
) -> Result<(), SchemaError> {
    if let Some(range) = key.range.as_mut() {
        *range = canonical_range(profile, range)?;
    } else if matches!(profile, FieldRangeProfile::TypedV1 { .. }) {
        return Err(SchemaError::InvalidData(
            "a typed range key requires a range coordinate".into(),
        ));
    }
    Ok(())
}
