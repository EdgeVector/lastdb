//! Re-exports `lastdb_model::schema::types` plus task-local key comparison and mutation IDs.

pub use key_comparison::KeyValuePageOrder;
pub use lastdb_model::schema::types::*;

use lastdb_model::record_replacement::{RecordExpectation, RecordTarget};
use serde_json::Value;
use std::collections::HashMap;

pub trait MutationIds {
    fn new(
        id: String,
        target: RecordTarget,
        record_fields: HashMap<String, Value>,
        pub_key: String,
        mutation_type: MutationType,
        expected: RecordExpectation,
    ) -> Self;
}

impl MutationIds for Mutation {
    fn new(
        id: String,
        target: RecordTarget,
        record_fields: HashMap<String, Value>,
        pub_key: String,
        mutation_type: MutationType,
        expected: RecordExpectation,
    ) -> Self {
        Self::with_uuid(id, target, record_fields, pub_key, mutation_type, expected)
    }
}

pub mod key_comparison {
    use lastdb_model::schema::types::field::{HashRangeFilter, KeyWindow};
    use lastdb_model::schema::types::{FieldValueType, KeyValue, Schema, SchemaError};
    use schema_types::field_range_profile::FieldRangeProfile;
    pub use schema_types::key_comparison::RangeKeyComparison;
    use sha2::{Digest, Sha256};
    use std::{
        cmp::Ordering,
        future::Future,
        sync::{Arc, OnceLock},
    };

    tokio::task_local! {
        static COMPARISON: Arc<KeyComparisonContext>;
    }

    #[derive(Debug, Clone)]
    pub struct KeyComparisonContext {
        identity: String,
        range: RangeKeyComparison,
        failure: OnceLock<String>,
        profile: FieldRangeProfile,
    }

    impl KeyComparisonContext {
        pub fn for_schema(schema: &Schema) -> Result<Self, SchemaError> {
            let keys = schema.require_record_keys()?;
            let range_type = keys
                .primary
                .range_type
                .clone()
                .unwrap_or(FieldValueType::String);
            let range_comparison =
                RangeKeyComparison::typed(range_type).map_err(SchemaError::InvalidData)?;
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
                &range_comparison,
            ))
            .map_err(|_| SchemaError::InvalidData("cannot encode key comparison context".into()))?;
            Ok(Self {
                identity: format!("kc1:{:x}", Sha256::digest(bytes)),
                range: range_comparison,
                failure: OnceLock::new(),
                profile,
            })
        }

        #[must_use]
        pub fn identity(&self) -> &str {
            &self.identity
        }

        #[must_use]
        pub fn range_comparison(&self) -> &RangeKeyComparison {
            &self.range
        }

        pub fn cursor_binding(
            &self,
            request: &impl serde::Serialize,
        ) -> Result<String, SchemaError> {
            let bytes = serde_json::to_vec(&(&self.identity, request)).map_err(|_| {
                SchemaError::InvalidData("cannot encode cursor comparison context".into())
            })?;
            Ok(format!("qc1:{:x}", Sha256::digest(bytes)))
        }

        pub async fn run<T, E>(
            self: Arc<Self>,
            future: impl Future<Output = Result<T, E>>,
        ) -> Result<T, E>
        where
            E: From<SchemaError>,
        {
            if let Ok(active) = COMPARISON.try_with(Arc::clone) {
                if active.identity != self.identity {
                    return Err(SchemaError::InvalidData(
                        "key comparison context changed during the read".into(),
                    )
                    .into());
                }
                let result = future.await?;
                active.require_valid_comparison()?;
                return Ok(result);
            }
            let result = COMPARISON.scope(Arc::clone(&self), future).await?;
            self.require_valid_comparison()?;
            Ok(result)
        }

        #[must_use]
        pub fn capture_current_range_comparison() -> Option<RangeKeyComparison> {
            COMPARISON.try_with(|ctx| ctx.range.clone()).ok()
        }

        #[must_use]
        pub fn compare_current_ranges(left: &str, right: &str) -> Ordering {
            COMPARISON
                .try_with(|ctx| ctx.compare_ranges(left, right))
                .unwrap_or_else(|_| left.cmp(right))
        }

        fn compare_ranges(&self, left: &str, right: &str) -> Ordering {
            match self.range.compare(left, right) {
                Ok(order) => order,
                Err(detail) => {
                    let _ = self.failure.set(detail);
                    self.range
                        .validate_value(left)
                        .is_ok()
                        .cmp(&self.range.validate_value(right).is_ok())
                        .then_with(|| left.cmp(right))
                }
            }
        }

        fn require_valid_comparison(&self) -> Result<(), SchemaError> {
            match self.failure.get() {
                Some(detail) => Err(SchemaError::InvalidData(detail.clone())),
                None => Ok(()),
            }
        }

        pub(crate) fn compare_current_keys(left: &KeyValue, right: &KeyValue) -> Ordering {
            left.cmp_page_order_with(right, Self::compare_current_ranges)
        }

        pub fn canonical_range(&self, value: &str) -> Result<String, SchemaError> {
            normalize::canonical_range(self.profile, value)
        }

        pub fn canonical_key(&self, mut key: KeyValue) -> Result<KeyValue, SchemaError> {
            normalize::canonical_key(self.profile, &mut key)?;
            Ok(key)
        }

        pub fn canonical_filter(
            &self,
            filter: Option<HashRangeFilter>,
        ) -> Result<Option<HashRangeFilter>, SchemaError> {
            normalize::canonical_filter(self.profile, filter)
        }

        pub fn canonical_window(
            &self,
            window: Option<KeyWindow>,
        ) -> Result<Option<KeyWindow>, SchemaError> {
            normalize::canonical_window(self.profile, window)
        }
    }

    pub trait KeyValuePageOrder {
        fn cmp_page_order(&self, other: &Self) -> Ordering;
    }

    impl KeyValuePageOrder for KeyValue {
        fn cmp_page_order(&self, other: &Self) -> Ordering {
            KeyComparisonContext::compare_current_keys(self, other)
        }
    }

    pub(crate) mod normalize {
        use lastdb_model::schema::types::field::{HashRangeFilter, KeyWindow};
        use lastdb_model::schema::types::{KeyValue, SchemaError};
        use schema_types::field_range_profile::FieldRangeProfile;

        pub(crate) fn canonical_window(
            profile: FieldRangeProfile,
            window: Option<KeyWindow>,
        ) -> Result<Option<KeyWindow>, SchemaError> {
            match window {
                Some(KeyWindow::After { mut after, limit }) => {
                    canonical_key(profile, &mut after)?;
                    Ok(Some(KeyWindow::after(after, limit)))
                }
                other => Ok(other),
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

        pub(crate) fn canonical_filter(
            profile: FieldRangeProfile,
            mut filter: Option<HashRangeFilter>,
        ) -> Result<Option<HashRangeFilter>, SchemaError> {
            use HashRangeFilter::*;
            match filter.as_mut() {
                Some(HashRangeKey { range, .. }) => *range = canonical_range(profile, range)?,
                Some(HashRangeKeys(keys)) => {
                    for (_, range) in keys {
                        *range = canonical_range(profile, range)?;
                    }
                }
                Some(HashRangeRange { start, end, .. }) => {
                    *start = canonical_range(profile, start)?;
                    *end = canonical_range(profile, end)?;
                }
                Some(HashRangePrefix { .. }) => {
                    profile
                        .require_text_prefix()
                        .map_err(SchemaError::InvalidData)?;
                }
                None | Some(HashKey(_)) => {}
            }
            Ok(filter)
        }
    }
}
