//! Re-exports `lastdb_model::schema::types`, schema state/retention types, and key comparison.

pub use key_comparison::KeyValuePageOrder;
pub use lastdb_model::schema::types::*;

pub mod schema_types {
    use lastdb_model::schema::types::declarative_schemas::DeclarativeSchemaDefinition;
    use lastdb_model::schema::types::schema::DeclarativeSchemaType;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
    pub enum SchemaState {
        #[default]
        Available,
        Blocked,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SchemaRetentionPolicy {
        pub ttl_seconds: u64,
        pub source: RetentionSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub overridden_manifest_ttl_seconds: Option<u64>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub hash_partitions: Vec<String>,
    }

    impl SchemaRetentionPolicy {
        pub fn operator(ttl_seconds: u64, hash_partitions: Vec<String>) -> Self {
            Self {
                ttl_seconds,
                source: RetentionSource::Operator,
                overridden_manifest_ttl_seconds: None,
                hash_partitions,
            }
        }

        pub fn manifest(ttl_seconds: u64) -> Self {
            Self {
                ttl_seconds,
                source: RetentionSource::Manifest,
                overridden_manifest_ttl_seconds: None,
                hash_partitions: Vec::new(),
            }
        }

        pub fn operator_over(
            previous: Option<&Self>,
            ttl_seconds: u64,
            mut hash_partitions: Vec<String>,
        ) -> Self {
            hash_partitions.sort();
            hash_partitions.dedup();
            Self {
                ttl_seconds,
                source: RetentionSource::Operator,
                overridden_manifest_ttl_seconds: previous.and_then(Self::manifest_ttl_seconds),
                hash_partitions,
            }
        }

        pub fn after_operator_clear(previous: Option<&Self>) -> Option<Self> {
            previous
                .filter(|p| p.source == RetentionSource::Operator)
                .and_then(|p| p.overridden_manifest_ttl_seconds)
                .map(Self::manifest)
        }

        pub fn manifest_ttl_seconds(&self) -> Option<u64> {
            match self.source {
                RetentionSource::Manifest => Some(self.ttl_seconds),
                RetentionSource::Operator => self.overridden_manifest_ttl_seconds,
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum RetentionSource {
        Operator,
        Manifest,
    }

    impl RetentionSource {
        pub fn as_str(self) -> &'static str {
            match self {
                Self::Operator => "operator",
                Self::Manifest => "manifest",
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SchemaWithState {
        #[serde(flatten)]
        pub schema: DeclarativeSchemaDefinition,
        pub state: SchemaState,
    }

    impl SchemaWithState {
        pub fn new(schema: DeclarativeSchemaDefinition, state: SchemaState) -> Self {
            Self { schema, state }
        }

        #[must_use]
        pub fn name(&self) -> &str {
            &self.schema.name
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SchemaListEntry {
        pub name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub descriptive_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub purpose_statement: Option<String>,
        pub schema_type: DeclarativeSchemaType,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub record_keys: Option<::schema_types::record_keys::RecordKeys>,
        pub fields: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub hash: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub identity_hash: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub owner_app_id: Option<String>,
        pub state: SchemaState,
    }

    impl SchemaListEntry {
        pub fn from_schema(s: &DeclarativeSchemaDefinition, state: SchemaState) -> Self {
            Self {
                name: s.name.clone(),
                descriptive_name: s.descriptive_name.clone(),
                purpose_statement: s.purpose_statement.clone(),
                schema_type: s.schema_type.clone(),
                record_keys: s.record_keys.clone(),
                fields: s.fields.clone(),
                hash: s.hash.clone(),
                identity_hash: s.identity_hash.clone(),
                owner_app_id: s.owner_app_id.clone(),
                state,
            }
        }
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
                identity: format!("kc1:{:x}", Sha256::digest(bytes)),
                range,
                failure: OnceLock::new(),
                profile,
            })
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

        pub async fn run<T, E: From<SchemaError>>(
            self: Arc<Self>,
            future: impl Future<Output = Result<T, E>>,
        ) -> Result<T, E> {
            let (result, ctx) = if let Ok(active) = COMPARISON.try_with(Arc::clone) {
                if active.identity != self.identity {
                    return Err(SchemaError::InvalidData(
                        "key comparison context changed during the read".into(),
                    )
                    .into());
                }
                (future.await?, active)
            } else {
                (COMPARISON.scope(Arc::clone(&self), future).await?, self)
            };
            if let Some(detail) = ctx.failure.get() {
                return Err(SchemaError::InvalidData(detail.clone()).into());
            }
            Ok(result)
        }

        #[must_use]
        pub fn capture_current_range_comparison() -> Option<RangeKeyComparison> {
            COMPARISON.try_with(|ctx| ctx.range.clone()).ok()
        }

        fn compare_current_ranges(l: &str, r: &str) -> Ordering {
            COMPARISON
                .try_with(|ctx| match ctx.range.compare(l, r) {
                    Ok(o) => o,
                    Err(detail) => {
                        let _ = ctx.failure.set(detail);
                        ctx.range
                            .validate_value(l)
                            .is_ok()
                            .cmp(&ctx.range.validate_value(r).is_ok())
                            .then_with(|| l.cmp(r))
                    }
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
}
