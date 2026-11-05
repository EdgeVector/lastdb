//! One captured schema/type/order contract for a complete range read.

use super::{FieldValueType, KeyValue, Schema, SchemaError};
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

pub(crate) mod normalize;

#[derive(Debug, Clone)]
pub struct KeyComparisonContext {
    identity: String,
    range: RangeKeyComparison,
    failure: OnceLock<String>,
    profile: FieldRangeProfile,
}

impl KeyComparisonContext {
    pub(crate) fn for_schema(schema: &Schema) -> Result<Self, SchemaError> {
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

    /// A compatibility tag, not an authentication token or an encryption key.
    pub fn cursor_binding(&self, request: &impl serde::Serialize) -> Result<String, SchemaError> {
        let bytes = serde_json::to_vec(&(&self.identity, request)).map_err(|_| {
            SchemaError::InvalidData("cannot encode cursor comparison context".into())
        })?;
        Ok(format!("qc1:{:x}", Sha256::digest(bytes)))
    }

    /// Nested reads must retain the same captured declaration.
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

    /// Capture the active range rule once for data-only response ordering.
    #[must_use]
    pub fn capture_current_range_comparison() -> Option<RangeKeyComparison> {
        COMPARISON.try_with(|ctx| ctx.range.clone()).ok()
    }

    /// Existing low-level field key/maintenance callers retain explicit UTF-8 order.
    /// Product schema reads install their captured context at the query boundary.
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
                // Keep a total temporary order: invalid values precede valid
                // values, and invalid values use text order. Returning Equal
                // for every error could violate a sort's ordering contract.
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
}

/// The canonical page order of [`KeyValue`] under the active key comparison
/// context: ascending `range` by the captured range rule, then ascending
/// `hash`. Outside a context the range order is UTF-8 text order.
///
/// The model's [`KeyValue::cmp_page_order_with`] takes the range comparison as
/// an argument; this trait supplies the task-local context that the read
/// installed with [`KeyComparisonContext::run`].
pub trait KeyValuePageOrder {
    /// See [`KeyValue::cmp_page_order_with`] for the order contract.
    fn cmp_page_order(&self, other: &Self) -> Ordering;
}

impl KeyValuePageOrder for KeyValue {
    fn cmp_page_order(&self, other: &Self) -> Ordering {
        KeyComparisonContext::compare_current_keys(self, other)
    }
}
