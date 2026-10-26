//! One captured schema/type/order contract for a complete range read.

use super::{FieldValueType, KeyValue, Schema, SchemaError};
pub use schema_types::key_comparison::{KeyOrderVersion, RangeKeyComparison};
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
}

impl KeyComparisonContext {
    pub(crate) fn for_schema(
        schema: &Schema,
        formats: &[(String, String, &'static str)],
    ) -> Result<Self, SchemaError> {
        schema
            .validate_key_field_types()
            .map_err(SchemaError::InvalidData)?;
        let hash = schema
            .key
            .as_ref()
            .and_then(|key| key.hash_field.as_deref());
        let range = schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.as_deref());
        let hash_type = hash.map(|path| schema.key_field_type(path));
        let range_type = range.map_or(FieldValueType::Any, |path| schema.key_field_type(path));
        let range_comparison = RangeKeyComparison::legacy(range_type);
        let bytes = serde_json::to_vec(&(
            "lastdb-key-comparison-v1",
            &schema.name,
            &schema.identity_hash,
            &schema.owner_app_id,
            &schema.schema_type,
            &schema.key,
            key_declaration(schema, hash),
            key_declaration(schema, range),
            hash_type,
            &range_comparison,
            formats,
        ))
        .map_err(|_| SchemaError::InvalidData("cannot encode key comparison context".into()))?;
        Ok(Self {
            identity: format!("kc1:{:x}", Sha256::digest(bytes)),
            range: range_comparison,
            failure: OnceLock::new(),
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

    #[must_use]
    pub fn accepts_legacy_cursor(&self) -> bool {
        matches!(self.range.order, KeyOrderVersion::LegacyUtf8V1)
    }

    /// Nested field reads must retain the same captured declaration and formats.
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

    /// Existing low-level molecule/maintenance callers retain explicit UTF-8 order.
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
        Self::compare_current_ranges(
            left.range.as_deref().unwrap_or(""),
            right.range.as_deref().unwrap_or(""),
        )
        .then_with(|| {
            left.hash
                .as_deref()
                .unwrap_or("")
                .cmp(right.hash.as_deref().unwrap_or(""))
        })
    }

    pub(crate) fn require_current_order(order: KeyOrderVersion) -> Result<(), SchemaError> {
        if COMPARISON
            .try_with(|ctx| ctx.range.order != order)
            .unwrap_or(false)
        {
            return Err(SchemaError::InvalidData(
                "range comparison differs from the stored key order".into(),
            ));
        }
        Ok(())
    }
}

#[derive(serde::Serialize)]
struct KeyDeclaration<'a> {
    field_type: Option<&'a FieldValueType>,
    field_identity: Option<&'a String>,
    version: Option<&'a u32>,
}

fn key_declaration<'a>(schema: &'a Schema, path: Option<&str>) -> KeyDeclaration<'a> {
    let path = path.unwrap_or("");
    let root = path.split_once('.').map_or(path, |(root, _)| root);
    KeyDeclaration {
        field_type: schema
            .field_types
            .get(path)
            .or_else(|| schema.field_types.get(root)),
        field_identity: schema
            .field_hashes
            .get(path)
            .or_else(|| schema.field_hashes.get(root)),
        version: schema
            .field_versions
            .get(path)
            .or_else(|| schema.field_versions.get(root)),
    }
}
