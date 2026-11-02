//! Share only independent field keys, never the node's account keys.

use super::{
    make_wrap, schema_field_key_ids, validate_identity, wrap_key_for, FieldKeyStore, StorageError,
    StorageResult,
};
use crate::{schema::Schema, storage::KvMutation};

impl FieldKeyStore {
    /// Resolve the complete schema before one batch publishes its domain wraps.
    pub async fn grant_schema_domain(
        &self,
        schema: &Schema,
        domain: &str,
        domain_wrap_key: &[u8; 32],
    ) -> StorageResult<usize> {
        if !self.is_enabled() {
            return Ok(0);
        }
        validate_identity(&schema.name, domain)?;
        let field_keys = schema_field_key_ids(schema);
        self.load_existing(&field_keys).await?;
        self.require_schema_profiles(schema)?;
        let mut rows = Vec::with_capacity(field_keys.len());
        for field_key in &field_keys {
            let bundle = self.cached_bundle(field_key)?.ok_or_else(|| {
                StorageError::InvalidOperation("schema share requires the field keys".into())
            })?;
            let wrap = make_wrap(field_key, domain, &bundle, domain_wrap_key)?;
            rows.push(KvMutation::put(
                wrap_key_for(field_key, domain).as_bytes(),
                serde_json::to_vec(&wrap)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?,
            ));
        }
        self.entries.inner().batch_mutate(rows).await?;
        self.entries.inner().flush().await?;
        Ok(field_keys.len())
    }
}
