//! Establish one complete schema contract before its native publication.

use super::{
    contracts::TYPED_METADATA_VERSION, make_wrap, metadata_key, schema_field_key_profiles,
    validate_identity, wrap_key_for, BundleMetadata, ConflictKeyIdentity, FieldKeyBundle,
    FieldKeyStore, KeyAuthority, StorageError, StorageResult, NODE_DOMAIN,
};
use crate::schema::Schema;
use crate::storage::KvMutation;
use schema_types::field_range_profile::FieldRangeProfile;

impl FieldKeyStore {
    pub(super) async fn establish_schema(&self, schema: &Schema) -> StorageResult<()> {
        self.schema_profile(schema)?;
        if !self.is_enabled() {
            return Ok(());
        }
        let populated;
        let schema = if schema.runtime_fields.is_empty() {
            populated = {
                let mut populated = schema.clone();
                populated.populate_runtime_fields().map_err(|error| {
                    StorageError::InvalidOperation(format!("resolve schema field keys: {error}"))
                })?;
                populated
            };
            &populated
        } else {
            schema
        };
        let profiles = schema_field_key_profiles(schema, self.record_home())?;
        let field_keys: Vec<_> = profiles.iter().map(|(id, _)| id.clone()).collect();
        if self.missing_contracts(&profiles)?.is_empty() {
            return Ok(());
        }
        self.load_existing(&field_keys).await?;
        if self.missing_contracts(&profiles)?.is_empty() {
            return Ok(());
        }
        // Hot schema writes use only admitted memory contracts. Only an actual
        // mint takes the native write gate, then the shared mint lock.
        crate::storage::disk_pack::transaction::scope(self.runtime.clone(), async {
            let _guard = self.mint_lock.lock().await;
            self.load_existing(&field_keys).await?;
            let missing = self.missing_contracts(&profiles)?;
            self.publish_new_bundles(missing).await
        })
        .await
    }

    fn missing_contracts(
        &self,
        field_keys: &[(String, FieldRangeProfile)],
    ) -> StorageResult<Vec<(String, FieldRangeProfile)>> {
        let mut missing = Vec::new();
        for (field_key, expected) in field_keys {
            validate_identity(field_key, NODE_DOMAIN)?;
            match self.cached_contract(field_key)? {
                Some(contract) if contract.range_profile != *expected => {
                    return Err(StorageError::InvalidOperation(
                        "the schema range type differs from the stored field key profile".into(),
                    ));
                }
                Some(_) => {}
                None => missing.push((field_key.clone(), *expected)),
            }
        }
        Ok(missing)
    }

    async fn publish_new_bundles(
        &self,
        field_keys: Vec<(String, FieldRangeProfile)>,
    ) -> StorageResult<()> {
        if field_keys.is_empty() {
            return Ok(());
        }
        let key = self.node_wrap_key.as_ref().ok_or_else(|| {
            StorageError::InvalidOperation("field keys require an account wrapping key".into())
        })?;
        let mut rows = Vec::with_capacity(field_keys.len().saturating_mul(2));
        let mut bundles = Vec::with_capacity(field_keys.len());
        for (field_key, profile) in field_keys {
            let bundle = FieldKeyBundle::generate();
            let wrap = make_wrap(&field_key, NODE_DOMAIN, &bundle, key)?;
            let metadata = BundleMetadata {
                version: TYPED_METADATA_VERSION,
                field_key_id: field_key.clone(),
                range_profile: profile,
                key_authority: KeyAuthority::Bundle,
                conflict_identity: ConflictKeyIdentity::StoredV1,
            };
            rows.push(KvMutation::put(
                wrap_key_for(&field_key, NODE_DOMAIN).as_bytes(),
                serde_json::to_vec(&wrap)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?,
            ));
            rows.push(KvMutation::put(
                metadata_key(&field_key).as_bytes(),
                serde_json::to_vec(&metadata)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?,
            ));
            bundles.push((field_key, profile, bundle));
        }
        self.entries.inner().batch_mutate(rows).await?;
        self.entries.inner().flush().await?;
        for (field_key, profile, bundle) in bundles {
            self.remember_contract(&field_key, profile, bundle)?;
        }
        Ok(())
    }
}
