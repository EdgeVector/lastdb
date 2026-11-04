//! Establish one complete schema contract before its native publication.

use super::{
    schema_field_key_profiles, validate_identity, FieldKeyStore, StorageError, StorageResult,
};
use crate::schema::Schema;
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
        let profiles = schema_field_key_profiles(schema)?;
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
            validate_identity(field_key)?;
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
        let contracts = self.format.stage_new(field_keys).await?;
        self.format.flush().await?;
        for contract in contracts {
            self.remember_contract(&contract.field_key, contract.profile, contract.bundle)?;
        }
        Ok(())
    }
}
