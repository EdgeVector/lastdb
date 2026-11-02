//! Key material stays private until its complete native write publishes.

use super::{CachedFieldKeyContract, FieldKeyBundle, FieldKeyStore, StorageError, StorageResult};
use crate::storage::disk_pack::transaction::publication;
use lastdb_model::field_key_codec::FieldKeyCodec;
use schema_types::field_range_profile::FieldRangeProfile;
use std::sync::Arc;

impl FieldKeyStore {
    pub(super) fn cached_contract(
        &self,
        field_key: &str,
    ) -> StorageResult<Option<CachedFieldKeyContract>> {
        if let Some(contract) = publication::get::<CachedFieldKeyContract>(
            &self.runtime,
            Arc::as_ptr(&self.cache) as usize,
            field_key,
        )? {
            return Ok(Some(contract.as_ref().clone()));
        }
        Ok(self
            .cache
            .read()
            .map_err(|_| cache_error())?
            .get(field_key)
            .cloned())
    }

    pub(crate) fn cached_bundle(&self, field_key: &str) -> StorageResult<Option<FieldKeyBundle>> {
        Ok(self
            .cached_contract(field_key)?
            .map(|contract| contract.bundle))
    }

    pub(crate) fn codec_for_field_key(
        &self,
        field_key: &str,
    ) -> StorageResult<Option<FieldKeyCodec>> {
        let Some(contract) = self.cached_contract(field_key)? else {
            return Ok(None);
        };
        Ok(Some(contract.bundle.key_codec(contract.range_profile)))
    }

    pub(super) fn remember_contract(
        &self,
        field_key: &str,
        profile: FieldRangeProfile,
        bundle: FieldKeyBundle,
    ) -> StorageResult<()> {
        let contract = CachedFieldKeyContract {
            range_profile: profile,
            bundle,
        };
        let cache = Arc::clone(&self.cache);
        let field_key = field_key.to_owned();
        publication::publish(
            &self.runtime,
            Arc::as_ptr(&cache) as usize,
            field_key.clone(),
            contract.clone(),
            move || {
                cache
                    .write()
                    .map_err(|_| cache_error())?
                    .insert(field_key, contract);
                Ok(())
            },
        )
    }
}

fn cache_error() -> StorageError {
    StorageError::BackendError("field key cache lock is poisoned".into())
}
