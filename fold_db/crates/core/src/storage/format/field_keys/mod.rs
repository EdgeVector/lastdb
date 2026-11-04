//! The configured node field-key writer and its nested encrypted bundle codec.
//!
//! Providers supply field identities/profiles; this component generates the
//! bundle and chooses its exact metadata/wrap rows. No raw batch escapes.

mod bundle;
mod codec;
mod frozen;

use super::read::ReadStore;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::{KvMutation, KvStore, StorageError, TypedKvStore};
use schema_types::field_range_profile::FieldRangeProfile;
use std::sync::Arc;

pub(crate) use super::FIELD_KEY_ROW_MAX_BYTES as NODE_KEY_ROW_MAX_BYTES;
pub use bundle::FieldKeyBundle;
pub(crate) use codec::{
    metadata_key, open_wrap, validate_identity, wrap_key_for, BundleMetadata, BundleWrap,
};
pub(crate) use frozen::{node_bundle_storage_keys, verify_frozen_typed_profile};

pub(crate) struct CreatedContract {
    pub(crate) field_key: String,
    pub(crate) profile: FieldRangeProfile,
    pub(crate) bundle: FieldKeyBundle,
}

struct EncodedFieldKeys {
    rows: Vec<KvMutation>,
}

pub(crate) struct FieldKeyFormat {
    field_store: Arc<TypedKvStore<dyn KvStore>>,
    runtime: Arc<PackRuntime>,
}

impl FieldKeyFormat {
    pub(crate) fn new(store: Arc<dyn KvStore>, runtime: Arc<PackRuntime>) -> Self {
        Self {
            field_store: Arc::new(TypedKvStore::new(store)),
            runtime,
        }
    }

    pub(crate) fn reader(&self) -> ReadStore {
        ReadStore::new(Arc::clone(&self.field_store))
    }
    pub(crate) fn runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }
    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.field_store.inner().flush().await
    }

    pub(crate) async fn stage_new(
        &self,
        fields: Vec<(String, FieldRangeProfile)>,
        wrapping_key: &[u8; 32],
    ) -> StorageResult<Vec<CreatedContract>> {
        if fields.is_empty() {
            return Ok(Vec::new());
        }
        self.require_scope()?;
        let (encoded, contracts) = codec::prepare(fields, wrapping_key)?;
        self.stage(encoded).await?;
        Ok(contracts)
    }

    fn require_scope(&self) -> StorageResult<()> {
        if !transaction::publication::active(&self.runtime) {
            return Err(StorageError::InvalidOperation(
                "field key mint requires its native scope".into(),
            ));
        }
        Ok(())
    }

    async fn stage(&self, encoded: EncodedFieldKeys) -> StorageResult<()> {
        self.require_scope()?;
        self.field_store.inner().batch_mutate(encoded.rows).await
    }
}
