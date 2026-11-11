//! Database operations coordinator over typed domain stores, schema lifecycle, and page scopes.

pub mod admin_db;
mod core {
    mod open;
}
pub mod record_store;
pub mod schema_store;
pub mod search_index;

pub use crate::storage::format::metadata::{OutgoingPortablePage, PortablePeerProgress};
pub use admin_db::{
    SchemaRecordKey, SchemaRecordKeysReport, SchemaRecordKeysRequest, SchemaStorage,
    SchemaStorageReport,
};
pub use record_store::{
    RecordDeleteLedgerEntry, RecordStore, WriterWatch, LEDGER_VERB_DELETE, LEDGER_VERB_PURGE,
};
pub use schema_store::SchemaStore;
pub use search_index::{IndexChange, IndexChangeBatch, IndexChangeKind};

use crate::schema::schema_types::{SchemaRetentionPolicy, SchemaState};
use crate::schema::types::{KeyValue, Schema, SchemaError};
use crate::storage::disk_pack::{PackReadScope, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::format::metadata::Metadata;
use futures::{future::BoxFuture, FutureExt};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

/// Database operations coordinator over typed domain stores.
#[derive(Clone)]
pub struct DbOperations {
    runtime: Arc<PackRuntime>,
    schema_store: SchemaStore,
    record_store: RecordStore,
    metadata: Metadata,
}

impl DbOperations {
    pub fn schemas(&self) -> &SchemaStore {
        &self.schema_store
    }

    pub fn records(&self) -> &RecordStore {
        &self.record_store
    }

    pub async fn read_scope(&self) -> StorageResult<PackReadScope> {
        PackReadScope::for_runtime(&self.runtime).await
    }

    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn native_runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }

    #[must_use]
    pub fn batch_operation_limit(&self) -> usize {
        self.runtime.batch_operation_limit()
    }

    pub async fn flush(&self) -> Result<(), SchemaError> {
        self.schema_store.flush().await?;
        self.record_store.flush().await?;
        self.metadata.flush().await?;
        Ok(())
    }

    pub fn store_schema<'a>(
        &'a self,
        schema_name: &'a str,
        schema: &'a Schema,
    ) -> BoxFuture<'a, Result<(), SchemaError>> {
        crate::storage::disk_pack::transaction::scope(
            self.native_runtime(),
            self.store_schema_in_scope(schema_name, schema),
        )
        .boxed()
    }

    async fn store_schema_in_scope(
        &self,
        schema_name: &str,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        schema
            .validate_definition()
            .map_err(SchemaError::InvalidData)?;
        let next = schema_field_key_profiles(schema)?;
        if let Some(old) = self.schemas().get_schema(schema_name).await? {
            let previous: HashMap<_, _> = schema_field_key_profiles(&old)?.into_iter().collect();
            for (field_key, profile) in &next {
                if previous
                    .get(field_key)
                    .is_some_and(|stored| stored != profile)
                {
                    return Err(SchemaError::InvalidData(
                        "the schema range type differs from the stored field key profile".into(),
                    ));
                }
            }
        }
        self.schemas().store_schema(schema_name, schema).await
    }

    pub async fn get_schema_retention_policy(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaRetentionPolicy>, SchemaError> {
        self.schemas()
            .get_schema_retention_policy(schema_name)
            .await
    }

    pub async fn list_schema_retention_policies(
        &self,
    ) -> Result<Vec<(String, SchemaRetentionPolicy)>, SchemaError> {
        self.schemas().list_schema_retention_policies().await
    }

    pub fn drop_schema<'a>(
        &'a self,
        schema_name: &'a str,
    ) -> BoxFuture<'a, Result<bool, SchemaError>> {
        async move {
            if let Some(schema) = self.block_schema_for_drop(schema_name).await? {
                let released = self.records().release_schema_rows(&schema).await?;
                tracing::info!(
                    target: "fold_node::database",
                    schema = schema_name,
                    records = released.records,
                    alternates = released.alternates,
                    "schema drop released the stored rows of the schema"
                );
            }
            self.schemas().drop_schema(schema_name).await
        }
        .boxed()
    }

    pub async fn block_schema_for_drop(
        &self,
        schema_name: &str,
    ) -> Result<Option<Schema>, SchemaError> {
        crate::storage::disk_pack::transaction::scope(self.native_runtime(), async {
            if self.metadata().shared_schema_bound(schema_name).await? {
                return Err(SchemaError::InvalidData(
                    "a shared schema requires coordinated retirement".into(),
                ));
            }
            let Some(schema) = self.schemas().get_schema(schema_name).await? else {
                return Ok(None);
            };
            if self.schemas().get_schema_state(schema_name).await? != Some(SchemaState::Blocked) {
                self.schemas()
                    .store_schema_state(schema_name, &SchemaState::Blocked)
                    .await?;
            }
            Ok(Some(schema))
        })
        .await
    }
}

fn schema_field_key_profiles(
    schema: &Schema,
) -> Result<Vec<(String, schema_types::field_range_profile::FieldRangeProfile)>, SchemaError> {
    let definitions = schema.require_record_keys()?;
    let primary = definitions
        .primary
        .range_profile()
        .map_err(SchemaError::InvalidData)?;
    let mut result = Vec::with_capacity(definitions.alternates.len() + 1);
    for (name, key) in &definitions.alternates {
        let profile = key.range_profile().map_err(SchemaError::InvalidData)?;
        let id = lastdb_model::field_key_id::alternate_field_key_id(schema, name);
        if !lastdb_model::field_key_id::is_canonical_field_key_id(&id) {
            return Err(SchemaError::InvalidData(
                "invalid alternate field key identity".into(),
            ));
        }
        result.push((id, profile));
    }
    let space = lastdb_model::record_space::record_space_uuid(&schema.name);
    if !lastdb_model::field_key_id::is_canonical_field_key_id(&space) {
        return Err(SchemaError::InvalidData(
            "invalid record space identity".into(),
        ));
    }
    result.push((space, primary));
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result.dedup();
    Ok(result)
}

tokio::task_local! {
    static PAGE_END: RefCell<PageEnd>;
}

/// Where one resumable page ended when key-examination budget ran out early.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageEnd {
    pub resume: Option<PageResume>,
}

/// Position after which the next page of a resumable read starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageResume {
    Key(KeyValue),
}

pub fn page_can_resume() -> bool {
    PAGE_END.try_with(|_| ()).is_ok()
}

pub fn note_page_resume(resume: PageResume) {
    let _ = PAGE_END.try_with(|end| end.borrow_mut().resume = Some(resume));
}

pub async fn with_query_page_tally<F: std::future::Future>(fut: F) -> (F::Output, PageEnd) {
    PAGE_END
        .scope(RefCell::new(PageEnd::default()), async move {
            let out = fut.await;
            (out, PAGE_END.with(RefCell::take))
        })
        .await
}
