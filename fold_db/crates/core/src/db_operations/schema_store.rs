//! Schema domain store: catalog reads, writes, state transitions, and drop cleanup.

use crate::db_operations::record_store::RangeWalk;
use crate::schema::schema_types::SchemaState;
use crate::schema::types::{Schema, SchemaError};
use crate::storage::disk_pack::transaction;
use crate::storage::format::read::ReadStore;
use crate::storage::format::schemas::keys::*;
use crate::storage::format::schemas::{
    schema_input, SchemaFormat, StateChange, StateDelete, StateKey, Tracking,
};
use crate::storage::format::SCHEMA_RELEASE_ROWS;
use futures::FutureExt;
use lastdb_model::kind_partition::exact_prefix_bounds;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::Arc;

mod retention;
mod retention_sweep;

pub use retention_sweep::RetentionFloors;

/// Domain store for schema-related persistence.
#[derive(Clone)]
pub struct SchemaStore {
    schemas_store: ReadStore,
    pub(super) schema_states_store: ReadStore,
    pub(super) format: SchemaFormat,
    runtime: Arc<crate::storage::disk_pack::PackRuntime>,
    content_key: [u8; 32],
    retention_floors: Arc<RetentionFloors>,
}

impl SchemaStore {
    pub(crate) fn new(format: SchemaFormat, content_key: [u8; 32]) -> Self {
        let (schemas_store, schema_states_store) = format.readers();
        let runtime = format.runtime();
        Self {
            schemas_store,
            schema_states_store,
            format,
            runtime,
            content_key,
            retention_floors: Arc::default(),
        }
    }

    pub(crate) async fn flush(&self) -> Result<(), SchemaError> {
        self.format.flush_schemas().await?;
        self.format.flush_states().await?;
        Ok(())
    }

    pub async fn get_schema(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        let Some(bytes) = self.schemas_store.get(schema_name.as_bytes()).await? else {
            return Ok(None);
        };
        let schema = decode_catalog_schema(&bytes).map_err(|error| SchemaError::StorageRead {
            detail: format!("schema catalog row {schema_name} is unreadable: {error}"),
        })?;
        schema.validate()?;
        Ok(Some(schema))
    }

    pub async fn get_schema_state(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaState>, SchemaError> {
        let Some(bytes) = self.schema_states_store.get(schema_name.as_bytes()).await? else {
            return Ok(None);
        };
        decode_catalog_json::<SchemaState>(&bytes)
            .map(Some)
            .map_err(|error| SchemaError::StorageRead {
                detail: format!("schema state row {schema_name} is unreadable: {error}"),
            })
    }

    async fn require_installed_schema(&self, schema_name: &str) -> Result<(), SchemaError> {
        if self.get_schema(schema_name).await?.is_some() {
            Ok(())
        } else {
            Err(SchemaError::NotFound(schema_name.to_string()))
        }
    }

    pub async fn get_all_schemas(&self) -> Result<HashMap<String, Schema>, SchemaError> {
        let rows = read_catalog_range::<Schema>(
            &self.schemas_store,
            crate::storage::format::SCHEMAS_NAMESPACE,
            b"",
            CATALOG_SCAN_END,
        )
        .await?;
        let mut schemas = HashMap::with_capacity(rows.len());
        for (key, schema) in rows {
            if schema.name != key {
                return Err(SchemaError::InvalidData(format!(
                    "schema catalog row {key} names another schema: {}",
                    schema.name
                )));
            }
            schema.validate().map_err(|error| {
                SchemaError::InvalidData(format!(
                    "schema catalog row {key} does not resolve: {error}"
                ))
            })?;
            schemas.insert(key, schema);
        }
        Ok(schemas)
    }

    pub async fn get_all_schema_states(&self) -> Result<HashMap<String, SchemaState>, SchemaError> {
        let rows = read_catalog_range::<SchemaState>(
            &self.schema_states_store,
            crate::storage::format::SCHEMA_STATES_NAMESPACE,
            CATALOG_STATE_SCAN_START,
            CATALOG_SCAN_END,
        )
        .await?;
        Ok(rows.into_iter().collect())
    }

    pub async fn store_schema(
        &self,
        schema_name: &str,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        transaction::scope(
            Arc::clone(&self.runtime),
            self.store_schema_native(schema_name, schema).boxed(),
        )
        .await
    }

    async fn store_schema_native(
        &self,
        schema_name: &str,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        let input = schema_input(schema_name, schema)?;
        if let Some(existing) = self.get_schema(schema_name).await? {
            if existing.file_required != schema.file_required {
                return Err(SchemaError::InvalidData(
                    "changing the file requirement needs a new schema identity".into(),
                ));
            }
            if durable_schema_eq(&existing, schema) {
                return Ok(());
            }
        }
        self.format.put_schema(input).await?;
        crate::storage::disk_backup::accepted::stage_schema_put(
            &self.runtime,
            &self.content_key,
            schema_name,
            schema,
        )?;
        self.format.flush_schemas().await?;
        Ok(())
    }

    pub async fn store_schema_state(
        &self,
        schema_name: &str,
        state: &SchemaState,
    ) -> Result<(), SchemaError> {
        transaction::scope(
            Arc::clone(&self.runtime),
            self.store_schema_state_native(schema_name, state).boxed(),
        )
        .await
    }

    async fn store_schema_state_native(
        &self,
        schema_name: &str,
        state: &SchemaState,
    ) -> Result<(), SchemaError> {
        if self
            .schema_states_store
            .get_item::<SchemaState>(schema_name)
            .await?
            .is_some_and(|existing| existing == *state)
        {
            return Ok(());
        }
        self.format
            .put_state(StateChange::State {
                schema: schema_name,
                state,
            })
            .await?;
        self.format.flush_states().await?;
        Ok(())
    }

    pub async fn drop_schema(&self, schema_name: &str) -> Result<bool, SchemaError> {
        self.release_drop_tracking(schema_name).await?;
        transaction::scope(
            Arc::clone(&self.runtime),
            self.drop_schema_native(schema_name).boxed(),
        )
        .await
    }

    async fn drop_schema_native(&self, schema_name: &str) -> Result<bool, SchemaError> {
        let existed = self.get_schema(schema_name).await?.is_some();
        let state_keys = vec![
            StateDelete::Key {
                schema: schema_name,
                kind: StateKey::State,
            },
            StateDelete::Key {
                schema: schema_name,
                kind: StateKey::Policy,
            },
        ];
        self.format.delete_schema(schema_name).await?;
        self.format.delete_states(state_keys).await?;
        if existed {
            crate::storage::disk_backup::accepted::stage_schema_drop(
                &self.runtime,
                &self.content_key,
                schema_name,
            )?;
        }
        self.flush().await?;
        Ok(existed)
    }

    async fn release_drop_tracking(&self, schema: &str) -> Result<(), SchemaError> {
        let ranges = [
            (Tracking::Age, retention_age_prefix(schema)),
            (Tracking::Latest, retention_age_latest_prefix(schema)),
            (Tracking::Partition, retention_hash_partition_prefix(schema)),
        ]
        .map(|(kind, prefix)| {
            let (start, end) = exact_prefix_bounds(&prefix);
            (kind, start, end)
        });
        for (kind, start, end) in ranges {
            let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), SCHEMA_RELEASE_ROWS);
            while let Some((start, end, asked)) = walk.request() {
                let result = transaction::scope(self.runtime.clone(), async {
                    let scan = self
                        .schema_states_store
                        .scan_range_yielding(&start, &end, asked, 1)
                        .await?;
                    let count = scan.rows.len();
                    let keys = scan
                        .rows
                        .into_iter()
                        .map(|(key, _)| {
                            String::from_utf8(key)
                                .map_err(|error| SchemaError::InvalidData(error.to_string()))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if !keys.is_empty() {
                        self.format
                            .delete_states(vec![StateDelete::Selected { schema, kind, keys }])
                            .await?;
                    }
                    Ok::<_, SchemaError>((count, scan.resume))
                })
                .await;
                match result {
                    Ok((count, resume)) => walk.finish_yield(count, resume.as_deref()),
                    Err(error @ SchemaError::ReadResourceLimit { .. }) => {
                        if !walk.read_too_big() {
                            return Err(error);
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }
}

async fn read_catalog_range<T: DeserializeOwned>(
    store: &ReadStore,
    catalog: &'static str,
    start: &[u8],
    end: &[u8],
) -> Result<Vec<(String, T)>, SchemaError> {
    let mut cursor = start.to_vec();
    let mut out = Vec::new();
    loop {
        let page = store
            .scan_range_paged(&cursor, end, CATALOG_PAGE_ROWS)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("read the {catalog} catalog: {error}"))
            })?;
        if page.is_empty() {
            break;
        }
        if page.len() > CATALOG_PAGE_ROWS
            || page.windows(2).any(|pair| pair[0].0 >= pair[1].0)
            || page
                .iter()
                .any(|(key, _)| key.as_slice() < cursor.as_slice() || key.as_slice() >= end)
        {
            return Err(SchemaError::InvalidData(format!(
                "the {catalog} catalog read received an invalid or unordered page"
            )));
        }
        let full = page.len() == CATALOG_PAGE_ROWS;
        let mut next = page.last().expect("non-empty page").0.clone();
        next.push(0);
        for (key_bytes, value_bytes) in page {
            let key = String::from_utf8(key_bytes).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "the {catalog} catalog has a non-UTF-8 key: {error}"
                ))
            })?;
            let value = decode_catalog_json::<T>(&value_bytes).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "{catalog} catalog row {key} is unreadable: {error}"
                ))
            })?;
            out.push((key, value));
        }
        if !full {
            let trailing = store.scan_range_paged(&next, end, 1).await?;
            if !trailing.is_empty() {
                return Err(SchemaError::InvalidData(format!(
                    "the {catalog} catalog read received a truncated page"
                )));
            }
            break;
        }
        cursor = next;
    }
    Ok(out)
}
