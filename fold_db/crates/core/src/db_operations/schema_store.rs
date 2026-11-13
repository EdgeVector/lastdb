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
use lastdb_model::record_space::record_space_uuid;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::Arc;

mod retention;
mod retention_sweep;

pub use retention_sweep::RetentionFloors;

#[derive(Clone)]
pub struct SchemaStore {
    schemas_store: ReadStore,
    pub(super) schema_states_store: ReadStore,
    pub(super) format: SchemaFormat,
    pub(super) runtime: Arc<crate::storage::disk_pack::PackRuntime>,
    content_key: [u8; 32],
    pub(super) retention_floors: Arc<RetentionFloors>,
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
        let schema = decode_catalog_schema(&bytes).map_err(|e| SchemaError::StorageRead {
            detail: format!("schema catalog row {schema_name} is unreadable: {e}"),
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
            .map_err(|e| SchemaError::StorageRead {
                detail: format!("schema state row {schema_name} is unreadable: {e}"),
            })
    }

    pub(super) async fn require_installed_schema(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        self.get_schema(schema_name)
            .await?
            .map(|_| ())
            .ok_or_else(|| SchemaError::NotFound(schema_name.to_string()))
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
            schema.validate().map_err(|e| {
                SchemaError::InvalidData(format!("schema catalog row {key} does not resolve: {e}"))
            })?;
            schemas.insert(key, schema);
        }
        Ok(schemas)
    }

    pub async fn get_all_schema_states(&self) -> Result<HashMap<String, SchemaState>, SchemaError> {
        Ok(read_catalog_range::<SchemaState>(
            &self.schema_states_store,
            crate::storage::format::SCHEMA_STATES_NAMESPACE,
            CATALOG_STATE_SCAN_START,
            CATALOG_SCAN_END,
        )
        .await?
        .into_iter()
        .collect())
    }

    pub async fn store_schema(
        &self,
        schema_name: &str,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        transaction::scope(
            Arc::clone(&self.runtime),
            async move {
                schema
                    .validate_definition()
                    .map_err(SchemaError::InvalidData)?;
                let next = schema_field_key_profiles(schema)?;
                let input = schema_input(schema_name, schema)?;
                if let Some(existing) = self.get_schema(schema_name).await? {
                    let prev: HashMap<_, _> =
                        schema_field_key_profiles(&existing)?.into_iter().collect();
                    if next
                        .iter()
                        .any(|(k, p)| prev.get(k).is_some_and(|s| s != p))
                    {
                        return Err(SchemaError::InvalidData(
                            "the schema range type differs from the stored field key profile"
                                .into(),
                        ));
                    }
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
                Ok(self.format.flush_schemas().await?)
            }
            .boxed(),
        )
        .await
    }

    pub async fn store_schema_state(
        &self,
        schema_name: &str,
        state: &SchemaState,
    ) -> Result<(), SchemaError> {
        transaction::scope(
            Arc::clone(&self.runtime),
            async move {
                if self
                    .schema_states_store
                    .get_item::<SchemaState>(schema_name)
                    .await?
                    .is_some_and(|s| s == *state)
                {
                    return Ok(());
                }
                self.format
                    .put_state(StateChange::State {
                        schema: schema_name,
                        state,
                    })
                    .await?;
                Ok(self.format.flush_states().await?)
            }
            .boxed(),
        )
        .await
    }

    pub(crate) async fn block_for_drop(
        &self,
        schema_name: &str,
    ) -> Result<Option<Schema>, SchemaError> {
        transaction::scope(Arc::clone(&self.runtime), async {
            let Some(schema) = self.get_schema(schema_name).await? else {
                return Ok(None);
            };
            if self.get_schema_state(schema_name).await? != Some(SchemaState::Blocked) {
                self.store_schema_state(schema_name, &SchemaState::Blocked)
                    .await?;
            }
            Ok(Some(schema))
        })
        .await
    }

    pub async fn drop_schema(&self, schema_name: &str) -> Result<bool, SchemaError> {
        for (kind, prefix) in [
            (Tracking::Age, retention_age_prefix(schema_name)),
            (Tracking::Latest, retention_age_latest_prefix(schema_name)),
            (
                Tracking::Partition,
                retention_hash_partition_prefix(schema_name),
            ),
        ] {
            let (start, end) = exact_prefix_bounds(&prefix);
            let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), SCHEMA_RELEASE_ROWS);
            while let Some((start, end, asked)) = walk.request() {
                let res = transaction::scope(self.runtime.clone(), async {
                    let scan = self
                        .schema_states_store
                        .scan_range_yielding(&start, &end, asked, 1)
                        .await?;
                    let count = scan.rows.len();
                    let keys = scan
                        .rows
                        .into_iter()
                        .map(|(k, _)| {
                            String::from_utf8(k)
                                .map_err(|e| SchemaError::InvalidData(e.to_string()))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if !keys.is_empty() {
                        self.format
                            .delete_states(vec![StateDelete::Selected {
                                schema: schema_name,
                                kind,
                                keys,
                            }])
                            .await?;
                    }
                    Ok::<_, SchemaError>((count, scan.resume))
                })
                .await;
                match res {
                    Ok((count, resume)) => walk.finish_yield(count, resume.as_deref()),
                    Err(SchemaError::ReadResourceLimit { .. }) if walk.read_too_big() => {}
                    Err(e) => return Err(e),
                }
            }
        }
        transaction::scope(
            Arc::clone(&self.runtime),
            async move {
                let existed = self.get_schema(schema_name).await?.is_some();
                self.format.delete_schema(schema_name).await?;
                self.format
                    .delete_states(vec![
                        StateDelete::Key {
                            schema: schema_name,
                            kind: StateKey::State,
                        },
                        StateDelete::Key {
                            schema: schema_name,
                            kind: StateKey::Policy,
                        },
                    ])
                    .await?;
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
            .boxed(),
        )
        .await
    }
}

fn schema_field_key_profiles(
    schema: &Schema,
) -> Result<Vec<(String, schema_types::field_range_profile::FieldRangeProfile)>, SchemaError> {
    let check = |id: String, msg: &'static str| {
        lastdb_model::field_key_id::is_canonical_field_key_id(&id)
            .then_some(id)
            .ok_or_else(|| SchemaError::InvalidData(msg.into()))
    };
    let defs = schema.require_record_keys()?;
    let mut out = Vec::with_capacity(defs.alternates.len() + 1);
    for (name, key) in &defs.alternates {
        out.push((
            check(
                lastdb_model::field_key_id::alternate_field_key_id(schema, name),
                "invalid alternate field key identity",
            )?,
            key.range_profile().map_err(SchemaError::InvalidData)?,
        ));
    }
    out.push((
        check(
            record_space_uuid(&schema.name),
            "invalid record space identity",
        )?,
        defs.primary
            .range_profile()
            .map_err(SchemaError::InvalidData)?,
    ));
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.dedup();
    Ok(out)
}

async fn read_catalog_range<T: DeserializeOwned>(
    store: &ReadStore,
    catalog: &'static str,
    start: &[u8],
    end: &[u8],
) -> Result<Vec<(String, T)>, SchemaError> {
    let (mut cursor, mut out) = (start.to_vec(), Vec::new());
    loop {
        let page = store
            .scan_range_paged(&cursor, end, CATALOG_PAGE_ROWS)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read the {catalog} catalog: {e}")))?;
        if page.is_empty() {
            break;
        }
        if page.len() > CATALOG_PAGE_ROWS
            || page.windows(2).any(|p| p[0].0 >= p[1].0)
            || page
                .iter()
                .any(|(k, _)| k.as_slice() < cursor.as_slice() || k.as_slice() >= end)
        {
            return Err(SchemaError::InvalidData(format!(
                "the {catalog} catalog read received an invalid or unordered page"
            )));
        }
        let full = page.len() == CATALOG_PAGE_ROWS;
        let mut next = page.last().expect("non-empty page").0.clone();
        next.push(0);
        for (k, v) in page {
            let key = String::from_utf8(k).map_err(|e| {
                SchemaError::InvalidData(format!("the {catalog} catalog has a non-UTF-8 key: {e}"))
            })?;
            let val = decode_catalog_json::<T>(&v).map_err(|e| {
                SchemaError::InvalidData(format!("{catalog} catalog row {key} is unreadable: {e}"))
            })?;
            out.push((key, val));
        }
        if !full {
            if !store.scan_range_paged(&next, end, 1).await?.is_empty() {
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
