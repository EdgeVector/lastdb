//! Schema domain store: catalog reads, internal catalog writes, and state transitions.

use crate::schema::types::catalog_types::SchemaState;
use crate::schema::types::{Schema, SchemaError};
use crate::storage::disk_backup::accepted;
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::{Metadata, ReplayTarget};
use crate::storage::format::read::ReadStore;
use crate::storage::format::schemas::*;
use lastdb_model::record_space::record_space_uuid;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::Arc;

mod replay;
mod retention;
mod retention_sweep;

pub(crate) use replay::SchemaDrop;
pub use retention_sweep::RetentionFloors;

#[derive(Clone)]
pub struct SchemaStore {
    schemas_store: ReadStore,
    pub(super) schema_states_store: ReadStore,
    pub(super) format: SchemaFormat,
    pub(super) runtime: Arc<crate::storage::disk_pack::PackRuntime>,
    pub(super) metadata: Metadata,
    /// The writer of the author clock that stamps record writes. A schema
    /// change takes its author time from the same clock.
    pub(super) writer_id: String,
    pub(super) retention_floors: Arc<RetentionFloors>,
}

impl SchemaStore {
    pub(crate) fn new(format: SchemaFormat, metadata: Metadata, writer_id: String) -> Self {
        let (schemas_store, schema_states_store) = format.readers();
        let runtime = format.runtime();
        Self {
            schemas_store,
            schema_states_store,
            format,
            runtime,
            metadata,
            writer_id,
            retention_floors: Arc::default(),
        }
    }

    pub(crate) async fn get_schema(
        &self,
        schema_name: &str,
    ) -> Result<Option<Schema>, SchemaError> {
        let Some(bytes) = self.schemas_store.get(schema_name.as_bytes()).await? else {
            return Ok(None);
        };
        let schema =
            serde_json::from_slice::<Schema>(&bytes).map_err(|e| SchemaError::StorageRead {
                detail: format!("schema catalog row {schema_name} is unreadable: {e}"),
            })?;
        schema.validate()?;
        Ok(Some(schema))
    }

    pub(crate) async fn get_schema_state(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaState>, SchemaError> {
        let Some(bytes) = self.schema_states_store.get(schema_name.as_bytes()).await? else {
            return Ok(None);
        };
        serde_json::from_slice::<SchemaState>(&bytes)
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

    pub(crate) async fn get_all_schemas(&self) -> Result<HashMap<String, Schema>, SchemaError> {
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

    pub(crate) async fn get_all_schema_states(
        &self,
    ) -> Result<HashMap<String, SchemaState>, SchemaError> {
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

    pub(crate) async fn store_schema(
        &self,
        schema_name: &str,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        transaction::scope(
            Arc::clone(&self.runtime),
            Box::pin(async move {
                schema
                    .validate_definition()
                    .map_err(SchemaError::InvalidData)?;
                if self.get_schema_state(schema_name).await? == Some(SchemaState::Blocked) {
                    return Err(SchemaError::Blocked("schema removal is in progress".into()));
                }
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
                    if existing == *schema {
                        return Ok(());
                    }
                }
                self.format.put_schema(input).await?;
                self.capture_local_change(schema_name, Some(schema)).await
            }),
        )
        .await
    }

    pub(crate) async fn store_schema_state(
        &self,
        schema_name: &str,
        state: &SchemaState,
    ) -> Result<(), SchemaError> {
        transaction::scope(
            Arc::clone(&self.runtime),
            Box::pin(async move {
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
                Ok(())
            }),
        )
        .await
    }

    /// Sign a schema put (`definition` given) or drop as the next entry of the
    /// device stream, in the transaction that commits it, and write its stamp
    /// row. A put or drop of another device is compared with that row when it
    /// arrives. A store that does not capture its writes does nothing here.
    pub(super) async fn capture_local_change(
        &self,
        schema: &str,
        definition: Option<&Schema>,
    ) -> Result<(), SchemaError> {
        if !accepted::capture_enabled(&self.runtime) {
            return Ok(());
        }
        // The author time comes from the clock that stamps record writes, so the
        // change sits above everything this device has written or seen.
        let order = self.metadata.stage_local_author_ms(&self.writer_id).await?;
        let previous = self.metadata.local_replay_head(&self.writer_id).await?;
        let staged = match definition {
            Some(definition) => accepted::stage_schema_put(
                &self.runtime,
                schema,
                definition,
                order,
                previous.as_ref(),
            )?,
            None => accepted::stage_schema_drop(&self.runtime, schema, order, previous.as_ref())?,
        };
        let Some(head) = staged else {
            return Ok(());
        };
        let stamp = self
            .metadata
            .local_replay_stamp(&self.writer_id, head.seq, head.order)?;
        self.metadata
            .stage_replay_stamps([(ReplayTarget::Schema(schema), None, &stamp)])
            .await?;
        self.metadata.stage_local_replay_head(&head).await
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
        let id = lastdb_model::field_key_id::alternate_field_key_id(schema, name);
        out.push((
            check(id, "invalid alternate field key identity")?,
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
            let val = serde_json::from_slice::<T>(&v).map_err(|e| {
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
