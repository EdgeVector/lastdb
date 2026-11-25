//! Reserve a schema for paged removal, sweep retention tracking, and finish with a durable receipt.

use super::SchemaStore;
use crate::db_operations::record_store::RangeWalk;
use crate::schema::types::catalog_types::SchemaState;
use crate::schema::types::{Schema, SchemaError};
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::SchemaReplayDrop;
use crate::storage::format::schemas::*;
use crate::storage::format::SCHEMA_RELEASE_ROWS;
use lastdb_model::kind_partition::exact_prefix_bounds;
use std::sync::Arc;

pub(crate) enum SchemaDrop {
    Done,
    Ready(Option<Box<Schema>>),
}

impl SchemaStore {
    pub(crate) async fn block_for_drop(
        &self,
        name: &str,
        replay: Option<&SchemaReplayDrop<'_>>,
        publish: impl FnOnce(Option<&Schema>) -> Result<(), SchemaError>,
    ) -> Result<SchemaDrop, SchemaError> {
        transaction::scope(Arc::clone(&self.runtime), async {
            let state = self.get_schema_state(name).await?;
            let schema = self.get_schema(name).await?;
            if !self.metadata.guard_schema_drop(name, replay, true).await? {
                return Ok(SchemaDrop::Done);
            }
            if state != Some(SchemaState::Blocked) {
                self.store_schema_state(name, &SchemaState::Blocked).await?;
            }
            publish(schema.as_ref())?;
            Ok(SchemaDrop::Ready(schema.map(Box::new)))
        })
        .await
    }

    pub(crate) async fn drop_reserved_schema(
        &self,
        schema_name: &str,
        replay: Option<&SchemaReplayDrop<'_>>,
        publish: impl FnOnce() -> Result<(), SchemaError>,
    ) -> Result<bool, SchemaError> {
        self.sweep_retention_tracking(schema_name).await?;
        self.finish_drop(schema_name, replay, publish).await
    }

    pub(super) async fn sweep_retention_tracking(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        for kind in Tracking::ALL {
            let (start, end) = exact_prefix_bounds(&kind.prefix(schema_name));
            let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), SCHEMA_RELEASE_ROWS);
            while let Some((start, end, asked)) = walk.request() {
                let scan = match self
                    .schema_states_store
                    .scan_range_yielding(&start, &end, asked, 1)
                    .await
                    .map_err(SchemaError::from)
                {
                    Ok(scan) => scan,
                    Err(SchemaError::ReadResourceLimit { .. }) if walk.read_too_big() => continue,
                    Err(error) => return Err(error),
                };
                let count = scan.rows.len();
                let keys: Vec<String> = scan
                    .rows
                    .into_iter()
                    .map(|(k, _)| {
                        String::from_utf8(k).map_err(|e| SchemaError::InvalidData(e.to_string()))
                    })
                    .collect::<Result<_, _>>()?;
                if !keys.is_empty() {
                    self.format
                        .delete_states(vec![StateDelete::Selected {
                            schema: schema_name,
                            kind,
                            keys,
                        }])
                        .await?;
                }
                walk.finish_yield(count, scan.resume.as_deref());
            }
        }
        Ok(())
    }

    async fn finish_drop(
        &self,
        name: &str,
        replay: Option<&SchemaReplayDrop<'_>>,
        publish: impl FnOnce() -> Result<(), SchemaError>,
    ) -> Result<bool, SchemaError> {
        transaction::scope(Arc::clone(&self.runtime), async {
            if !self.metadata.guard_schema_drop(name, replay, false).await? {
                return Ok(false);
            }
            let existed = self.get_schema(name).await?.is_some();
            self.format.delete_schema(name).await?;
            self.format
                .delete_states(vec![
                    StateDelete::Key {
                        schema: name,
                        kind: StateKey::State,
                    },
                    StateDelete::Key {
                        schema: name,
                        kind: StateKey::Policy,
                    },
                ])
                .await?;
            if let Some(replay) = replay {
                self.metadata.finish_schema_drop(name, replay).await?;
            } else if existed {
                self.capture_local_change(name, None).await?;
            }
            publish()?;
            Ok(existed)
        })
        .await
    }
}
