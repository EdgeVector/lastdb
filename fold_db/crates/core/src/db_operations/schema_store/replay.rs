//! Reserve a schema for paged removal, sweep retention tracking, and finish with a durable receipt.

use super::SchemaStore;
use crate::db_operations::record_store::RangeWalk;
use crate::schema::types::catalog_types::SchemaState;
use crate::schema::types::{Schema, SchemaError};
use crate::storage::disk_backup::accepted;
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
        for kind in Tracking::ALL {
            let (start, end) = exact_prefix_bounds(&kind.prefix(schema_name));
            let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), SCHEMA_RELEASE_ROWS);
            while let Some((start, end, asked)) = walk.request() {
                let res = transaction::scope(self.runtime.clone(), async {
                    let scan = self
                        .schema_states_store
                        .scan_range_yielding(&start, &end, asked, 1)
                        .await?;
                    let count = scan.rows.len();
                    let keys: Vec<String> = scan
                        .rows
                        .into_iter()
                        .map(|(k, _)| {
                            String::from_utf8(k)
                                .map_err(|e| SchemaError::InvalidData(e.to_string()))
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
        self.finish_drop(schema_name, replay, publish).await
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
                let authored_at_ms = self.local_author_ms().await?;
                let captured = accepted::stage_schema_drop(
                    &self.runtime,
                    &self.content_key,
                    name,
                    authored_at_ms,
                )?;
                if captured {
                    self.stamp_local_change(name, authored_at_ms).await?;
                }
            }
            publish()?;
            Ok(existed)
        })
        .await
    }
}
