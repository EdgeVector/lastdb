//! Owner-directed declaration changes: block, backfill named keys, publish, retire.

use super::FoldDB;
use crate::alternate_keys::invalid;
use crate::db_operations::atom_store::MoleculeRefEdge;
use crate::schema::{
    types::{KeyValue, Schema},
    SchemaError, SchemaState,
};
use crate::storage::{disk_pack::transaction, traits::KvStore, KvMutation};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

mod begin;
mod finish;
mod page;

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum AlternateKeyChange {
    Status {
        schema: String,
    },
    Begin {
        schema: String,
        expected_revision: String,
        alternate_keys: BTreeMap<String, schema_types::AlternateKey>,
    },
    Backfill {
        schema: String,
        change_id: String,
        keys: Vec<KeyValue>,
    },
    Commit {
        schema: String,
        change_id: String,
    },
    Abort {
        schema: String,
        change_id: String,
    },
    Cleanup {
        schema: String,
        change_id: String,
    },
}

#[derive(Debug, Serialize)]
pub struct AlternateKeyChangeReport {
    pub schema: String,
    pub change_id: Option<String>,
    pub phase: String,
    pub active_revision: String,
    pub target_revision: Option<String>,
    pub processed_keys: u64,
    pub expected_keys: u64,
    pub durable: bool,
}

// The change row is a record: schema refresh checks it below the engine.
use crate::records::alternate_change::{job_key, Change, Phase};

impl Change {
    fn report(&self) -> Result<AlternateKeyChangeReport, SchemaError> {
        let committed = matches!(self.phase, Phase::Retiring | Phase::Complete);
        Ok(AlternateKeyChangeReport {
            schema: self.source.name.clone(),
            change_id: Some(self.id.clone()),
            phase: serde_json::to_value(self.phase)
                .expect("phase serializes")
                .as_str()
                .expect("phase string")
                .into(),
            active_revision: if committed {
                &self.target
            } else {
                &self.source
            }
            .alternate_key_revision(),
            target_revision: Some(self.target.alternate_key_revision()),
            processed_keys: self.processed,
            expected_keys: self
                .expected
                .get(primary(&self.source)?)
                .copied()
                .unwrap_or_default(),
            durable: true,
        })
    }

    fn staging_edges(&self) -> Vec<MoleculeRefEdge> {
        self.target
            .alternate_keys
            .keys()
            .map(|name| MoleculeRefEdge {
                molecule_uuid: crate::alternate_keys::molecule(&self.target, name),
                source: format!("alternate-change:{}:{name}", self.id),
                edge_type: "alternate-change".into(),
            })
            .collect()
    }
}

fn primary(schema: &Schema) -> Result<&str, SchemaError> {
    schema
        .key
        .as_ref()
        .and_then(|key| key.hash_field.as_deref())
        .ok_or_else(|| invalid("alternate changes require a primary hash field"))
}

async fn load(
    db: &FoldDB,
    store: &dyn KvStore,
    schema: &str,
    id: &str,
) -> Result<Change, SchemaError> {
    let bytes = store
        .get(job_key(schema).as_bytes())
        .await?
        .ok_or_else(|| invalid("alternate change does not exist"))?;
    let mut change: Change =
        serde_json::from_slice(&bytes).map_err(|_| invalid("alternate change is unreadable"))?;
    if change.id != id || change.source.name != schema {
        return Err(invalid("alternate change identity differs"));
    }
    change.source.populate_runtime_fields()?;
    change.target.populate_runtime_fields()?;
    db.db_ops
        .molecule_keys()
        .load_schema(&change.source)
        .await?;
    db.db_ops
        .molecule_keys()
        .load_schema(&change.target)
        .await?;
    Ok(change)
}

async fn save(store: &dyn KvStore, change: &Change) -> Result<(), SchemaError> {
    store
        .put(
            job_key(&change.source.name).as_bytes(),
            serde_json::to_vec(change).map_err(|_| invalid("cannot encode alternate change"))?,
        )
        .await?;
    Ok(())
}

impl FoldDB {
    pub async fn change_alternate_keys(
        &self,
        request: AlternateKeyChange,
    ) -> Result<AlternateKeyChangeReport, SchemaError> {
        let admission = self.acquire_local_mutation_admission().await;
        admission
            .run(transaction::scope(
                self.db_ops.namespaced_store().disk_pack_runtime(),
                async {
                    let store = self.db_ops.open_namespace("alternate_changes").await?;
                    let change = match request {
                        AlternateKeyChange::Status { schema } => {
                            if let Some(bytes) = store.get(job_key(&schema).as_bytes()).await? {
                                let change: Change = serde_json::from_slice(&bytes)
                                    .map_err(|_| invalid("alternate change is unreadable"))?;
                                return change.report();
                            }
                            let source = self
                                .db_ops
                                .get_schema(&schema)
                                .await?
                                .ok_or_else(|| invalid("schema is absent"))?;
                            return Ok(AlternateKeyChangeReport {
                                schema,
                                change_id: None,
                                phase: "idle".into(),
                                active_revision: source.alternate_key_revision(),
                                target_revision: None,
                                processed_keys: 0,
                                expected_keys: 0,
                                durable: true,
                            });
                        }
                        AlternateKeyChange::Begin {
                            schema,
                            expected_revision,
                            alternate_keys,
                        } => {
                            begin::execute(
                                self,
                                store.as_ref(),
                                &schema,
                                &expected_revision,
                                alternate_keys,
                            )
                            .await?
                        }
                        AlternateKeyChange::Backfill {
                            schema,
                            change_id,
                            keys,
                        } => {
                            page::execute(
                                self,
                                store.as_ref(),
                                load(self, store.as_ref(), &schema, &change_id).await?,
                                &keys,
                            )
                            .await?
                        }
                        AlternateKeyChange::Commit { schema, change_id } => {
                            finish::commit(
                                self,
                                store.as_ref(),
                                load(self, store.as_ref(), &schema, &change_id).await?,
                            )
                            .await?
                        }
                        AlternateKeyChange::Abort { schema, change_id } => {
                            finish::abort(
                                self,
                                store.as_ref(),
                                load(self, store.as_ref(), &schema, &change_id).await?,
                            )
                            .await?
                        }
                        AlternateKeyChange::Cleanup { schema, change_id } => {
                            finish::cleanup(
                                self,
                                store.as_ref(),
                                load(self, store.as_ref(), &schema, &change_id).await?,
                            )
                            .await?
                        }
                    };
                    change.report()
                },
            ))
            .await
    }
}
