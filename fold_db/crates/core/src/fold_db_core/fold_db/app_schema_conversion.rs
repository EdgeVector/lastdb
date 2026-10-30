//! Explicit maintenance: freeze, copy owned tips, publish keys, retire aliases, resume.

use super::FoldDB;
use crate::alternate_keys::invalid;
use crate::db_operations::{atom_store::MoleculeRefEdge, DbOperations};
use crate::schema::{types::Schema, SchemaError, SchemaState};
use crate::storage::{disk_pack::transaction, traits::KvStore};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

mod begin;
mod finish;
mod page;
mod tips;

pub(crate) const NAMESPACE: &str = "app_schema_conversion";
pub(crate) const ACTIVE: &[u8] = b"active";
const JOB: &[u8] = b"job";
tokio::task_local! { static CONVERSION_OWNER: (); }

pub(crate) fn authorized() -> bool {
    CONVERSION_OWNER.try_with(|_| ()).is_ok()
}

pub(crate) async fn require_unfrozen(db: &DbOperations) -> Result<(), SchemaError> {
    if !authorized() && active(db).await? {
        return Err(invalid("app schema conversion maintenance is active"));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedSchemaConversion {
    pub schema: String,
    pub expected_identity: Option<String>,
    pub alternate_keys: BTreeMap<String, schema_types::AlternateKey>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum AppSchemaConversion {
    AppStatus,
    AppBegin {
        schemas: Vec<OwnedSchemaConversion>,
        retire: Vec<String>,
        maintenance: bool,
    },
    AppPage {
        change_id: String,
    },
    AppCommit {
        change_id: String,
    },
    AppCleanup {
        change_id: String,
    },
    AppAbort {
        change_id: String,
    },
    AppFinish {
        change_id: String,
    },
}

#[derive(Serialize)]
pub struct AppSchemaConversionReport {
    pub change_id: Option<String>,
    pub phase: String,
    pub processed_keys: u64,
    pub expected_keys: u64,
    pub converted_schemas: Vec<String>,
    pub retired_schemas: Vec<String>,
    pub durable: bool,
}

#[derive(Serialize, Deserialize)]
struct Target {
    source: Schema,
    target: Schema,
    expected: BTreeMap<String, u64>,
    observed: BTreeMap<String, u64>,
    processed: u64,
    after: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Change {
    id: String,
    phase: Phase,
    targets: Vec<Target>,
    states: BTreeMap<String, SchemaState>,
    retire: Vec<String>,
    position: usize,
    retired: usize,
    cleaned: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Backfill,
    Retiring,
    Ready,
    Aborting,
    Complete,
    Aborted,
}

impl Change {
    fn report(&self) -> Result<AppSchemaConversionReport, SchemaError> {
        Ok(AppSchemaConversionReport {
            change_id: Some(self.id.clone()),
            phase: serde_json::to_value(self.phase)
                .map_err(|_| invalid("conversion phase"))?
                .as_str()
                .ok_or_else(|| invalid("conversion phase"))?
                .into(),
            processed_keys: self.targets.iter().map(|target| target.processed).sum(),
            expected_keys: self
                .targets
                .iter()
                .map(|target| {
                    target
                        .expected
                        .get(primary(&target.source)?)
                        .copied()
                        .ok_or_else(|| invalid("conversion source count is absent"))
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .sum(),
            converted_schemas: self
                .targets
                .iter()
                .map(|target| target.target.name.clone())
                .collect(),
            retired_schemas: self.retire.clone(),
            durable: true,
        })
    }

    fn edges(&self) -> Vec<MoleculeRefEdge> {
        self.targets
            .iter()
            .flat_map(|target| {
                let fields = target
                    .target
                    .field_molecule_uuids
                    .iter()
                    .flat_map(|ids| ids.values())
                    .cloned();
                let alternates = target
                    .target
                    .alternate_keys
                    .keys()
                    .map(|name| crate::alternate_keys::molecule(&target.target, name));
                fields
                    .chain(alternates)
                    .map(|molecule_uuid| MoleculeRefEdge {
                        source: format!("app-conversion:{}:{molecule_uuid}", self.id),
                        molecule_uuid,
                        edge_type: "app-conversion".into(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}

fn primary(schema: &Schema) -> Result<&str, SchemaError> {
    schema
        .key
        .as_ref()
        .and_then(|key| key.hash_field.as_deref())
        .ok_or_else(|| invalid("conversion requires a primary hash field"))
}

pub(crate) async fn active(db: &DbOperations) -> Result<bool, SchemaError> {
    Ok(db
        .open_namespace(NAMESPACE)
        .await?
        .get(ACTIVE)
        .await?
        .is_some())
}

pub(crate) async fn require_load_allowed(
    db: &DbOperations,
    schema: &Schema,
) -> Result<(), SchemaError> {
    if !authorized() && active(db).await? {
        let stored = db.get_schema(&schema.name).await?;
        if stored.as_ref().is_none_or(|stored| {
            schema_types::DeclarativeSchemaDefinition::from(stored)
                != schema_types::DeclarativeSchemaDefinition::from(schema)
        }) {
            return Err(invalid(
                "an app schema conversion freezes declaration changes until it finishes",
            ));
        }
    }
    Ok(())
}

async fn load(store: &dyn KvStore, id: &str) -> Result<Change, SchemaError> {
    let bytes = store
        .get(JOB)
        .await?
        .ok_or_else(|| invalid("app conversion is absent"))?;
    let mut change: Change =
        serde_json::from_slice(&bytes).map_err(|_| invalid("app conversion is unreadable"))?;
    if change.id != id {
        return Err(invalid("app conversion identity differs"));
    }
    for target in &mut change.targets {
        target.source.populate_runtime_fields()?;
        target.target.populate_runtime_fields()?;
    }
    Ok(change)
}

async fn save(store: &dyn KvStore, change: &Change) -> Result<(), SchemaError> {
    let bytes = serde_json::to_vec(change).map_err(|_| invalid("encode app conversion"))?;
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(invalid("app conversion declaration budget exceeds 4 MiB"));
    }
    store.put(JOB, &bytes).await?;
    Ok(())
}

impl FoldDB {
    pub fn app_schema_conversion_active(&self) -> bool {
        self.app_schema_conversion_active
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn publish_conversion_flag(&self, enabled: bool) -> Result<(), SchemaError> {
        let flag = std::sync::Arc::clone(&self.app_schema_conversion_active);
        crate::storage::disk_pack::transaction::publication::publish(
            self.db_ops.namespaced_store().disk_pack_runtime(),
            std::sync::Arc::as_ptr(&flag) as usize,
            "app-conversion".into(),
            enabled,
            move || {
                flag.store(enabled, std::sync::atomic::Ordering::Release);
                Ok(())
            },
        )?;
        Ok(())
    }

    pub async fn convert_app_schemas(
        &self,
        request: AppSchemaConversion,
    ) -> Result<AppSchemaConversionReport, SchemaError> {
        let admission = self.acquire_local_mutation_admission().await;
        admission
            .run(CONVERSION_OWNER.scope((), async {
                if let AppSchemaConversion::AppCleanup { change_id } = &request {
                    finish::retire(self, change_id).await?;
                }
                transaction::scope(self.db_ops.namespaced_store().disk_pack_runtime(), async {
                    let store = self.db_ops.open_namespace(NAMESPACE).await?;
                    let change = match request {
                        AppSchemaConversion::AppStatus => {
                            let bytes = store
                                .get(JOB)
                                .await?
                                .ok_or_else(|| invalid("app conversion is absent"))?;
                            let change: Change = serde_json::from_slice(&bytes)
                                .map_err(|_| invalid("app conversion is unreadable"))?;
                            return change.report();
                        }
                        AppSchemaConversion::AppBegin {
                            schemas,
                            retire,
                            maintenance,
                        } => {
                            begin::execute(self, store.as_ref(), schemas, retire, maintenance)
                                .await?
                        }
                        AppSchemaConversion::AppPage { change_id } => {
                            page::execute(
                                self,
                                store.as_ref(),
                                load(store.as_ref(), &change_id).await?,
                            )
                            .await?
                        }
                        AppSchemaConversion::AppCommit { change_id } => {
                            finish::commit(
                                self,
                                store.as_ref(),
                                load(store.as_ref(), &change_id).await?,
                            )
                            .await?
                        }
                        AppSchemaConversion::AppCleanup { change_id } => {
                            load(store.as_ref(), &change_id).await?
                        }
                        AppSchemaConversion::AppAbort { change_id } => {
                            finish::abort(
                                self,
                                store.as_ref(),
                                load(store.as_ref(), &change_id).await?,
                            )
                            .await?
                        }
                        AppSchemaConversion::AppFinish { change_id } => {
                            finish::complete(
                                self,
                                store.as_ref(),
                                load(store.as_ref(), &change_id).await?,
                            )
                            .await?
                        }
                    };
                    change.report()
                })
                .await
            }))
            .await
    }
}
