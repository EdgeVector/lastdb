//! Shared schema definitions come from the writer logs, not from a service.
//! The home records each verified definition beside the shared records.

use super::{portable_schema::PortableReplaySchema, MutationManager};
use crate::schema::{Schema, SchemaError};
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::DefinitionSet;
use schema_types::PortableSchemaDefinition;
use std::collections::{BTreeMap, BTreeSet};

impl MutationManager {
    /// The recorded definition of each shared alias in this home.
    pub async fn portable_definitions(
        &self,
    ) -> Result<BTreeMap<String, PortableSchemaDefinition>, SchemaError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(self.db_ops.metadata().portable_definitions())
            .await
    }

    /// Every definition each shared alias accepted, its current one included.
    /// A shared record names one of them.
    pub async fn portable_accepted_definitions(
        &self,
    ) -> Result<BTreeMap<String, Vec<PortableSchemaDefinition>>, SchemaError> {
        let sets = self
            .db_ops
            .read_scope()
            .await?
            .run_result(self.db_ops.metadata().portable_definition_sets())
            .await?;
        Ok(sets
            .into_iter()
            .map(|(alias, set)| (alias, set.accepted.into_values().collect()))
            .collect())
    }

    /// Record definitions of schemas that a snapshot or an owner installed
    /// under these aliases in the shared scope.
    pub async fn record_portable_definitions(
        &self,
        definitions: &BTreeMap<String, PortableSchemaDefinition>,
    ) -> Result<(), SchemaError> {
        transaction::request_scope(
            self.db_ops.native_runtime(),
            self.stage_recorded_definitions(definitions),
        )
        .await
    }

    /// Afterward every shared alias has its recorded definition.
    pub(super) async fn stage_recorded_definitions(
        &self,
        definitions: &BTreeMap<String, PortableSchemaDefinition>,
    ) -> Result<(), SchemaError> {
        let runtime = self.db_ops.native_runtime();
        runtime.require_shared_schema_subset(definitions.keys().map(String::as_str))?;
        futures::future::try_join_all(definitions.iter().map(|(alias, definition)| async move {
            let schema = self.load_schema_for_delta(alias).await?;
            PortableReplaySchema::bind(definition, definition.id(), &schema)?;
            Ok::<_, SchemaError>(())
        }))
        .await?;
        let metadata = self.db_ops.metadata();
        let recorded = metadata.portable_definitions().await?;
        if runtime
            .shared_schema_aliases()
            .iter()
            .any(|alias| !recorded.contains_key(alias) && !definitions.contains_key(alias))
        {
            return Err(invalid("every shared alias requires its schema definition"));
        }
        metadata.stage_portable_definitions(definitions).await
    }

    /// Install log definitions that this home lacks before their records
    /// apply. A definition of a new name takes that name, which no schema here
    /// may use yet. A later definition of a shared name joins the definitions
    /// it accepted when it only adds fields
    /// (`decision-2026-11-07-lastdb-schema-name-slot-definition-per-record`).
    /// The name's schema becomes their current definition, and a bound
    /// writer follows it.
    pub async fn install_portable_definitions(
        &self,
        definitions: &[PortableSchemaDefinition],
    ) -> Result<(), SchemaError> {
        if !definitions.is_empty() {
            transaction::request_scope(
                self.db_ops.native_runtime(),
                self.stage_log_definitions(definitions),
            )
            .await?;
        }
        // Its own request reads the schema the install published. It also
        // completes an install that stopped before the writer followed.
        self.follow_portable_definitions().await
    }

    async fn stage_log_definitions(
        &self,
        definitions: &[PortableSchemaDefinition],
    ) -> Result<(), SchemaError> {
        let metadata = self.db_ops.metadata();
        let mut sets = metadata.portable_definition_sets().await?;
        let mut changed = BTreeSet::new();
        let mut added = BTreeMap::<String, Vec<PortableSchemaDefinition>>::new();
        for definition in definitions {
            definition
                .verify(definition.id())
                .map_err(SchemaError::InvalidData)?;
            if sets
                .values()
                .any(|set| set.accepted.contains_key(definition.id()))
            {
                continue;
            }
            let name = definition.canonical_name();
            let mut holders = sets
                .iter_mut()
                .filter(|(_, set)| set.current.canonical_name() == name);
            match (holders.next(), holders.next()) {
                (Some((alias, set)), None) => {
                    set.accepted
                        .insert(definition.id().clone(), definition.clone());
                    changed.insert(alias.clone());
                }
                (None, _) => {
                    let versions = added.entry(name.to_owned()).or_default();
                    if versions.iter().all(|known| known.id() != definition.id()) {
                        versions.push(definition.clone());
                    }
                }
                (Some(_), Some(_)) => {
                    return Err(invalid("two shared aliases hold one shared schema name"));
                }
            }
        }
        let mut staged = BTreeMap::new();
        let mut schemas = Vec::new();
        for alias in changed {
            let mut set = sets
                .remove(&alias)
                .ok_or_else(|| invalid("a changed shared alias has no definitions"))?;
            let accepted: Vec<_> = set.accepted.values().cloned().collect();
            let current = PortableSchemaDefinition::current_of(&accepted)
                .map_err(SchemaError::InvalidData)?;
            if current.id() != set.current.id() {
                schemas.push(local_schema(&alias, &current)?);
                set = DefinitionSet::new(current, accepted);
            }
            staged.insert(alias, set);
        }
        for (name, versions) in &added {
            if sets.contains_key(name) || self.db_ops.schemas().get_schema(name).await?.is_some() {
                return Err(invalid(
                    "a shared schema definition names a schema already in this home",
                ));
            }
            let current =
                PortableSchemaDefinition::current_of(versions).map_err(SchemaError::InvalidData)?;
            schemas.push(local_schema(name, &current)?);
            staged.insert(name.clone(), DefinitionSet::new(current, versions.clone()));
        }
        if staged.is_empty() {
            return Ok(());
        }
        metadata
            .stage_shared_aliases(added.keys().map(String::as_str))
            .await?;
        metadata.stage_definition_sets(&staged).await?;
        self.schema_manager.load_schema_batch(schemas).await
    }
}

/// The installed schema of a current definition under its local alias.
fn local_schema(alias: &str, current: &PortableSchemaDefinition) -> Result<Schema, SchemaError> {
    let mut local = current
        .verify(current.id())
        .map_err(SchemaError::InvalidData)?
        .clone();
    local.name = alias.to_owned();
    Ok(Schema::from(local))
}

fn invalid(detail: &str) -> SchemaError {
    SchemaError::InvalidData(detail.into())
}
