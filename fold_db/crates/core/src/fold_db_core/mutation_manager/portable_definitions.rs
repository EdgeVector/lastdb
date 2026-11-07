//! Shared schema definitions come from the writer logs, not from a service.
//! The home records each verified definition beside the shared records.

use super::{portable_schema::PortableReplaySchema, MutationManager};
use crate::schema::{Schema, SchemaError};
use crate::storage::disk_pack::transaction;
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
    /// apply. Each takes its canonical name, which no schema here may use yet.
    pub async fn install_portable_definitions(
        &self,
        definitions: &[PortableSchemaDefinition],
    ) -> Result<(), SchemaError> {
        if definitions.is_empty() {
            return Ok(());
        }
        transaction::request_scope(self.db_ops.native_runtime(), async {
            let metadata = self.db_ops.metadata();
            let recorded = metadata.portable_definitions().await?;
            let mut known: BTreeSet<_> = recorded
                .values()
                .map(|definition| definition.id().clone())
                .collect();
            let mut added = BTreeMap::new();
            let mut schemas = Vec::new();
            for definition in definitions {
                let wire = definition
                    .verify(definition.id())
                    .map_err(SchemaError::InvalidData)?;
                if !known.insert(definition.id().clone()) {
                    continue;
                }
                let alias = wire.name.clone();
                if recorded.contains_key(&alias)
                    || added.contains_key(&alias)
                    || self.db_ops.schemas().get_schema(&alias).await?.is_some()
                {
                    return Err(invalid(
                        "a shared schema definition names a schema already in this home",
                    ));
                }
                schemas.push(Schema::from(wire.clone()));
                added.insert(alias, definition.clone());
            }
            if added.is_empty() {
                return Ok(());
            }
            metadata
                .stage_shared_aliases(added.keys().map(String::as_str))
                .await?;
            metadata.stage_portable_definitions(&added).await?;
            self.schema_manager.load_schema_batch(schemas).await
        })
        .await
    }
}

fn invalid(detail: &str) -> SchemaError {
    SchemaError::InvalidData(detail.into())
}
