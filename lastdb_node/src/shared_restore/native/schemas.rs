//! Resolve immutable definitions into the names admitted by the local home.

use super::SnapshotSchema;
use crate::byte_cloud_restore::{RestoreError, Result};
use fold_db::portable_mutation::{MutationEntry, SchemaIdentity};
use fold_db::schema::Schema;
use fold_db::FoldDB;
use futures::{StreamExt, TryStreamExt};
use schema_service_client::SchemaServiceClient;
use schema_types::PortableSchemaId;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) enum SchemaTarget {
    ActivePeer,
    StagedRestore,
}

pub(crate) async fn resolve_schemas(
    db: &FoldDB,
    service: &SchemaServiceClient,
    schemas: &mut BTreeMap<PortableSchemaId, SnapshotSchema>,
    entries: &[&MutationEntry],
    target: SchemaTarget,
) -> Result<()> {
    let mut missing = BTreeSet::new();
    for entry in entries {
        let id = schema_id(&entry.schema)?;
        if !schemas.contains_key(&id) {
            missing.insert(id);
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    let bindings = if matches!(target, SchemaTarget::ActivePeer) {
        Some(
            db.mutation_manager()
                .portable_writer_schemas()
                .await
                .map_err(|_| RestoreError::Invalid("read bound shared schema aliases"))?,
        )
    } else {
        None
    };
    let resolved: Vec<_> = futures::stream::iter(missing)
        .map(|id| fetch_definition(service, id, bindings.as_ref()))
        .buffered(4)
        .try_collect()
        .await?;
    let manager = db.schema_manager();
    let mut aliases: BTreeSet<_> = schemas
        .values()
        .map(|schema| &schema.local_schema)
        .collect();
    let mut install = Vec::new();
    for (binding, schema) in &resolved {
        if matches!(target, SchemaTarget::ActivePeer) {
            db.db_ops()
                .native_runtime()
                .require_shared_replay_alias(&binding.local_schema)
                .map_err(|_| {
                    RestoreError::Invalid("peer schema is outside the dedicated disk scope")
                })?;
        }
        if !aliases.insert(&binding.local_schema) {
            return Err(RestoreError::Invalid("portable schema alias collision"));
        }
        if let Some(existing) = manager
            .get_schema_metadata(&binding.local_schema)
            .map_err(|_| RestoreError::Invalid("resolved schema read"))?
        {
            binding
                .definition
                .bind_local_alias(binding.definition.id(), &existing.to_wire())
                .map_err(|_| RestoreError::Invalid("resolved schema alias collision"))?;
        } else {
            install.push(schema.clone());
        }
    }
    if !install.is_empty() {
        let loaded = match target {
            SchemaTarget::ActivePeer => manager.load_schema_batch(install).await,
            SchemaTarget::StagedRestore => {
                db.mutation_manager()
                    .install_staged_shared_restore_schemas(install)
                    .await
            }
        };
        loaded.map_err(|_| RestoreError::Invalid("install resolved schema definitions"))?;
    }
    schemas.extend(
        resolved
            .into_iter()
            .map(|(binding, _)| (binding.definition.id().clone(), binding)),
    );
    Ok(())
}

async fn fetch_definition(
    service: &SchemaServiceClient,
    id: PortableSchemaId,
    bindings: Option<&BTreeMap<String, String>>,
) -> Result<(SnapshotSchema, Schema)> {
    let definition = service
        .get_portable_schema(&id)
        .await
        .map_err(|_| RestoreError::Source("immutable schema definition"))?;
    let mut wire = definition
        .verify(&id)
        .map_err(|_| RestoreError::Invalid("resolved schema identity"))?
        .clone();
    if let Some(bindings) = bindings {
        wire.name
            .clone_from(bindings.get(id.as_str()).ok_or(RestoreError::Invalid(
                "peer replay schema is outside the bound shared database",
            ))?);
    }
    let local_schema = wire.name.clone();
    Ok((
        SnapshotSchema {
            local_schema,
            definition,
        },
        wire.into(),
    ))
}

pub(crate) fn schema_id(identity: &SchemaIdentity) -> Result<PortableSchemaId> {
    let SchemaIdentity::Shared(id) = identity else {
        return Err(RestoreError::Invalid("local schema in shared replay"));
    };
    id.parse()
        .map_err(|_| RestoreError::Invalid("portable schema ID"))
}
