//! DEV catalog publication and exact local schema bindings.

use super::super::recovery_home::files;
use super::DEV;
use app_identity_crypto::{Env, SigningKey};
use fold_db::schema::Schema;
use fold_db::security::Ed25519KeyPair;
use fold_db::FoldDB;
use folddb_profile::endpoints::schema_service_url_for;
use futures::{StreamExt, TryStreamExt};
use schema_service_client::SchemaServiceClient;
use schema_types::{DeclarativeSchemaDefinition, PortableSchemaDefinition, PortableSchemaId};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use zeroize::Zeroizing;

const MAX_BINDINGS_BYTES: u64 = 32 * 1024;

pub(super) async fn publish(
    namespace: &str,
    path: &Path,
    signer: &Ed25519KeyPair,
) -> Result<(), String> {
    let mut schema: DeclarativeSchemaDefinition = serde_json::from_slice(&private_input(path)?)
        .map_err(|_| "invalid typed shared schema declaration")?;
    if schema.record_keys.is_none() {
        return Err("shared schema requires explicit record keys".into());
    }
    schema.normalize_owned_declaration("namespace", namespace)?;
    let seed = Zeroizing::new(signer.secret_key_bytes());
    let service = SchemaServiceClient::new(schema_service_url_for(DEV))
        .with_node_identity(SigningKey::from_bytes(&seed), Env::Dev);
    let registered = service
        .add_schema(&schema, HashMap::new())
        .await
        .map_err(|_| "DEV schema registration refused")?;
    if registered.system || registered.composed || registered.replaced_schema.is_some() {
        return Err(
            "DEV shared schema publication did not create or match a standalone schema".into(),
        );
    }
    let catalog_id = registered
        .schema
        .identity_hash
        .ok_or("DEV catalog identity is missing")?;
    let candidate = service
        .prepare_portable_schema(&catalog_id)
        .await
        .map_err(|_| "cannot prepare the exact DEV catalog definition")?;
    let pinned = service
        .pin_portable_schema(&candidate)
        .await
        .map_err(|_| "cannot pin the exact DEV catalog definition")?;
    let verified = service
        .get_portable_schema(pinned.id())
        .await
        .map_err(|_| "cannot read back the pinned DEV schema")?;
    if verified != candidate {
        return Err("pinned DEV schema differs from the exact catalog revision".into());
    }
    println!(
        "{}",
        serde_json::json!({"catalog_id": catalog_id, "portable_id": verified.id()})
    );
    Ok(())
}

pub(super) async fn definitions(
    path: &Path,
) -> Result<BTreeMap<String, PortableSchemaDefinition>, String> {
    let aliases: BTreeMap<String, PortableSchemaId> = serde_json::from_slice(&private_input(path)?)
        .map_err(|_| "invalid shared schema bindings JSON")?;
    if aliases.is_empty() || aliases.len() > 64 {
        return Err("shared schema bindings require one to 64 entries".into());
    }
    let service = SchemaServiceClient::new(schema_service_url_for(DEV));
    futures::stream::iter(aliases)
        .map(|(alias, id)| {
            let service = &service;
            async move {
                let definition = service
                    .get_portable_schema(&id)
                    .await
                    .map_err(|_| "shared schema definition is unavailable".to_owned())?;
                Ok((alias, definition))
            }
        })
        .buffered(4)
        .try_collect()
        .await
}

fn private_input(path: &Path) -> Result<Vec<u8>, String> {
    if !path.is_absolute() {
        return Err("shared schema input requires an absolute path".into());
    }
    let parent = path.parent().ok_or("shared schema input parent")?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("shared schema input name")?;
    let directory = files::open_directory(parent)?;
    files::require_owned_directory(&directory)?;
    files::read_private_file(&directory, name, MAX_BINDINGS_BYTES)
}

pub(super) async fn install(
    db: &FoldDB,
    definitions: &BTreeMap<String, PortableSchemaDefinition>,
) -> Result<(), String> {
    let manager = db.schema_manager();
    let mut install = Vec::new();
    for (alias, definition) in definitions {
        let wire = definition.verify(definition.id())?;
        if let Some(local) = manager
            .get_schema_metadata(alias)
            .map_err(|error| error.to_string())?
        {
            definition.bind_local_alias(definition.id(), &local.to_wire())?;
        } else {
            let mut local = wire.clone();
            local.name.clone_from(alias);
            install.push(Schema::from(local));
        }
    }
    if !install.is_empty() {
        manager
            .load_schema_batch(install)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}
