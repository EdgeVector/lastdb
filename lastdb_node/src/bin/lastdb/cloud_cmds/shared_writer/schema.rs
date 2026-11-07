//! Locally computed shared schema definitions and exact local bindings.
//! A shared database carries its definitions in the writer logs, so no
//! schema service registers, pins or serves them.

use super::super::recovery_home::files;
use fold_db::schema::Schema;
use fold_db::FoldDB;
use schema_types::{DeclarativeSchemaDefinition, PortableSchemaDefinition};
use std::collections::BTreeMap;
use std::path::Path;

const MAX_BINDINGS_BYTES: u64 = 1024 * 1024;

/// Print the definition of one typed declaration under its portable ID.
pub(super) fn define(namespace: &str, path: &Path) -> Result<(), String> {
    let mut schema: DeclarativeSchemaDefinition = serde_json::from_slice(&private_input(path)?)
        .map_err(|_| "invalid typed shared schema declaration")?;
    if schema.record_keys.is_none() {
        return Err("shared schema requires explicit record keys".into());
    }
    schema.normalize_owned_declaration("namespace", namespace)?;
    let catalog_id = schema
        .get_identity_hash()
        .cloned()
        .ok_or("shared schema identity is missing")?;
    let definition = PortableSchemaDefinition::from_catalog(&catalog_id, schema)?;
    println!(
        "{}",
        serde_json::json!({
            "catalog_id": catalog_id,
            "portable_id": definition.id(),
            "definition": definition,
        })
    );
    Ok(())
}

/// Verified definitions by local alias from a private JSON file.
pub(super) fn definitions(
    path: &Path,
) -> Result<BTreeMap<String, PortableSchemaDefinition>, String> {
    let definitions: BTreeMap<String, PortableSchemaDefinition> =
        serde_json::from_slice(&private_input(path)?)
            .map_err(|_| "invalid shared schema bindings JSON")?;
    if definitions.is_empty() || definitions.len() > 64 {
        return Err("shared schema bindings require one to 64 entries".into());
    }
    for definition in definitions.values() {
        definition.verify(definition.id())?;
    }
    Ok(definitions)
}

/// A later schema takes its canonical name in every home that installs it.
pub(super) fn admitted(path: &Path) -> Result<Vec<PortableSchemaDefinition>, String> {
    definitions(path)?
        .into_iter()
        .map(|(alias, definition)| {
            if definition.verify(definition.id())?.name != alias {
                return Err("an admitted shared schema alias must be its canonical name".into());
            }
            Ok(definition)
        })
        .collect()
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
