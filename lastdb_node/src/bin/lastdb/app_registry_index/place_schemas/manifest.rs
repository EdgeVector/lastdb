//! Manifest schema entries, after the field-object expansion and the
//! normalization declare already applies.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::path::Path;

use fold_db::schema::types::DeclarativeSchemaDefinition;
use lastdb_node::exec::normalize_app_manifest_schema;
use schema_types::expand_manifest_schema;
use serde_json::Value;

use super::super::str_err;
use super::definition_id;

#[derive(Clone)]
pub(super) struct Prepared {
    pub(super) schema: DeclarativeSchemaDefinition,
    pub(super) retention: Option<Value>,
}

enum Slot {
    Ready(Box<Prepared>),
    Bad(String),
}

pub(super) struct Catalog {
    entries: BTreeMap<String, Slot>,
}

impl Catalog {
    pub(super) fn definition(&self, name: &str, signed_id: &str) -> Result<Prepared, String> {
        match self.entries.get(name) {
            None => Err(format!("no entry named '{name}'")),
            Some(Slot::Bad(error)) => Err(error.clone()),
            Some(Slot::Ready(item)) => {
                let id = definition_id::portable_id(&item.schema)
                    .map_err(|error| format!("definition id could not be computed: {error}"))?;
                if id == signed_id {
                    Ok(item.as_ref().clone())
                } else {
                    Err(format!(
                        "definition id {id}, which does not match the signed id"
                    ))
                }
            }
        }
    }
}

pub(super) fn load(source: &Path, app_id: &str) -> Result<Catalog, String> {
    let raw = std::fs::read(source.join("lastdb-app.json")).map_err(str_err)?;
    let manifest: Value = serde_json::from_slice(&raw).map_err(str_err)?;
    let Some(schemas) = manifest.get("schemas").and_then(Value::as_array) else {
        return Err("manifest `schemas` must be a list of schema declarations".into());
    };
    let mut entries = BTreeMap::new();
    for entry in schemas {
        match prepare_one(app_id, entry) {
            Ok(prepared) => put(
                &mut entries,
                prepared.schema.name.clone(),
                Slot::Ready(Box::new(prepared)),
            ),
            Err((label, error)) => put(
                &mut entries,
                declared_name(app_id, &label),
                Slot::Bad(error),
            ),
        }
    }
    Ok(Catalog { entries })
}

fn put(entries: &mut BTreeMap<String, Slot>, name: String, slot: Slot) {
    match entries.entry(name) {
        Entry::Occupied(mut entry) => {
            let label = entry.key().clone();
            entry.insert(Slot::Bad(format!("lists '{label}' more than once")));
        }
        Entry::Vacant(entry) => {
            entry.insert(slot);
        }
    }
}

fn declared_name(app_id: &str, raw_name: &str) -> String {
    match raw_name.split_once('/') {
        Some((namespace, local)) if !namespace.is_empty() && !local.is_empty() => {
            raw_name.to_owned()
        }
        _ => format!("{app_id}/{raw_name}"),
    }
}

fn prepare_one(app_id: &str, entry: &Value) -> Result<Prepared, (String, String)> {
    let label = entry
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("<unnamed>")
        .to_owned();
    let mut raw = entry.clone();
    let retention = {
        let Some(object) = raw.as_object_mut() else {
            return Err((label, "schema is not an object".into()));
        };
        object.remove("retention")
    };
    if let Err(error) = expand_manifest_schema(&mut raw) {
        return Err((label, error));
    }
    let schema: DeclarativeSchemaDefinition = match serde_json::from_value(raw) {
        Ok(schema) => schema,
        Err(error) => return Err((label, format!("not a schema declaration: {error}"))),
    };
    let schema = match normalize_app_manifest_schema(app_id, schema) {
        Ok(schema) => schema,
        Err(error) => return Err((label, format!("cannot be declared: {error}"))),
    };
    Ok(Prepared { schema, retention })
}
