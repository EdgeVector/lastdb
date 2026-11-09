use super::SCHEMAS;
use crate::{get_query_body, parse_json_response, socket_client::post_json_admin};
use fold_db::schema::types::KeyValue;
use lastdb_node::byte_cloud_backup::share_write_gate;
use lastdb_node::share_file::{self, CatalogRecord, FileManifest, CATALOG_HASH};
use serde_json::{json, Value};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};

const APP_ID: &str = "lastdb-files";

pub(super) fn add(home: &Path, input: &Path) -> Result<(), String> {
    let socket = publication_socket(home)?;
    let edit = share_write_gate::begin_edit(home)?;
    let result = add_file(&socket, input);
    if result.is_ok() || catalog_is_complete(&socket) {
        edit.finish()?;
    }
    let manifest = result?;
    println!(
        "{}",
        serde_json::to_value(manifest).map_err(|_| "cannot encode share file receipt")?
    );
    Ok(())
}

fn publication_socket(home: &Path) -> Result<PathBuf, String> {
    lastdb_node::offline_home::refuse_primary(home)?;
    let marker = std::fs::symlink_metadata(home.join(lastdb_node::share_publication::MARKER_FILE))
        .map_err(|_| "file sharing requires the home created by share create")?;
    if !marker.is_file()
        || marker.uid() != unsafe { libc::geteuid() }
        || marker.mode() & 0o077 != 0
        || marker.nlink() != 1
    {
        return Err("publication marker must be an owner-only regular file".into());
    }
    let data = home.join("data");
    let socket = lastdb_uds::uds::socket_path(&data);
    if data.canonicalize().ok().as_ref() != Some(&data)
        || !std::fs::symlink_metadata(&socket).is_ok_and(|entry| entry.file_type().is_socket())
    {
        return Err("start the publication's LastDB daemon before adding files".into());
    }
    Ok(socket)
}

fn add_file(socket: &Path, input: &Path) -> Result<FileManifest, String> {
    let definitions = declare_schemas(socket)?;
    let mut row = load_catalog(socket)?;
    if let Some(catalog) = catalog(row.as_ref())? {
        let recovered = share_file::recover_pending(
            &SCHEMAS,
            catalog,
            |keys| read_revisions(socket, keys),
            |operations| write(socket, &definitions, operations),
        )?;
        if recovered {
            row = load_catalog(socket)?;
        }
    }
    share_file::add_file(input, &SCHEMAS, catalog(row.as_ref())?, |operations| {
        write(socket, &definitions, operations)
    })
}

fn catalog_is_complete(socket: &Path) -> bool {
    load_catalog(socket).is_ok_and(|row| {
        catalog(row.as_ref()).is_ok_and(|value| {
            share_file::require_complete_catalog(value.map(|v| v.entries)).is_ok()
        })
    })
}

struct Definitions {
    catalog: String,
    chunks: String,
}

fn declare_schemas(socket: &Path) -> Result<Definitions, String> {
    std::thread::scope(|scope| {
        let catalog = scope.spawn(|| declare(socket, "FileCatalog", "entries", false));
        let chunks = declare(socket, "FileChunk", "data", true);
        Ok(Definitions {
            catalog: catalog
                .join()
                .map_err(|_| "file catalog declaration failed")??,
            chunks: chunks?,
        })
    })
}

fn declare(socket: &Path, name: &str, field: &str, ranged: bool) -> Result<String, String> {
    let (schema_type, primary) = if ranged {
        (
            "HashRange",
            json!({"kind":"hash_range","range_type":"String"}),
        )
    } else {
        ("Hash", json!({"kind":"hash"}))
    };
    let declared = post(
        socket,
        "/api/schemas/declare",
        &json!({
            "namespace": APP_ID,
            "schema": {
                "name": name,
                "descriptive_name": format!("LastDB Files {name}"),
                "schema_type": schema_type,
                "record_keys": {"primary":primary,"alternates":{}},
                "fields": [field],
                "field_types": { (field): "String" },
                "field_descriptions": { (field): format!("LastDB Files {name} {field} version one") },
            },
        }),
    )?;
    let expected = format!("{APP_ID}/{name}");
    if declared["canonical"].as_str() != Some(expected.as_str()) {
        return Err("file-sharing schema resolved to an unexpected name".into());
    }
    let id = declared["definition_id"]
        .as_str()
        .ok_or("missing file-sharing definition ID")?;
    cloud_backup_protocol::Digest::try_from(id.to_owned())
        .map_err(|_| "invalid file-sharing definition ID")?;
    Ok(id.to_owned())
}

fn load_catalog(socket: &Path) -> Result<Option<Value>, String> {
    let key = KeyValue::new(Some(CATALOG_HASH.to_owned()), None);
    let mut rows = query(socket, SCHEMAS.catalog, &[key])?;
    Ok(rows.pop().flatten())
}

fn catalog(row: Option<&Value>) -> Result<Option<CatalogRecord<'_>>, String> {
    row.map(|row| {
        Ok(CatalogRecord {
            revision: row["revision"]
                .as_str()
                .ok_or("file catalog revision is missing")?,
            entries: row
                .pointer("/fields/entries")
                .and_then(Value::as_str)
                .ok_or("file catalog entries are missing")?,
        })
    })
    .transpose()
}

fn read_revisions(socket: &Path, keys: &[KeyValue]) -> Result<Vec<(KeyValue, String)>, String> {
    let rows = query(socket, SCHEMAS.chunks, keys)?;
    keys.iter()
        .zip(rows)
        .filter_map(|(key, row)| row.map(|row| (key, row)))
        .map(|(key, row)| {
            let revision = row["revision"]
                .as_str()
                .ok_or("file chunk revision is missing")?;
            Ok((key.clone(), revision.to_owned()))
        })
        .collect()
}

fn query(socket: &Path, schema: &str, keys: &[KeyValue]) -> Result<Vec<Option<Value>>, String> {
    let queries = keys
        .iter()
        .map(|key| {
            let hash = key
                .hash
                .as_deref()
                .ok_or("file sharing requires a hash key")?;
            Ok(get_query_body(schema, hash, key.range.as_deref()))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let value = post(socket, "/api/queries/batch", &json!({"queries":queries}))?;
    let replies = value["results"]
        .as_array()
        .filter(|v| v.len() == keys.len())
        .ok_or("file-sharing read returned incomplete results")?;
    replies
        .iter()
        .zip(keys)
        .map(|(reply, key)| exact_row(reply, key))
        .collect()
}

fn exact_row(reply: &Value, key: &KeyValue) -> Result<Option<Value>, String> {
    let response = &reply["response"];
    if reply["status"].as_u64() != Some(200)
        || response["ok"].as_bool() != Some(true)
        || response["has_more"].as_bool() != Some(false)
    {
        return Err("file-sharing read did not return a complete result".into());
    }
    let rows = response["results"]
        .as_array()
        .ok_or("file-sharing read omitted its rows")?;
    match rows.as_slice() {
        [] => Ok(None),
        [row] if row["key"] == json!(key) => Ok(Some(row.clone())),
        _ => Err("file-sharing read returned a different key".into()),
    }
}

fn write(
    socket: &Path,
    definitions: &Definitions,
    mut operations: Vec<Value>,
) -> Result<String, String> {
    let count = operations.len();
    let needs_revision = count == 1 && operations[0]["schema"].as_str() == Some(SCHEMAS.catalog);
    for operation in &mut operations {
        let definition = match operation["schema"].as_str() {
            Some(schema) if schema == SCHEMAS.catalog => &definitions.catalog,
            Some(schema) if schema == SCHEMAS.chunks => &definitions.chunks,
            _ => return Err("file sharing tried to write outside its schemas".into()),
        };
        operation["definition_id"] = json!(definition);
    }
    let receipt = post(
        socket,
        "/api/mutations/batch",
        &json!({"mutations":operations}),
    )?;
    if receipt["count"].as_u64() != Some(count as u64) {
        return Err("file-sharing write returned an incomplete receipt".into());
    }
    if !needs_revision {
        return Ok(String::new());
    }
    let id = receipt["mutation_ids"][0]
        .as_str()
        .ok_or("file catalog write returned no mutation ID")?;
    receipt["record_revisions"][id]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| "file-sharing write returned no record revision".into())
}

fn post(socket: &Path, path: &str, body: &Value) -> Result<Value, String> {
    let response = post_json_admin(socket, path, body)?;
    parse_json_response(&response, path)
}
