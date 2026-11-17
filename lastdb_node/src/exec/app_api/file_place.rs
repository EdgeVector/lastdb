//! Where a record's file is on this device, for one `/v1/read` reply.
//!
//! Reporting `local` does not create a file. Asking for `path` writes a file
//! only when the calling app's grant names a data folder and LastDB holds the bytes.

use std::path::Path;

use fold_db::record_value::FileMarker;
use fold_db::schema::types::KeyValue;
use serde_json::Value;

use super::super::app_data;
use super::super::calling_app::active_app;
use super::super::reply::HostError;
use super::super::schema_routes::resolve_schema_name;
use crate::host::Host;

pub(super) async fn attach(
    host: &Host,
    schema: &str,
    page: &Value,
    out: &mut Value,
    with_paths: bool,
) -> Result<(), HostError> {
    let Some(records) = out.get_mut("records").and_then(Value::as_array_mut) else {
        return Ok(());
    };
    let rows = page["results"]
        .as_array()
        .ok_or_else(|| HostError::internal("read page is incomplete"))?;
    if rows.len() != records.len() {
        return Err(HostError::internal("read page is incomplete"));
    }
    let canonical = resolve_schema_name(host, schema)?.unwrap_or_else(|| schema.to_owned());
    let data = active_app().and_then(|app| app.data_dir.clone());
    for (row, record) in rows.iter().zip(records.iter_mut()) {
        if record["file"].is_null() {
            continue;
        }
        place_one(host, &canonical, data.as_deref(), row, record, with_paths).await?;
    }
    Ok(())
}

async fn place_one(
    host: &Host,
    schema: &str,
    data: Option<&Path>,
    row: &Value,
    record: &mut Value,
    with_paths: bool,
) -> Result<(), HostError> {
    let key: KeyValue = serde_json::from_value(record["key"].clone())
        .map_err(|_| HostError::internal("read returned an invalid key"))?;
    match host.records().record_file_local_path(schema, &key).await? {
        Some(file) if file.intact => {
            set_here(&mut record["file"], &file.path, with_paths);
            Ok(())
        }
        Some(file) if with_paths => Err(HostError::new(
            404,
            format!(
                "the file of this record is missing or changed at {}",
                file.path.display()
            ),
        )),
        Some(_) => Ok(()),
        None => place_owned(host, schema, data, &key, row, record, with_paths).await,
    }
}

async fn place_owned(
    host: &Host,
    schema: &str,
    data: Option<&Path>,
    key: &KeyValue,
    row: &Value,
    record: &mut Value,
    with_paths: bool,
) -> Result<(), HostError> {
    let hash = row["metadata"]["file"]["sha256"].as_str().unwrap_or("");
    let size = record["file"]["size"]
        .as_u64()
        .ok_or_else(|| HostError::internal("read returned an invalid file size"))?;
    if let Some(data) = data {
        if let Some(path) = app_data::existing_export(data, &host.home, hash, size) {
            set_here(&mut record["file"], &path, with_paths);
            return Ok(());
        }
    }
    if !with_paths {
        return Ok(());
    }
    let Some(data) = data else {
        record["file"]["path"] = Value::Null;
        return Ok(());
    };
    let Some((marker, bytes)) = host.records().record_file_owned_bytes(schema, key).await? else {
        record["file"]["path"] = Value::Null;
        return Ok(());
    };
    if !bytes_match(&marker, &bytes, size) {
        return Err(HostError::internal("record file differs from its marker"));
    }
    let path = app_data::write_export(data, &host.home, &marker.sha256, &bytes)?;
    set_here(&mut record["file"], &path, true);
    Ok(())
}

fn bytes_match(marker: &FileMarker, bytes: &[u8], size: u64) -> bool {
    bytes.len() as u64 == size
        && marker.size_bytes == size
        && marker.has_valid_hash()
        && fold_db::hex::sha256_hex(bytes) == marker.sha256
}

fn set_here(file: &mut Value, path: &Path, with_paths: bool) {
    let text = Value::String(path.to_string_lossy().into_owned());
    file["local"] = text.clone();
    if with_paths {
        file["path"] = text;
    }
}
