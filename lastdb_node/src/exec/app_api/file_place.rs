//! Where a record's file is on this device, for one `/v1/read` reply.
//!
//! Reporting `local` does not create a file. Asking for `path` writes a file
//! only when LastDB holds the bytes: into the calling app's data folder, or
//! into the folder the owner named with `file_dir`.

use std::path::Path;

use fold_db::record_value::FileMarker;
use fold_db::schema::types::KeyValue;
use serde_json::Value;

use super::super::app_data;
use super::super::calling_app::active_app;
use super::super::reply::HostError;
use super::super::schema_routes::resolve_schema_name;
use crate::host::Host;

struct Folders<'a> {
    app: Option<&'a Path>,
    owner: Option<&'a Path>,
    with_paths: bool,
}

pub(super) async fn attach(
    host: &Host,
    schema: &str,
    page: &Value,
    out: &mut Value,
    with_paths: bool,
    owner_dir: Option<&Path>,
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
    let folders = Folders {
        app: data.as_deref(),
        owner: owner_dir,
        with_paths,
    };
    for (row, record) in rows.iter().zip(records.iter_mut()) {
        if record["file"].is_null() {
            continue;
        }
        place_one(host, &canonical, &folders, row, record).await?;
    }
    Ok(())
}

async fn place_one(
    host: &Host,
    schema: &str,
    folders: &Folders<'_>,
    row: &Value,
    record: &mut Value,
) -> Result<(), HostError> {
    let key: KeyValue = serde_json::from_value(record["key"].clone())
        .map_err(|_| HostError::internal("read returned an invalid key"))?;
    match host.records().record_file_local_path(schema, &key).await? {
        Some(file) if file.intact => {
            set_here(&mut record["file"], &file.path, folders.with_paths);
            Ok(())
        }
        Some(file) if folders.with_paths => Err(HostError::new(
            404,
            format!(
                "the file of this record is missing or changed at {}",
                file.path.display()
            ),
        )),
        Some(_) => Ok(()),
        None => place_owned(host, schema, folders, &key, row, record).await,
    }
}

async fn place_owned(
    host: &Host,
    schema: &str,
    folders: &Folders<'_>,
    key: &KeyValue,
    row: &Value,
    record: &mut Value,
) -> Result<(), HostError> {
    let hash = row["metadata"]["file"]["sha256"].as_str().unwrap_or("");
    let size = record["file"]["size"]
        .as_u64()
        .ok_or_else(|| HostError::internal("read returned an invalid file size"))?;
    if let Some(dir) = folders.owner {
        super::owner_file::confirm(dir)?;
    }
    if let Some(path) = already(folders, &host.home, hash, size) {
        set_here(&mut record["file"], &path, folders.with_paths);
        return Ok(());
    }
    if !folders.with_paths {
        return Ok(());
    }
    let Some((marker, bytes)) = host.records().record_file_owned_bytes(schema, key).await? else {
        record["file"]["path"] = Value::Null;
        return Ok(());
    };
    if !bytes_match(&marker, &bytes, size) {
        return Err(HostError::internal("record file differs from its marker"));
    }
    let path = write_held(folders, &host.home, &marker.sha256, &bytes)?;
    match path {
        Some(path) => set_here(&mut record["file"], &path, true),
        None => record["file"]["path"] = Value::Null,
    }
    Ok(())
}

fn already(
    folders: &Folders<'_>,
    home: &Path,
    hash: &str,
    size: u64,
) -> Option<std::path::PathBuf> {
    match folders.owner {
        Some(dir) => app_data::existing_named(dir, hash, size),
        None => folders
            .app
            .and_then(|data| app_data::existing_export(data, home, hash, size)),
    }
}

fn write_held(
    folders: &Folders<'_>,
    home: &Path,
    hash: &str,
    bytes: &[u8],
) -> Result<Option<std::path::PathBuf>, HostError> {
    match folders.owner {
        Some(dir) => app_data::write_named(dir, hash, bytes).map(Some),
        None => match folders.app {
            Some(data) => app_data::write_export(data, home, hash, bytes).map(Some),
            None => Ok(None),
        },
    }
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
