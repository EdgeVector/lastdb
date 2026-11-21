//! Link a local file into an owned `/v1/write` mutation.

use std::path::PathBuf;

use fold_db::record_value::FileMarker;
use fold_db::schema::types::{FileWrite, KeyValue, Mutation};

use super::bad;
use crate::exec::reply::HostError;
use crate::exec::schema_routes::resolve_schema_name;
use crate::host::Host;

/// A file reference to remember once its record is committed.
pub(super) struct LinkedFile {
    pub schema: String,
    pub key: KeyValue,
    pub path: PathBuf,
    pub marker: FileMarker,
}

/// Link the file in place. Every path that cannot be linked gets the same refusal.
const UNUSABLE_FILE: &str =
    "the file cannot be linked: it must be a regular file outside the LastDB home";

pub(super) async fn link_file(
    host: &Host,
    mutation: &mut Mutation,
    path: String,
    name: Option<String>,
    media_type: Option<String>,
) -> Result<LinkedFile, HostError> {
    // The whole LastDB home is refused, through any symlink: its key file and
    // every database in it, not only the data folder of the selected one.
    let real = std::fs::canonicalize(&path).map_err(|_| bad(UNUSABLE_FILE))?;
    let home = std::fs::canonicalize(&host.home).unwrap_or_else(|_| host.home.clone());
    if real.starts_with(&home) {
        return Err(bad(UNUSABLE_FILE));
    }
    let records = host.records();
    let write = records
        .db_ops()
        .records()
        .reference_local_file(&real, name, media_type)
        .await
        .map_err(|_| bad(UNUSABLE_FILE))?;
    let FileWrite::LocalPath(reference) = &write else {
        return Err(HostError::internal(
            "a local file reference is a local path",
        ));
    };
    let (marker, path) = (reference.marker().clone(), reference.path().to_path_buf());
    let schema = resolve_schema_name(host, &mutation.schema_name)?
        .unwrap_or_else(|| mutation.schema_name.clone());
    mutation.file = Some(marker.clone());
    mutation.file_write = Some(write);
    Ok(LinkedFile {
        schema,
        key: mutation.key_value.clone(),
        path,
        marker,
    })
}
