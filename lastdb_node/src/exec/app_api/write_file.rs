//! Link a local file into an owned `/v1/write` mutation.

use fold_db::schema::types::{FileWrite, Mutation};

use super::bad;
use crate::exec::reply::HostError;
use crate::host::Host;

/// Link the file in place. Every path that cannot be linked gets the same refusal.
const UNUSABLE_FILE: &str =
    "the file cannot be linked: it must be a regular file outside the LastDB home";

pub(super) async fn link_file(
    host: &Host,
    mutation: &mut Mutation,
    path: String,
    name: Option<String>,
    media_type: Option<String>,
) -> Result<(), HostError> {
    // The whole LastDB home is refused, through any symlink: its key file and
    // every database in it, not only the data folder of the selected one.
    let real = std::fs::canonicalize(&path).map_err(|_| bad(UNUSABLE_FILE))?;
    let home = std::fs::canonicalize(&host.home).unwrap_or_else(|_| host.home.clone());
    if real.starts_with(&home) {
        return Err(bad(UNUSABLE_FILE));
    }
    // The size limit gets its own answer: the refusal below hides every reason.
    // `reference_local_file` enforces the same limit; this states it.
    let length = std::fs::metadata(&real)
        .map_err(|_| bad(UNUSABLE_FILE))?
        .len();
    let limit = fold_db::storage::MAX_RECORD_FILE_BYTES;
    if length > limit {
        return Err(bad(format!(
            "the file is {length} bytes; a record file holds at most {limit} bytes"
        )));
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
    mutation.file = Some(reference.marker().clone());
    mutation.file_write = Some(write);
    Ok(())
}
