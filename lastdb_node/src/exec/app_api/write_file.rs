//! Link a local file or inline file into an owned `/v1/write` mutation.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use fold_db::record_value::{FileMarker, InlineFile, InlineWrite, MAX_INLINE_FILE_BYTES};
use fold_db::schema::types::{FileWrite, Mutation};
use sha2::{Digest, Sha256};

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
    let real = outside_home(host, &path)?;
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

/// Read a small file into an inline attachment. Paths follow the same home
/// rule as [`link_file`]; a file over [`MAX_INLINE_FILE_BYTES`] is refused.
pub(super) fn link_inline(
    host: &Host,
    mutation: &mut Mutation,
    path: String,
    name: Option<String>,
    media_type: Option<String>,
) -> Result<(), HostError> {
    let real = outside_home(host, &path)?;
    let plain = std::fs::read(&real).map_err(|_| bad(UNUSABLE_FILE))?;
    if plain.len() > MAX_INLINE_FILE_BYTES {
        return Err(bad(format!(
            "the inline file is {} bytes; an inline file holds at most {MAX_INLINE_FILE_BYTES} bytes",
            plain.len()
        )));
    }
    let marker = FileMarker {
        name,
        media_type,
        size_bytes: plain.len() as u64,
        sha256: app_identity_crypto::hex_lower(&Sha256::digest(&plain)),
    };
    let file = InlineFile {
        marker,
        data: STANDARD.encode(&plain),
    };
    if !file.is_well_formed() {
        return Err(bad(UNUSABLE_FILE));
    }
    mutation.inline_write = Some(InlineWrite::Put(file));
    Ok(())
}

fn outside_home(host: &Host, path: &str) -> Result<std::path::PathBuf, HostError> {
    // The whole LastDB home is refused, through any symlink: its key file and
    // every database in it, not only the data folder of the selected one.
    let real = std::fs::canonicalize(path).map_err(|_| bad(UNUSABLE_FILE))?;
    let home = std::fs::canonicalize(&host.home).unwrap_or_else(|_| host.home.clone());
    if real.starts_with(&home) {
        return Err(bad(UNUSABLE_FILE));
    }
    Ok(real)
}
