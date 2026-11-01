use super::*;
use std::path::Path;

/// Dispatch the file-blob transfer verbs.
pub(super) fn db_blob_command(socket: &Path, action: DbBlobCommand) -> Result<(), String> {
    match action {
        DbBlobCommand::FetchFileBlob {
            pointer_json,
            out,
            json,
            raw,
        } => db_fetch_file_blob(socket, &pointer_json, out, json, raw),
        DbBlobCommand::PutBlobLocal(args) => db_put_blob_local(socket, &args),
    }
}
