use super::*;
use std::path::Path;

/// Dispatch the file verbs of a record.
pub(super) fn db_blob_command(socket: &Path, action: DbBlobCommand) -> Result<(), String> {
    match action {
        DbBlobCommand::PutBlob(args) => db_put_blob(socket, &args),
        DbBlobCommand::FetchBlob { target, out, json } => db_fetch_blob(socket, &target, out, json),
        DbBlobCommand::DeleteBlob(target) => db_delete_blob(socket, &target),
    }
}
