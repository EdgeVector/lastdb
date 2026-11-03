use super::*;
use std::path::Path;

/// Dispatch the file verbs of a record.
pub(super) fn db_blob_command(socket: &Path, action: DbBlobCommand) -> Result<(), String> {
    match action {
        DbBlobCommand::Put(args) => db_put_blob(socket, &args),
        DbBlobCommand::Fetch { target, out, json } => db_fetch_blob(socket, &target, out, json),
        DbBlobCommand::Delete(target) => db_delete_blob(socket, &target),
    }
}
