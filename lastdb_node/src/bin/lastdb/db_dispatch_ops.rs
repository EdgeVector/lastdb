use super::*;
use std::path::Path;

/// Dispatch the repair and legacy-drain verbs.
pub(super) fn db_repair_command(socket: &Path, action: DbRepairCommand) -> Result<(), String> {
    match action {
        DbRepairCommand::RepairHashrangeKeyFields {
            schema,
            hash,
            execute,
            json,
        } => db_repair_hashrange_key_fields(socket, &schema, &hash, execute, json),
    }
}

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
