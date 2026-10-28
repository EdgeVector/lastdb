use super::*;
use std::path::Path;

/// Dispatch the repair and legacy-drain verbs.
pub(super) fn db_repair_command(socket: &Path, action: DbRepairCommand) -> Result<(), String> {
    match action {
        DbRepairCommand::RepairSchemaMoleculeMap {
            schema,
            map_file,
            write_map_file,
            execute,
            expected_current_fingerprint,
            json,
        } => db_repair_schema_molecule_map(
            socket,
            &schema,
            map_file.as_deref(),
            write_map_file.as_deref(),
            execute,
            expected_current_fingerprint.as_deref(),
            json,
        ),
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
