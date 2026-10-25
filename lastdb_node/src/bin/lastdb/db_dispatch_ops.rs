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

/// Dispatch the at-rest sealing and partition re-key verbs.
pub(super) fn db_seal_command(socket: &Path, action: DbSealCommand) -> Result<(), String> {
    match action {
        DbSealCommand::ResealAtRest {
            collection,
            target,
            execute,
            max_rows,
            max_secs,
            progress,
            restart,
            json,
            out,
            timeout,
        } => db_reseal_at_rest(
            socket,
            &ResealAtRestCliOpts {
                collection: collection.as_str(),
                target,
                execute,
                max_rows,
                max_secs,
                progress_only: progress,
                restart,
                json_only: json,
                out: out.as_deref(),
                timeout_secs: timeout,
            },
        ),
        DbSealCommand::ReapUnsealed {
            collection,
            execute,
            max_rows,
            max_secs,
            progress,
            restart,
            all,
            json,
            out,
            timeout,
        } => db_reap_unsealed(
            socket,
            &ReapUnsealedCliOpts {
                collection: collection.as_str(),
                execute,
                max_rows,
                max_secs,
                progress_only: progress,
                restart,
                all,
                json_only: json,
                out: out.as_deref(),
                timeout_secs: timeout,
            },
        ),
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
