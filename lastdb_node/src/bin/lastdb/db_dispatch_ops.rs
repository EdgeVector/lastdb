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
        DbRepairCommand::DrainLegacyTombstones {
            schema,
            execute,
            max_keys,
            json,
        } => db_drain_legacy_tombstones(socket, schema.as_deref(), execute, max_keys, json),
        DbRepairCommand::DrainLegacyKeyForks {
            execute,
            max_keys,
            json,
        } => db_legacy_key_forks(socket, execute, max_keys, json),
        DbRepairCommand::CompactOrderLog {
            execute,
            max_keys,
            retention_seconds,
            json,
        } => db_compact_order_log(socket, execute, max_keys, retention_seconds, json),
        DbRepairCommand::RepairOrderLogShortfall {
            execute,
            max_keys,
            json,
        } => db_repair_order_log_shortfall(socket, execute, max_keys, json),
        DbRepairCommand::DrainPlaneResidue {
            family,
            source,
            target,
            execute,
            after,
            limit,
            drop_empty_source,
            until_complete,
            json,
        } => db_drain_plane_residue(
            socket,
            &DrainPlaneResidueCliOpts {
                family,
                source,
                target,
                execute,
                after,
                limit,
                drop_empty_source,
                until_complete,
                json_only: json,
            },
        ),
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
                json_only: json,
                out: out.as_deref(),
                timeout_secs: timeout,
            },
        ),
        DbSealCommand::RekeyAtomPartitionPrefix {
            execute,
            remove_flat,
            max_ops,
            tip_page,
            audit,
            audit_limit,
            progress,
            until_complete,
            compact_after,
            json,
        } => db_rekey_atom_partition_prefix(
            socket,
            RekeyCliOpts {
                execute,
                remove_flat,
                max_ops,
                tip_page,
                audit_unresolved: audit.then_some(audit_limit),
                progress,
                until_complete,
                compact_after,
                json_only: json,
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
        DbBlobCommand::PutFileBlob {
            schema,
            field,
            key_hash,
            key_range,
            bytes,
            mutation_type,
            name,
            media_type,
            cache_local_plaintext,
            additional_fields_json,
            pointer_out,
            raw,
        } => db_put_file_blob(
            socket,
            &schema,
            &field,
            &key_hash,
            key_range.as_deref(),
            &bytes,
            &mutation_type,
            name.as_deref(),
            media_type.as_deref(),
            cache_local_plaintext,
            additional_fields_json,
            pointer_out,
            raw,
        ),
        DbBlobCommand::PutBlobLocal(args) => db_put_blob_local(socket, &args),
        DbBlobCommand::ForkFileBlob {
            schema,
            field,
            key,
            key_json,
            pointer_json,
            name,
            media_type,
            cache_local_plaintext,
            json,
        } => db_fork_file_blob(
            socket,
            ForkFileBlobArgs {
                schema,
                field,
                key,
                key_json,
                pointer_json,
                name,
                media_type,
                cache_local_plaintext,
                json,
            },
        ),
    }
}
