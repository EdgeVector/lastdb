use super::*;

#[path = "db_app_storage_cmds.rs"]
mod db_app_storage_cmds;
pub(crate) use db_app_storage_cmds::*;
#[path = "db_blob_cmds.rs"]
mod db_blob_cmds;
pub(crate) use db_blob_cmds::*;
#[path = "db_dangling_tips_cmds.rs"]
mod db_dangling_tips_cmds;
pub(crate) use db_dangling_tips_cmds::*;
#[path = "db_gc_atoms_cmds.rs"]
mod db_gc_atoms_cmds;
pub(crate) use db_gc_atoms_cmds::*;
#[path = "db_inventory_print_cmds.rs"]
mod db_inventory_print_cmds;
pub(crate) use db_inventory_print_cmds::*;
#[path = "db_key_forks_cmds.rs"]
mod db_key_forks_cmds;
pub(crate) use db_key_forks_cmds::*;
#[path = "db_ledger_cmds.rs"]
mod db_ledger_cmds;
pub(crate) use db_ledger_cmds::*;
#[path = "db_order_log_bloat_cmds.rs"]
mod db_order_log_bloat_cmds;
pub(crate) use db_order_log_bloat_cmds::*;
#[path = "db_order_log_repair_cmds.rs"]
mod db_order_log_repair_cmds;
pub(crate) use db_order_log_repair_cmds::*;
#[path = "db_put_blob_local_cmds.rs"]
mod db_put_blob_local_cmds;
pub(crate) use db_put_blob_local_cmds::*;
#[path = "db_reap_cmds.rs"]
mod db_reap_cmds;
pub(crate) use db_reap_cmds::*;
#[path = "db_reclaim_cmds.rs"]
mod db_reclaim_cmds;
pub(crate) use db_reclaim_cmds::*;
#[path = "db_rekey_cmds.rs"]
mod db_rekey_cmds;
pub(crate) use db_rekey_cmds::*;
#[path = "db_rekey_partition_cmds.rs"]
mod db_rekey_partition_cmds;
pub(crate) use db_rekey_partition_cmds::*;
#[path = "db_reseal_cmds.rs"]
mod db_reseal_cmds;
pub(crate) use db_reseal_cmds::*;
#[path = "db_retain_cmds.rs"]
mod db_retain_cmds;
pub(crate) use db_retain_cmds::*;
#[path = "db_thin_tips_cmds.rs"]
mod db_thin_tips_cmds;
pub(crate) use db_thin_tips_cmds::*;
#[path = "db_tombstone_audit_cmds.rs"]
mod db_tombstone_audit_cmds;
pub(crate) use db_tombstone_audit_cmds::*;
#[path = "db_tombstone_cmds.rs"]
mod db_tombstone_cmds;
pub(crate) use db_tombstone_cmds::*;

pub(super) fn db_command(data_dir: Option<PathBuf>, action: DbCommand) -> Result<(), String> {
    if let DbCommand::RetainSupersededVersionsOffline {
        execute,
        version_cutoff_nanos,
        json,
    } = action
    {
        return db_retain_superseded_versions_offline(
            data_dir,
            execute,
            version_cutoff_nanos,
            json,
        );
    }
    let (home, socket) = resolve_client_home_and_socket(data_dir)?;
    if lastdb_node::health_alert::probe_health(&socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        ));
    }
    match action {
        DbCommand::Inventory { json, out, timeout } => {
            db_inventory(&home, &socket, json, out.as_deref(), timeout)
        }
        DbCommand::Schemas {
            json,
            out,
            timeout,
            limit,
        } => db_schemas(&socket, json, out.as_deref(), timeout, limit),
        DbCommand::RepairSchemaMoleculeMap {
            schema,
            map_file,
            write_map_file,
            execute,
            expected_current_fingerprint,
            json,
        } => db_repair_schema_molecule_map(
            &socket,
            &schema,
            map_file.as_deref(),
            write_map_file.as_deref(),
            execute,
            expected_current_fingerprint.as_deref(),
            json,
        ),
        DbCommand::RepairHashrangeKeyFields {
            schema,
            hash,
            execute,
            json,
        } => db_repair_hashrange_key_fields(&socket, &schema, &hash, execute, json),
        DbCommand::ClearHistory {
            schema,
            keep_last,
            execute,
            json,
        } => db_clear_history(&socket, schema.as_deref(), keep_last, execute, json),
        DbCommand::Compact {
            collection,
            all,
            execute,
            json,
        } => db_compact(&socket, collection.as_deref(), all, execute, json),
        DbCommand::StampPurgedAtomRetirements { execute, json } => {
            db_stamp_purged_atom_retirements(&socket, execute, json)
        }
        DbCommand::PurgeSchemaidx { json } => db_purge_schemaidx(&socket, json),
        DbCommand::GcAtoms {
            schema,
            execute,
            prune_live_history,
            json,
        } => db_gc_atoms(
            &socket,
            schema.as_deref(),
            execute,
            prune_live_history,
            json,
        ),
        DbCommand::ReapDroppedSchema {
            schema,
            fields,
            execute,
            max_ops,
            cursor,
            json,
        } => db_reap_dropped_schema(
            &socket,
            &schema,
            &fields,
            execute,
            max_ops,
            cursor.as_deref(),
            json,
        ),
        DbCommand::ProbeDroppedTip {
            schema,
            molecule,
            key_hash,
            key_range,
            expected_key_fingerprint,
            json,
        } => db_probe_dropped_tip(
            &socket,
            &schema,
            &molecule,
            &key_hash,
            &key_range,
            expected_key_fingerprint.as_deref(),
            json,
        ),
        DbCommand::GcFileBlobs { execute, json } => db_gc_file_blobs(&socket, execute, json),
        DbCommand::GcProteins { execute, json } => db_gc_proteins(&socket, execute, json),
        DbCommand::PurgeRefBlobs { execute, json } => db_purge_ref_blobs(&socket, execute, json),
        DbCommand::ReclaimKeepSmallLegacy { execute, json } => {
            db_reclaim_keep_small_legacy(&socket, execute, json)
        }
        DbCommand::ReclaimKeepSmallSnapshot { execute, json } => {
            db_reclaim_keep_small_snapshot(&socket, execute, json)
        }
        DbCommand::RepairDanglingTips {
            execute,
            max_ops,
            tip_page,
            audit_limit,
            schema,
            hash_key,
            json,
        } => db_repair_dangling_tips(
            &socket,
            execute,
            &RepairDanglingTipsArgs {
                max_ops,
                tip_page,
                audit_limit,
                schema,
                hash_key,
            },
            json,
        ),
        DbCommand::UnresolvedAtoms { json } => db_unresolved_atoms(&socket, json),
        DbCommand::DrainTipHistory {
            execute,
            max_keys,
            max_prunes,
            after_key,
            from_checkpoint,
            json,
        } => db_drain_tip_history(
            &socket,
            execute,
            max_keys,
            max_prunes,
            after_key,
            from_checkpoint,
            json,
        ),
        DbCommand::RetainSupersededVersions {
            execute,
            max_keys,
            max_prunes,
            after_key,
            from_checkpoint,
            retention_seconds,
            json,
        } => db_retain_superseded_versions(
            &socket,
            &RetainSupersededVersionsCliOpts {
                execute,
                max_keys,
                max_prunes,
                after_key,
                from_checkpoint,
                retention_seconds,
                json_only: json,
            },
        ),
        DbCommand::RetainSupersededVersionsOffline { .. } => {
            Err("retain-superseded-versions-offline is handled before the daemon probe".into())
        }
        DbCommand::ProbeLocatorOnly {
            max_tips,
            tip_page,
            json,
        } => db_probe_locator_only(&socket, max_tips, tip_page, json),
        DbCommand::DeleteLedger { limit, json } => db_delete_ledger(&socket, limit, json),
        DbCommand::MigratePhotoBlobs { execute, json } => {
            db_migrate_photo_blobs(&socket, execute, json)
        }
        DbCommand::MigrateThinTips { execute, json } => {
            db_migrate_thin_tips(&socket, execute, json)
        }
        DbCommand::ResealAtRest {
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
            &socket,
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
        DbCommand::ReapUnsealed {
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
            &socket,
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
        DbCommand::RekeyAtomPartitionPrefix {
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
            &socket,
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
        DbCommand::TombstoneFlagAudit {
            schema,
            execute,
            max_keys,
            after_key,
            once,
            json,
        } => db_tombstone_flag_audit(
            &socket,
            &TombstoneFlagAuditCliOpts {
                schema,
                execute,
                max_keys,
                after_key,
                once,
                json_only: json,
            },
        ),
        DbCommand::DrainLegacyTombstones {
            schema,
            execute,
            max_keys,
            json,
        } => db_drain_legacy_tombstones(&socket, schema.as_deref(), execute, max_keys, json),
        DbCommand::LegacyKeyForkAudit { max_keys, json } => {
            db_legacy_key_forks(&socket, false, max_keys, json)
        }
        DbCommand::DrainLegacyKeyForks {
            execute,
            max_keys,
            json,
        } => db_legacy_key_forks(&socket, execute, max_keys, json),
        DbCommand::OrderLogAudit { max_keys, json } => db_order_log_audit(&socket, max_keys, json),
        DbCommand::PinLogAudit {
            max_keys,
            after_key,
            json,
        } => db_pin_log_audit(&socket, max_keys, after_key.as_deref(), json),
        DbCommand::OrderLogBloatAudit { max_keys, json } => {
            db_order_log_bloat_audit(&socket, max_keys, json)
        }
        DbCommand::CompactOrderLog {
            execute,
            max_keys,
            retention_seconds,
            json,
        } => db_compact_order_log(&socket, execute, max_keys, retention_seconds, json),
        DbCommand::RepairOrderLogShortfall {
            execute,
            max_keys,
            json,
        } => db_repair_order_log_shortfall(&socket, execute, max_keys, json),
        DbCommand::MoleculeKeys {
            molecule,
            max_keys,
            json,
        } => db_molecule_keys(&socket, &molecule, max_keys, json),
        DbCommand::DrainPlaneResidue {
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
            &socket,
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
        DbCommand::FetchFileBlob {
            pointer_json,
            out,
            json,
            raw,
        } => db_fetch_file_blob(&socket, &pointer_json, out, json, raw),
        DbCommand::PutFileBlob {
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
            &socket,
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
        DbCommand::PutBlobLocal(args) => db_put_blob_local(&socket, &args),
        DbCommand::ForkFileBlob {
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
            &socket,
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

#[path = "db_client_util.rs"]
mod db_client_util;
pub(crate) use db_client_util::*;
#[path = "db_repair_cmds.rs"]
mod db_repair_cmds;
pub(crate) use db_repair_cmds::*;
#[path = "db_report_cmds.rs"]
mod db_report_cmds;
pub(crate) use db_report_cmds::*;
#[path = "db_response_cmds.rs"]
mod db_response_cmds;
pub(crate) use db_response_cmds::*;
#[path = "db_schema_policy_cmds.rs"]
mod db_schema_policy_cmds;
pub(crate) use db_schema_policy_cmds::*;
