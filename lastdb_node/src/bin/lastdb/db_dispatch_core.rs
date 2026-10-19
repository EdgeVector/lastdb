use super::*;
use std::path::Path;

/// Dispatch the read-only inspection and audit verbs.
pub(super) fn db_inspect_command(
    home: &Path,
    socket: &Path,
    action: DbInspectCommand,
) -> Result<(), String> {
    match action {
        DbInspectCommand::Inventory { json, out, timeout } => {
            db_inventory(home, socket, json, out.as_deref(), timeout)
        }
        DbInspectCommand::Schemas {
            json,
            out,
            timeout,
            limit,
        } => db_schemas(socket, json, out.as_deref(), timeout, limit),
        DbInspectCommand::UnresolvedAtoms { json } => db_unresolved_atoms(socket, json),
        DbInspectCommand::MoleculeKeys {
            molecule,
            max_keys,
            json,
        } => db_molecule_keys(socket, &molecule, max_keys, json),
        DbInspectCommand::OrderLogAudit { max_keys, json } => {
            db_order_log_audit(socket, max_keys, json)
        }
        DbInspectCommand::PinLogAudit {
            max_keys,
            after_key,
            json,
        } => db_pin_log_audit(socket, max_keys, after_key.as_deref(), json),
        DbInspectCommand::OrderLogBloatAudit { max_keys, json } => {
            db_order_log_bloat_audit(socket, max_keys, json)
        }
        DbInspectCommand::TombstoneFlagAudit {
            schema,
            execute,
            max_keys,
            after_key,
            once,
            json,
        } => db_tombstone_flag_audit(
            socket,
            &TombstoneFlagAuditCliOpts {
                schema,
                execute,
                max_keys,
                after_key,
                once,
                json_only: json,
            },
        ),
        DbInspectCommand::LegacyKeyForkAudit { max_keys, json } => {
            db_legacy_key_forks(socket, false, max_keys, json)
        }
    }
}

/// Dispatch the space reclaim, GC and compaction verbs.
pub(super) fn db_reclaim_command(socket: &Path, action: DbReclaimCommand) -> Result<(), String> {
    match action {
        DbReclaimCommand::Compact {
            collection,
            all,
            execute,
            json,
        } => db_compact(socket, collection.as_deref(), all, execute, json),
        DbReclaimCommand::ClearHistory {
            schema,
            keep_last,
            execute,
            json,
        } => db_clear_history(socket, schema.as_deref(), keep_last, execute, json),
        DbReclaimCommand::GcFileBlobs { execute, json } => db_gc_file_blobs(socket, execute, json),
        DbReclaimCommand::GcProteins { execute, json } => db_gc_proteins(socket, execute, json),
        DbReclaimCommand::ReclaimKeepSmallLegacy { execute, json } => {
            db_reclaim_keep_small_legacy(socket, execute, json)
        }
        DbReclaimCommand::ReclaimKeepSmallSnapshot { execute, json } => {
            db_reclaim_keep_small_snapshot(socket, execute, json)
        }
        DbReclaimCommand::StampPurgedAtomRetirements { execute, json } => {
            db_stamp_purged_atom_retirements(socket, execute, json)
        }
        DbReclaimCommand::DeleteLedger { limit, json } => db_delete_ledger(socket, limit, json),
    }
}

/// Dispatch the tip-history retention, drain and migration verbs.
pub(super) fn db_tips_command(socket: &Path, action: DbTipsCommand) -> Result<(), String> {
    match action {
        DbTipsCommand::MigrateThinTips { execute, json } => {
            db_migrate_thin_tips(socket, execute, json)
        }
        DbTipsCommand::MigratePhotoBlobs { execute, json } => {
            db_migrate_photo_blobs(socket, execute, json)
        }
    }
}
