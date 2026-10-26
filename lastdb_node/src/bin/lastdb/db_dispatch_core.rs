use super::*;
use std::path::Path;

/// Dispatch the read-only inspection and audit verbs.
pub(super) fn db_inspect_command(socket: &Path, action: DbInspectCommand) -> Result<(), String> {
    match action {
        DbInspectCommand::Inventory { json, out, timeout } => {
            db_inventory(socket, json, out.as_deref(), timeout)
        }
        DbInspectCommand::Schemas {
            json,
            out,
            timeout,
            limit,
        } => db_schemas(socket, json, out.as_deref(), timeout, limit),
        DbInspectCommand::VerifyRefs {
            json,
            last,
            steps,
            cursor,
            max_pages,
            max_ms,
            pause_ms,
            page_rows,
            scope,
            all_scopes,
            max_atoms,
        } => db_verify_refs(
            socket,
            &VerifyRefsOpts {
                json,
                last,
                steps,
                cursor,
                max_pages,
                max_ms,
                pause_ms,
                page_rows,
                scope,
                all_scopes,
                max_atoms,
            },
        ),
        DbInspectCommand::MoleculeDrain {
            molecule,
            scope,
            json,
        } => db_molecule_drain(socket, molecule.as_deref(), scope.as_deref(), json),
        DbInspectCommand::UnresolvedAtoms { json } => db_unresolved_atoms(socket, json),
        DbInspectCommand::MoleculeKeys {
            molecule,
            max_keys,
            json,
        } => db_molecule_keys(socket, &molecule, max_keys, json),
    }
}

/// Dispatch the space reclaim, GC and compaction verbs.
pub(super) fn db_reclaim_command(socket: &Path, action: DbReclaimCommand) -> Result<(), String> {
    match action {
        DbReclaimCommand::CleanupDeleteMarkers {
            molecule,
            scope,
            after_key,
            max_keys,
            execute,
            json,
        } => db_cleanup_delete_markers(
            socket,
            &serde_json::json!({
                "molecule": molecule, "scope": scope, "after_key": after_key,
                "limit": max_keys, "execute": execute, "cleanup_delete_markers": true,
            }),
            json,
        ),
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
        DbReclaimCommand::GcProteins { execute, json } => db_gc_proteins(socket, execute, json),
        DbReclaimCommand::DeleteLedger { limit, json } => db_delete_ledger(socket, limit, json),
    }
}
