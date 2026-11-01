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
    }
}

/// Dispatch the cleanup and delete ledger verbs.
pub(super) fn db_reclaim_command(socket: &Path, action: DbReclaimCommand) -> Result<(), String> {
    match action {
        DbReclaimCommand::Cleanup {
            status,
            wait,
            timeout,
            json,
        } => db_cleanup(socket, status, wait, timeout, json),
        DbReclaimCommand::DeleteLedger { limit, json } => db_delete_ledger(socket, limit, json),
    }
}
