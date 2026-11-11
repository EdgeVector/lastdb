use super::*;

#[path = "db_cleanup_cmds.rs"]
mod db_cleanup_cmds;
pub(crate) use db_cleanup_cmds::*;

#[path = "db_ledger_cmds.rs"]
mod db_ledger_cmds;
pub(crate) use db_ledger_cmds::*;
#[path = "record_file_input.rs"]
mod record_file_input;
pub(crate) use record_file_input::*;

pub(super) fn db_storage_command(
    data_dir: Option<PathBuf>,
    action: AdminStorageCommand,
) -> Result<(), String> {
    let (_home, socket) = resolve_client_home_and_socket(data_dir)?;
    if probe_health(&socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        ));
    }
    match action {
        AdminStorageCommand::Inventory {
            by_schema: _,
            limit,
            json,
            out,
            timeout,
        } => db_schemas(&socket, json, out.as_deref(), timeout, limit.unwrap_or(50)),
        AdminStorageCommand::Reclaim(DbReclaimCommand::Cleanup {
            status,
            wait,
            timeout,
            json,
        }) => db_cleanup(&socket, status, wait, timeout, json),
        AdminStorageCommand::Reclaim(DbReclaimCommand::DeleteLedger { limit, json }) => {
            db_delete_ledger(&socket, limit, json)
        }
    }
}

#[path = "db_client_util.rs"]
mod db_client_util;
pub(crate) use db_client_util::*;
#[path = "db_report_cmds.rs"]
mod db_report_cmds;
pub(crate) use db_report_cmds::*;
#[path = "db_response_cmds.rs"]
mod db_response_cmds;
pub(crate) use db_response_cmds::*;
#[path = "db_schema_policy_cmds.rs"]
mod db_schema_policy_cmds;
pub(crate) use db_schema_policy_cmds::*;
