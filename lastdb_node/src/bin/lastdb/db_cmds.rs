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
    if let DbCommand::Tips(DbTipsCommand::RetainSupersededVersionsOffline {
        execute,
        version_cutoff_nanos,
        json,
    }) = action
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
        DbCommand::Inspect(action) => db_inspect_command(&home, &socket, action),
        DbCommand::Reclaim(action) => db_reclaim_command(&socket, action),
        DbCommand::Tips(action) => db_tips_command(&socket, action),
        DbCommand::Repair(action) => db_repair_command(&socket, action),
        DbCommand::Seal(action) => db_seal_command(&socket, action),
        DbCommand::Blob(action) => db_blob_command(&socket, action),
    }
}

#[path = "db_dispatch_core.rs"]
mod db_dispatch_core;
use db_dispatch_core::*;
#[path = "db_dispatch_ops.rs"]
mod db_dispatch_ops;
use db_dispatch_ops::*;

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
