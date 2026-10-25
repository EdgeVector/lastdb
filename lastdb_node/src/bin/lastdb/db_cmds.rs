use super::*;

#[path = "db_delete_marker_cmds.rs"]
mod db_delete_marker_cmds;
pub(crate) use db_delete_marker_cmds::*;

#[path = "db_reap_unsealed_cmds.rs"]
mod db_reap_unsealed_cmds;
pub(crate) use db_reap_unsealed_cmds::*;

#[path = "db_pin_log_cmds.rs"]
mod db_pin_log_cmds;
pub(crate) use db_pin_log_cmds::*;

#[path = "db_app_storage_cmds.rs"]
mod db_app_storage_cmds;
pub(crate) use db_app_storage_cmds::*;
#[path = "db_blob_cmds.rs"]
mod db_blob_cmds;
pub(crate) use db_blob_cmds::*;
#[path = "db_inventory_print_cmds.rs"]
mod db_inventory_print_cmds;
pub(crate) use db_inventory_print_cmds::*;
#[path = "db_ledger_cmds.rs"]
mod db_ledger_cmds;
pub(crate) use db_ledger_cmds::*;
#[path = "db_proof_cmds.rs"]
mod db_proof_cmds;
use db_proof_cmds::*;
#[path = "db_put_blob_local_cmds.rs"]
mod db_put_blob_local_cmds;
pub(crate) use db_put_blob_local_cmds::*;
#[path = "db_reclaim_cmds.rs"]
mod db_reclaim_cmds;
pub(crate) use db_reclaim_cmds::*;
#[path = "db_unresolved_cmds.rs"]
mod db_unresolved_cmds;
pub(crate) use db_unresolved_cmds::*;
#[path = "db_molecule_drain_cmds.rs"]
mod db_molecule_drain_cmds;
pub(crate) use db_molecule_drain_cmds::*;
#[path = "db_verify_refs_cmds.rs"]
mod db_verify_refs_cmds;
pub(crate) use db_verify_refs_cmds::*;
#[path = "db_report_util_cmds.rs"]
mod db_report_util_cmds;
pub(crate) use db_report_util_cmds::*;
#[path = "db_reseal_cmds.rs"]
mod db_reseal_cmds;
pub(crate) use db_reseal_cmds::*;

pub(super) fn db_command(data_dir: Option<PathBuf>, action: DbCommand) -> Result<(), String> {
    let (home, socket) = resolve_client_home_and_socket(data_dir)?;
    // The offline verbs work on a stopped home. They never reach the daemon.
    if let DbCommand::Offline(action) = action {
        return db_offline_command(&home, action);
    }
    if lastdb_node::health_alert::probe_health(&socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        ));
    }
    match action {
        DbCommand::Inspect(action) => db_inspect_command(&home, &socket, action),
        DbCommand::Reclaim(action) => db_reclaim_command(&socket, action),
        DbCommand::Repair(action) => db_repair_command(&socket, action),
        DbCommand::Seal(action) => db_seal_command(&socket, action),
        DbCommand::Blob(action) => db_blob_command(&socket, action),
        DbCommand::Proof(action) => db_proof_command(&socket, action),
        DbCommand::Offline(_) => unreachable!("handled before the daemon probe"),
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

/// Dispatch the verbs that work on a stopped home.
fn db_offline_command(home: &Path, action: DbOfflineCommand) -> Result<(), String> {
    match action {
        DbOfflineCommand::PrepareNativeTransfer {
            source_home,
            i_know_this_is_primary,
        } => {
            lastdb_node::native_transfer_target::prepare(&source_home, home, i_know_this_is_primary)
        }
        DbOfflineCommand::Recount {
            execute,
            scopes,
            all_scopes,
            blobs_exact,
            molecules,
            delete_cutoff_unix_secs,
            shards,
            i_know_this_is_primary,
            json,
        } => lastdb_node::offline_recount::recount_offline(
            &lastdb_node::offline_recount::RecountArgs {
                home,
                execute,
                scopes,
                all_scopes,
                blobs_exact,
                molecules,
                delete_cutoff_unix_secs,
                shards,
                i_know_this_is_primary,
                json,
            },
        ),
    }
}
