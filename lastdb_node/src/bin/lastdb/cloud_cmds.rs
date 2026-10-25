//! Explicit V2 backup and local credential safety commands.
use super::*;

#[path = "cloud_cmds/byte_backup.rs"]
mod byte_backup;
#[path = "cloud_cmds/off.rs"]
mod off;
#[path = "cloud_cmds/recovery_home.rs"]
mod recovery_home;

pub(super) fn cloud_command(data_dir: Option<PathBuf>, action: CloudCommand) -> Result<(), String> {
    let action = match action {
        CloudCommand::BackupV2 { action } if byte_backup::is_setup(&action) => {
            return byte_backup::setup(data_dir.as_deref(), action);
        }
        action => action,
    };
    if recovery_home::handles(&action) {
        return recovery_home::command(data_dir.as_deref(), action);
    }
    if matches!(action, CloudCommand::BackupV2 { .. })
        && !data_dir.as_ref().is_some_and(|home| home.is_absolute())
    {
        return Err("V2 backup requires an explicit absolute --data-dir home".into());
    }
    let home = lastdb_node::host::resolve_home(data_dir)?;
    match action {
        CloudCommand::BackupV2 { action } => byte_backup::run(&home, action),
        CloudCommand::Off { json } => off::run(&home, json),
        CloudCommand::PrepareRecoveryHome { .. }
        | CloudCommand::RecoverCredentialsHome { .. }
        | CloudCommand::RecoverDevCredentialsHome { .. } => {
            unreachable!("handled before home resolution")
        }
    }
}
