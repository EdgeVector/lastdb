//! Explicit V2 backup and credentials-only recovery-home commands.
use super::*;
use std::io::Read;

#[path = "cloud_cmds/byte_backup.rs"]
mod byte_backup;
#[path = "cloud_cmds/payments.rs"]
mod payments;
#[path = "cloud_cmds/recovery_home.rs"]
mod recovery_home;
#[path = "cloud_cmds/shared_restore.rs"]
mod shared_restore;
#[path = "cloud_cmds/shared_writer.rs"]
mod shared_writer;

pub(super) fn cloud_command(data_dir: Option<PathBuf>, action: CloudCommand) -> Result<(), String> {
    let action = match action {
        CloudCommand::SharedWriter(selection) => {
            return shared_writer::run(data_dir.as_deref(), selection);
        }
        CloudCommand::RestoreShared(selection) => {
            return shared_restore::run(data_dir.as_deref(), selection);
        }
        CloudCommand::ResumeSharedRestore { workspace, json } => {
            return shared_restore::resume(data_dir.as_deref(), &workspace, json);
        }
        CloudCommand::Payments { action, json } => {
            return payments::command(data_dir.as_deref(), action, json);
        }
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
        CloudCommand::PrepareRecoveryHome { .. }
        | CloudCommand::RecoverCredentialsHome { .. }
        | CloudCommand::RecoverDevCredentialsHome { .. }
        | CloudCommand::SharedWriter(_)
        | CloudCommand::RestoreShared(_)
        | CloudCommand::ResumeSharedRestore { .. }
        | CloudCommand::Payments { .. } => {
            unreachable!("handled before home resolution")
        }
    }
}

fn read_recovery_phrase() -> Result<zeroize::Zeroizing<String>, String> {
    let mut bytes = zeroize::Zeroizing::new(Vec::new());
    std::io::stdin()
        .lock()
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read recovery phrase")?;
    if bytes.len() > 4096 {
        return Err("recovery phrase exceeds its byte limit".into());
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| "recovery phrase must be UTF8")?
        .trim();
    if text.is_empty() {
        return Err("recovery phrase is empty".into());
    }
    Ok(zeroize::Zeroizing::new(text.to_owned()))
}
