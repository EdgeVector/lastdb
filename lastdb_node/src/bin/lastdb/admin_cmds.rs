use super::*;

pub(super) fn run(data_dir: Option<PathBuf>, action: AdminCommand) -> Result<(), String> {
    match action {
        AdminCommand::ServiceHome(action) => service_home_command(action),
        AdminCommand::Schema(AdminSchemaCommand::Drop(args)) => schema_drop_command(
            data_dir,
            args.schema,
            args.owner_app,
            args.must_exist,
            args.json,
        ),
        AdminCommand::Retention(action) => schema_retention_command(data_dir, action),
        AdminCommand::Storage(action) => db_storage_command(data_dir, action),
        AdminCommand::Backup(action) => cloud_command(data_dir, backup_action(action)?),
        AdminCommand::Shared(action) => shared(data_dir, action),
    }
}

fn backup_action(action: AdminBackupCommand) -> Result<CloudCommand, String> {
    let action = match action {
        AdminBackupCommand::Init(args) => ByteBackupCommand::Init(args),
        AdminBackupCommand::Resume(args) => ByteBackupCommand::Resume(args),
        AdminBackupCommand::Close(args) => ByteBackupCommand::Close(args),
        AdminBackupCommand::InspectDependencies(args) => {
            ByteBackupCommand::InspectDependencies(args)
        }
        AdminBackupCommand::RecoverCredentialsHome { args, existing_dev } => {
            if !existing_dev {
                return Ok(CloudCommand::RecoverCredentialsHome(args));
            }
            if args.env != "dev" {
                return Err("--existing-dev requires --env dev".into());
            }
            return Ok(CloudCommand::RecoverDevCredentialsHome {
                into: args.into,
                expected_user_hash: args.expected_user_hash,
                identity_seed_stdin: args.identity_seed_stdin,
                json: args.json,
            });
        }
        AdminBackupCommand::PrepareRecoveryHome(args) => {
            return Ok(CloudCommand::PrepareRecoveryHome(args));
        }
    };
    Ok(CloudCommand::BackupV2 { action })
}

fn shared(data_dir: Option<PathBuf>, action: AdminSharedCommand) -> Result<(), String> {
    let action = match action {
        AdminSharedCommand::Snapshot(args) => return snapshot_cmd::run(data_dir.as_deref(), args),
        AdminSharedCommand::Restore(args) => CloudCommand::RestoreShared(args),
        AdminSharedCommand::ResumeRestore(args) => CloudCommand::ResumeSharedRestore(args),
        AdminSharedCommand::Writer(args) => CloudCommand::SharedWriter(args),
    };
    cloud_command(data_dir, action)
}
