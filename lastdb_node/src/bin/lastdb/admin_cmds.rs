use super::*;

pub(super) fn run(data_dir: Option<PathBuf>, action: AdminCommand) -> Result<(), String> {
    match action {
        AdminCommand::Ops(args) => ops(data_dir, &args),
        AdminCommand::AlertCheck(args) => alert_check(data_dir, args),
        AdminCommand::LogFilter { directive, json } => {
            log_filter_command(data_dir, directive.as_deref(), json)
        }
        AdminCommand::ServiceHome(action) => service_home_command(action),
        AdminCommand::Schema(action) => match action {
            AdminSchemaCommand::Drop(args) => schema_drop_command(
                data_dir,
                args.schema,
                args.owner_app,
                args.must_exist,
                args.json,
            ),
            AdminSchemaCommand::NameClaim(action) => schema_name_claim_command(data_dir, action),
        },
        AdminCommand::Retention(action) => schema_retention_command(data_dir, action),
        AdminCommand::Storage(action) => db_command(data_dir, storage_action(action)),
        AdminCommand::SearchRebuild { page_size, json } => {
            search_rebuild_command(data_dir, page_size, json)
        }
        AdminCommand::Backup(action) => cloud_command(data_dir, backup_action(action)?),
        AdminCommand::Payments { action, json } => {
            cloud_command(data_dir, CloudCommand::Payments { action, json })
        }
        AdminCommand::Shared(action) => shared(data_dir, action),
    }
}

fn storage_action(action: AdminStorageCommand) -> DbCommand {
    match action {
        AdminStorageCommand::Inventory {
            by_schema: true,
            limit,
            json,
            out,
            timeout,
        } => DbCommand::Inspect(DbInspectCommand::Schemas {
            limit: limit.unwrap_or(50),
            json,
            out,
            timeout,
        }),
        AdminStorageCommand::Inventory {
            json, out, timeout, ..
        } => DbCommand::Inspect(DbInspectCommand::Inventory { json, out, timeout }),
        AdminStorageCommand::Reclaim(action) => DbCommand::Reclaim(action),
        AdminStorageCommand::ReplayCutoff(args) => DbCommand::Offline(args),
    }
}

fn backup_action(action: AdminBackupCommand) -> Result<CloudCommand, String> {
    let action = match action {
        AdminBackupCommand::Init(args) => ByteBackupCommand::Init(args),
        AdminBackupCommand::StartStopped(args) => ByteBackupCommand::StartStopped(args),
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
