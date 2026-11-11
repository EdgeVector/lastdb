use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AdminCommand {
    /// Reclaim deleted space in active stores.
    #[command(subcommand)]
    Storage(AdminStorageCommand),
    /// Prepare recovery credentials and operate on exact backup jobs.
    #[command(subcommand)]
    Backup(AdminBackupCommand),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminStorageCommand {
    /// Reclaim already-deleted v2 data in both active stores.
    Cleanup {
        #[arg(long)]
        status: bool,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..=86400))]
        timeout: u64,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminBackupCommand {
    /// Create one fresh DEV identity without starting a daemon.
    Init(BackupInitArgs),
    /// Resume the exact saved backup job on the owner daemon.
    Resume(BackupJsonArgs),
    /// Fence the saved job while retaining uncertain cloud byte obligations.
    Close(BackupJsonArgs),
    /// Inspect the exact recovery dependency closure without deleting anything.
    InspectDependencies(BackupDependenciesArgs),
    /// Register a credentials-only recovery device without opening a store.
    RecoverCredentialsHome {
        #[command(flatten)]
        args: RecoverCredentialsArgs,
        #[arg(long, conflicts_with = "invite_code_file")]
        existing_dev: bool,
    },
    /// Prepare recovery credentials from an existing account without registering.
    PrepareRecoveryHome(PrepareRecoveryArgs),
}
