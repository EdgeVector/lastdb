use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AdminCommand {
    /// Manage the configured service home.
    #[command(subcommand)]
    ServiceHome(ServiceHomeCommand),
    /// Drop installed schemas.
    #[command(subcommand)]
    Schema(AdminSchemaCommand),
    /// Manage schema record expiration.
    #[command(subcommand)]
    Retention(SchemaRetentionCommand),
    /// Inspect storage or reclaim deleted space.
    #[command(subcommand)]
    Storage(AdminStorageCommand),
    /// Prepare recovery credentials and operate on exact backup jobs.
    #[command(subcommand)]
    Backup(AdminBackupCommand),
    /// Publish, synchronize or restore an enrolled shared database.
    #[command(subcommand)]
    Shared(AdminSharedCommand),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminSchemaCommand {
    /// Drop an installed schema or all schemas owned by one app.
    #[command(group(
        clap::ArgGroup::new("drop_target")
            .required(true)
            .args(["schema", "owner_app"])
    ))]
    Drop(SchemaDropArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminStorageCommand {
    /// Inspect live record storage by schema. This may walk a large part of the store.
    Inventory {
        /// Compatibility flag (schema-level record storage is always reported).
        #[arg(long)]
        by_schema: bool,
        /// Limit human output rows (0 prints all).
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        timeout: Option<u64>,
    },
    #[command(flatten)]
    Reclaim(DbReclaimCommand),
}

#[derive(Subcommand, Debug)]
pub(crate) enum DbReclaimCommand {
    /// Reclaim already-deleted v2 data in both active stores.
    /// The request survives a restart. Backup copies are outside this operation.
    Cleanup {
        /// Read the last request without a new cleanup request.
        #[arg(long)]
        status: bool,
        /// Wait for completion, read release, or the client deadline.
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..=86400))]
        timeout: u64,
        #[arg(long)]
        json: bool,
    },
    /// Read the durable record hard-delete audit trail (purge and delete), oldest first.
    DeleteLedger {
        /// Return at most N rows, oldest first. 0 (default) returns everything.
        #[arg(long, default_value_t = 0)]
        limit: u64,
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
        /// Use the restricted existing DEV account path and temporary home rules.
        #[arg(long, conflicts_with = "invite_code_file")]
        existing_dev: bool,
    },
    /// Prepare recovery credentials from an existing account without registering.
    PrepareRecoveryHome(PrepareRecoveryArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminSharedCommand {
    /// Upload a frozen DEV database snapshot, then upload the writer’s queued changes.
    Snapshot(SnapshotArgs),
    /// Restore a held shared snapshot and its captured tails into a fresh home.
    Restore(SharedRestoreCommand),
    /// Resume publication of a sealed shared restore workspace.
    ResumeRestore(ResumeSharedRestoreArgs),
    /// Operate the journal of a stopped enrolled DEV writer.
    Writer(SharedWriterCommand),
}
