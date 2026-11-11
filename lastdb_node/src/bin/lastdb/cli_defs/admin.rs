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
    /// Inspect live record storage by schema.
    Inventory {
        #[arg(long)]
        by_schema: bool,
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
    /// Read the durable record hard-delete audit trail, oldest first.
    DeleteLedger {
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
        #[arg(long, conflicts_with = "invite_code_file")]
        existing_dev: bool,
    },
    /// Prepare recovery credentials from an existing account without registering.
    PrepareRecoveryHome(PrepareRecoveryArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminSharedCommand {
    /// Upload a frozen DEV database snapshot, then upload the writer's queued changes.
    Snapshot(SnapshotArgs),
    /// Restore a held shared snapshot and its captured tails into a fresh home.
    Restore(SharedRestoreCommand),
    /// Resume publication of a sealed shared restore workspace.
    ResumeRestore(ResumeSharedRestoreArgs),
    /// Operate the journal of a stopped enrolled DEV writer.
    Writer(SharedWriterCommand),
}

#[derive(Subcommand, Debug)]
pub(crate) enum SchemaInspectCommand {
    /// Print identity, key fields, field list, and legal query shapes.
    Show {
        name: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum SchemaRetentionCommand {
    /// Read the current retention policy for one installed schema.
    Get {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        json: bool,
    },
    /// Set an operator retention TTL (`15m`, `24h`, `7d`).
    Set {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        ttl: String,
        #[arg(long = "hash")]
        hash_partitions: Vec<String>,
        #[arg(long)]
        json: bool,
    },
    /// Track existing personal records under the current TTL in durable pages.
    Backfill {
        #[arg(long)]
        schema: String,
        #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u16).range(1..=64))]
        page_size: u16,
        #[arg(long)]
        confirm_expire_existing: bool,
        #[arg(long)]
        json: bool,
    },
    /// Clear the current operator retention policy for one installed schema.
    Clear {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        confirm_expire_existing: bool,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Args, Debug)]
pub(crate) struct SchemaDropArgs {
    #[arg(long)]
    pub(crate) schema: Option<String>,
    #[arg(long)]
    pub(crate) owner_app: Option<String>,
    #[arg(long)]
    pub(crate) must_exist: bool,
    #[arg(long)]
    pub(crate) json: bool,
}
