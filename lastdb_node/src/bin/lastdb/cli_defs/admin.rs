use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AdminCommand {
    /// Diagnose request and mutation latency.
    Ops(OpsArgs),
    /// Check local liveness and degradation, with optional notification.
    AlertCheck(AlertCheckArgs),
    /// Read or change the live tracing filter until the next daemon restart.
    LogFilter {
        directive: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Manage the configured service home.
    #[command(subcommand)]
    ServiceHome(ServiceHomeCommand),
    /// Drop installed schemas or manage descriptive-name claims.
    #[command(subcommand)]
    Schema(AdminSchemaCommand),
    /// Manage schema record expiration.
    #[command(subcommand)]
    Retention(SchemaRetentionCommand),
    /// Inspect storage, reclaim deleted space or raise a stopped home's replay floor.
    #[command(subcommand)]
    Storage(AdminStorageCommand),
    /// Rebuild the Search inbox from a stopped home.
    SearchRebuild {
        #[arg(long, default_value_t = 32)]
        page_size: usize,
        #[arg(long)]
        json: bool,
    },
    /// Prepare recovery credentials and operate on exact backup jobs.
    #[command(subcommand)]
    Backup(AdminBackupCommand),
    /// Read payment status or open Stripe-hosted account actions.
    Payments {
        #[command(subcommand)]
        action: CloudPaymentCommand,
        #[arg(long, global = true)]
        json: bool,
    },
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
    /// Retire or restore a schema's descriptive-name claim.
    #[command(subcommand)]
    NameClaim(SchemaNameClaimCommand),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminStorageCommand {
    /// Inspect live storage. This may walk a large part of the store.
    Inventory {
        /// Walk only live records and report their sizes by schema.
        #[arg(long)]
        by_schema: bool,
        /// Limit human output for the schema-only view.
        #[arg(long, requires = "by_schema")]
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
    /// Preview or raise the replay cutoff on a cleanly stopped home.
    ReplayCutoff(ReplayCutoffArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminBackupCommand {
    /// Create one fresh DEV identity without starting a daemon.
    Init(BackupInitArgs),
    /// Back up the exact last flush of a cleanly stopped home.
    StartStopped(BackupStartStoppedArgs),
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
    /// Verify, stage and optionally read an encrypted snapshot using a guest permission.
    StageGuest(GuestStageArgs),
    /// Upload a frozen DEV database snapshot, then upload the writer’s queued changes.
    Snapshot(SnapshotArgs),
    /// Restore a held shared snapshot and its captured tails into a fresh home.
    Restore(SharedRestoreCommand),
    /// Resume publication of a sealed shared restore workspace.
    ResumeRestore(ResumeSharedRestoreArgs),
    /// Operate the journal of a stopped enrolled DEV writer.
    Writer(SharedWriterCommand),
}

#[derive(Args, Debug)]
pub(crate) struct GuestStageArgs {
    /// Read trusted JSON from stdin with authorization, publisher_public_key,
    /// and content_key. The keys are 32-byte hex strings. Authorization is the
    /// complete LastDBShare header value. Use a publication-specific content key.
    #[arg(long, required = true)]
    pub permission_stdin: bool,
    #[command(flatten)]
    pub selection: GuestStageSelection,
}

#[derive(Args, Debug)]
#[command(group(clap::ArgGroup::new("guest_target").required(true).args(["into", "resume"])))]
pub(crate) struct GuestStageSelection {
    /// Absent absolute path. A private workspace is created next to it.
    #[arg(long)]
    pub into: Option<PathBuf>,
    /// Absolute path of a private workspace. Resume uses its exact saved point.
    #[arg(long)]
    pub resume: Option<PathBuf>,
    /// Emit the verified stage receipt as JSON.
    #[arg(long)]
    pub json: bool,
    /// Exact schema name in the staged snapshot.
    #[arg(long, requires_all = ["json", "key_hash"])]
    pub schema: Option<String>,
    /// Exact primary hash key of the staged record.
    #[arg(long, requires = "schema")]
    pub key_hash: Option<String>,
    /// Exact primary range key for a HashRange schema.
    #[arg(long, requires = "schema")]
    pub key_range: Option<String>,
    /// Include the record's inline file as base64 with its marker.
    #[arg(long = "inline", requires = "schema")]
    pub inline_file: bool,
}
