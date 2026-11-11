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

#[derive(Subcommand, Debug)]
pub(crate) enum SchemaInspectCommand {
    /// Print identity, key fields, field list, and legal query shapes.
    Show {
        /// Schema identity hash, `app/Name`, or descriptive name.
        name: String,
        /// Emit the raw `schema` object.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum SchemaRetentionCommand {
    /// Read the current policy for one installed schema.
    ///
    /// Prints the TTL and its source: `manifest` (declared by an app manifest)
    /// or `operator` (set with `set`).
    Get {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        json: bool,
    },
    /// Set a positive human-readable duration such as `15m`, `24h`, or `7d`.
    ///
    /// A policy deletes records for good. Range and HashRange records expire
    /// by the entry time at the front of the range key: unix seconds,
    /// zero-padded to 20 digits. A range key without that prefix never
    /// expires. A Range schema loses every old record at the next sweep, those
    /// written before the policy included. A HashRange schema does the same in
    /// each partition that receives a later write. Hash and Single records
    /// expire by the time since the last write after the policy is set; older
    /// records are not tracked.
    ///
    /// This sets an OPERATOR policy. It wins over a TTL that an app manifest
    /// declared (`retention` key) until `clear` removes it. A schema declared
    /// from a manifest with `retention` has two installed names that share one
    /// policy: setting it through either name covers both. For any other
    /// schema the policy covers only the schema name given here. Every change
    /// writes a row to `<home>/logs/retention-audit.jsonl`.
    Set {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        ttl: String,
        /// HashRange partitions to cover that have no write since the policy (repeatable).
        #[arg(long = "hash")]
        hash_partitions: Vec<String>,
        #[arg(long)]
        json: bool,
    },
    /// Track existing personal records under the current TTL, in durable pages.
    /// The original write times stay intact. Old records can expire at once.
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
    /// Clear the current policy for one installed schema.
    ///
    /// An operator policy that overrides a manifest policy is removed and the
    /// manifest policy is back in force. When the manifest TTL is shorter, the
    /// clear needs `--confirm-expire-existing`: records between the two TTLs
    /// expire for good. Any other policy is removed with its age tracking and
    /// the manifest-name aliases; a manifest policy then returns at the next
    /// declare.
    Clear {
        #[arg(long)]
        schema: String,
        /// Confirm that clearing puts a SHORTER manifest TTL in force, so that
        /// records between the two TTLs expire for good.
        #[arg(long)]
        confirm_expire_existing: bool,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Args, Debug)]
pub(crate) struct SchemaDropArgs {
    /// Canonical name or identity hash of one installed schema.
    #[arg(long)]
    pub(crate) schema: Option<String>,
    /// Drop every installed identity owned by this app id.
    #[arg(long)]
    pub(crate) owner_app: Option<String>,
    /// Refuse if nothing was dropped.
    #[arg(long)]
    pub(crate) must_exist: bool,
    #[arg(long)]
    pub(crate) json: bool,
}
