//! Explicit byte-backup and credentials-only recovery-home commands.
use super::*;

#[path = "cloud/byte_backup.rs"]
mod byte_backup;
pub(crate) use byte_backup::ByteBackupCommand;

#[derive(clap::Args, Debug)]
pub(crate) struct SnapshotArgs {
    /// Registered shared writer device ID for the running DEV home.
    #[arg(long)]
    pub device_id: String,
    /// Absolute owner-only path to the enrolled device's signing key.
    #[arg(long)]
    pub device_key_file: PathBuf,
    /// Absolute private workspace. Reuse it to resume the same snapshot.
    #[arg(long)]
    pub workspace: PathBuf,
    /// Print the snapshot and uploaded writer position as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Subcommand, Debug)]
pub(crate) enum CloudCommand {
    /// Operate the shared journal and snapshots of an explicit stopped DEV home.
    SharedWriter(SharedWriterCommand),
    /// Read or manage Stripe payments for this home's existing native account.
    /// Requires an explicit absolute --data-dir; never starts a daemon or store.
    Payments {
        #[command(subcommand)]
        action: CloudPaymentCommand,
        #[arg(long, global = true)]
        json: bool,
    },
    /// Explicit V2 byte backup and source-free restore.
    BackupV2 {
        #[command(subcommand)]
        action: ByteBackupCommand,
    },
    /// Restore a held shared snapshot and its captured tails into a fresh home.
    RestoreShared(SharedRestoreCommand),
    /// Complete a sealed shared restore after an interrupted directory publication.
    ResumeSharedRestore {
        #[arg(long)]
        workspace: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Register a credentials-only device from stdin; never open a source home.
    RecoverCredentialsHome {
        #[arg(long)]
        into: PathBuf,
        #[arg(long, value_parser = ["dev", "prod"])]
        env: String,
        #[arg(long)]
        expected_user_hash: String,
        #[arg(long, required = true)]
        identity_seed_stdin: bool,
        /// Optional invite in a private owner file; stdin is reserved for the seed.
        #[arg(long)]
        invite_code_file: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Recover credentials for an existing DEV account from a protected seed on stdin.
    /// Registers one fresh device; creates no store and keeps Cloud Sync Off.
    RecoverDevCredentialsHome {
        /// Absent child of an empty owner-only lastdb-dev-recovery.* parent.
        /// Create the parent under /private/tmp on macOS, or /tmp elsewhere.
        /// The fixed root does not use TMPDIR or HOME overrides.
        #[arg(long)]
        into: PathBuf,
        /// Require this exact public account hash before any target or network change.
        #[arg(long)]
        expected_user_hash: String,
        /// Read one base64-encoded 32-byte identity seed from stdin.
        #[arg(long, required = true)]
        identity_seed_stdin: bool,
        /// Emit a receipt without credential values.
        #[arg(long)]
        json: bool,
    },
    /// Prepare a source-free recovery home from this home's existing credentials.
    /// Reads credentials only; no daemon, store, registration, or cloud calls.
    PrepareRecoveryHome {
        /// Absent, absolute destination home; its parent must already exist.
        #[arg(long)]
        into: PathBuf,
        /// Require the saved URL to equal the compiled endpoint for this environment.
        #[arg(long, value_parser = ["dev", "prod"])]
        env: String,
        /// Read the existing account API key from stdin; must match the saved key.
        #[arg(long, required = true)]
        api_key_stdin: bool,
        /// Emit a value-free receipt.
        #[arg(long)]
        json: bool,
    },
}

#[derive(clap::Args, Debug)]
pub(crate) struct SharedWriterCommand {
    /// Registered device ID of the explicit DEV credential home.
    #[arg(long)]
    pub device_id: String,
    /// Absolute owner-only file holding this registered device's 32-byte key.
    #[arg(long)]
    pub device_key_file: PathBuf,
    /// Read the recovery phrase only from bounded stdin.
    #[arg(long, required = true)]
    pub recovery_phrase_stdin: bool,
    #[command(subcommand)]
    pub action: SharedWriterAction,
}

#[derive(Subcommand, Debug)]
pub(crate) enum SharedWriterAction {
    /// Publish one new typed DEV catalog declaration and pin its immutable identity.
    PublishSchema {
        #[arg(long)]
        namespace: String,
        #[arg(long)]
        declaration: PathBuf,
    },
    /// Install the exact immutable service definitions at their local aliases.
    Install {
        #[arg(long)]
        schema_bindings: PathBuf,
    },
    /// Bind a fresh writer to immutable schema IDs in a private JSON file.
    Bind {
        #[arg(long)]
        database_id: String,
        #[arg(long)]
        source_stream: String,
        #[arg(long)]
        schema_bindings: PathBuf,
    },
    /// Upload a bounded number of journal batches, stopping when caught up.
    Push {
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..=32))]
        batches: u16,
    },
    /// Fetch and apply a bounded number of verified peer ranges.
    Pull {
        #[arg(long)]
        schema_bindings: PathBuf,
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..=32))]
        ranges: u16,
    },
    /// Capture and publish one shared checkpoint under a fresh private path.
    Publish {
        #[arg(long)]
        workspace: PathBuf,
        #[arg(long)]
        snapshot_id: String,
        #[arg(long)]
        schema_bindings: PathBuf,
        #[arg(long, default_value_t = 900, value_parser = clap::value_parser!(u32).range(1..=3600))]
        deadline_seconds: u32,
        /// Save the cut and release the source before a separate resume publishes it.
        #[arg(long)]
        prepare_only: bool,
    },
    /// Resume the exact prior publication from its private workspace.
    Resume {
        #[arg(long)]
        workspace: PathBuf,
    },
}

#[derive(clap::Args, Debug)]
pub(crate) struct SharedRestoreCommand {
    /// Shared database to recover.
    #[arg(long)]
    pub database_id: String,
    /// Refuse unless the recovery hold selects this exact snapshot.
    #[arg(long)]
    pub snapshot_id: Option<String>,
    /// Registered device ID for the explicit credential home.
    #[arg(long)]
    pub device_id: String,
    /// Absolute owner-only file containing this device's 32-byte signing seed.
    #[arg(long)]
    pub device_key_file: PathBuf,
    /// Require the saved credentials to match this compiled cloud environment.
    #[arg(long, value_parser = ["dev", "prod"])]
    pub env: String,
    /// Fresh, absolute destination home. The parent must already exist.
    #[arg(long)]
    pub into: PathBuf,
    /// Read the recovery phrase only from bounded stdin.
    #[arg(long, required = true)]
    pub recovery_phrase_stdin: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Subcommand, Clone, Copy, Debug)]
pub(crate) enum CloudPaymentCommand {
    /// Read payment status through the saved native account API key.
    Status,
    /// Print a Stripe-hosted Checkout URL for the same native account.
    Checkout,
    /// Print a Stripe-hosted customer portal URL for the same native account.
    Portal,
}
