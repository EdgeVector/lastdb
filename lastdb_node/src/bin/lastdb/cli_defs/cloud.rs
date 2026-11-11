//! Explicit byte-backup and credentials-only recovery-home commands.
use super::*;

#[derive(clap::Args, Debug)]
pub(crate) struct SnapshotArgs {
    /// Registered shared writer device ID for the running DEV home.
    ///
    /// Create the home with LASTDB_NATIVE_SHARED_SCHEMAS set to a JSON array
    /// of its shared schema aliases on first start.
    #[arg(long)]
    pub device_id: String,
    /// Absolute owner-only path to the enrolled device's signing key.
    #[arg(long)]
    pub device_key_file: PathBuf,
    /// Absolute private workspace. Reuse it to resume the same snapshot.
    #[arg(long)]
    pub workspace: PathBuf,
    /// Print the snapshot, provider file counts, and uploaded writer position as JSON.
    #[arg(long)]
    pub json: bool,
}

pub(crate) enum CloudCommand {
    /// Operate the shared journal of an explicit stopped DEV home.
    SharedWriter(SharedWriterCommand),
    /// Explicit V2 byte backup and source-free restore.
    BackupV2 { action: ByteBackupCommand },
    /// Restore a held shared snapshot and its captured tails into a fresh home.
    RestoreShared(SharedRestoreCommand),
    /// Complete a sealed shared restore after an interrupted directory publication.
    ResumeSharedRestore(ResumeSharedRestoreArgs),
    /// Register a credentials-only device from stdin; never open a source home.
    RecoverCredentialsHome(RecoverCredentialsArgs),
    /// Recover credentials for an existing DEV account from a protected seed on stdin.
    /// Registers one fresh device; creates no store and keeps Cloud Sync Off.
    RecoverDevCredentialsHome {
        /// Absent child of an empty owner-only lastdb-dev-recovery.* parent.
        /// Create the parent under /private/tmp on macOS, or /tmp elsewhere.
        /// The fixed root does not use TMPDIR or HOME overrides.
        into: PathBuf,
        /// Require this exact public account hash before any target or network change.
        expected_user_hash: String,
        /// Read one base64-encoded 32-byte identity seed from stdin.
        identity_seed_stdin: bool,
        /// Emit a receipt without credential values.
        json: bool,
    },
    /// Prepare a source-free recovery home from this home's existing credentials.
    /// Reads credentials only; no daemon, store, registration, or cloud calls.
    PrepareRecoveryHome(PrepareRecoveryArgs),
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
    /// Run one bounded staging operation on the generated DEV qualification scope.
    #[command(hide = true)]
    QualifyStaging {
        #[arg(long)]
        plan: PathBuf,
    },
    /// Print the definition of one typed declaration under its portable ID.
    /// No service registers it. The writer log carries it to other writers.
    DefineSchema {
        #[arg(long)]
        namespace: String,
        #[arg(long)]
        declaration: PathBuf,
    },
    /// Install verified definitions from a private JSON file at their local aliases.
    Install {
        #[arg(long)]
        schema_bindings: PathBuf,
    },
    /// Bind a restored writer to the verified definitions in a private JSON file.
    Bind {
        /// Start an empty home as the designated publisher before the first snapshot.
        #[arg(long)]
        initialize_publisher: bool,
        #[arg(long)]
        database_id: String,
        #[arg(long)]
        source_stream: String,
        #[arg(long)]
        schema_bindings: PathBuf,
    },
    /// Admit later schemas to a bound writer. Each alias is its canonical name.
    Admit {
        #[arg(long)]
        schema_bindings: PathBuf,
    },
    /// Upload a bounded number of journal batches, stopping when caught up.
    Push {
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..=32))]
        batches: u16,
    },
    /// Upload pending mutations and replace a stopped writer's stale shared data.
    Rejoin {
        /// Maximum pending batches to drain before staging the replacement.
        #[arg(long, default_value_t = 32, value_parser = clap::value_parser!(u16).range(1..=32))]
        batches: u16,
    },
    /// Fetch and apply a bounded number of verified peer ranges.
    Pull {
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..=32))]
        ranges: u16,
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

#[derive(Args, Debug)]
pub(crate) struct RecoverCredentialsArgs {
    #[arg(long)]
    pub(crate) into: PathBuf,
    #[arg(long, value_parser = ["dev", "prod"])]
    pub(crate) env: String,
    #[arg(long)]
    pub(crate) expected_user_hash: String,
    #[arg(long, required = true)]
    pub(crate) identity_seed_stdin: bool,
    /// Optional invite in a private owner file; stdin is reserved for the seed.
    #[arg(long)]
    pub(crate) invite_code_file: Option<PathBuf>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct PrepareRecoveryArgs {
    /// Absent, absolute destination home; its parent must already exist.
    #[arg(long)]
    pub(crate) into: PathBuf,
    /// Require the saved URL to equal the compiled endpoint for this environment.
    #[arg(long, value_parser = ["dev", "prod"])]
    pub(crate) env: String,
    /// Read the existing account API key from stdin; must match the saved key.
    #[arg(long, required = true)]
    pub(crate) api_key_stdin: bool,
    /// Emit a value-free receipt.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct ResumeSharedRestoreArgs {
    #[arg(long)]
    pub(crate) workspace: PathBuf,
    #[arg(long)]
    pub(crate) json: bool,
}

pub(crate) enum ByteBackupCommand {
    /// Create one fresh DEV identity without a daemon, store, or cloud request.
    Init(BackupInitArgs),
    /// Connect explicit backup credentials; keep automatic backup and Cloud Sync Off.
    Connect(BackupConnectArgs),
    /// Save one manual V2 backup on the owner daemon. Automatic backup stays off.
    Start(BackupStartArgs),
    /// Resume the exact saved capture and transfer identities.
    Resume(BackupJsonArgs),
    /// Read local V2 status without a cloud request or activation.
    Status(BackupJsonArgs),
    /// Fence the saved job while retaining uncertain cloud byte obligations.
    Close(BackupJsonArgs),
    /// Enable native mutation-log updates and independent periodic snapshots.
    EnableAutomatic(BackupEnableArgs),
    /// Stop new automatic requests and retries. Active work can finish safely.
    DisableAutomatic(BackupJsonArgs),
    /// Read one account-scoped page of V2 streams and accepted heads.
    Discover(BackupListArgs),
    /// Inspect the exact accepted recovery dependency closure. Deletes nothing.
    InspectDependencies(BackupDependenciesArgs),
    /// Restore one frozen V2 accepted backup into an absent home.
    /// Requires saved cloud credentials and a recovery phrase. Starts no daemon.
    Restore(BackupRestoreArgs),
}

#[derive(Args, Debug)]
pub(crate) struct BackupInitArgs {
    /// Absent absolute home; its real parent must already exist.
    #[arg(long)]
    pub(crate) into: PathBuf,
    /// New protected file at <into>/recovery-phrase.txt; never print the phrase.
    #[arg(long)]
    pub(crate) recovery_phrase_out: PathBuf,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct BackupConnectArgs {
    /// Use only the compiled DEV auth endpoint.
    #[arg(
        long,
        required_unless_present = "existing",
        conflicts_with = "existing"
    )]
    pub(crate) dev: bool,
    /// Import a verified API key on the current owner daemon without enrollment.
    #[arg(long, conflicts_with_all = ["dev", "invite_code_stdin", "restored"], requires_all = ["env", "api_key_stdin"])]
    pub(crate) existing: bool,
    #[arg(long, value_parser = ["dev", "prod"], requires = "existing")]
    pub(crate) env: Option<String>,
    /// Read the existing account API key only from bounded stdin.
    #[arg(long, requires = "existing")]
    pub(crate) api_key_stdin: bool,
    /// Refuse a daemon whose existing identity differs from this account.
    #[arg(long, requires = "existing")]
    pub(crate) expected_user_hash: Option<String>,
    /// Read the first-account invite from bounded stdin, never from an argument.
    #[arg(long)]
    pub(crate) invite_code_stdin: bool,
    /// Reconnect an inactive native restore without changing its identity or data.
    /// Existing accounts need no new invitation. Backup remains off.
    #[arg(long, conflicts_with = "invite_code_stdin")]
    pub(crate) restored: bool,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct BackupStartArgs {
    #[arg(long)]
    pub(crate) request_id: Option<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct BackupJsonArgs {
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct BackupEnableArgs {
    /// Time between automatic upload and download checks, in seconds.
    #[arg(long, default_value_t = 60)]
    pub(crate) log_interval_seconds: u64,
    /// Time between full snapshots, in seconds (60 through 604800).
    #[arg(long, default_value_t = 86400)]
    pub(crate) snapshot_interval_seconds: u64,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct BackupListArgs {
    #[arg(long, required = true)]
    pub(crate) recovery_phrase_stdin: bool,
    #[arg(long)]
    pub(crate) generation: Option<u64>,
    #[arg(long)]
    pub(crate) cursor: Option<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct BackupDependenciesArgs {
    #[arg(long, required = true)]
    pub(crate) recovery_phrase_stdin: bool,
    #[arg(long)]
    pub(crate) stream: String,
    #[arg(long)]
    pub(crate) generation: Option<u64>,
    #[arg(long)]
    pub(crate) commit: String,
    #[arg(long)]
    pub(crate) commit_sha256: String,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct BackupRestoreArgs {
    #[arg(long, required = true)]
    pub(crate) recovery_phrase_stdin: bool,
    #[arg(long, required_unless_present = "resume", conflicts_with = "resume")]
    pub(crate) stream: Option<String>,
    #[arg(long, conflicts_with = "resume")]
    pub(crate) generation: Option<u64>,
    #[arg(long, conflicts_with = "resume")]
    pub(crate) commit: Option<String>,
    #[arg(long, conflicts_with = "resume")]
    pub(crate) commit_sha256: Option<String>,
    #[arg(long, required_unless_present = "resume", conflicts_with = "resume")]
    pub(crate) into: Option<PathBuf>,
    #[arg(long)]
    pub(crate) resume: Option<PathBuf>,
    #[arg(long)]
    pub(crate) json: bool,
}
