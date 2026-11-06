use super::*;

pub(crate) enum ByteBackupCommand {
    /// Create one fresh DEV identity without a daemon, store, or cloud request.
    Init(BackupInitArgs),
    /// Connect explicit backup credentials; keep automatic backup and Cloud Sync Off.
    Connect(BackupConnectArgs),

    /// Save one manual V2 backup on the owner daemon. Automatic backup stays off.
    Start(BackupStartArgs),
    /// Back up the final flushed cut without a daemon, copy, or database boot.
    /// Automatic backup can stay on. A saved automatic job must finish first.
    StartStopped(BackupStartStoppedArgs),
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
pub(crate) struct BackupStartStoppedArgs {
    /// Preserve this UUID across retries of the same stopped session.
    #[arg(long)]
    pub(crate) request_id: String,
    #[arg(long)]
    pub(crate) expected_user_hash: String,
    /// Exact PID from the clean session and its shutdown flush receipt.
    #[arg(long)]
    pub(crate) stopped_pid: u32,
    #[arg(long)]
    pub(crate) stopped_start_ts: u64,
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
    /// Time between log checks, in seconds (60 through 604800).
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
