use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum ByteBackupCommand {
    /// Create one fresh DEV identity without a daemon, store, or cloud request.
    Init {
        /// Absent absolute home; its real parent must already exist.
        #[arg(long)]
        into: PathBuf,
        /// New protected file at <into>/recovery-phrase.txt; never print the phrase.
        #[arg(long)]
        recovery_phrase_out: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Connect explicit backup credentials; keep automatic backup and Cloud Sync Off.
    Connect {
        /// Use only the compiled DEV auth endpoint.
        #[arg(
            long,
            required_unless_present = "existing",
            conflicts_with = "existing"
        )]
        dev: bool,
        /// Import a verified API key on the current owner daemon without enrollment.
        #[arg(long, conflicts_with_all = ["dev", "invite_code_stdin", "restored"], requires_all = ["env", "api_key_stdin"])]
        existing: bool,
        #[arg(long, value_parser = ["dev", "prod"], requires = "existing")]
        env: Option<String>,
        /// Read the existing account API key only from bounded stdin.
        #[arg(long, requires = "existing")]
        api_key_stdin: bool,
        /// Refuse a daemon whose existing identity differs from this account.
        #[arg(long, requires = "existing")]
        expected_user_hash: Option<String>,
        /// Read the first-account invite from bounded stdin, never from an argument.
        #[arg(long)]
        invite_code_stdin: bool,
        /// Reconnect an inactive native restore without changing its identity or data.
        /// Existing accounts need no new invitation. Backup remains off.
        #[arg(long, conflicts_with = "invite_code_stdin")]
        restored: bool,
        #[arg(long)]
        json: bool,
    },

    /// Save one manual V2 backup on the owner daemon. Automatic backup stays off.
    Start {
        #[arg(long)]
        request_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Back up the final flushed cut without a daemon, copy, or database boot.
    StartStopped {
        /// Preserve this UUID across retries of the same stopped session.
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        expected_user_hash: String,
        /// Exact PID from the clean session and its shutdown flush receipt.
        #[arg(long)]
        stopped_pid: u32,
        #[arg(long)]
        stopped_start_ts: u64,
        #[arg(long)]
        json: bool,
    },
    /// Resume the exact saved capture and transfer identities.
    Resume {
        #[arg(long)]
        json: bool,
    },
    /// Read local V2 status without a cloud request or activation.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Fence the saved job while retaining uncertain cloud byte obligations.
    Close {
        #[arg(long)]
        json: bool,
    },
    /// Enable native mutation-log updates and independent periodic snapshots.
    EnableAutomatic {
        /// Time between log checks, in seconds (60 through 604800).
        #[arg(long, default_value_t = 60)]
        log_interval_seconds: u64,
        /// Time between full snapshots, in seconds (60 through 604800).
        #[arg(long, default_value_t = 86400)]
        snapshot_interval_seconds: u64,
        #[arg(long)]
        json: bool,
    },
    /// Stop new automatic requests and retries. Active work can finish safely.
    DisableAutomatic {
        #[arg(long)]
        json: bool,
    },
    /// Read one account-scoped page of V2 streams and accepted heads.
    Discover {
        #[arg(long, required = true)]
        recovery_phrase_stdin: bool,
        #[arg(long)]
        generation: Option<u64>,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Restore one frozen V2 accepted backup into an absent home.
    Restore {
        #[arg(long, required = true)]
        recovery_phrase_stdin: bool,
        #[arg(long, required_unless_present = "resume", conflicts_with = "resume")]
        stream: Option<String>,
        #[arg(long, conflicts_with = "resume")]
        generation: Option<u64>,
        #[arg(long, conflicts_with = "resume")]
        commit: Option<String>,
        #[arg(long, conflicts_with = "resume")]
        commit_sha256: Option<String>,
        #[arg(long, required_unless_present = "resume", conflicts_with = "resume")]
        into: Option<PathBuf>,
        #[arg(long)]
        resume: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
}
