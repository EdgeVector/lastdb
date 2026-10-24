use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum NativeBackupCommand {
    /// Set or read explicit automatic backup consent for this home.
    Auto {
        #[command(subcommand)]
        action: NativeBackupAutoCommand,
    },
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
    /// Register this fresh identity for native backup; keep Cloud Sync Off.
    Connect {
        /// Use only the compiled DEV auth endpoint.
        #[arg(long, required = true)]
        dev: bool,
        /// Read the first-account invite from bounded stdin, never from an argument.
        #[arg(long)]
        invite_code_stdin: bool,
        #[arg(long)]
        json: bool,
    },
    /// Save one durable backup request on the owner daemon.
    Start {
        /// Exact UUID to reuse after a lost response. Omission creates one before submission.
        #[arg(long)]
        request_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Resume the exact saved job without a new capture.
    Resume {
        #[arg(long)]
        json: bool,
    },
    /// Read local status without cloud work or feature activation.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Close the exact saved job and retain unknown byte obligations.
    Close {
        #[arg(long)]
        json: bool,
    },
    /// Read one bounded page of personal backup streams and their accepted heads.
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
    /// Restore one exact stream into a fresh home, or resume a saved selection.
    Restore {
        #[arg(long, required = true)]
        recovery_phrase_stdin: bool,
        #[arg(long, required_unless_present = "resume", conflicts_with = "resume")]
        stream: Option<String>,
        #[arg(long, required_unless_present = "resume", conflicts_with = "resume")]
        source_home: Option<String>,
        #[arg(long, conflicts_with = "resume")]
        generation: Option<u64>,
        #[arg(long, conflicts_with = "resume")]
        commit: Option<String>,
        #[arg(long, required_unless_present = "resume", conflicts_with = "resume")]
        into: Option<PathBuf>,
        #[arg(long)]
        resume: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum NativeBackupAutoCommand {
    /// Permit automatic backup for the current account and native home.
    On {
        #[arg(long)]
        json: bool,
    },
    /// Stop future automatic work without closing the saved backup job.
    Off {
        #[arg(long)]
        json: bool,
    },
    /// Read local automatic backup state without a cloud request.
    Status {
        #[arg(long)]
        json: bool,
    },
}
