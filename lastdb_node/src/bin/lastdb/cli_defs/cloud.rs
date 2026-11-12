//! Explicit byte-backup and credentials-only recovery-home commands.
use super::*;

pub(crate) enum CloudCommand {
    BackupV2 {
        action: ByteBackupCommand,
    },
    RecoverCredentialsHome(RecoverCredentialsArgs),
    RecoverDevCredentialsHome {
        into: PathBuf,
        expected_user_hash: String,
        identity_seed_stdin: bool,
        json: bool,
    },
    PrepareRecoveryHome(PrepareRecoveryArgs),
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
    #[arg(long)]
    pub(crate) invite_code_file: Option<PathBuf>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct PrepareRecoveryArgs {
    #[arg(long)]
    pub(crate) into: PathBuf,
    #[arg(long, value_parser = ["dev", "prod"])]
    pub(crate) env: String,
    #[arg(long, required = true)]
    pub(crate) api_key_stdin: bool,
    #[arg(long)]
    pub(crate) json: bool,
}

pub(crate) enum ByteBackupCommand {
    Init(BackupInitArgs),
    Connect(BackupConnectArgs),
    Start(BackupStartArgs),
    Resume(BackupJsonArgs),
    Status(BackupJsonArgs),
    Close(BackupJsonArgs),
    EnableAutomatic(BackupEnableArgs),
    DisableAutomatic(BackupJsonArgs),
    Discover(BackupListArgs),
    InspectDependencies(BackupDependenciesArgs),
    Restore(BackupRestoreArgs),
}

#[derive(Args, Debug)]
pub(crate) struct BackupInitArgs {
    #[arg(long)]
    pub(crate) into: PathBuf,
    #[arg(long)]
    pub(crate) recovery_phrase_out: PathBuf,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct BackupConnectArgs {
    #[arg(
        long,
        required_unless_present = "existing",
        conflicts_with = "existing"
    )]
    pub(crate) dev: bool,
    #[arg(long, conflicts_with_all = ["dev", "invite_code_stdin", "restored"], requires_all = ["env", "api_key_stdin"])]
    pub(crate) existing: bool,
    #[arg(long, value_parser = ["dev", "prod"], requires = "existing")]
    pub(crate) env: Option<String>,
    #[arg(long, requires = "existing")]
    pub(crate) api_key_stdin: bool,
    #[arg(long, requires = "existing")]
    pub(crate) expected_user_hash: Option<String>,
    #[arg(long)]
    pub(crate) invite_code_stdin: bool,
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
    #[arg(long, default_value_t = 60)]
    pub(crate) log_interval_seconds: u64,
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
    /// Recover an owned database from the signed cloud log.
    #[arg(long, conflicts_with_all = ["resume", "commit", "commit_sha256"])]
    pub(crate) signed: bool,
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
