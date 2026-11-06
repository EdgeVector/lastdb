use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum BackupCommand {
    /// Read backup policy, job state and last acceptance from the owner daemon.
    Status(BackupJsonArgs),
    /// Connect a prepared account without enabling automatic backup.
    Connect(BackupConnectArgs),
    /// Enable or disable automatic backup explicitly.
    Policy(BackupPolicyArgs),
    /// Start one manual backup on the owner daemon.
    Start(BackupStartArgs),
    /// List accepted stream heads. Prepare credentials with `admin backup` first.
    List(BackupListArgs),
    /// Restore an accepted cut into a fresh home, or resume a saved workspace.
    ///
    /// Prepare credentials with `admin backup recover-credentials-home` or
    /// `prepare-recovery-home`, then select a cut from `backup list`.
    Restore(BackupRestoreSelection),
}

impl BackupCommand {
    pub(crate) fn into_cloud(self) -> Result<CloudCommand, String> {
        let action = match self {
            Self::Status(args) => ByteBackupCommand::Status(args),
            Self::Connect(args) => ByteBackupCommand::Connect(args),
            Self::Policy(args) => args.into_action()?,
            Self::Start(args) => ByteBackupCommand::Start(args),
            Self::List(args) => ByteBackupCommand::Discover(args),
            Self::Restore(selection) => ByteBackupCommand::Restore(selection.validate()?),
        };
        Ok(CloudCommand::BackupV2 { action })
    }
}

#[derive(Args, Debug)]
pub(crate) struct BackupRestoreSelection {
    #[command(flatten)]
    args: BackupRestoreArgs,
    /// Select the current accepted head once, before staging the restore.
    #[arg(long, conflicts_with_all = ["resume", "commit", "commit_sha256"])]
    latest: bool,
}

impl BackupRestoreSelection {
    fn validate(self) -> Result<BackupRestoreArgs, String> {
        let args = self.args;
        if args.resume.is_none()
            && !self.latest
            && (args.generation.is_none() || args.commit.is_none() || args.commit_sha256.is_none())
        {
            return Err(
                "select --generation, --commit and --commit-sha256, or use --latest".into(),
            );
        }
        Ok(args)
    }
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum PolicyMode {
    On,
    Off,
}

#[derive(Args, Debug)]
pub(crate) struct BackupPolicyArgs {
    #[arg(value_enum)]
    mode: PolicyMode,
    /// Time between log checks while enabled (default: 60 seconds).
    #[arg(long)]
    log_interval_seconds: Option<u64>,
    /// Time between full snapshots while enabled (default: 86400 seconds).
    #[arg(long)]
    snapshot_interval_seconds: Option<u64>,
    #[arg(long)]
    json: bool,
}

impl BackupPolicyArgs {
    fn into_action(self) -> Result<ByteBackupCommand, String> {
        match self.mode {
            PolicyMode::On => Ok(ByteBackupCommand::EnableAutomatic(BackupEnableArgs {
                log_interval_seconds: self.log_interval_seconds.unwrap_or(60),
                snapshot_interval_seconds: self.snapshot_interval_seconds.unwrap_or(86400),
                json: self.json,
            })),
            PolicyMode::Off => {
                if self.log_interval_seconds.is_some() || self.snapshot_interval_seconds.is_some() {
                    return Err("interval options require backup policy on".into());
                }
                Ok(ByteBackupCommand::DisableAutomatic(BackupJsonArgs {
                    json: self.json,
                }))
            }
        }
    }
}
