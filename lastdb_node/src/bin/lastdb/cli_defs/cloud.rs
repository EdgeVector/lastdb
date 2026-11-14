//! Explicit cloud connection, payment, developer-key, and signed restore commands.
use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum CloudAccountCommand {
    /// Inspect or manage the pseudonymous Stripe subscription for this home.
    #[command(subcommand)]
    Payment(CloudPaymentCommand),
    /// Create, list, or revoke developer API keys for this home's cloud account.
    #[command(subcommand)]
    DeveloperKey(CloudDeveloperKeyCommand),
}

#[derive(Subcommand, Debug)]
pub(crate) enum CloudPaymentCommand {
    /// Query current cloud subscription status without exposing customer details.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Create or resume a hosted Stripe Checkout session for this home.
    Checkout {
        #[arg(long)]
        json: bool,
    },
    /// Open a hosted Stripe Billing Portal session for this home's subscription.
    Portal {
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum CloudDeveloperKeyCommand {
    /// Mint a new developer API key (requires an active paid cloud subscription).
    Create {
        /// Save the key atomically to `<home>/developer-api-key` (0600) and omit the raw key from stdout.
        #[arg(long)]
        save: bool,
        #[arg(long)]
        json: bool,
    },
    /// List developer API keys for this cloud account (excludes device sync keys).
    List {
        #[arg(long)]
        json: bool,
    },
    /// Revoke an active developer API key by its `em_<10 hex>...` prefix.
    Revoke {
        prefix: String,
        #[arg(long)]
        json: bool,
    },
}

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
    Restore(BackupRestoreArgs),
    Snapshot { database: String, wait: bool },
}

#[derive(Args, Debug)]
pub(crate) struct BackupInitArgs {
    #[arg(long)]
    pub(crate) into: PathBuf,
    #[arg(long)]
    pub(crate) recovery_phrase_out: PathBuf,
    #[arg(long, default_value = "dev", value_parser = ["dev", "prod"])]
    pub(crate) env: String,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct BackupConnectArgs {
    /// Shorthand for `--env dev`.
    #[arg(
        long,
        required_unless_present = "env",
        conflicts_with_all = ["env", "existing"]
    )]
    pub(crate) dev: bool,
    #[arg(long, conflicts_with_all = ["dev", "invite_code_stdin"], requires_all = ["env", "api_key_stdin"])]
    pub(crate) existing: bool,
    #[arg(
        long,
        value_parser = ["dev", "prod"],
        required_unless_present = "dev",
        conflicts_with = "dev"
    )]
    pub(crate) env: Option<String>,
    #[arg(long, requires = "existing")]
    pub(crate) api_key_stdin: bool,
    #[arg(long, requires = "existing")]
    pub(crate) expected_user_hash: Option<String>,
    #[arg(long)]
    pub(crate) invite_code_stdin: bool,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct BackupRestoreArgs {
    /// Recover an owned database from the signed cloud log.
    #[arg(long, required = true)]
    pub(crate) recovery_phrase_stdin: bool,
    /// Stable database id. `--stream` is the same flag.
    #[arg(long, visible_alias = "stream")]
    pub(crate) database: String,
    #[arg(long)]
    pub(crate) generation: Option<u64>,
    #[arg(long)]
    pub(crate) into: PathBuf,
    #[arg(long)]
    pub(crate) json: bool,
}
