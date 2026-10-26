//! Explicit byte-backup and paused credential commands.
use super::*;

#[path = "cloud/byte_backup.rs"]
mod byte_backup;
pub(crate) use byte_backup::ByteBackupCommand;

#[derive(Subcommand, Debug)]
pub(crate) enum CloudCommand {
    /// Explicit V2 byte backup and source-free restore.
    BackupV2 {
        #[command(subcommand)]
        action: ByteBackupCommand,
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
        /// Store the minted key through the trusted local LastSecrets stdin route.
        /// Require lastdb-dev-recovery-api-key-<expected-user-hash>-<UUIDv4>.
        #[arg(long)]
        api_key_lastsecrets_slug: Option<String>,
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
    /// Pause legacy credentials locally. This does not cancel an explicit backup.
    #[command(alias = "pause")]
    Off {
        #[arg(long)]
        json: bool,
    },
}
