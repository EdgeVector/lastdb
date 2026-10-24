use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum ByteBackupCommand {
    /// Upload from an existing home without a daemon or a local database writer.
    Standalone {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        credentials_home: PathBuf,
        #[arg(long, value_parser = ["dev", "prod"])]
        env: String,
        #[command(subcommand)]
        action: ByteStandaloneCommand,
    },
    /// Read the exact stopped native boundary without state or cloud requests.
    Boundary {
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

#[derive(Subcommand, Debug)]
pub(crate) enum ByteStandaloneCommand {
    /// Freeze and upload this request, or resume the same saved request.
    Start {
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Resume the selected request with its original cut and transfer IDs.
    Resume {
        #[arg(long)]
        json: bool,
    },
    /// Read external local state only.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Close the selected request and retain uncertain cloud obligations.
    Close {
        #[arg(long)]
        json: bool,
    },
}
