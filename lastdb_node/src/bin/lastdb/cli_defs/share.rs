use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum ShareCommand {
    /// Prepare an empty publication home with its own database identity.
    Create {
        /// Absent absolute path for the new publication home.
        #[arg(long)]
        into: PathBuf,
        /// Exemem environment of the publisher's account.
        #[arg(long, value_parser = ["dev", "prod"])]
        env: String,
        /// Whether this share accepts privately submitted contributor mutations.
        #[arg(long, default_value = "publisher-only", value_parser = ["publisher-only", "publisher-validated"])]
        write_mode: String,
    },
    /// Export this LastDB identity's public contact code.
    Contact {
        /// New path for the public contact file.
        #[arg(long)]
        out: PathBuf,
    },
    /// Create a recipient-encrypted invitation for a published database.
    Invite {
        /// Absolute path of the publication home.
        #[arg(long)]
        home: PathBuf,
        /// Contact file obtained from the intended recipient.
        #[arg(long)]
        recipient: PathBuf,
        /// Recipient's Ed25519 public key in lowercase hex from a trusted channel.
        #[arg(long)]
        recipient_key: String,
        /// New path for the encrypted invitation file.
        #[arg(long)]
        out: PathBuf,
        /// Lifetime of the read permission in hours.
        #[arg(long, default_value_t = 24, value_parser = clap::value_parser!(u32).range(1..=168))]
        expires_in_hours: u32,
    },
    /// Open an invitation and verify its current read-only snapshot.
    Accept {
        /// Encrypted invitation file.
        invitation: PathBuf,
        /// Publisher's Ed25519 public key in lowercase hex from a trusted channel.
        #[arg(long)]
        publisher: String,
        #[command(flatten)]
        selection: GuestStageSelection,
    },
    /// Read an exact record from a locally verified share snapshot.
    Get {
        #[command(flatten)]
        share: ShareFilesSelection,
        /// Canonical schema name in the share.
        #[arg(long)]
        schema: String,
        /// Hash component of the record's primary key.
        #[arg(long)]
        key_hash: String,
        /// Range component of a HashRange primary key.
        #[arg(long)]
        key_range: Option<String>,
    },
    /// Manage and submit privately routed contributor mutations.
    #[command(subcommand)]
    Contributors(ShareContributorCommand),
    /// Share files with the LastDB Files app.
    #[command(subcommand)]
    Files(ShareFilesCommand),
}

#[derive(Subcommand, Debug)]
pub(crate) enum ShareContributorCommand {
    /// Authorize a recipient and write their sealed contributor grant.
    Grant {
        /// Absolute path of the publication home.
        #[arg(long)]
        home: PathBuf,
        /// Contact file obtained from the intended recipient.
        #[arg(long)]
        recipient: PathBuf,
        /// Recipient's Ed25519 public key in lowercase hex from a trusted channel.
        #[arg(long)]
        recipient_key: String,
        /// File containing a JSON array of allowed write rules.
        #[arg(long)]
        rules: PathBuf,
        /// Stable operation UUID for retries.
        #[arg(long)]
        operation_id: String,
        /// Current grant version. Use zero for a writer who has never had a grant.
        #[arg(long)]
        expected_version: u64,
        /// New path for the sealed contributor grant.
        #[arg(long)]
        out: PathBuf,
    },
    /// Revoke the recipient's write grant without changing their read invitation.
    Revoke {
        /// Absolute path of the publication home.
        #[arg(long)]
        home: PathBuf,
        /// Contributor's Ed25519 public key in lowercase hex.
        #[arg(long)]
        recipient_key: String,
        /// Current grant version.
        #[arg(long)]
        expected_version: u64,
    },
    /// Submit one encrypted mutation for the publisher to validate.
    Submit {
        /// Sealed contributor grant from the publisher.
        #[arg(long)]
        grant: PathBuf,
        /// Publisher's Ed25519 public key in lowercase hex from a trusted channel.
        #[arg(long)]
        publisher: String,
        /// File containing the JSON mutation request.
        #[arg(long)]
        request_file: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum ShareFilesCommand {
    /// Add a regular file to the running publication database in --data-dir.
    Add { file: PathBuf },
    /// Accept a sealed invitation into a local read-only cache.
    Receive {
        invitation: PathBuf,
        /// Publisher's public key obtained through a trusted channel.
        #[arg(long)]
        publisher: String,
        /// An absolute, private cache directory.
        #[arg(long)]
        cache: PathBuf,
        /// Optional UUID to reuse after an interrupted first download.
        #[arg(long)]
        handle: Option<String>,
    },
    /// Keep a received share current while this app is open.
    Follow {
        #[command(flatten)]
        share: ShareFilesSelection,
        /// Seconds between checks of the published snapshot.
        #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=60))]
        interval_seconds: u64,
    },
    /// List files from the locally verified copy, including while offline.
    List(ShareFilesSelection),
    /// Save and verify a file from the locally verified copy.
    Save {
        #[command(flatten)]
        share: ShareFilesSelection,
        /// File ID from the list command.
        #[arg(long)]
        file: String,
        /// New path for the complete file.
        #[arg(long)]
        out: PathBuf,
    },
}

#[derive(Args, Debug)]
pub(crate) struct ShareFilesSelection {
    /// Absolute path of the cache used by receive.
    #[arg(long)]
    pub cache: PathBuf,
    /// Share handle returned by receive.
    #[arg(long)]
    pub handle: String,
}
