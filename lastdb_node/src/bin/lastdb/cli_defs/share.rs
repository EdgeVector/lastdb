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
        /// Catalog spec name. Omit it to keep the legacy publication path.
        #[arg(long)]
        spec: Option<String>,
        /// Registry spec version for `--spec`. Omitted version means `1`.
        #[arg(long)]
        spec_version: Option<u32>,
    },
    /// Export this LastDB identity's public contact code.
    Contact {
        /// New path for the public contact file.
        #[arg(long)]
        out: PathBuf,
    },
    /// Create a recipient-encrypted invitation for a published database.
    Invite {
        /// Absolute path of the publication home. Omit this when the parent `--db` is set.
        #[arg(long)]
        home: Option<PathBuf>,
        /// Contact file obtained from the intended recipient.
        #[arg(long)]
        recipient: PathBuf,
        /// Recipient's Ed25519 public key in lowercase hex from a trusted channel.
        #[arg(long)]
        recipient_key: String,
        /// New path for the encrypted invitation file.
        #[arg(long)]
        out: PathBuf,
        /// Lifetime of the read permission in hours. The member row does not expire.
        #[arg(long, default_value_t = 24, value_parser = clap::value_parser!(u32).range(1..=168))]
        expires_in_hours: u32,
        /// Restore a revoked named-database member. A publication invite rejects this.
        #[arg(long)]
        regrant: bool,
    },
    /// Register an invitation as a database on this node and start recovery.
    Accept {
        /// Encrypted invitation file.
        invitation: PathBuf,
        /// Publisher's Ed25519 public key in lowercase hex from a trusted channel.
        #[arg(long)]
        publisher: String,
        /// Optional local database name.
        #[arg(long)]
        name: Option<String>,
    },
    /// Manage contributor grants for ordinary database mutations.
    #[command(subcommand)]
    Contributors(ShareContributorCommand),
    /// Share files with the LastDB Files app.
    #[command(subcommand)]
    Files(ShareFilesCommand),
    /// Publish a saved keyed query into one owned share database.
    #[command(subcommand)]
    Queries(ShareQueriesCommand),
}

#[derive(Subcommand, Debug)]
pub(crate) enum ShareQueriesCommand {
    /// Check the query and store it in the source database. This copies no records.
    Save {
        /// Stable query id. A repeated save keeps the same destination and spec.
        #[arg(long)]
        id: String,
        /// App that owns the source schemas.
        #[arg(long)]
        app: String,
        /// Source database name or id. Use `personal` for the owner database.
        #[arg(long)]
        source: String,
        /// Owned shareable database name or id.
        #[arg(long)]
        destination: String,
        /// Spec name stored on the destination database.
        #[arg(long)]
        spec: String,
        /// Spec version stored on the destination database.
        #[arg(long, default_value_t = 1)]
        spec_version: u32,
        /// JSON file of keyed reads and links.
        #[arg(long)]
        query: PathBuf,
    },
    /// Copy eligible records into the destination and remove rows this query no longer selects.
    Reconcile {
        /// Query id stored in the source database.
        #[arg(long)]
        id: String,
        /// Source database name or id used when the query was saved.
        #[arg(long)]
        source: String,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum ShareContributorCommand {
    /// Authorize a recipient and write their sealed contributor grant.
    Grant {
        /// Absolute path of the publication home. Omit this when the parent `--db` is set.
        #[arg(long)]
        home: Option<PathBuf>,
        /// Contact file obtained from the intended recipient.
        #[arg(long)]
        recipient: PathBuf,
        /// Recipient's Ed25519 public key in lowercase hex from a trusted channel.
        #[arg(long)]
        recipient_key: String,
        /// File containing a JSON array of allowed write rules. Publication grants only.
        #[arg(long)]
        rules: Option<PathBuf>,
        /// Stable operation UUID for retries. Publication grants only.
        #[arg(long)]
        operation_id: Option<String>,
        /// Current grant version. Use zero for a writer who has never had a grant.
        #[arg(long)]
        expected_version: Option<u64>,
        /// New path for the sealed contributor grant.
        #[arg(long)]
        out: PathBuf,
        /// Restore a revoked named-database member. A publication grant rejects this.
        #[arg(long)]
        regrant: bool,
        /// Bearer lifetime in hours for a named database. The member row does not expire.
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=168))]
        expires_in_hours: Option<u32>,
    },
    /// Revoke the recipient's write grant without changing their read invitation.
    Revoke {
        /// Absolute path of the publication home. Omit this when the parent `--db` is set.
        #[arg(long)]
        home: Option<PathBuf>,
        /// Contributor's Ed25519 public key in lowercase hex.
        #[arg(long)]
        recipient_key: String,
        /// Current grant version. Publication revokes only.
        #[arg(long)]
        expected_version: Option<u64>,
    },
    /// Attach a contributor grant to a registered database.
    Accept {
        /// Registered database ID or local name.
        db: String,
        /// Sealed contributor grant from the publisher.
        #[arg(long)]
        grant: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum ShareFilesCommand {
    /// Add a regular file to the running publication database in --data-dir.
    Add { file: PathBuf },
    /// Register a sealed invitation as a database on this node.
    Receive {
        invitation: PathBuf,
        /// Publisher's public key obtained through a trusted channel.
        #[arg(long)]
        publisher: String,
        /// Optional local name for use with --db.
        #[arg(long, conflicts_with = "cache")]
        name: Option<String>,
        /// Use the legacy app-managed cache at this absolute private path.
        #[arg(long)]
        cache: Option<PathBuf>,
        /// Optional UUID to reuse with a legacy cache.
        #[arg(long, requires = "cache")]
        handle: Option<String>,
    },
    /// Keep a legacy app-managed cache current.
    Follow {
        #[command(flatten)]
        share: ShareFilesSelection,
        /// Seconds between checks for a new accepted recovery point.
        #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=60))]
        interval_seconds: u64,
    },
    /// List files from the locally verified copy, including while offline.
    List(ShareFilesReadSelection),
    /// Save and verify a file from the locally verified copy.
    Save {
        #[command(flatten)]
        share: ShareFilesReadSelection,
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

#[derive(Args, Debug)]
pub(crate) struct ShareFilesReadSelection {
    /// Legacy absolute cache directory. Use --db for a registered database.
    #[arg(long, requires = "handle")]
    pub cache: Option<PathBuf>,
    /// Legacy handle returned when receiving into --cache.
    #[arg(long, requires = "cache")]
    pub handle: Option<String>,
}
