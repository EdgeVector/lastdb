use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum ShareCommand {
    /// Export this LastDB identity's public contact code.
    Contact {
        /// New path for the public contact file.
        #[arg(long)]
        out: PathBuf,
    },
    /// Verify a contact file against an independently trusted node key.
    VerifyContact {
        contact: PathBuf,
        #[arg(long)]
        recipient_key: String,
    },
    /// Create a recipient-encrypted invitation for the named database selected by --db.
    Invite {
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
        /// Restore a revoked named-database member.
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
}

#[derive(Subcommand, Debug)]
pub(crate) enum ShareContributorCommand {
    /// Authorize a recipient and write their sealed contributor grant.
    Grant {
        /// Contact file obtained from the intended recipient.
        #[arg(long)]
        recipient: PathBuf,
        /// Recipient's Ed25519 public key in lowercase hex from a trusted channel.
        #[arg(long)]
        recipient_key: String,
        /// New path for the sealed contributor grant.
        #[arg(long)]
        out: PathBuf,
        /// Restore a revoked named-database member.
        #[arg(long)]
        regrant: bool,
        /// Bearer lifetime in hours for a named database. The member row does not expire.
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=168))]
        expires_in_hours: Option<u32>,
    },
    /// Revoke the recipient's write grant without changing their read invitation.
    Revoke {
        /// Contributor's Ed25519 public key in lowercase hex.
        #[arg(long)]
        recipient_key: String,
    },
}
