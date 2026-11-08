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
    },
    /// Publish a snapshot of a cleanly stopped publication home.
    Publish {
        /// Absolute path of the publication home.
        #[arg(long)]
        home: PathBuf,
        /// UUID for this publication attempt. Repeat it to resume the same job.
        #[arg(long)]
        request_id: String,
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
}
