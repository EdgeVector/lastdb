use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum CloudPurgeResyncCommand {
    /// Keep an exact local source, purge this account's cloud data, and upload its snapshot.
    Begin {
        /// Select an org account. Its current owner or admin must authorize the operation.
        #[arg(long)]
        org: Option<String>,
        /// Reuse this UUID after a lost response.
        #[arg(long)]
        request_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Read the durable job and its cloud control file.
    Status {
        #[arg(long)]
        job: Option<String>,
        /// Verify a saved product proof receipt against the current cloud operation.
        #[arg(long)]
        proof_receipt: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Continue the same retained source and operation after an interruption.
    Resume {
        #[arg(long)]
        job: String,
        #[arg(long)]
        json: bool,
    },
    /// Cancel the operation before cloud deletion starts.
    Cancel {
        #[arg(long)]
        job: String,
        #[arg(long)]
        json: bool,
    },
}
