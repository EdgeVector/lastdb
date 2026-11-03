//! Clap subcommands for `lastdb db`: the file of one record.

use super::*;

/// The record that a file verb names: its schema and its key.
#[derive(clap::Args, Debug)]
pub(crate) struct BlobTargetArgs {
    /// Schema of the record.
    #[arg(long)]
    pub(crate) schema: String,
    /// Hash of the record key.
    #[arg(long)]
    pub(crate) hash: String,
    /// Range of the record key, for a HashRange schema.
    #[arg(long)]
    pub(crate) range: Option<String>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum DbBlobCommand {
    /// Store the file of one record, with the record, in one write.
    PutBlob(PutBlobArgs),
    /// Fetch the file of one record by schema and key.
    ///
    /// The command writes the verified bytes to --out or stdout. A record
    /// without a file returns 404.
    FetchBlob {
        #[command(flatten)]
        target: BlobTargetArgs,
        /// Write fetched bytes to this path instead of stdout.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Emit the daemon's JSON report instead of raw bytes.
        #[arg(long)]
        json: bool,
    },
    /// Remove the file and the file marker of one record. The record stays.
    DeleteBlob(BlobTargetArgs),
}
