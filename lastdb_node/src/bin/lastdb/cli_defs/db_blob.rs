//! Clap subcommands for `lastdb db`: file-blob transfer verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbBlobCommand {
    /// Fetch exactly one `$lastdb_file` pointer from local storage.
    ///
    /// The pointer JSON must carry file blob access metadata. The command writes
    /// verified local bytes to --out or stdout. A missing local blob returns 404.
    FetchFileBlob {
        /// JSON file containing the `$lastdb_file` field value.
        #[arg(long)]
        pointer_json: PathBuf,
        /// Write fetched bytes to this path instead of stdout.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Emit the daemon's JSON report instead of raw bytes / human summary.
        #[arg(long)]
        json: bool,
        /// Request the verified plaintext as a binary socket response.
        #[arg(long)]
        raw: bool,
    },
    /// Store one durable local blob and return its pointer without a record write.
    PutBlobLocal(PutBlobLocalArgs),
}
