//! Clap subcommands for `lastdb db` that work on a STOPPED home.

use super::*;

#[derive(Subcommand, Debug, Clone, Copy)]
pub(crate) enum DbOfflineCommand {
    /// Raise the replay cutoff of the home. OFFLINE: it refuses a home that a
    /// daemon serves.
    ///
    /// Default is a dry run that reports the change. `--execute` writes it.
    /// It is safe to run again: a second run with the same cutoff changes
    /// nothing.
    ///
    /// Stop the daemon gracefully first (the last session must end cleanly and
    /// leave a flush receipt).
    Recount {
        /// Write the change. Without it the command only reports it.
        #[arg(long)]
        execute: bool,
        /// The new replay cutoff of the home, in unix seconds: a replayed write
        /// at or before it is refused. The cutoff is one record for the whole
        /// home. Stale peers need resync.
        #[arg(long)]
        delete_cutoff_unix_secs: u64,
        /// Allow a home under the primary directory.
        #[arg(long)]
        i_know_this_is_primary: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
}
