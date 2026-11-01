//! Clap subcommands for `lastdb db` that work on a STOPPED home.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbOfflineCommand {
    /// Preserve this stopped device's identity in an inactive foreign snapshot.
    /// The prepared destination cannot start a daemon or replace the source.
    SnapshotIdentityPrepare(crate::db_cmds::db_snapshot_identity_cmds::SnapshotIdentityArgs),
    /// Verify an inactive snapshot's prepared identity and import bases.
    SnapshotIdentityInspect(crate::db_cmds::db_snapshot_identity_cmds::SnapshotIdentityArgs),
    /// Repair the blob counters and the retention queue. OFFLINE: it refuses a
    /// home that a daemon serves.
    ///
    /// It counts the file blob references of the records and the field
    /// history, and repairs the blob counters and the blob queue. A queue
    /// entry of the retention queue whose bucket has no directory row
    /// (`queue_entry_without_directory`) gets the row.
    ///
    /// Default is a dry run that reports every change. `--execute` writes the
    /// changes in whole batches. It is safe to run again: a second run changes
    /// nothing.
    ///
    /// Stop the daemon gracefully first (the last session must end cleanly and
    /// leave a flush receipt). Start the daemon after, and run
    /// `lastdb db verify-refs`: it must report no fault.
    Recount {
        /// Write the changes. Without it the command only reports them.
        #[arg(long)]
        execute: bool,
        /// A storage scope prefix to recount besides the personal scope.
        /// Repeat the flag for more scopes.
        #[arg(long = "scope")]
        scopes: Vec<String>,
        /// Recount each scope that the database catalog names, besides the
        /// personal scope. A scope with data and no catalog entry needs --scope.
        #[arg(long)]
        all_scopes: bool,
        /// Set the blob counters to the exact count: lower a counter that is too
        /// high, clear a `suspect` flag, and start the dwell of a blob that no
        /// record and no field history entry names. Without it the blob step only raises
        /// counters and makes missing ones. Requires --all-scopes, and every
        /// scope of the home must be listed: a scope that is missing makes the
        /// count too low.
        #[arg(long, requires = "all_scopes")]
        blobs_exact: bool,
        /// Raise the replay cutoff of the home to this unix second: a replayed
        /// write at or before it is refused. The cutoff is one record for the
        /// whole home, so it needs no --all-scopes. Stale peers need resync.
        /// Without --execute this reports only.
        #[arg(long)]
        delete_cutoff_unix_secs: Option<u64>,
        /// Allow a home under the primary directory.
        #[arg(long)]
        i_know_this_is_primary: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
}
