//! Clap subcommands for `lastdb db` that work on a STOPPED home.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbOfflineCommand {
    /// Rebuild every atom counter from the tip records and the conflict
    /// records. OFFLINE: it refuses a home that a daemon serves.
    ///
    /// This is the one scan the counter design keeps. It repairs a counter
    /// that `lastdb db verify-refs` reported: too low, too high, missing, or at
    /// zero. It deletes a counter that has no body and a body that no tip and
    /// no record names. It does not repair an atom that tips name and that has
    /// no body (a lost body).
    ///
    /// It also checks the retention rows of each conflict record. A record
    /// that no queue entry reaches (`record_without_queue_entry`) gets a queue
    /// entry that expires one window from now, its slot directory entry and
    /// the directory row of the bucket. A queue entry whose bucket has no
    /// directory row (`queue_entry_without_directory`) gets the row.
    ///
    /// Default is a dry run that reports every change. `--execute` writes the
    /// changes in whole batches. It is safe to run again: a second run changes
    /// nothing. It writes nothing while a tip or conflict row does not decode.
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
        /// atom counter and no tip names. Without it the blob step only raises
        /// counters and makes missing ones. Requires --all-scopes, and every
        /// scope of the home must be listed: a scope that is missing makes the
        /// count too low.
        #[arg(long, requires = "all_scopes")]
        blobs_exact: bool,
        /// Repair molecule slots, conflict-record and binding counts. The home
        /// must be stopped. Missing live headers or undecodable holders refuse repair.
        #[arg(long)]
        molecules: bool,
        /// Reject replay at or before this unix second, then prune older Delete
        /// barriers. This is an explicit global cutoff. Stale peers need resync.
        /// Without --execute this reports only. Older programs refuse the fenced home.
        #[arg(long, requires = "all_scopes")]
        delete_cutoff_unix_secs: Option<u64>,
        /// Passes over the tips and records. More passes use less memory.
        /// Molecule repair permits 100,000 molecules and 64 MiB of header bytes
        /// in one pass; increase this count if either budget refuses the run.
        #[arg(long, default_value_t = 1)]
        shards: u32,
        /// Allow a home under the primary directory.
        #[arg(long)]
        i_know_this_is_primary: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
}
