//! Clap subcommands for `lastdb db`: space reclaim and GC verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbReclaimCommand {
    /// Reclaim already-deleted v2 data in both active stores.
    /// The request survives a restart. Backup copies are outside this operation.
    Cleanup {
        /// Read the last request without a new cleanup request.
        #[arg(long)]
        status: bool,
        /// Wait for completion, read release, or the client deadline.
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..=86400))]
        timeout: u64,
        #[arg(long)]
        json: bool,
    },
    /// Read the durable atom hard-delete audit trail (purge and delete), oldest first.
    ///
    /// Answers "was this atom body deleted on purpose, or lost?" from the store's
    /// own evidence rather than a ~15h log window. The negative answer is the
    /// strong one: no row covering the window means no delete path removed it.
    ///
    /// Rows carry counts, never atom content, atom uuids (which are content
    /// hashes), or the purged record key (a `key_fingerprint` stands in).
    DeleteLedger {
        /// Return at most N rows, oldest first. 0 (default) returns everything.
        #[arg(long, default_value_t = 0)]
        limit: u64,
        #[arg(long)]
        json: bool,
    },
}
