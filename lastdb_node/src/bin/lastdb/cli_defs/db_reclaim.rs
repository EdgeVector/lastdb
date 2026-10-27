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
    /// Preview one bounded page of redundant generation delete markers.
    /// Active generations and in-flight generation preparations stay protected.
    CleanupDeleteMarkers {
        #[arg(long)]
        molecule: String,
        #[arg(long)]
        scope: Option<String>,
        /// Opaque exclusive cursor from the previous page, including a deleted key.
        #[arg(long)]
        after_key: Option<String>,
        #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u16).range(1..=256))]
        max_keys: u16,
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
    /// Trim or purge mutation-history rows.
    ///
    /// Default is dry-run (preview). Pass `--execute` to actually delete.
    /// Current tips (`mk:`) and atoms are never deleted.
    ///
    /// - `--keep-last 1` (default): drop older events, keep newest 1 per field key
    /// - `--keep-last 0`: full purge — delete all history (latest-only = tip only)
    ClearHistory {
        /// Limit to one schema name (default: all schemas with history).
        #[arg(long)]
        schema: Option<String>,
        /// Keep the newest N history events per field key.
        /// `0` = purge all history (no version log; tip is the only version).
        #[arg(long, default_value_t = 1)]
        keep_last: usize,
        /// Actually delete (without this flag, only reports what would be removed).
        #[arg(long)]
        execute: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// GC: delete empty, unbound `protein:` rows.
    ///
    /// Historical clients leaked one empty protein per failed bind probe.
    /// Bound proteins are protected by their member list and by `molprot:`
    /// back-refs. Default is dry-run. Pass `--execute` to apply.
    GcProteins {
        #[arg(long)]
        execute: bool,
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
