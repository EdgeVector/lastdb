//! Clap subcommands for `lastdb db`: space reclaim, GC and compaction verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbReclaimCommand {
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
    /// Pack the native home and reclaim the files that no generation needs.
    ///
    /// Without `--execute`, show a preview report and change nothing.
    /// With `--execute`, the node copies every live record into new parts and
    /// reclaims old files. The native route works on the whole home, so it
    /// refuses `--collection` with HTTP 400. The node also runs this work by
    /// itself when the dependency part count passes its mark
    /// (`docs/lastdb-storage-parts.md`).
    Compact {
        /// One collection. A native home refuses it with HTTP 400: omit it.
        #[arg(long)]
        collection: Option<String>,
        /// The whole home. A native home is always packed as a whole, so this
        /// changes nothing.
        #[arg(long)]
        all: bool,
        /// Actually pack and reclaim (without this flag, only a preview).
        #[arg(long)]
        execute: bool,
        /// Emit raw JSON only.
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
