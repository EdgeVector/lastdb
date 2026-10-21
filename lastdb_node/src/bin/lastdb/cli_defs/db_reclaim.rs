//! Clap subcommands for `lastdb db`: space reclaim, GC and compaction verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbReclaimCommand {
    /// Compact current allowlisted LastStore collections.
    ///
    /// Without `--execute`, report the live keys and file sizes only.
    /// Omit `--collection` to compact all allowed collections except `cas_blobs`.
    /// Name `cas_blobs` explicitly. The command pauses cloud sync when required.
    ///
    /// Current collections: schemas, schema_states, schema_index, tips,
    /// metadata, idempotency, change_feed, sync_pin_log, sync_capture_reexport,
    /// keep_small, atoms, proteins and cas_blobs.
    Compact {
        /// One collection (e.g. `schemas`). Omit with `--all` (or omit both)
        /// to compact every allowlisted plane except `cas_blobs`.
        #[arg(long)]
        collection: Option<String>,
        /// Compact every allowlisted collection but `cas_blobs` in one pass.
        #[arg(long)]
        all: bool,
        /// Actually compact (without this flag, only reports sizes).
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
    /// Drop the current `keep_small` group only when its sidecar proves it
    /// holds the one rebuildable `keep_small:meters` snapshot. This returns
    /// superseded snapshot bytes without loading the cold group. Default is a
    /// dry run. Pass `--execute` to remove the directory.
    ReclaimKeepSmallSnapshot {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
    /// Stamp committed atom successor-history SHAs into pending purged
    /// retirements so the next photograph cut can drop them with a receipt.
    ///
    /// Default is dry-run (no sidecar write). `--execute` stamps groups that
    /// still have a verified local atom chunk. Missing groups are not stamped.
    /// Skips while a backup cut is held. Owner `compact --execute` rewrites
    /// the whole home and is not this one-shot.
    StampPurgedAtomRetirements {
        /// Actually write pending_shas (without this flag, report only).
        #[arg(long)]
        execute: bool,
        /// Emit raw JSON only.
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
