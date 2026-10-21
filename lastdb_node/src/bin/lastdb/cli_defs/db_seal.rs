//! Clap subcommands for `lastdb db`: at-rest encryption maintenance.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbSealCommand {
    /// Rewrite sealed values to an explicit binary `ENB:` target.
    ///
    /// One allowlisted plane per call: atoms, tips, metadata, change_feed.
    /// Default is dry-run.
    /// Pass `--execute` to rewrite in place under the same key (no dual-key
    /// copies). Bounded (`--max-rows` / `--max-secs`) and resumable; a killed
    /// run continues from the durable checkpoint. `--progress` reports the
    /// checkpoint without scanning. `--target binary` forbids compression;
    /// `--target binary-compress` permits deflate at 256 B. When omitted, the
    /// server keeps the legacy binary-compress behavior. The target does not consult the
    /// process write switches, so the verb is provable with those switches off.
    ///
    /// **Heavy op.** Prefer `--out PATH` over shell redirect. This is not a
    /// cheap status gauge. Do not run as a routine against the live primary;
    /// owner-gated there. Sequence one plane at a time, smallest first, with a
    /// backup cut published between planes and never while a cut is staged.
    ///
    /// `cas_blobs` is not on the allowlist (exempt).
    ResealAtRest {
        /// One allowlisted collection (required).
        #[arg(long)]
        collection: String,
        /// Target envelope policy. Omit to preserve legacy behavior.
        #[arg(long, value_enum, value_name = "TARGET")]
        target: Option<ResealAtRestTargetArg>,
        /// Rewrite rows. Without this flag the call is a dry-run plan.
        #[arg(long)]
        execute: bool,
        /// Cap on rows decided this invocation.
        #[arg(long)]
        max_rows: Option<usize>,
        /// Cap on wall time this invocation, in seconds.
        #[arg(long)]
        max_secs: Option<u64>,
        /// Print the durable checkpoint and exit. No scan, no writes.
        #[arg(long)]
        progress: bool,
        /// Drop the durable checkpoint and start at the first key.
        #[arg(long)]
        restart: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
        /// Write the report JSON to this path (temp file + rename).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Client socket deadline in seconds. Defaults to the admin-scan budget.
        #[arg(long, value_name = "SECS")]
        timeout: Option<u64>,
    },
    /// Remove un-enveloped rows from one encrypted plane and return their bytes.
    ///
    /// A row with no `ENC:`/`ENZ:`/`ENB:` envelope in a namespace LastDB
    /// encrypts already reads as absent (drop-dual-read, 2026-09-14); reads
    /// never delete it, so it stays on disk and rides into every backup and
    /// restore. This verb is the bounded reaper. One sealed plane per call:
    /// atoms, tips, metadata, change_feed.
    /// Default is dry-run. Pass `--execute` to delete.
    /// Bounded (`--max-rows` / `--max-secs`) and resumable from a durable
    /// checkpoint; `--progress` reports it without scanning. The unsealed rows of
    /// each page are deleted in one commit. `--execute --all` repeats the call
    /// from the checkpoint until the collection is done, and prints the progress
    /// of each call.
    ///
    /// A sealed row is never touched, even one that does not open under this
    /// node's key. Every plaintext-by-policy namespace (schemas, db_catalog,
    /// molecule_keys, …) and the reserved marker namespace are refused by name.
    ///
    /// **Heavy op, on request only.** On a healthy home the correct count is
    /// zero; run the dry-run first and read a non-zero count as a signal (a
    /// restore that replayed unsealed) before you execute. Do not schedule.
    ReapUnsealed {
        /// One allowlisted, encrypted collection (required).
        #[arg(long)]
        collection: String,
        /// Delete rows. Without this flag the call is a dry-run plan.
        #[arg(long)]
        execute: bool,
        /// Cap on rows decided this invocation.
        #[arg(long)]
        max_rows: Option<usize>,
        /// Cap on wall time this invocation, in seconds.
        #[arg(long)]
        max_secs: Option<u64>,
        /// Print the durable checkpoint and exit. No scan, no writes.
        #[arg(long)]
        progress: bool,
        /// Drop the durable checkpoint and start at the first key.
        #[arg(long)]
        restart: bool,
        /// Repeat the call until the collection is done, from the durable
        /// checkpoint, and print the progress of each call to stderr.
        #[arg(long, requires = "execute")]
        all: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
        /// Write the report JSON to this path (temp file + rename).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Client socket deadline in seconds. Defaults to the admin-scan budget.
        #[arg(long, value_name = "SECS")]
        timeout: Option<u64>,
    },
}
