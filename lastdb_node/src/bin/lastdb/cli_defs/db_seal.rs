//! Clap subcommands for `lastdb db`: at-rest sealing and partition re-key verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbSealCommand {
    /// Rewrite sealed values to an explicit binary `ENB:` target.
    ///
    /// One allowlisted plane per call: atoms, tips, indexes, metadata,
    /// change_feed, atom_locators, field_update_order_log. Default is dry-run.
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
    /// atoms, tips, indexes, metadata, change_feed, atom_locators,
    /// field_update_order_log. Default is dry-run. Pass `--execute` to delete.
    /// Bounded (`--max-rows` / `--max-secs`) and resumable from a durable
    /// checkpoint; `--progress` reports it without scanning.
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
    /// Dual-write tip-referenced atom bodies to partition-prefixed keys (+ locators).
    ///
    /// Default is dry-run. Pass `--execute` to write. Optional `--remove-flat`
    /// deletes the flat key only after the prefixed body is verified.
    ///
    /// `--audit` classifies every tip that resolves to no atom body — the
    /// `missing_body` count on its own cannot say whether a body still exists at
    /// an address the pass did not try, which is exactly what `--remove-flat`
    /// needs to know before it deletes flat keys.
    RekeyAtomPartitionPrefix {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        remove_flat: bool,
        #[arg(long)]
        max_ops: Option<usize>,
        /// Tips per range page (default 1024).
        ///
        /// Since the pass batches per page, a page costs a fixed number of store
        /// round trips *at any page size* — so this is the knob that trades
        /// resident memory against wall clock, and it is the only one. A larger
        /// page amortizes those round trips over more tips; it also makes the
        /// page's body read (`get_many` over the flat bodies needing a copy)
        /// materialize proportionally more atom content at once, and bodies run
        /// to `LASTDB_MAX_ATOM_CONTENT_BYTES`. Raise it to go faster on a node
        /// with headroom; lower it on one whose resident budget is the binding
        /// constraint. Measure both before changing it on a long run.
        #[arg(long)]
        tip_page: Option<usize>,
        /// Classify unresolved tips (locator-based) and print per-tip detail.
        /// Read-only: adds point reads, never writes.
        #[arg(long)]
        audit: bool,
        /// Cap on printed detail rows. Classification counters always cover
        /// every unresolved tip. Only meaningful with `--audit`.
        #[arg(long, default_value_t = 2000)]
        audit_limit: usize,
        /// Report the durable checkpoint and exit. No scan, no writes — the only
        /// mode that is safe to point at a live primary to ask "how far along?".
        #[arg(long)]
        progress: bool,
        /// Keep issuing `--execute` passes until the checkpoint reports the
        /// migration complete, treating a lost socket as retryable.
        ///
        /// A supervised restart mid-migration is an expected event, not a
        /// terminal error: a shell loop that aborted after five consecutive
        /// socket failures is what silently parked this migration at 8.8% on
        /// 2026-07-29. Requires `--execute`.
        #[arg(long)]
        until_complete: bool,
        /// Compact the `atoms` plane after a complete flat-key retirement pass.
        ///
        /// LastStore deletes append tombstones, so `--remove-flat` alone can
        /// grow the plane. This flag keeps logical retirement and physical byte
        /// return in one operator command. Requires `--execute --remove-flat
        /// --until-complete` and cannot be combined with `--json`.
        #[arg(
            long,
            requires_all = ["execute", "remove_flat", "until_complete"],
            conflicts_with = "json"
        )]
        compact_after: bool,
        #[arg(long)]
        json: bool,
    },
}
