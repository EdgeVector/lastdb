//! Clap subcommands for `lastdb db`: read-only inspection and audit verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbInspectCommand {
    /// Decrypt/read live store: main key-class + per-schema atom/history sizes.
    ///
    /// **Heavy op:** full-prefix walks over multi-GiB stores can take minutes
    /// and load many cold shards (see `lastdb ops`). Prefer `--out PATH` over
    /// shell redirect (`> file`) so a client deadline cannot leave a 0-byte
    /// product file that looks like an empty store.
    ///
    /// **Not read-only:** this command also durably writes attribution rows
    /// (schema/system/retention root walks) before it reports the summary.
    /// The writes are idempotent, so repeat runs are safe.
    ///
    /// Client deadline defaults to the admin-scan budget (600s, or
    /// `LASTDB_UDS_ADMIN_TIMEOUT_SECS`). Raise with `--timeout <secs>` on this
    /// command **and** the matching server env when walks exceed the default.
    Inventory {
        /// Emit raw JSON only (no human table).
        #[arg(long)]
        json: bool,
        /// Write inventory JSON to this path (temp file + rename; never a
        /// half-empty product file on failure). Prefer this over shell
        /// redirect so a timed-out run does not leave a 0-byte inventory.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Client socket deadline for this inventory walk, in seconds.
        /// Defaults to the admin-scan budget (`LASTDB_UDS_ADMIN_TIMEOUT_SECS`
        /// or 600). The server deadline must be at least this high for the
        /// walk to finish — raise that env on the daemon too when you raise
        /// the client flag past the default.
        #[arg(long, value_name = "SECS")]
        timeout: Option<u64>,
    },
    /// Per-schema logical atom sizes (catalog names joined).
    ///
    /// **Heavy op:** walks live `atom:` rows only — cheaper than
    /// `db inventory` (no history / order-log / tip-format), but still an
    /// admin scan. Prefer `--out PATH` over shell redirect.
    ///
    /// Do not run this as a routine against a daily-driver primary.
    Schemas {
        /// Emit raw JSON only (no human table).
        #[arg(long)]
        json: bool,
        /// Write the report JSON to this path (temp file + rename).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Client socket deadline in seconds. Defaults to the admin-scan
        /// budget (`LASTDB_UDS_ADMIN_TIMEOUT_SECS` or 600).
        #[arg(long, value_name = "SECS")]
        timeout: Option<u64>,
        /// Cap the human table (JSON stays complete). Default 50.
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Report faults in the atom counters. Read-only: it writes nothing and
    /// takes no write gate, so it does not stall writers.
    ///
    /// Reads the tip records, the conflict records, and the body and counter
    /// records of atoms in pages with a cursor, and checks that every counter
    /// holds the number of tips and records that name its atom, that body and
    /// counter exist together, and that no counter holds zero or is suspect.
    /// A step reads pages until its page or time budget is spent; the command
    /// follows the cursor until the run is complete. Exits non-zero when it
    /// finds a fault. Repair on a stopped node with `lastdb db recount`.
    VerifyRefs {
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
        /// Show the last result the daemon holds and run nothing.
        #[arg(long)]
        last: bool,
        /// Stop after this many steps (0 runs to the end). Print the cursor to continue.
        #[arg(long, default_value_t = 0)]
        steps: u32,
        /// Continue the run that a previous step reported.
        #[arg(long)]
        cursor: Option<String>,
        /// Pages one step reads at most (default 64).
        #[arg(long)]
        max_pages: Option<u32>,
        /// Milliseconds one step runs at most (default 3000).
        #[arg(long)]
        max_ms: Option<u64>,
        /// Milliseconds to pause after each page (default 2).
        #[arg(long)]
        pause_ms: Option<u64>,
        /// Rows asked for in each read (default 1000).
        #[arg(long)]
        page_rows: Option<usize>,
        /// Storage scope prefix to check (default: the personal scope). An org
        /// or share scope is not checked unless you name it or use --all-scopes.
        #[arg(long)]
        scope: Option<String>,
        /// Check the personal scope and each scope that the database catalog
        /// names. A scope that holds data and has no catalog entry needs --scope.
        #[arg(long)]
        all_scopes: bool,
        /// The most atoms the totals hold (default 1000000).
        #[arg(long)]
        max_atoms: Option<usize>,
    },
    /// Show the bounded identities behind skipped atom reads on this daemon.
    /// Owner socket only. The result can contain user keys.
    UnresolvedAtoms {
        #[arg(long)]
        json: bool,
    },
    /// List one molecule's live `mk:` storage keys (decoded hash/range +
    /// collision flag). Read-only diagnostic for H1 vs H2 order-log shortfall.
    MoleculeKeys {
        /// Molecule uuid (the `M` in `mk:{M}:…`).
        #[arg(long)]
        molecule: String,
        /// Cap rows returned (default 10_000).
        #[arg(long)]
        max_keys: Option<usize>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Detect molecules whose `update_order` append-log is shorter than their
    /// live key set — a truncated log that no point read can see.
    ///
    /// `update_order` only grows: a write appends an entry every time a key's
    /// value changes and never removes one, so `moc:{M} >= count(mk:{M}:…)`
    /// holds for every molecule that has a log. Below that bound, entries were
    /// lost. Because a truncation restamps `moc:` and deletes the `mord:` rows
    /// above it together, the log stays self-consistent and only `SampleN`
    /// (the sole reader that walks the log) ever sees the hole — the `mk:`
    /// count is the only witness left.
    ///
    /// Read-only, and cheap: the molecule uuid comes out of the key, so no atom
    /// body is ever fetched. Exits non-zero when a short molecule is found.
    OrderLogAudit {
        /// `mk:` records one daemon call walks before returning a resume
        /// cursor. Soft — a call always finishes the molecule it is inside.
        #[arg(long)]
        max_keys: Option<usize>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Bounded, resumable, read-only pin-log plane audit (`sync_pin_log`).
    ///
    /// Classifies entry rows as confirmed-orphan (frontier ≤ durable published
    /// F for their writer) vs genuinely pending. Never deletes. Returns at
    /// most one bounded page and an explicit resume cursor when more remains.
    PinLogAudit {
        /// Pin-log keys this bounded audit page may walk.
        #[arg(long)]
        max_keys: Option<usize>,
        /// Resume after a prior page's `next_after_key`.
        #[arg(long)]
        after_key: Option<String>,
        /// Emit the page report as raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Measure append-only order-log excess (stale entries + zero-live residue)
    /// with exact stored bytes. Read-only; follows bounded daemon cursors.
    OrderLogBloatAudit {
        /// `mk:` records one daemon call walks before returning a resume cursor.
        #[arg(long)]
        max_keys: Option<usize>,
        /// Emit raw aggregate JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Count `mk:` records whose `meta.tombstoned` flag disagrees with their
    /// atom content, and optionally stamp the flag onto them.
    ///
    /// A record written before the flag existed reads as `tombstoned: false`
    /// while its atom content is a tombstone. The page window is spent on such a
    /// row and the content gate rejects it afterwards, so a page comes back
    /// short. `content_tombstoned_meta_false` is how many such rows remain.
    ///
    /// Reads every atom body under the selected molecules — narrow it with
    /// `--schema`. Default is dry-run; `--execute` stamps the flag (idempotent,
    /// so an interrupted run resumes by re-running).
    ///
    /// `--max-keys` bounds one daemon POST, not the whole walk. The CLI follows
    /// `more_remaining` until the store is exhausted unless `--once` is set.
    TombstoneFlagAudit {
        /// Audit only this schema's field molecules (default: whole store).
        #[arg(long)]
        schema: Option<String>,
        /// Stamp `meta.tombstoned` on records whose content says tombstone.
        #[arg(long)]
        execute: bool,
        /// Records one daemon call decides before returning a resume cursor.
        /// This is a per-call cap, not a total. Lower it if a pass approaches
        /// the control socket's read deadline.
        #[arg(long)]
        max_keys: Option<usize>,
        /// Resume after a prior page's `next_after_key`.
        #[arg(long = "after-key", alias = "after")]
        after_key: Option<String>,
        /// Issue one daemon POST, print that page (including `more_remaining`
        /// and `next_after_key`), then exit. Without this flag the CLI follows
        /// cursors until the store is exhausted. During a multi-pass `--json`
        /// walk, per-pass progress is written to stderr while stdout remains a
        /// single final JSON document.
        #[arg(long)]
        once: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Survey legacy/plain HashKey-encoding tips and classify safe forks.
    LegacyKeyForkAudit {
        #[arg(long)]
        max_keys: Option<usize>,
        #[arg(long)]
        json: bool,
    },
}
