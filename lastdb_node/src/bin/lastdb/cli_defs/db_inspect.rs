//! Clap subcommands for `lastdb db`: read-only inspection and audit verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbInspectCommand {
    /// Decrypt/read live store: main key-class + per-schema atom/history sizes.
    ///
    /// **Heavy op:** full-prefix walks over multi-GiB stores can take minutes
    /// (see `lastdb ops`). Prefer `--out PATH` over
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
    /// Report faults in the retention queue and the blob counters. Read-only:
    /// it writes nothing and takes no write gate, so it does not stall writers.
    ///
    /// Surveys the retention queue for buckets without a directory row, and
    /// checks each blob counter and each entry of the blob queue, in pages with
    /// a cursor. A step reads pages until its page or time budget is spent; the
    /// command follows the cursor until the run is complete. Exits non-zero
    /// when it finds a fault. Repair on a stopped node with `lastdb db recount`.
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
    },
}
