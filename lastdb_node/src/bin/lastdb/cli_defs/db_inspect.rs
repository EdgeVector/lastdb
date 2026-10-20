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
    /// Show the bounded identities behind skipped atom reads on this daemon.
    /// Owner socket only. The result can contain user keys.
    UnresolvedAtoms {
        #[arg(long)]
        json: bool,
    },
    /// List one molecule's live `mk:` storage keys (decoded hash/range +
    /// collision flag). Read-only diagnostic for the current key format.
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
}
