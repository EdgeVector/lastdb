//! Internal storage inventory modes.

use super::*;

#[derive(Debug)]
pub(crate) enum DbInspectCommand {
    /// Decrypt/read live store: main key-class sizes + per-schema live record sizes.
    ///
    /// **Heavy op:** full-prefix walks over multi-GiB stores can take minutes
    /// (see `lastdb admin ops`). Prefer `--out PATH` over
    /// shell redirect (`> file`) so a client deadline cannot leave a 0-byte
    /// product file that looks like an empty store. It writes nothing.
    ///
    /// Client deadline defaults to the admin-scan budget (600s, or
    /// `LASTDB_UDS_ADMIN_TIMEOUT_SECS`). Raise with `--timeout <secs>` on this
    /// command **and** the matching server env when walks exceed the default.
    Inventory {
        /// Emit raw JSON only (no human table).
        json: bool,
        /// Write inventory JSON to this path (temp file + rename; never a
        /// half-empty product file on failure). Prefer this over shell
        /// redirect so a timed-out run does not leave a 0-byte inventory.
        out: Option<PathBuf>,
        /// Client socket deadline for this inventory walk, in seconds.
        /// Defaults to the admin-scan budget (`LASTDB_UDS_ADMIN_TIMEOUT_SECS`
        /// or 600). The server deadline must be at least this high for the
        /// walk to finish — raise that env on the daemon too when you raise
        /// the client flag past the default.
        timeout: Option<u64>,
    },
    /// Per-schema live record sizes (catalog names joined).
    ///
    /// **Heavy op:** walks every stored record (`rk:` rows) once — cheaper
    /// than the full inventory (no main key-class walk), but still an admin scan.
    /// Prefer `--out PATH` over shell redirect.
    ///
    /// Do not run this as a routine against a daily-driver primary.
    Schemas {
        /// Emit raw JSON only (no human table).
        json: bool,
        /// Write the report JSON to this path (temp file + rename).
        out: Option<PathBuf>,
        /// Client socket deadline in seconds. Defaults to the admin-scan
        /// budget (`LASTDB_UDS_ADMIN_TIMEOUT_SECS` or 600).
        timeout: Option<u64>,
        /// Cap the human table (JSON stays complete). Default 50.
        limit: usize,
    },
}
