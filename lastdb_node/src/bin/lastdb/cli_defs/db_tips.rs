//! Clap subcommands for `lastdb db`: tip-history retention, drain and migration verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbTipsCommand {
    /// Drop superseded `tv:` / tip-chain versions older than 7 days on **live**
    /// records. Tombstoned (deleted) heads are left alone. Default is dry-run.
    /// The CLI follows bounded daemon cursors to the end. Execute skips while
    /// a backup cut is held.
    RetainSupersededVersions {
        #[arg(long)]
        execute: bool,
        /// Max `mk:` tips examined per daemon pass (default 256).
        #[arg(long)]
        max_keys: Option<usize>,
        /// Optional cap on tips truncated per pass (defaults to max_keys).
        /// `--max-ops` is accepted as the same budget, the write-budget name
        /// the other cleanup verbs (reap-dropped-schema, repair) use.
        #[arg(long, visible_alias = "max-ops")]
        max_prunes: Option<usize>,
        /// Exclusive resume cursor: full storage key of the last tip walked.
        #[arg(long)]
        after_key: Option<String>,
        /// Resume from and advance the durable retention checkpoint.
        #[arg(long)]
        from_checkpoint: bool,
        /// Retention window in seconds. Default 7 days. `0` uses the default.
        #[arg(long)]
        retention_seconds: Option<u64>,
        #[arg(long)]
        json: bool,
    },
    /// Remove versions of a live record under the 7-day rule while the daemon is stopped.
    ///
    /// Rewrites tip files directly. Does not admit records into the warm set.
    /// Refuses when `folddb.sock` accepts a connection. Default counts only.
    /// `--execute` rewrites. Stop the daemon before this command. Start the
    /// same daemon after it. A tombstoned head is left alone. A version with
    /// no live head is left alone.
    RetainSupersededVersionsOffline {
        #[arg(long)]
        execute: bool,
        /// `written_at` cutoff in nanoseconds. Default is now minus 7 days.
        #[arg(long)]
        version_cutoff_nanos: Option<u64>,
        #[arg(long)]
        json: bool,
    },
    /// Drain legacy live tip-version (`tv:`) chains without collection compact.
    ///
    /// Bounded reclaim for history left after tip history became write-opt-in.
    /// Default is dry-run. Pass `--execute` to clear chains. Prefer
    /// `--from-checkpoint` for the resumable automatic-drain path.
    DrainTipHistory {
        #[arg(long)]
        execute: bool,
        /// Max `mk:` tips examined this pass (default 256).
        #[arg(long)]
        max_keys: Option<usize>,
        /// Optional cap on tips pruned this pass (defaults to max_keys).
        /// `--max-ops` is accepted as the same budget, the write-budget name
        /// the other cleanup verbs (reap-dropped-schema, repair) use.
        #[arg(long, visible_alias = "max-ops")]
        max_prunes: Option<usize>,
        /// Exclusive resume cursor: full storage key of the last tip walked.
        #[arg(long)]
        after_key: Option<String>,
        /// Resume from and advance the durable automatic-drain checkpoint.
        #[arg(long)]
        from_checkpoint: bool,
        #[arg(long)]
        json: bool,
    },
    /// Remove live `mk:` tips whose atom body is unreachable by every reader route.
    ///
    /// Default is dry-run. Pass `--execute` to rewrite affected molecules.
    RepairDanglingTips {
        #[arg(long)]
        execute: bool,
        /// Bound how many tips this invocation scans.
        #[arg(long)]
        max_ops: Option<usize>,
        /// Raw `mk:` rows per range page.
        #[arg(long)]
        tip_page: Option<usize>,
        /// Include up to N unresolved detail rows in the report (default:
        /// none, or 1000 with `--schema`).
        #[arg(long)]
        audit_limit: Option<usize>,
        /// Walk only this schema's tips (catalog name, descriptive name such
        /// as `BoardCards`, or identity hash). One prefix range per field
        /// molecule, so the cost is the schema's rows, not the whole store.
        #[arg(long)]
        schema: Option<String>,
        /// With `--schema`: walk only this HashKey (partition), e.g. a board.
        #[arg(long, requires = "schema")]
        hash_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Rewrite legacy fat `mk:` tip values to thin (atom_uuid + written_at + device_id).
    ///
    /// Same keys, thinner payloads. Default is dry-run. Pass `--execute` to rewrite.
    /// Safe under dual-read: readers accept both formats; local writes already emit thin.
    MigrateThinTips {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
    /// Move `photos/Photo.file_bytes` (inline base64) into local CAS blobs.
    ///
    /// Default is dry-run. Pass `--execute` to write CAS + clear the field.
    /// Follow with `gc-atoms --execute` to drop tombstoned tip history + orphan atoms.
    MigratePhotoBlobs {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
}
