//! Clap subcommands for `lastdb db`: tip-history retention, drain and migration verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbTipsCommand {
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
}
