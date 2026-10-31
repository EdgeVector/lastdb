//! Clap subcommands for `lastdb db`: repair verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbRepairCommand {
    /// Repair sparse declared key-field molecules in one HashRange partition.
    ///
    /// The declared range-field molecule supplies the member list. The command
    /// is a dry run unless `--execute` is present.
    RepairHashrangeKeyFields {
        /// Installed schema name or identity hash.
        #[arg(long)]
        schema: String,
        /// Exact API-form hash partition. This command never crosses it.
        #[arg(long)]
        hash: String,
        /// Apply the planned normal mutations. Omit for a dry run.
        #[arg(long)]
        execute: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
}
