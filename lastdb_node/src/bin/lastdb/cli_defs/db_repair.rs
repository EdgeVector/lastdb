//! Clap subcommands for `lastdb db`: repair and legacy-drain verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbRepairCommand {
    /// Repair one installed schema's field-to-molecule metadata.
    ///
    /// The command changes no atom data. It performs a dry run unless
    /// `--execute` is present. Execute also requires the fingerprint returned
    /// by a prior dry run.
    ///
    /// Omit `--map-file` to inspect: the command then prints the installed
    /// map, shows which field molecules still hold live rows, and lists
    /// candidate maps from other installed schemas. Use that report to write
    /// the map file, or pass `--write-map-file` to save the suggested map.
    RepairSchemaMoleculeMap {
        /// Installed schema name or identity hash.
        #[arg(long)]
        schema: String,
        /// JSON object that maps every field name to one molecule UUID.
        #[arg(long)]
        map_file: Option<PathBuf>,
        /// Write the suggested map from an inspect run to this path.
        #[arg(long, conflicts_with = "map_file")]
        write_map_file: Option<PathBuf>,
        /// Apply the metadata change. Omit for a dry run.
        #[arg(long)]
        execute: bool,
        /// Current fingerprint from the reviewed dry run.
        #[arg(long, requires = "execute")]
        expected_current_fingerprint: Option<String>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
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
