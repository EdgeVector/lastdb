//! Clap subcommands for `lastdb db`: the two-home proof tooling.
//!
//! Both verbs are hidden from `--help` and refused by the daemon unless it
//! booted with `LASTDB_PROOF_REPLAY=1` on a home that holds a `.synthetic-home`
//! marker (see `lastdb_node::proof_replay`). They move sealed mutation-log
//! segments between two homes of one owner through a plain directory.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbProofCommand {
    /// Proof tooling: seal this home's pending mutation-log records and write
    /// the segments under a directory. Refused on any home that is not a
    /// guarded synthetic proof home.
    #[command(hide = true)]
    ExportMutationLog {
        /// Directory to write `log/<writer>/...` segment files under.
        #[arg(long)]
        to: PathBuf,
        /// Print the raw JSON report only.
        #[arg(long)]
        json: bool,
    },
    /// Proof tooling: apply the sealed segments under a directory to this
    /// home, through the same peer-apply path the live cloud cycle uses.
    /// Refused on any home that is not a guarded synthetic proof home.
    #[command(hide = true)]
    ReplayMutationLog {
        /// Directory that holds `log/<writer>/...` segment files.
        #[arg(long)]
        from: PathBuf,
        /// Print the raw JSON report only.
        #[arg(long)]
        json: bool,
    },
}
