//! Arguments for moving the replay cutoff of a stopped home.

use super::*;

#[derive(Args, Debug, Clone, Copy)]
pub(crate) struct ReplayCutoffArgs {
    /// Write the change. Without it the command only reports it.
    #[arg(long)]
    pub(crate) execute: bool,
    /// The new replay cutoff of the home, in unix seconds: a replayed write
    /// at or before it is refused. The cutoff is one record for the whole
    /// home. Stale peers need resync.
    #[arg(long)]
    pub(crate) delete_cutoff_unix_secs: u64,
    /// Allow a home under the primary directory.
    #[arg(long)]
    pub(crate) i_know_this_is_primary: bool,
    /// Emit raw JSON only.
    #[arg(long)]
    pub(crate) json: bool,
}
