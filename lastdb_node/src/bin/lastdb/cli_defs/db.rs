//! Clap subcommands for `lastdb db`: storage inspection, repair and maintenance verbs.
//!
//! The verbs are grouped into per-theme enums under `cli_defs/db_*.rs` and
//! flattened here, so every command keeps its existing top-level name.

use super::*;

#[path = "db_blob.rs"]
mod blob;
#[path = "db_inspect.rs"]
mod inspect;
#[path = "db_offline.rs"]
mod offline;
#[path = "db_reclaim.rs"]
mod reclaim;
#[path = "db_repair.rs"]
mod repair;

pub(crate) use blob::*;
pub(crate) use inspect::*;
pub(crate) use offline::*;
pub(crate) use reclaim::*;
pub(crate) use repair::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbCommand {
    #[command(flatten)]
    Inspect(DbInspectCommand),
    #[command(flatten)]
    Reclaim(DbReclaimCommand),
    #[command(flatten)]
    Repair(DbRepairCommand),
    #[command(flatten)]
    Blob(DbBlobCommand),
    #[command(flatten)]
    Offline(DbOfflineCommand),
}
