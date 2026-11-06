//! Internal storage action selection.

use super::*;

#[path = "db_inspect.rs"]
mod inspect;
#[path = "db_offline.rs"]
mod offline;
#[path = "db_reclaim.rs"]
mod reclaim;

pub(crate) use inspect::*;
pub(crate) use offline::*;
pub(crate) use reclaim::*;

#[derive(Debug)]
pub(crate) enum DbCommand {
    Inspect(DbInspectCommand),
    Reclaim(DbReclaimCommand),
    Offline(ReplayCutoffArgs),
}
