//! Apply one served mutation-log page. The node does not check who wrote an entry.

use mutation_log::StoredEntry;

use crate::host::Host;

use super::cloud::CloudLog;
use super::{delivered_entry, restore, PagePlan};

pub(super) async fn apply_served(
    host: &Host,
    log: &CloudLog,
    publisher: &str,
    entries: &[StoredEntry],
    cursor: u64,
) -> Result<PagePlan, String> {
    let delivered: Vec<_> = entries.iter().map(delivered_entry).collect();
    let mut files = restore::file_source(log.clone());
    crate::database_replay::replay_delivered(
        host,
        log.database_id(),
        publisher,
        &delivered,
        &mut files,
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(PagePlan::Ack(cursor))
}
