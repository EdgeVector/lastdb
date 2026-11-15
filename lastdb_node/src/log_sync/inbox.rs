//! The publisher's download and settlement of the separate pending mutation log.
//!
//! Intake checks each encrypted submission using the publisher's own settings.
//! The publisher uploads accepted changes to the published log before marking
//! the pending entries processed. Receivers sync on the published position.

use cloud_backup_protocol::v2::share_access::{
    MUTATION_LOG_INBOX_LIST_PATH, MUTATION_LOG_INBOX_SETTLE_PATH,
};
use cloud_backup_protocol::{canonical_bytes, DecimalU64};
use mutation_log::ListPage;

use crate::host::Host;

use super::{cloud::CloudLog, contributions, http, wire};

pub(super) async fn drain(host: &Host, log: &CloudLog) -> Result<(), String> {
    let Some(page) = list(log).await? else {
        return Ok(());
    };
    if page.entries.is_empty() {
        return Ok(());
    }
    for stored in &page.entries {
        if &stored.entry.writer.key == log.principal() {
            return Err("publisher entry was returned by the inbox".into());
        }
        contributions::apply(host, log, &stored.entry).await?;
    }
    contributions::publish_before_settlement(host, log).await?;
    let request = serde_json::json!({
        "database_id": log.database_id(),
        "through": DecimalU64::new(page.next),
    });
    let body = canonical_bytes(&request).map_err(|_| "inbox settlement was not encoded")?;
    log.post(MUTATION_LOG_INBOX_SETTLE_PATH, body)
        .await
        .map_err(|error| format!("inbox settlement failed: {error}"))?;
    Ok(())
}

async fn list(log: &CloudLog) -> Result<Option<ListPage>, String> {
    let database = log.database_id();
    let body = wire::database_body(database).map_err(|error| error.to_string())?;
    let (status, reply) = log
        .post_status_limit(
            MUTATION_LOG_INBOX_LIST_PATH,
            body,
            http::Proof::Data,
            http::JSON_LIMIT,
        )
        .await
        .map_err(|_| "inbox list request failed")?;
    match status {
        200 => wire::list_page(database, &reply)
            .map(Some)
            .map_err(|error| error.to_string()),
        // During rollout the previous service still accepts and delivers legacy
        // submissions through the old publisher pull.
        404 => Ok(None),
        _ => Err(format!("inbox list returned HTTP {status}")),
    }
}
