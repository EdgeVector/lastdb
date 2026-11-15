//! The publisher's download and settlement of the separate pending mutation log.
//!
//! Intake checks each encrypted submission using the publisher's own settings.
//! The publisher uploads accepted changes to the published log before marking
//! the pending entries processed. Receivers sync on the published position.

use cloud_backup_protocol::v2::share_access::{
    MUTATION_LOG_INBOX_LIST_PATH, MUTATION_LOG_INBOX_SETTLE_PATH,
};
use cloud_backup_protocol::{canonical_bytes, DecimalU64, ProtocolError};
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
    // Schema and record validation must see the published database before this
    // device can classify a proposal as invalid and settle it.
    require_current_prefix(host, log).await?;
    let mut results = Vec::with_capacity(page.entries.len());
    for stored in &page.entries {
        if &stored.entry.writer.key == log.principal() {
            return Err("publisher entry was returned by the inbox".into());
        }
        results.push(contributions::apply(host, log, &stored.entry).await?);
    }
    contributions::publish_before_settlement(host, log).await?;
    settle(log, page.next, &results).await
}

async fn settle(
    log: &CloudLog,
    next: u64,
    results: &[contributions::ProposalResult],
) -> Result<(), String> {
    let mut request = serde_json::json!({
        "database_id": log.database_id(),
        "through": DecimalU64::new(next),
        "results": results,
    });
    let body = canonical_bytes(&request).map_err(|_| "inbox settlement was not encoded")?;
    let (status, body) = log
        .post_status(MUTATION_LOG_INBOX_SETTLE_PATH, body, http::Proof::Data)
        .await
        .map_err(|_| "inbox settlement request failed")?;
    if status == 200 {
        return Ok(());
    }
    // Older ExeMem cannot decode the optional result field.
    let legacy = status == 400
        && serde_json::from_slice::<ProtocolError>(&body)
            .is_ok_and(|error| error.field == "http.body");
    if !legacy {
        return Err(format!("inbox settlement returned HTTP {status}"));
    }
    request
        .as_object_mut()
        .expect("settlement object")
        .remove("results");
    let body = canonical_bytes(&request).map_err(|_| "inbox settlement was not encoded")?;
    log.post(MUTATION_LOG_INBOX_SETTLE_PATH, body)
        .await
        .map_err(|error| format!("inbox settlement failed: {error}"))?;
    Ok(())
}

async fn require_current_prefix(host: &Host, log: &CloudLog) -> Result<(), String> {
    let database = log.database_id();
    let through = super::owner_recovery::head(log).await?;
    if super::session::load_cursor(&host.home, database)? == through
        && crate::database_replay::prefix_durable(&host.home, database)
    {
        return Ok(());
    }
    Err("publisher must finish syncing published changes before processing proposals".into())
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
