//! Pull, apply, save local download progress, and drain the proposal log.

use std::path::Path;
use std::sync::atomic::AtomicBool;

use cloud_backup_protocol::v2::share_access::{
    MUTATION_LOG_INBOX_LIST_PATH, MUTATION_LOG_INBOX_SETTLE_PATH,
};
use cloud_backup_protocol::{canonical_bytes, DecimalU64};
use mutation_log::{DatabaseId, DeviceId, ListPage, LogError};

use crate::host::{BlockedDelivery, Host};

use super::accepted;
use super::cloud::CloudLog;
use super::turn::{self, Pause};
use super::{apply_page, contributions, http, session, wire, PagePlan, SyncOutcome, SyncState};

static SNAPSHOT_WARNED: AtomicBool = AtomicBool::new(false);

pub(super) struct Ready {
    pub(super) database_id: String,
    /// Publisher of a received log. An owned database leaves this empty.
    pub(super) publisher: Option<String>,
    pub(super) log: CloudLog,
    pub(super) state: SyncState,
}

impl Ready {
    fn reports_personal(&self, host: &Host) -> bool {
        self.publisher.is_none()
            && host
                .owned
                .is_private_id(&self.database_id)
                .is_ok_and(|private| private)
    }

    fn record_download(
        &self,
        host: &Host,
        through: u64,
        error: Option<String>,
        blocked: Option<BlockedDelivery>,
    ) {
        if self.reports_personal(host) {
            host.cloud_sync
                .record_download(&self.database_id, through, error, blocked);
        }
    }

    fn download_failed(&self, host: &Host, error: impl Into<String>) -> Pause {
        self.record_download(host, self.state.cursor, Some(error.into()), None);
        Pause::Retry
    }
}

pub(super) async fn run_ready(host: &Host, ready: Ready) -> Pause {
    if ready.publisher.is_none() {
        if let Err(reason) = accepted::publish_owned(host, &ready.log).await {
            if ready.reports_personal(host) {
                host.cloud_sync
                    .record_upload_error(&ready.database_id, reason);
            }
            host.cloud_sync.record_error(reason);
            tracing::warn!(target: "lastdbd::log_sync", database_id = %ready.database_id,
                %reason, "owned pull awaits safe local capture");
            return Pause::Retry;
        }
    }
    let pause = pull_and_checkpoint(host, &ready).await;
    if ready.publisher.is_none()
        && crate::host::owned::find_shareable(&host.home, &ready.database_id)
            .is_ok_and(|entry| entry.whole_database())
    {
        if let Err(error) = drain_inbox(host, &ready.log).await {
            tracing::warn!(target: "lastdbd::log_sync", database_id = %ready.database_id,
                %error, "private submission inbox awaits retry");
            return Pause::Retry;
        }
    }
    pause
}

async fn pull_and_checkpoint(host: &Host, ready: &Ready) -> Pause {
    if super::native_reset::supports(host, ready) {
        let after = ready.state.cursor;
        match ready.log.list_with_native_reset(after).await {
            Ok(Ok(page)) => {
                return apply_contiguous(host, ready, after, page).await;
            }
            Ok(Err(cut)) => {
                return match super::native_reset::prepare(host, &ready.log, after, &cut).await {
                    Ok(()) => Pause::Reopen,
                    Err(error) => {
                        tracing::warn!(target: "lastdbd::log_sync", %error, "native recovery awaits retry");
                        ready.download_failed(host, "native recovery awaits retry")
                    }
                };
            }
            Err(error) => {
                if matches!(error, LogError::CursorExpired { .. }) {
                    turn::warn_once(
                        &SNAPSHOT_WARNED,
                        "snapshot restore stopped before this database could advance",
                    );
                }
                tracing::warn!(target: "lastdbd::log_sync", %error, "pull failed");
                return ready.download_failed(host, format!("cloud pull failed: {error}"));
            }
        }
    }
    let outcome = match ready.log.drive_once(ready.state).await {
        Ok(pulled) => pulled,
        Err(error) => {
            if matches!(error, LogError::CursorExpired { .. }) {
                turn::warn_once(
                    &SNAPSHOT_WARNED,
                    "snapshot restore stopped before this database could advance",
                );
            }
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "pull failed");
            return ready.download_failed(host, format!("cloud pull failed: {error}"));
        }
    };
    apply_and_checkpoint(host, ready, &outcome).await
}

fn contiguous(after: u64, page: &ListPage) -> bool {
    page.entries.iter().try_fold(after, |last, entry| {
        (last.checked_add(1) == Some(entry.delivery_seq)).then_some(entry.delivery_seq)
    }) == Some(page.next)
}

async fn apply_contiguous(host: &Host, ready: &Ready, after: u64, page: ListPage) -> Pause {
    if !contiguous(after, &page) {
        tracing::warn!(target: "lastdbd::log_sync", "personal delivery page has a gap");
        return ready.download_failed(host, "personal delivery page has a gap");
    }
    let outcome = SyncOutcome {
        entries: page.entries,
        cursor: page.next,
    };
    apply_and_checkpoint(host, ready, &outcome).await
}

async fn apply_and_checkpoint(host: &Host, ready: &Ready, outcome: &SyncOutcome) -> Pause {
    let plan = match apply_page(host, &ready.log, outcome, ready.publisher.as_deref()).await {
        Ok(plan) => plan,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error.message, "pulled page was not applied");
            ready.record_download(host, ready.state.cursor, Some(error.message), error.blocked);
            return Pause::Retry;
        }
    };
    match plan {
        PagePlan::Ack(seq) | PagePlan::Hold(Some(seq)) => checkpoint_download(host, ready, seq),
        PagePlan::Hold(None) => Pause::Retry,
    }
}

fn checkpoint_download(host: &Host, ready: &Ready, seq: u64) -> Pause {
    if let Err(error) = session::store_cursor(&host.home, &ready.database_id, seq) {
        tracing::warn!(target: "lastdbd::log_sync", error = %error, "delivery cursor was not stored");
        return ready.download_failed(host, error);
    }
    if let Err(error) = session::mark_prefix(&host.home, &ready.database_id, seq) {
        tracing::warn!(target: "lastdbd::log_sync", error = %error, "durable prefix was not stored");
        ready.record_download(host, seq, Some(error), None);
        return Pause::Retry;
    }
    ready.record_download(host, seq, None, None);
    Pause::Idle
}

pub(super) fn opened_state(
    database_id: &str,
    log: &CloudLog,
    device: &str,
    home: &Path,
) -> Result<SyncState, String> {
    let database_id = DatabaseId::new(database_id).map_err(|_| "database id was refused")?;
    let device = DeviceId::new(device).map_err(|_| "device id was refused")?;
    if log.device() != &device {
        return Err("device id was refused".into());
    }
    let cursor = session::load_cursor(home, database_id.as_str())?;
    Ok(SyncState { cursor })
}

async fn drain_inbox(host: &Host, log: &CloudLog) -> Result<(), String> {
    let page = inbox_list(log).await?;
    if page.entries.is_empty() {
        return Ok(());
    }
    let database = log.database_id();
    let through = super::owner_recovery::head(log).await?;
    if session::load_cursor(&host.home, database)? != through
        || !crate::database_replay::prefix_durable(&host.home, database)
    {
        return Err(
            "publisher must finish syncing published changes before processing proposals".into(),
        );
    }
    // The first active proposal confirms the settings statement with the
    // service and keeps that map for the page. A failed confirmation returns
    // before settlement, so the page stays on the proposal log.
    let mut member_writes = None;
    let mut results = Vec::with_capacity(page.entries.len());
    for stored in &page.entries {
        if &stored.entry.writer.key == log.principal() {
            return Err("publisher entry was returned by the inbox".into());
        }
        results.push(contributions::apply(host, log, &stored.entry, &mut member_writes).await?);
    }
    contributions::publish_before_settlement(host, log).await?;
    let body = canonical_bytes(&serde_json::json!({
        "database_id": database,
        "through": DecimalU64::new(page.next),
        "results": results,
    }))
    .map_err(|_| "inbox settlement was not encoded")?;
    let (status, _) = log
        .post_status(MUTATION_LOG_INBOX_SETTLE_PATH, body, http::Proof::Data)
        .await
        .map_err(|_| "inbox settlement request failed")?;
    if status == 200 {
        Ok(())
    } else {
        Err(format!("inbox settlement returned HTTP {status}"))
    }
}

async fn inbox_list(log: &CloudLog) -> Result<ListPage, String> {
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
    if status == 200 {
        wire::list_page(database, &reply).map_err(|error| error.to_string())
    } else {
        Err(format!("inbox list returned HTTP {status}"))
    }
}
