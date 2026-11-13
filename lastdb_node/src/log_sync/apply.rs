//! Acknowledge local writes and replay published entries into the selected database.

use std::path::{Path, PathBuf};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use mutation_log::{Entry, Incorporation, StoredEntry};
use mutation_log_sync::Outcome;

use crate::database_replay::{DeliveredEntry, IncorporatedFrom, SnapshotEntry};
use crate::host::Host;

use super::cloud::CloudLog;
use super::disk;
use super::session;

const MARKER_LIMIT: usize = 64;

pub(crate) enum PagePlan {
    /// The page was handled through this delivery sequence.
    Ack(u64),
    /// Stopped mid-page. Ack only when a delivery sequence was fully handled.
    Hold(Option<u64>),
}

pub(crate) async fn apply_page(
    host: &Host,
    log: &CloudLog,
    outcome: &Outcome,
    publisher: Option<&str>,
) -> Result<PagePlan, String> {
    let Outcome::Pulled {
        entries,
        cursor,
        reset,
    } = outcome
    else {
        return Err("pull did not return a page".into());
    };
    if reset.is_some() {
        return Err("pulled page still carries a snapshot reset".to_owned());
    }
    if let Some(publisher) = publisher {
        return received_page(host, log, publisher, entries, *cursor).await;
    }
    let owned_private = host
        .owned
        .is_private_id(log.database_id())
        .map_err(|_| "owned database was not resolved".to_owned())?;
    if owned_private {
        let delivered: Vec<_> = entries.iter().map(delivered_entry).collect();
        let mut files = super::restore::file_source(log.clone());
        crate::database_replay::replay_owned(
            host,
            log.database_id(),
            Some(log.device().as_str()),
            &delivered,
            &mut files,
        )
        .await
        .map_err(|error| error.to_string())?;
        return Ok(PagePlan::Ack(*cursor));
    }
    let mut handled = None;
    for stored in entries {
        let entry = &stored.entry;
        // An owned store may have independent local writes beyond its delivery
        // cursor. Only acknowledge writes this device already applied here.
        if entry.database_id.as_str() != log.database_id()
            || &entry.writer.key != log.principal()
            || &entry.writer.device != log.device()
            || !mark_local(host, log.database_id(), entry)?
        {
            return Ok(PagePlan::Hold(handled));
        }
        handled = Some(stored.delivery_seq);
    }
    Ok(PagePlan::Ack(*cursor))
}

fn mark_local(host: &Host, database_id: &str, entry: &Entry) -> Result<bool, String> {
    let name = format!("{}-{}", entry.writer.device.as_str(), entry.writer_seq);
    if !session::label_ok(entry.writer.device.as_str()) {
        return Ok(false);
    }
    let marker = marker_path(&host.home, database_id, &name);
    match disk::read_limited(&marker, MARKER_LIMIT) {
        Ok(Some(_)) => Ok(true),
        Ok(None) => disk::write_private(&marker, b"1")
            .map(|()| true)
            .map_err(|_| "applied marker was not stored".to_owned()),
        Err(_) => Ok(false),
    }
}

async fn received_page(
    host: &Host,
    log: &CloudLog,
    publisher: &str,
    entries: &[StoredEntry],
    cursor: u64,
) -> Result<PagePlan, String> {
    let delivered: Vec<DeliveredEntry> = entries.iter().map(delivered_entry).collect();
    let mut files = super::restore::file_source(log.clone());
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

pub(super) fn delivered_entry(stored: &StoredEntry) -> DeliveredEntry {
    let entry = &stored.entry;
    DeliveredEntry {
        delivery_seq: stored.delivery_seq,
        database_id: entry.database_id.as_str().to_owned(),
        entry: SnapshotEntry {
            accepted_at_ms: stored.accepted_at_ms,
            writer_key: entry.writer.key.as_str().to_owned(),
            writer_device: entry.writer.device.as_str().to_owned(),
            writer_seq: entry.writer_seq,
            authored_at_ms: entry.authored_at_ms,
            namespace: entry.namespace.as_str().to_owned(),
            incorporated_from: entry.incorporated_from.as_ref().map(incorporated_from),
            payload_b64: URL_SAFE_NO_PAD.encode(&entry.payload),
        },
    }
}

fn incorporated_from(source: &Incorporation) -> IncorporatedFrom {
    IncorporatedFrom {
        writer_key: source.writer.key.as_str().to_owned(),
        writer_device: source.writer.device.as_str().to_owned(),
        writer_seq: source.writer_seq,
    }
}

fn marker_path(home: &Path, database_id: &str, mutation_id: &str) -> PathBuf {
    home.join("log-sync")
        .join(database_id)
        .join("applied")
        .join(mutation_id)
}
