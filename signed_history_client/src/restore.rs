//! Restore: page every named stream from seq 1 and run AcceptEntry.

use mutation_log::{AcceptOutcome, DatabaseId, SignedEntry, VerifiedHistory};

use crate::relay::RelayClient;

/// One device stream to pull (`writer_key` is hex node key).
#[derive(Clone, Debug)]
pub struct StreamRef {
    /// Hex-encoded writer node key.
    pub writer_key: String,
    /// Hex32 device id for that writer stream.
    pub device_id: String,
}

/// Inputs for a restore-from-start against a grant-gating relay.
pub struct RestoreRequest {
    /// Relay origin URL.
    pub relay_url: String,
    /// Signing key for HTTP `nodeSignature` (the relay requires publisher, a read grant, or `grant#any`).
    pub signing_key: app_identity_crypto::SigningKey,
    /// Hex node key matching `signing_key`.
    pub node_key_hex: String,
    /// Device id for HTTP signatures (need not match a stream being restored).
    pub device_id: String,
    /// Hex32 database id.
    pub database_id: String,
    /// Streams to pull from seq 1 (typically publisher devices first).
    pub streams: Vec<StreamRef>,
}

/// Rebuilt history plus restore counters.
pub struct RestoreReport {
    /// Verified history after AcceptEntry on every pulled entry.
    pub history: VerifiedHistory,
    /// Non-duplicate accepted entries.
    pub replayed: u64,
    /// Max `writer_seq` among accepted (non-duplicate) entries.
    pub through: u64,
}

const PAGE: u32 = 90;

/// Pull each stream from seq 1, AcceptEntry locally, fail if reject/pending remain.
pub async fn restore_from_relay(req: RestoreRequest) -> Result<RestoreReport, String> {
    if req.streams.is_empty() {
        return Err("signed restore requires at least one --device-id stream".into());
    }
    let database_id =
        DatabaseId::from_hex(&req.database_id).map_err(|_| "database_id is not hex32")?;
    let client = RelayClient::new(
        req.relay_url,
        req.signing_key,
        req.node_key_hex,
        req.device_id,
    );
    let mut history = VerifiedHistory::new(database_id);
    let mut replayed = 0u64;
    let mut through = 0u64;
    for stream in &req.streams {
        pull_stream(
            &client,
            &mut history,
            &req.database_id,
            stream,
            &mut replayed,
            &mut through,
        )
        .await?;
    }
    if history.pending_len() > 0 {
        return Err(format!(
            "signed restore left {} pending entries (missing seen_heads)",
            history.pending_len()
        ));
    }
    if !history.rejects().is_empty() {
        return Err(format!(
            "signed restore rejected {} entries",
            history.rejects().len()
        ));
    }
    Ok(RestoreReport {
        history,
        replayed,
        through,
    })
}

async fn pull_stream(
    client: &RelayClient,
    history: &mut VerifiedHistory,
    database_id: &str,
    stream: &StreamRef,
    replayed: &mut u64,
    through: &mut u64,
) -> Result<(), String> {
    let mut from_seq = 1u64;
    let mut cursor: Option<String> = None;
    let mut empty_pages = 0u32;
    loop {
        let (entries, next) = client
            .read(
                database_id,
                &stream.writer_key,
                &stream.device_id,
                from_seq,
                cursor.as_deref(),
                PAGE,
            )
            .await?;
        let empty = entries.is_empty();
        let page_len = entries.len();
        if let Some(last) = entries.last() {
            let last_seq = last.header().writer_seq;
            if cursor.is_none() && last_seq < from_seq {
                return Err(format!(
                    "relay returned seq {last_seq} for a read from {from_seq}"
                ));
            }
            from_seq = last_seq.saturating_add(1);
        }
        for entry in entries {
            accept_one(history, entry, replayed, through)?;
        }
        if let Some(next_cursor) = next {
            if empty {
                empty_pages += 1;
                if cursor.as_deref() == Some(next_cursor.as_str()) || empty_pages > 256 {
                    return Err("relay log.read cursor did not advance".into());
                }
            } else {
                empty_pages = 0;
            }
            cursor = Some(next_cursor);
            continue;
        }
        if empty || (page_len as u32) < PAGE {
            break;
        }
        cursor = None;
    }
    Ok(())
}

fn accept_one(
    history: &mut VerifiedHistory,
    entry: SignedEntry,
    replayed: &mut u64,
    through: &mut u64,
) -> Result<(), String> {
    let seq = entry.header().writer_seq;
    match history.accept_entry(entry) {
        AcceptOutcome::Accept { duplicate, .. } => {
            if !duplicate {
                *replayed = replayed.saturating_add(1);
                *through = (*through).max(seq);
            }
            Ok(())
        }
        AcceptOutcome::Pending { reason } => {
            // Buffered inside VerifiedHistory; later accepts may drain it.
            let _ = reason;
            Ok(())
        }
        AcceptOutcome::Reject { reason } => Err(format!("restore AcceptEntry reject: {reason:?}")),
    }
}
