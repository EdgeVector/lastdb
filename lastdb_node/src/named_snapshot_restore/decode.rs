//! Open one entry. No object fetch and no write.
//!
//! The part payload is an envelope, not canonical JSON. The opened bytes are
//! the accepted body from [`super::payload`]. Plaintext is refused.

use std::collections::HashMap;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use cloud_backup_protocol::bytes_sha256;

use crate::log_envelope::{self, DatabaseContentKey, IncorporatedRef, LogBinding};

use super::chain::Link;
use super::payload::{self, Change};
use super::types::{IncorporatedFrom, SnapshotEntry};
use super::RestoreError;

pub(super) use payload::mutation_label;

pub(super) struct Prepared {
    pub id: String,
    pub hash: String,
    pub change: Change,
}

pub(super) fn check_chain(
    key: &DatabaseContentKey,
    database_id: &str,
    links: &[Link],
) -> Result<(), RestoreError> {
    let mut seen = HashMap::new();
    for link in links {
        for part in &link.parts {
            for entry in &part.entries {
                note_entry(&mut seen, &decode_entry(key, database_id, entry)?)?;
            }
        }
    }
    Ok(())
}

pub(super) fn decode_entry(
    key: &DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
) -> Result<Prepared, RestoreError> {
    let sealed = URL_SAFE_NO_PAD
        .decode(entry.payload_b64.as_bytes())
        .map_err(|_| RestoreError::Entry("snapshot entry"))?;
    let opened = open_sealed(
        key,
        database_id,
        entry,
        &sealed,
        RestoreError::Entry("snapshot entry"),
    )?;
    let body = payload::parse_accepted(&opened)?;
    check_namespace(&body.schema, &entry.namespace)?;
    Ok(Prepared {
        id: body.mutation_id.clone(),
        hash: bytes_sha256(&opened).as_str().to_owned(),
        change: payload::classify(&body)?,
    })
}

/// Open one envelope. Plaintext and a bad binding are refused.
pub(super) fn open_sealed(
    key: &DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
    sealed: &[u8],
    failure: RestoreError,
) -> Result<Vec<u8>, RestoreError> {
    match log_envelope::open(key, &binding(database_id, entry), sealed) {
        Ok(plain) => Ok(plain),
        Err(log_envelope::LogEnvelopeError::Key) => Err(RestoreError::Scope("database store")),
        Err(_) => Err(failure),
    }
}

fn binding<'a>(database_id: &'a str, entry: &'a SnapshotEntry) -> LogBinding<'a> {
    LogBinding {
        database_id,
        writer_key: &entry.writer_key,
        writer_device: &entry.writer_device,
        writer_seq: entry.writer_seq,
        authored_at_ms: entry.authored_at_ms,
        namespace: &entry.namespace,
        incorporated_from: entry.incorporated_from.as_ref().map(incorporated),
    }
}

fn incorporated(from: &IncorporatedFrom) -> IncorporatedRef<'_> {
    IncorporatedRef {
        writer_key: &from.writer_key,
        writer_device: &from.writer_device,
        writer_seq: from.writer_seq,
    }
}

fn note_entry(seen: &mut HashMap<String, String>, prepared: &Prepared) -> Result<(), RestoreError> {
    if let Some(prior) = seen.insert(prepared.id.clone(), prepared.hash.clone()) {
        if prior != prepared.hash {
            return Err(RestoreError::Entry("mutation id"));
        }
    }
    Ok(())
}

fn check_namespace(schema: &str, namespace: &str) -> Result<(), RestoreError> {
    let prefix = schema.split_once('/').map_or(schema, |(prefix, _)| prefix);
    if prefix == namespace {
        Ok(())
    } else {
        Err(RestoreError::Entry("snapshot entry"))
    }
}
