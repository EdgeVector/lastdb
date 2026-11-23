//! After a lease is closed, this node signs again, as proposals, the direct
//! changes of its own device that the closing excluded.
//!
//! An excluded entry stays in its stream and writes nothing. The history says
//! what each record is now, so the proposal names that state as its expected
//! one, and the publisher decides it like any other proposal. Each excluded
//! entry gets one proposal: its id comes from the entry hash, and the ledger of
//! proposals remembers it.

use mutation_log::{decode_record_payload, entry_hash, EntryHash, SignedEntry, VerifiedHistory};
use serde_json::{json, Map, Value};

use super::chain_author::{self, AuthorIdentity};
use super::proposal::{self, ContributionError, Submitted};
use crate::host::Host;

struct Redo {
    id: String,
    namespace: String,
    record_key: String,
    envelope: Vec<u8>,
}

/// Sign again each excluded change of this device that is not yet a proposal.
/// Returns how many proposals were signed. A member that may no longer propose
/// signs none.
pub(super) fn resign_excluded(host: &Host, database_id: &str) -> Result<usize, String> {
    let identity = chain_author::identity_of(host).map_err(|e| e.reply().message.to_owned())?;
    let redo = super::histories_for(host)
        .with_mut(database_id, |history| excluded_changes(history, &identity))?;
    if redo.is_empty() {
        return Ok(0);
    }
    let mut ledger = proposal::load_ledger(&host.home, database_id).map_err(|e| e.to_string())?;
    let mut signed = 0;
    for item in redo {
        if ledger.contains_key(&item.id) {
            continue;
        }
        let digest = proposal::hex_digest(&item.envelope);
        match proposal::author_proposal(
            host,
            database_id,
            item.namespace,
            &item.record_key,
            &item.envelope,
        ) {
            Ok(authored) => {
                ledger.insert(
                    item.id,
                    Submitted {
                        writer_seq: authored.writer_seq,
                        entry_hash: authored.entry_hash,
                        body_sha256: digest,
                    },
                );
                proposal::store_ledger(&host.home, database_id, &ledger)
                    .map_err(|e| e.to_string())?;
                signed += 1;
            }
            // The role no longer lets this member propose. Nothing more to do.
            Err(ContributionError::Forbidden) => break,
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(signed)
}

fn excluded_changes(history: &VerifiedHistory, me: &AuthorIdentity) -> Vec<Redo> {
    let mut found = Vec::new();
    for entry in history.excluded_entries() {
        let SignedEntry::Data { header, body } = entry else {
            continue;
        };
        if header.node_key != me.node_key || header.device_id != me.device_id {
            continue;
        }
        let (Ok(payload), Ok(hash)) = (decode_record_payload(&body.payload), entry_hash(entry))
        else {
            continue;
        };
        let key = (body.namespace.clone(), payload.record_key.clone());
        let winner = history
            .records()
            .get(&key)
            .filter(|record| !record.value.is_empty())
            .map(|record| record.entry);
        let record = serde_json::from_slice::<Value>(&payload.value)
            .ok()
            .filter(Value::is_object);
        let (kind, expected, record) = match (payload.value.is_empty(), winner, record) {
            (true, None, _) | (false, _, None) => continue,
            (true, Some(won), _) => ("delete", entry_expected(won), None),
            (false, None, Some(record)) => ("create", json!({ "type": "absent" }), Some(record)),
            (false, Some(won), Some(record)) => ("replace", entry_expected(won), Some(record)),
        };
        let mut envelope = Map::new();
        envelope.insert("mutation_type".into(), json!(kind));
        envelope.insert("expected".into(), expected);
        if let Some(record) = record {
            envelope.insert("record".into(), record);
        }
        let Ok(envelope) = serde_json::to_vec(&Value::Object(envelope)) else {
            continue;
        };
        found.push(Redo {
            id: id_of(&hash),
            namespace: key.0,
            record_key: key.1,
            envelope,
        });
    }
    found
}

fn entry_expected(won: EntryHash) -> Value {
    json!({ "type": "entry", "entry": won.to_hex() })
}

/// The proposal id of an excluded entry: the first 32 hex digits of its hash,
/// written as a UUID, so the same entry always gets the same id.
fn id_of(hash: &EntryHash) -> String {
    let hex = hash.to_hex();
    uuid::Uuid::parse_str(&hex[..32]).map_or(hex, |id| id.to_string())
}
