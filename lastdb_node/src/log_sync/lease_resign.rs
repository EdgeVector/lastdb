//! After a lease is closed, this node signs again, as proposals, the direct
//! changes of its own device that the closing excluded.
//!
//! Multiple excluded entries for the same `(namespace, record_key)` coalesce
//! to the final state per key, while recording superseded intermediate entry
//! IDs (and `Create` + `Delete` net no-ops) in `proposals.json` so they are
//! never proposed later.

use std::collections::BTreeMap;

use mutation_log::{entry_hash, EntryHash, SignedEntry, VerifiedHistory};
use serde_json::{json, Map, Value};

use super::chain_author::{self, AuthorIdentity};
use super::proposal::{self, ContributionError, Ledger, Submitted};
use crate::host::Host;

struct Redo {
    ids: Vec<String>,
    namespace: String,
    record_key: String,
    /// `None` when the net effect against `winner == None` is a no-op (e.g. `Create` + `Delete`).
    envelope: Option<Vec<u8>>,
}

struct KeyAccum {
    ids: Vec<String>,
    namespace: String,
    record_key: String,
    final_record: Option<Value>,
}

/// Sign again each excluded change of this device that is not yet in `proposals.json`,
/// coalesced per `(namespace, record_key)`. Returns how many proposals were signed.
pub(super) async fn resign_excluded(host: &Host, database_id: &str) -> Result<usize, String> {
    let identity = chain_author::identity_of(host).map_err(|e| e.reply().message.to_owned())?;
    let has_any = super::histories_for(host)
        .with_mut(database_id, |history| has_own_excluded(history, &identity))?;
    if !has_any {
        return Ok(0);
    }
    let lock = proposal::proposal_submit_lock(&host.home, database_id);
    let _guard = lock.lock().await;
    let mut ledger = proposal::load_ledger(&host.home, database_id).map_err(|e| e.to_string())?;
    let redo = super::histories_for(host).with_mut(database_id, |history| {
        excluded_changes(history, &identity, &ledger)
    })?;
    if redo.is_empty() {
        return Ok(0);
    }
    let mut signed = 0;
    let mut finish = Ok(());
    for item in redo {
        match apply_redo_item(host, database_id, &mut ledger, item) {
            Ok(true) => signed += 1,
            Ok(false) => {}
            Err(ContributionError::Forbidden) => break,
            Err(error) => {
                finish = Err(error.to_string());
                break;
            }
        }
    }
    if signed > 0 {
        if let Ok(pending) = super::history_store::pending_count(&host.home, database_id) {
            super::publish_state::note_pending(database_id, pending);
        }
    }
    finish.map(|()| signed)
}

fn apply_redo_item(
    host: &Host,
    database_id: &str,
    ledger: &mut Ledger,
    item: Redo,
) -> Result<bool, ContributionError> {
    let Some(envelope) = item.envelope else {
        for id in item.ids {
            proposal::fits_proposal_ledger(ledger, &id, "")?;
            ledger.insert(
                id,
                Submitted {
                    writer_seq: 0,
                    entry_hash: EntryHash::ZERO,
                    body_sha256: String::new(),
                },
            );
        }
        proposal::store_ledger(&host.home, database_id, ledger)?;
        return Ok(false);
    };
    let digest = proposal::hex_digest(&envelope);
    let mut probe = ledger.clone();
    for id in &item.ids {
        proposal::fits_proposal_ledger(&probe, id, &digest)?;
        probe.insert(
            id.clone(),
            Submitted {
                writer_seq: u64::MAX,
                entry_hash: EntryHash::ZERO,
                body_sha256: digest.clone(),
            },
        );
    }
    let authored = proposal::author_proposal(
        host,
        database_id,
        item.namespace,
        &item.record_key,
        &envelope,
    )?;
    for id in item.ids {
        ledger.insert(
            id,
            Submitted {
                writer_seq: authored.writer_seq,
                entry_hash: authored.entry_hash,
                body_sha256: digest.clone(),
            },
        );
    }
    proposal::store_ledger(&host.home, database_id, ledger)?;
    Ok(true)
}

fn has_own_excluded(history: &VerifiedHistory, me: &AuthorIdentity) -> bool {
    history.excluded_entries().into_iter().any(|entry| {
        matches!(
            entry,
            SignedEntry::Data { header, .. }
                if header.node_key == me.node_key && header.device_id == me.device_id
        )
    })
}

fn excluded_changes(history: &VerifiedHistory, me: &AuthorIdentity, ledger: &Ledger) -> Vec<Redo> {
    let mut groups: Vec<KeyAccum> = Vec::new();
    let mut index_by_key: BTreeMap<(String, String), usize> = BTreeMap::new();
    for entry in history.excluded_entries() {
        let SignedEntry::Data { header, .. } = entry else {
            continue;
        };
        if header.node_key != me.node_key || header.device_id != me.device_id {
            continue;
        }
        let Some((namespace, payload)) = history.decode_data_entry(entry) else {
            continue;
        };
        let Ok(hash) = entry_hash(entry) else {
            continue;
        };
        let id = id_of(&hash);
        if ledger.contains_key(&id) {
            continue;
        }
        let final_record = if payload.value.is_empty() {
            None
        } else {
            let Some(obj) = serde_json::from_slice::<Value>(&payload.value)
                .ok()
                .filter(Value::is_object)
            else {
                continue;
            };
            Some(obj)
        };
        let key = (namespace, payload.record_key);
        if let Some(&idx) = index_by_key.get(&key) {
            groups[idx].ids.push(id);
            groups[idx].final_record = final_record;
        } else {
            let idx = groups.len();
            index_by_key.insert(key.clone(), idx);
            groups.push(KeyAccum {
                ids: vec![id],
                namespace: key.0,
                record_key: key.1,
                final_record,
            });
        }
    }
    groups
        .into_iter()
        .filter_map(|acc| build_redo(history, acc))
        .collect()
}

fn build_redo(history: &VerifiedHistory, acc: KeyAccum) -> Option<Redo> {
    let key = (acc.namespace.clone(), acc.record_key.clone());
    let winner = history
        .records()
        .get(&key)
        .filter(|record| !record.value.is_empty())
        .map(|record| record.entry);
    let (kind, expected, record) = match (acc.final_record, winner) {
        (None, None) => {
            return Some(Redo {
                ids: acc.ids,
                namespace: acc.namespace,
                record_key: acc.record_key,
                envelope: None,
            });
        }
        (None, Some(won)) => ("delete", entry_expected(won), None),
        (Some(record), None) => ("create", json!({ "type": "absent" }), Some(record)),
        (Some(record), Some(won)) => ("replace", entry_expected(won), Some(record)),
    };
    let mut envelope = Map::new();
    envelope.insert("mutation_type".into(), json!(kind));
    envelope.insert("expected".into(), expected);
    if let Some(record) = record {
        envelope.insert("record".into(), record);
    }
    let envelope = serde_json::to_vec(&Value::Object(envelope)).ok()?;
    Some(Redo {
        ids: acc.ids,
        namespace: acc.namespace,
        record_key: acc.record_key,
        envelope: Some(envelope),
    })
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
