//! Publisher auto-incorporation of member proposal entries.
//!
//! After the publisher pulls member streams, each unresolved proposal that
//! applies cleanly is incorporated (Direct + `incorporated_from`). Invalid or
//! CAS-failing proposals are left alone (Decision 11: silent rejection — no
//! discard call, no discard log entry).

use std::collections::{BTreeMap, HashMap};

use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::{CasExpectation, Mutation, MutationType};
use mutation_log::{
    author_data, data_record, decode_record_payload, decode_signature_b64url, entry_hash,
    EntryHash, Hex32, IncorporatedFrom, SignedEntry, StreamKind,
};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use super::chain_author::AuthorIdentity;
use super::chain_author::{self, Target};
use super::chain_error::ChainError;
use super::history_store::HistoryMap;
use super::materialize;
use crate::host::Host;

/// Resolve every unresolved proposal on an owned shareable database.
pub(super) async fn resolve_owned(
    host: &Host,
    histories: &HistoryMap,
    store: &FoldDB,
    database_id: &str,
) -> Result<(), String> {
    let identity = chain_author::identity_of(host).map_err(|e| e.reply().message.to_owned())?;
    let pending = histories
        .peek_with(database_id, |history| {
            history
                .unresolved_proposals()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if pending.is_empty() {
        return Ok(());
    }
    for entry in pending {
        match decide(store, histories, database_id, &entry).await? {
            Decision::Publish(ready) => {
                // FoldDB already holds the validated write from decide; author
                // the chain entry so receivers converge on the same winner.
                publish(host, histories, database_id, &identity, &entry, ready)?;
            }
            // Invalid / CAS-fail: silent. No discard RPC, field, or log entry.
            Decision::Skip => {}
        }
    }
    let history = histories.snapshot(database_id)?;
    materialize::apply_history_records(store, &history).await
}

enum Decision {
    Publish(Ready),
    Skip,
}

struct Ready {
    namespace: String,
    key: String,
    value: Vec<u8>,
}

/// The record state a proposal expects, in history terms.
enum Expect {
    Absent,
    /// The history entry that wrote the record. `None` names no entry, so it
    /// never matches.
    Entry(Option<EntryHash>),
}

struct Intent {
    mutation_type: MutationType,
    expected: Expect,
    fields: HashMap<String, Value>,
    value: Vec<u8>,
}

async fn decide(
    store: &FoldDB,
    histories: &HistoryMap,
    database_id: &str,
    entry: &SignedEntry,
) -> Result<Decision, String> {
    let SignedEntry::Data { body, .. } = entry else {
        return Ok(Decision::Skip);
    };
    let Ok(payload) = decode_record_payload(&body.payload) else {
        return Ok(Decision::Skip);
    };
    let Ok(intent) = parse_intent(&payload.value) else {
        return Ok(Decision::Skip);
    };
    let author = entry.header().node_key.to_hex();
    let Some(expected) = local_expected(
        store,
        histories,
        database_id,
        (&body.namespace, &payload.record_key),
        &intent.expected,
    )
    .await?
    else {
        return Ok(Decision::Skip);
    };
    let Ok(key) =
        signed_history_materialize::key_value(store, &body.namespace, &payload.record_key)
    else {
        return Ok(Decision::Skip);
    };
    let target = RecordTarget {
        schema: body.namespace.clone(),
        key,
        alternates: BTreeMap::default(),
    };
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        intent.fields,
        intent.mutation_type,
        expected,
    );
    mutation.writer = Some(author);
    // Apply now: validates schema/types, advances CAS state for the next
    // proposal in this turn, and stamps the member as content author.
    match store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await
    {
        Ok(_) => Ok(Decision::Publish(Ready {
            namespace: body.namespace.clone(),
            key: payload.record_key,
            value: intent.value,
        })),
        Err(_) => Ok(Decision::Skip),
    }
}

/// Check a proposal's expectation against this publisher's history, then give
/// FoldDB the local expectation that matches it. A record's revision is node
/// local, so the check is on the history entry that wrote the record.
async fn local_expected(
    store: &FoldDB,
    histories: &HistoryMap,
    database_id: &str,
    key: (&str, &str),
    expected: &Expect,
) -> Result<Option<CasExpectation>, String> {
    let (namespace, record_key) = key;
    let owned_key = (namespace.to_owned(), record_key.to_owned());
    let winner = histories.with_mut(database_id, |history| {
        history
            .records()
            .get(&owned_key)
            .map(|record| (record.entry, record.value.is_empty()))
    })?;
    match (expected, winner) {
        (Expect::Absent, None | Some((_, true))) => Ok(Some(CasExpectation::Absent)),
        (Expect::Entry(Some(want)), Some((have, false))) if *want == have => {
            let revision =
                signed_history_materialize::read_revision(store, namespace, record_key).await?;
            Ok(revision.map(|revision| CasExpectation::Revision { revision }))
        }
        _ => Ok(None),
    }
}

fn parse_intent(value: &[u8]) -> Result<Intent, ()> {
    let Value::Object(doc) = serde_json::from_slice(value).map_err(|_| ())? else {
        return Err(());
    };
    let kind = doc.get("mutation_type").and_then(Value::as_str).ok_or(())?;
    let mutation_type = match kind {
        "create" => MutationType::Create,
        "replace" => MutationType::Replace,
        "delete" => MutationType::Delete,
        _ => return Err(()),
    };
    let expected = match doc.get("expected") {
        Some(Value::Object(exp)) if exp.get("type").and_then(Value::as_str) == Some("absent") => {
            Expect::Absent
        }
        Some(Value::Object(exp)) if exp.get("type").and_then(Value::as_str) == Some("entry") => {
            match exp.get("entry") {
                Some(Value::String(hex)) => {
                    Expect::Entry(Some(EntryHash::from_hex(hex).map_err(|_| ())?))
                }
                Some(Value::Null) => Expect::Entry(None),
                _ => return Err(()),
            }
        }
        _ => return Err(()),
    };
    let (fields, wire_value) = if kind == "delete" {
        (HashMap::new(), Vec::new())
    } else {
        let record = doc.get("record").ok_or(())?;
        let Value::Object(map) = record else {
            return Err(());
        };
        let fields = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let wire = serde_json::to_vec(record).map_err(|_| ())?;
        (fields, wire)
    };
    Ok(Intent {
        mutation_type,
        expected,
        fields,
        value: wire_value,
    })
}

fn cite_proposal(ctx: &mut mutation_log::AuthorCtx, proposal: &SignedEntry) -> Result<(), String> {
    let hash = entry_hash(proposal).map_err(|e| e.to_string())?;
    ctx.seen_heads.push(hash);
    ctx.seen_lamports.push(proposal.header().lamport);
    Ok(())
}

fn publish(
    host: &Host,
    histories: &HistoryMap,
    database_id: &str,
    identity: &AuthorIdentity,
    proposal: &SignedEntry,
    ready: Ready,
) -> Result<(), String> {
    let from = citation(proposal)?;
    let target = Target {
        home: &host.home,
        database_id,
        identity,
    };
    chain_author::author_publisher(histories, &target, |mut ctx, _history| {
        cite_proposal(&mut ctx, proposal).map_err(|_| ChainError::Unconfirmed)?;
        let mut body = data_record(
            StreamKind::Direct,
            ready.namespace,
            &ready.key,
            &ready.value,
        );
        body.incorporated_from = Some(from);
        author_data(&ctx, &identity.signing_key, body).map_err(|_| ChainError::Unconfirmed)
    })
    .map(|_| ())
    .map_err(|e| e.reply().message.to_owned())
}

fn citation(proposal: &SignedEntry) -> Result<IncorporatedFrom, String> {
    let header = proposal.header();
    let sig = decode_signature_b64url(&header.signature).map_err(|e| e.to_string())?;
    let digest = Sha256::digest(sig);
    Ok(IncorporatedFrom {
        node_key: header.node_key,
        device_id: header.device_id,
        writer_seq: header.writer_seq,
        signature_sha256: Hex32::from_bytes(digest.into()),
    })
}
