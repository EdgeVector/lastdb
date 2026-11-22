//! Publisher auto-incorporation of member proposal entries.
//!
//! After the publisher pulls member streams, each unresolved proposal is
//! either incorporated (Direct + `incorporated_from`) or discarded (a Heads
//! attestation that cites the proposal in `seen_heads` without merging it).

use std::collections::{BTreeMap, HashMap};

use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::{CasExpectation, KeyValue, Mutation, MutationType};
use mutation_log::{
    author_data, author_heads, data_record, decode_record_payload, decode_signature_b64url,
    entry_hash, DeviceHead, Hex32, IncorporatedFrom, SignedEntry, StreamKind,
};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use super::apply::AuthorIdentity;
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
        match decide(store, &entry).await? {
            Decision::Publish(ready) => {
                // FoldDB already holds the validated write from decide; author
                // the chain entry so receivers converge on the same winner.
                publish(host, histories, database_id, &identity, &entry, ready)?;
            }
            Decision::Discard => {
                discard(host, histories, database_id, &identity, &entry)?;
            }
            Decision::Skip => {}
        }
    }
    let history = histories.snapshot(database_id)?;
    materialize::apply_history_records(store, &history).await
}

enum Decision {
    Publish(Ready),
    Discard,
    Skip,
}

struct Ready {
    namespace: String,
    key: String,
    value: Vec<u8>,
}

struct Intent {
    mutation_type: MutationType,
    expected: CasExpectation,
    fields: HashMap<String, Value>,
    value: Vec<u8>,
}

async fn decide(store: &FoldDB, entry: &SignedEntry) -> Result<Decision, String> {
    let SignedEntry::Data { body, .. } = entry else {
        return Ok(Decision::Skip);
    };
    let Ok(payload) = decode_record_payload(&body.payload) else {
        return Ok(Decision::Discard);
    };
    let Ok(intent) = parse_intent(&payload.value) else {
        return Ok(Decision::Discard);
    };
    let author = entry.header().node_key.to_hex();
    let target = RecordTarget {
        schema: body.namespace.clone(),
        key: KeyValue::new(Some(payload.record_key.clone()), None),
        alternates: BTreeMap::default(),
    };
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        intent.fields,
        intent.mutation_type,
        intent.expected,
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
        Err(_) => Ok(Decision::Discard),
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
            CasExpectation::Absent
        }
        Some(Value::Object(exp)) if exp.get("type").and_then(Value::as_str) == Some("revision") => {
            let revision = exp
                .get("revision")
                .and_then(Value::as_str)
                .ok_or(())?
                .to_owned();
            CasExpectation::Revision { revision }
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

fn discard(
    host: &Host,
    histories: &HistoryMap,
    database_id: &str,
    identity: &AuthorIdentity,
    proposal: &SignedEntry,
) -> Result<(), String> {
    let target = Target {
        home: &host.home,
        database_id,
        identity,
    };
    chain_author::author_publisher(histories, &target, |mut ctx, history| {
        cite_proposal(&mut ctx, proposal).map_err(|_| ChainError::Unconfirmed)?;
        let tips: Vec<DeviceHead> = history
            .stream_heads_of(identity.node_key)
            .into_iter()
            .map(|(device_id, head)| DeviceHead {
                device_id,
                writer_seq: head.writer_seq,
                entry_hash: head.entry_hash,
            })
            .collect();
        author_heads(&ctx, &identity.signing_key, tips).map_err(|_| ChainError::Unconfirmed)
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
