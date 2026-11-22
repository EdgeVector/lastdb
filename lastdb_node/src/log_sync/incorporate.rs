//! Publisher auto-incorporation of member proposal entries.
//!
//! After the publisher pulls member streams, each unresolved proposal is
//! either incorporated (Direct + `incorporated_from`) or discarded (a Heads
//! attestation that cites the proposal in `seen_heads` without merging it).

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::{CasExpectation, MutationType};
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

struct PublishArgs<'a> {
    host: &'a Host,
    histories: &'a HistoryMap,
    database_id: &'a str,
    identity: &'a AuthorIdentity,
    proposal: &'a SignedEntry,
    ready: Ready,
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
    // CAS only: FoldDB writes wait for rematerialize so authorship is the member.
    let revision =
        signed_history_materialize::read_revision(store, &body.namespace, &payload.record_key)
            .await?;
    let live = revision.is_some();
    let matches = match &intent.expected {
        CasExpectation::Absent => !live,
        CasExpectation::Revision { revision: want } => revision.as_deref() == Some(want.as_str()),
    };
    if !matches {
        return Ok(Decision::Discard);
    }
    if intent.mutation_type == MutationType::Delete && !live {
        return Ok(Decision::Discard);
    }
    Ok(Decision::Publish(Ready {
        namespace: body.namespace.clone(),
        key: payload.record_key,
        value: intent.value,
    }))
}

struct Intent {
    mutation_type: MutationType,
    expected: CasExpectation,
    value: Vec<u8>,
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
    let wire_value = if kind == "delete" {
        Vec::new()
    } else {
        let record = doc.get("record").ok_or(())?;
        if !record.is_object() {
            return Err(());
        }
        serde_json::to_vec(record).map_err(|_| ())?
    };
    Ok(Intent {
        mutation_type,
        expected,
        value: wire_value,
    })
}

fn publish(
    host: &Host,
    histories: &HistoryMap,
    database_id: &str,
    identity: &AuthorIdentity,
    proposal: &SignedEntry,
    ready: Ready,
) -> Result<(), String> {
    let args = PublishArgs {
        host,
        histories,
        database_id,
        identity,
        proposal,
        ready,
    };
    publish_ready(args)
}

fn publish_ready(args: PublishArgs<'_>) -> Result<(), String> {
    let from = citation(args.proposal)?;
    let proposal_hash = entry_hash(args.proposal).map_err(|e| e.to_string())?;
    let target = Target {
        home: &args.host.home,
        database_id: args.database_id,
        identity: args.identity,
    };
    let Ready {
        namespace,
        key,
        value,
    } = args.ready;
    chain_author::author_publisher(args.histories, &target, |mut ctx, _history| {
        ctx.seen_heads.push(proposal_hash);
        let mut body = data_record(StreamKind::Direct, namespace, &key, &value);
        body.incorporated_from = Some(from);
        author_data(&ctx, &args.identity.signing_key, body).map_err(|_| ChainError::Unconfirmed)
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
    let proposal_hash = entry_hash(proposal).map_err(|e| e.to_string())?;
    let target = Target {
        home: &host.home,
        database_id,
        identity,
    };
    chain_author::author_publisher(histories, &target, |mut ctx, history| {
        ctx.seen_heads.push(proposal_hash);
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
