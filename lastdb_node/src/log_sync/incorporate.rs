//! Publisher auto-incorporation of member proposal entries.
//!
//! Only the publisher's signature changes a database. After the publisher
//! pulls member streams, it decides each undecided proposal once, in the order
//! the members signed them:
//!
//! - accepted: the change is written and signed as a direct entry that cites
//!   the proposal in `incorporated_from`;
//! - not accepted: the publisher signs a `heads` entry that cites the proposal.
//!
//! A `write` grant lets a member change any record. A `submit` grant lets it
//! create records and change the ones it created. A proposal whose expected
//! record state no longer holds is not accepted. A failure that is not about
//! the proposal (storage, signing) decides nothing, and the next turn retries.

use std::collections::HashMap;

use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::{CasExpectation, Mutation, MutationType};
use fold_db::schema::SchemaError;
use mutation_log::{
    author_data, author_heads, data_record, decode_record_payload, decode_signature_b64url,
    entry_hash, EntryHash, Hex32, IncorporatedFrom, NodeKey, Role, SignedEntry, StreamKind,
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
    let lock = histories.write_lock(database_id);
    let _guard = lock.lock().await;
    let pending = histories.with_mut(database_id, |history| {
        history
            .unresolved_proposals()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>()
    })?;
    if pending.is_empty() {
        return Ok(());
    }
    for entry in pending {
        match decide(store, histories, database_id, &entry).await? {
            Decision::Accept(ready) => {
                // The store already holds the validated write from decide. An
                // entry that cannot be signed leaves no record behind.
                let key = (ready.namespace.clone(), ready.key.clone());
                if let Err(error) = publish(host, histories, database_id, &identity, &entry, ready)
                {
                    materialize::revert_unsigned(host, database_id, store, &[key]).await;
                    return Err(error);
                }
            }
            Decision::Decline => decline(host, histories, database_id, &identity, &entry)?,
        }
    }
    let history = histories.snapshot(database_id)?;
    materialize::apply_history_records(store, &history).await
}

enum Decision {
    Accept(Ready),
    Decline,
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
    alternates: signed_history_materialize::Alternates,
}

/// Decide one proposal. `Err` means nothing was decided.
async fn decide(
    store: &FoldDB,
    histories: &HistoryMap,
    database_id: &str,
    entry: &SignedEntry,
) -> Result<Decision, String> {
    let SignedEntry::Data { body, .. } = entry else {
        return Ok(Decision::Decline);
    };
    let Ok(payload) = decode_record_payload(&body.payload) else {
        return Ok(Decision::Decline);
    };
    let Ok(intent) = parse_intent(&payload.value) else {
        return Ok(Decision::Decline);
    };
    let proposer = entry.header().node_key;
    let Ok(canon_key) = signed_history_materialize::canonical_record_key_from_joined(
        store,
        &body.namespace,
        &payload.record_key,
    ) else {
        return Ok(Decision::Decline);
    };
    let key = (body.namespace.clone(), canon_key);
    let history = histories.snapshot(database_id)?;
    signed_history_materialize::reconcile_keys(store, &history, std::slice::from_ref(&key)).await?;
    let standing = Standing {
        role: history.grants().role_of(&proposer),
        winner: history
            .records()
            .get(&key)
            .filter(|record| !record.value.is_empty())
            .map(|record| (record.entry, record.creator)),
    };
    if !standing.allows(&intent.expected, &proposer) {
        return Ok(Decision::Decline);
    }
    let Ok(at) = signed_history_materialize::key_value(store, &key.0, &key.1) else {
        return Ok(Decision::Decline);
    };
    let expected = match intent.expected {
        Expect::Absent => CasExpectation::Absent,
        Expect::Entry(_) => {
            let revision = signed_history_materialize::read_revision(store, &key.0, &key.1)
                .await?
                .ok_or("the store lacks a record its history holds")?;
            CasExpectation::Revision { revision }
        }
    };
    let Some(prepared) = prepare_target_payload(store, &key.0, &key.1, &intent).await? else {
        return Ok(Decision::Decline);
    };
    let target = RecordTarget {
        schema: key.0.clone(),
        key: at,
        alternates: prepared.alternates,
    };
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        intent.fields,
        intent.mutation_type,
        expected,
    );
    mutation.writer = Some(proposer.to_hex());
    // The store validates the change (schema, field types, key shape) and
    // writes it, so the next proposal in this turn sees it.
    match store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await
    {
        Ok(_) => Ok(Decision::Accept(Ready {
            namespace: key.0,
            key: key.1,
            value: prepared.wire_value,
        })),
        Err(error) if about_the_proposal(&error) => Ok(Decision::Decline),
        Err(error) => Err(error.to_string()),
    }
}

struct TargetPayload {
    alternates: signed_history_materialize::Alternates,
    wire_value: Vec<u8>,
}

async fn prepare_target_payload(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
    intent: &Intent,
) -> Result<Option<TargetPayload>, String> {
    if intent.mutation_type == MutationType::Delete {
        let alternates = signed_history_materialize::read_alternates(store, namespace, record_key)
            .await?
            .unwrap_or_default();
        return Ok(Some(TargetPayload {
            alternates,
            wire_value: Vec::new(),
        }));
    }
    let Ok(alternates) =
        signed_history_materialize::resolve_alternates(store, namespace, intent.alternates.clone())
    else {
        return Ok(None);
    };
    let fields_json = Value::Object(
        intent
            .fields
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    );
    let Ok(wire_value) = signed_history_materialize::encode_record_value(&fields_json, &alternates)
    else {
        return Ok(None);
    };
    Ok(Some(TargetPayload {
        alternates,
        wire_value,
    }))
}

/// What the publisher's history says about a proposer and the record it names.
struct Standing {
    role: Option<Role>,
    /// The live record's winning entry and its creator, when one exists.
    winner: Option<(EntryHash, NodeKey)>,
}

impl Standing {
    /// A `write` grant may change any record. A `submit` grant may create a
    /// record and change one it created. The expected state must be the one
    /// the history holds: a record's revision is node local, so a proposal
    /// names the history entry that wrote the record.
    fn allows(&self, expected: &Expect, proposer: &NodeKey) -> bool {
        let Some(role @ (Role::Write | Role::Submit)) = self.role else {
            return false;
        };
        match (expected, self.winner) {
            (Expect::Absent, None) => true,
            (Expect::Entry(Some(want)), Some((have, creator))) => {
                *want == have && (role == Role::Write || creator == *proposer)
            }
            _ => false,
        }
    }
}

/// True when the store refused the write because of what the proposal asks,
/// not because the store could not serve it.
fn about_the_proposal(error: &SchemaError) -> bool {
    matches!(
        error,
        SchemaError::NotFound(_)
            | SchemaError::Blocked(_)
            | SchemaError::InvalidField(_)
            | SchemaError::InvalidData(_)
            | SchemaError::CasConflict { .. }
            | SchemaError::RecordTooLarge { .. }
    )
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
    let (fields, alternates) = if kind == "delete" {
        (
            HashMap::new(),
            signed_history_materialize::Alternates::new(),
        )
    } else {
        let record = doc.get("record").ok_or(())?;
        let raw = serde_json::to_vec(record).map_err(|_| ())?;
        let mut decoded = signed_history_materialize::decode_record_value(&raw)
            .map_err(|_| ())?
            .ok_or(())?;
        if let Some(val) = doc.get("alternates").filter(|v| !v.is_null()) {
            decoded.alternates = serde_json::from_value(val.clone()).map_err(|_| ())?;
        }
        (decoded.fields, decoded.alternates)
    };
    Ok(Intent {
        mutation_type,
        expected,
        fields,
        alternates,
    })
}

fn cite_proposal(ctx: &mut mutation_log::AuthorCtx, proposal: &SignedEntry) -> Result<(), String> {
    let hash = entry_hash(proposal).map_err(|e| e.to_string())?;
    ctx.cite(hash, proposal.header().lamport);
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
    chain_author::author_publisher(histories, &target, |mut ctx, history| {
        if let Some((hash, lamport)) = history.birth_entry() {
            ctx.cite(hash, lamport);
        }
        cite_proposal(&mut ctx, proposal).map_err(|_| ChainError::Unconfirmed)?;
        let key = (ready.namespace.clone(), ready.key.clone());
        if let Some(winner) = history.records().get(&key) {
            ctx.cite(winner.entry, winner.position.lamport);
        }
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

/// Sign that a proposal is not accepted: a `heads` entry that cites it. The
/// decision is final, and the proposer's node reads it as `not_accepted`.
fn decline(
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
        history.cite_publisher_streams(&mut ctx);
        cite_proposal(&mut ctx, proposal).map_err(|_| ChainError::Unconfirmed)?;
        author_heads(&ctx, &identity.signing_key, history.publisher_stream_tips())
            .map_err(|_| ChainError::Unconfirmed)
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
