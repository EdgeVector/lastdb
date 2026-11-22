//! Author a member direct `data` entry after a received-database `/v1/write`.
//!
//! FoldDB already holds the local commit. The signed entry lands on this node's
//! own `(node_key, device_id)` stream with `grant_at` bound to a publisher write
//! grant in `seen_heads`, then joins the received database's signed log for upload.

use std::collections::BTreeSet;

use mutation_log::{
    author_data, data_record, AuthorCtx, DeviceId, EntryHash, Grantee, NodeKey, Role, StreamKind,
    VerifiedHistory,
};

use super::chain_author::{self, Target};
use super::chain_error::ChainError;
use super::mirror::OwnedMutation;
use crate::host::Host;
use crate::registered_share::RegisteredShare;

/// Author one signed direct entry per mutation on the member stream.
pub(crate) fn mirror_received_write(
    host: &Host,
    share: &RegisteredShare,
    mutations: &[OwnedMutation],
) -> Result<(), ChainError> {
    if mutations.is_empty() {
        return Ok(());
    }
    let identity = chain_author::identity_of(host)?;
    let target = Target {
        home: &host.home,
        database_id: share.id.as_str(),
        identity: &identity,
    };
    let histories = super::histories_for(&host.home);
    for item in mutations {
        chain_author::author_member(&histories, &target, |mut ctx, history| {
            require_writer(history, &identity.node_key)?;
            let (grant_device, grant_seq) = write_grant_at(history, &identity.node_key)?;
            cite_observed_heads(&mut ctx, history);
            let value = if item.op == fold_db::schema::types::MutationType::Delete {
                Vec::new()
            } else {
                let fields = item.fields.as_ref().ok_or(ChainError::Unconfirmed)?;
                serde_json::to_vec(fields).map_err(|_| ChainError::Unconfirmed)?
            };
            let mut body = data_record(
                StreamKind::Direct,
                item.schema.clone(),
                &item.record_key,
                &value,
            );
            body.grant_at_device = Some(grant_device);
            body.grant_at_seq = Some(grant_seq);
            author_data(&ctx, &identity.signing_key, body).map_err(|_| ChainError::Unconfirmed)
        })?;
    }
    Ok(())
}

/// Grants for auth, plus every observed stream tip so Lamport follows the
/// state this node just read (and LWW cannot silently keep an older value).
fn cite_observed_heads(ctx: &mut AuthorCtx, history: &VerifiedHistory) {
    for (hash, lamport) in history.publisher_grants() {
        push_seen(ctx, hash, lamport);
    }
    let mut nodes = BTreeSet::new();
    if let Some(owner) = history.owner() {
        nodes.insert(owner);
    }
    for (key, _) in history.grants().members() {
        nodes.insert(key);
    }
    for node in nodes {
        for (_device, head) in history.stream_heads_of(node) {
            push_seen(ctx, head.entry_hash, head.lamport);
        }
    }
}

fn push_seen(ctx: &mut AuthorCtx, hash: EntryHash, lamport: u64) {
    if ctx.seen_heads.contains(&hash) {
        return;
    }
    ctx.seen_heads.push(hash);
    ctx.seen_lamports.push(lamport);
}

fn require_writer(history: &VerifiedHistory, node_key: &NodeKey) -> Result<(), ChainError> {
    if history.owner().is_none() || history.pending_len() > 0 {
        return Err(ChainError::Unconfirmed);
    }
    match history.grants().role_of(node_key) {
        Some(Role::Write) => Ok(()),
        _ => Err(ChainError::Forbidden(
            "this member cannot write this shared database",
        )),
    }
}

/// Latest publisher grant that raises this node to `write`, for `grant_at`.
fn write_grant_at(
    history: &VerifiedHistory,
    node_key: &NodeKey,
) -> Result<(DeviceId, u64), ChainError> {
    let mut best: Option<(u64, NodeKey, DeviceId, EntryHash, DeviceId, u64)> = None;
    for (hash, lamport) in history.publisher_grants() {
        let Some(meta) = history.get_accepted(&hash) else {
            continue;
        };
        let Some(body) = meta.grant_body.as_ref() else {
            continue;
        };
        if body.role != Role::Write {
            continue;
        }
        let Grantee::Member(grantee) = body.grantee else {
            continue;
        };
        if grantee != *node_key {
            continue;
        }
        let header = meta.entry.header();
        let rank = (
            lamport,
            header.node_key,
            header.device_id,
            hash,
            header.device_id,
            header.writer_seq,
        );
        if best.as_ref().is_none_or(|held| rank > *held) {
            best = Some(rank);
        }
    }
    best.map(|(_, _, _, _, device, seq)| (device, seq))
        .ok_or(ChainError::Forbidden(
            "this member holds no write grant in the publisher history",
        ))
}
