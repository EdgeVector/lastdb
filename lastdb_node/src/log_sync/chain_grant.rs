//! Publisher grant and revoke: signed `grant` entries on the owner's stream.
//!
//! The signed chain is the only authority. A reduction or revoke carries a
//! cutoff for every device stream of the grantee this node holds. Devices this
//! node has not seen fold to cutoff 0, so nothing from them is accepted later.

use mutation_log::{
    author_grant, grant_raise, grant_reduce, GrantBody, Grantee, RevocationCutoff, Role,
    VerifiedHistory,
};
use serde_json::{json, Value};

use super::chain_author::{self, Authored, Target};
use super::chain_error::{require_enrolled, ChainError};
use super::chain_read;
use crate::host::Host;

const CUTOFFS_MAX: usize = 256;

/// Append a publisher-signed grant for `grantee` at `role`.
pub(crate) fn grant(
    host: &Host,
    selector: &str,
    grantee: Grantee,
    role: Role,
) -> Result<Value, ChainError> {
    if role == Role::Revoked {
        return Err(ChainError::Invalid("role must be read, submit, or write"));
    }
    publish(host, selector, grantee, role)
}

/// Append a publisher-signed revoke for a member key.
pub(crate) fn revoke(host: &Host, selector: &str, grantee: Grantee) -> Result<Value, ChainError> {
    if grantee == Grantee::Any {
        return Err(ChainError::Invalid("public read cannot be revoked"));
    }
    publish(host, selector, grantee, Role::Revoked)
}

fn publish(host: &Host, selector: &str, grantee: Grantee, role: Role) -> Result<Value, ChainError> {
    let entry = chain_read::owned_database(host, selector)?;
    require_enrolled(host, &entry.id)?;
    let identity = chain_author::identity_of(host)?;
    if grantee == Grantee::Member(identity.node_key) {
        return Err(ChainError::Invalid(
            "the publisher already holds every role",
        ));
    }
    let target = Target {
        home: &host.home,
        database_id: &entry.id,
        identity: &identity,
    };
    let authored = chain_author::author_publisher(&host.histories, &target, |ctx, history| {
        let body = grant_body(history, grantee, role)?;
        author_grant(&ctx, &identity.signing_key, body).map_err(|_| ChainError::Unconfirmed)
    })?;
    Ok(ack(&entry.id, role, &authored))
}

fn ack(database_id: &str, role: Role, authored: &Authored) -> Value {
    json!({
        "databaseId": database_id,
        "entryHash": authored.entry_hash.to_hex(),
        "writerSeq": authored.writer_seq,
        "role": role,
    })
}

fn grant_body(
    history: &VerifiedHistory,
    grantee: Grantee,
    role: Role,
) -> Result<GrantBody, ChainError> {
    let Grantee::Member(key) = grantee else {
        return match role {
            Role::Read => Ok(grant_raise(grantee, role)),
            _ => Err(ChainError::Invalid("public access can only be read")),
        };
    };
    match history.grants().role_of(&key) {
        None if role == Role::Revoked => Err(ChainError::NotFound("the grantee has no grant")),
        Some(previous) if role == Role::Revoked || role.is_lower_than(previous) => {
            Ok(grant_reduce(grantee, role, cutoffs(history, key)?))
        }
        _ => Ok(grant_raise(grantee, role)),
    }
}

fn cutoffs(
    history: &VerifiedHistory,
    grantee: mutation_log::NodeKey,
) -> Result<Vec<RevocationCutoff>, ChainError> {
    let tips = history.stream_heads_of(grantee);
    if tips.len() > CUTOFFS_MAX {
        return Err(ChainError::Conflict(
            "the grantee has too many device streams to cut off",
        ));
    }
    Ok(tips
        .into_iter()
        .map(|(device_id, head)| RevocationCutoff {
            device_id,
            writer_seq: head.writer_seq,
            entry_hash: head.entry_hash,
        })
        .collect())
}
