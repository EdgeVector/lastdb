//! Sign the publisher's decision on a proposal: incorporate or decline.

use mutation_log::{author_heads, decode_signature_b64url, Hex32, IncorporatedFrom, SignedEntry};
use sha2::{Digest as _, Sha256};

use super::chain_author::AuthorIdentity;
use super::chain_author::{self, Target};
use super::chain_error::ChainError;
use super::history_store::HistoryMap;
use super::shared_key;
use crate::host::Host;

pub(super) struct Ready {
    pub(super) namespace: String,
    pub(super) key: String,
    pub(super) plain: Vec<u8>,
}

/// Sign the accepting direct entry that cites `proposal`.
pub(super) fn publish(
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
    let sealer = shared_key::sealer(host, database_id)?;
    chain_author::author_publisher(histories, &target, |mut ctx, history| {
        if let Some((hash, lamport)) = history.birth_entry() {
            ctx.cite(hash, lamport);
        }
        if let Some((hash, lamport)) = history.schema_entry(&ready.namespace) {
            ctx.cite(hash, lamport);
        }
        cite_proposal(&mut ctx, proposal).map_err(|_| ChainError::Unconfirmed)?;
        let key = (ready.namespace.clone(), ready.key.clone());
        if let Some(winner) = history.records().get(&key) {
            ctx.cite(winner.entry, winner.position.lamport);
        }
        crate::database_author::sign_direct(&ctx, identity, &sealer, &ready.plain, None, Some(from))
            .map_err(|_| ChainError::Unconfirmed)
    })
    .map(|_| ())
    .map_err(|e| e.reply().message.to_owned())
}

/// Sign that a proposal is not accepted: a `heads` entry that cites it.
pub(super) fn decline(
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
        if let Some((hash, lamport)) = history.birth_entry() {
            ctx.cite(hash, lamport);
        }
        cite_proposal(&mut ctx, proposal).map_err(|_| ChainError::Unconfirmed)?;
        history.cite_publisher_streams(&mut ctx);
        author_heads(&ctx, &identity.signing_key, history.publisher_stream_tips())
            .map_err(|_| ChainError::Unconfirmed)
    })
    .map(|_| ())
    .map_err(|e| e.reply().message.to_owned())
}

fn cite_proposal(ctx: &mut mutation_log::AuthorCtx, proposal: &SignedEntry) -> Result<(), String> {
    ctx.cite(
        mutation_log::entry_hash(proposal).map_err(|e| e.to_string())?,
        proposal.header().lamport,
    );
    Ok(())
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
