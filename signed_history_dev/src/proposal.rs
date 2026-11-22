//! Member proposals and the publisher's decision on them.
//!
//! Only the publisher's signature changes a database. A member signs a
//! proposal; the publisher incorporates it (a direct entry that cites it) or
//! declines it (a `heads` entry that cites it).

use mutation_log::{
    author_data, author_heads, data_record, decode_signature_b64url, entry_hash, EntryHash, Hex32,
    IncorporatedFrom, SignedEntry, StreamKind, VerifiedHistory,
};
use sha2::{Digest, Sha256};

use crate::device::{must_accept, stream_head_of, DataWrite, Device};

/// Author a member entry on `stream` without accepting it. `grant` is the
/// publisher grant the member cites in `seen_heads`, or `None` for no citation.
pub fn author_member_entry(
    history: &VerifiedHistory,
    member: &Device,
    stream: StreamKind,
    grant: Option<&SignedEntry>,
    write: &DataWrite<'_>,
) -> Result<SignedEntry, String> {
    let (seen, lamports) = match grant {
        Some(g) => (
            vec![entry_hash(g).map_err(|e| e.to_string())?],
            vec![g.header().lamport],
        ),
        None => (Vec::new(), Vec::new()),
    };
    let head = stream_head_of(history, member);
    let ctx = member.author_ctx(
        write.database_id,
        head,
        seen,
        lamports,
        write.authored_at_ms,
    );
    let body = data_record(stream, write.namespace, write.record_key, write.value);
    author_data(&ctx, &member.signing_key, body).map_err(|e| e.to_string())
}

/// Publisher incorporation of `proposal`: a direct entry that carries `write`
/// and cites the proposal. Accepted into `history`.
pub fn owner_incorporate(
    history: &mut VerifiedHistory,
    owner: &Device,
    proposal: &SignedEntry,
    write: &DataWrite<'_>,
) -> Result<SignedEntry, String> {
    let head = stream_head_of(history, owner);
    let ctx = owner.author_ctx(
        write.database_id,
        head,
        vec![entry_hash(proposal).map_err(|e| e.to_string())?],
        vec![proposal.header().lamport],
        write.authored_at_ms,
    );
    let mut body = data_record(
        StreamKind::Direct,
        write.namespace,
        write.record_key,
        write.value,
    );
    body.incorporated_from = Some(citation(proposal)?);
    let entry = author_data(&ctx, &owner.signing_key, body).map_err(|e| e.to_string())?;
    must_accept(history, entry.clone())?;
    Ok(entry)
}

/// Publisher "not accepted" for `proposal`: a `heads` entry that cites it.
/// Accepted into `history`.
pub fn owner_decline(
    history: &mut VerifiedHistory,
    owner: &Device,
    proposal: &SignedEntry,
    authored_at_ms: u64,
) -> Result<SignedEntry, String> {
    let head = stream_head_of(history, owner);
    let ctx = owner.author_ctx(
        proposal.header().database_id,
        head,
        vec![entry_hash(proposal).map_err(|e| e.to_string())?],
        vec![proposal.header().lamport],
        authored_at_ms,
    );
    let heads = history.publisher_stream_tips();
    let entry = author_heads(&ctx, &owner.signing_key, heads).map_err(|e| e.to_string())?;
    must_accept(history, entry.clone())?;
    Ok(entry)
}

/// The hash of an entry.
pub fn hash_of(entry: &SignedEntry) -> Result<EntryHash, String> {
    entry_hash(entry).map_err(|e| e.to_string())
}

/// True when the publisher incorporated `proposal` in `history`.
pub fn incorporated(history: &VerifiedHistory, proposal: &SignedEntry) -> bool {
    let h = proposal.header();
    history.proposal_incorporated(
        &VerifiedHistory::stream(h.node_key, h.device_id),
        h.writer_seq,
    )
}

/// True when the publisher declined `proposal` in `history`.
pub fn declined(history: &VerifiedHistory, proposal: &SignedEntry) -> Result<bool, String> {
    Ok(history.proposal_declined(&hash_of(proposal)?))
}

fn citation(proposal: &SignedEntry) -> Result<IncorporatedFrom, String> {
    let h = proposal.header();
    let sig = decode_signature_b64url(&h.signature).map_err(|e| e.to_string())?;
    let digest = Sha256::digest(sig);
    Ok(IncorporatedFrom {
        node_key: h.node_key,
        device_id: h.device_id,
        writer_seq: h.writer_seq,
        signature_sha256: Hex32::from_bytes(digest.into()),
    })
}
