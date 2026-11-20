//! Off-chain (side-channel) revoke must not change AcceptEntry's outcome.
//! After a real on-chain revoke, an ordinary next-seq member append is refused.

use mutation_log::{
    author_grant, entry_hash, grant_reduce, AcceptOutcome, RejectReason, RevocationCutoff, Role,
    VerifiedHistory,
};

use crate::device::{
    author_member_write, bootstrap_owner, member_write, must_accept, must_reject, now_ms,
    owner_grant_write, owner_revoke, stream_head_of, DataWrite, DbBirth, Device,
};
pub fn run() -> Result<String, String> {
    let owner = Device::generate();
    let member = Device::generate();
    let outsider = Device::generate();
    let birth = DbBirth::generate(owner.node_key);
    let t0 = now_ms();
    let (mut history, birth_entry) = bootstrap_owner(&owner, &birth, t0)?;
    let grant = owner_grant_write(
        &mut history,
        &owner,
        birth.database_id,
        member.node_key,
        t0 + 1,
    )?;
    let member_entry = accept_member_once(
        &mut history,
        &birth_entry,
        &grant,
        &member,
        birth.database_id,
        t0 + 2,
    )?;

    // Side-channel: non-publisher "revoke" offered to AcceptEntry → Reject; S unchanged.
    let fake = outsider_revoke(&outsider, &member, birth.database_id, &member_entry, t0 + 3)?;
    match must_reject(&mut history, fake)? {
        RejectReason::Unauthorized(msg) if msg.contains("publisher") => {}
        other => return Err(format!("expected publisher Unauthorized, got {other:?}")),
    }

    // Side-channel: a real owner-signed revoke that never enters S must be ignored.
    let withheld = author_owner_revoke_unaccepted(
        &history,
        &owner,
        birth.database_id,
        member.node_key,
        &member_entry,
        t0 + 4,
    )?;
    // Do not AcceptEntry(withheld). Member ordinary append must still Accept.
    let still_ok = author_member_write(
        &history,
        &member,
        &grant,
        &DataWrite {
            database_id: birth.database_id,
            namespace: "demo",
            record_key: "row",
            value: b"still-granted",
            authored_at_ms: t0 + 5,
        },
    )?;
    must_accept(&mut history, still_ok)?;

    // On-chain revoke (AcceptEntry the withheld one would also work; author a fresh tip).
    let _ = withheld;
    let tip = history
        .stream_head(&VerifiedHistory::stream(member.node_key, member.device_id))
        .ok_or("missing member tip after off-chain ignore")?;
    let cutoff = RevocationCutoff {
        device_id: member.device_id,
        writer_seq: tip.writer_seq,
        entry_hash: tip.entry_hash,
    };
    let _revoke = owner_revoke(
        &mut history,
        &owner,
        birth.database_id,
        member.node_key,
        cutoff,
        t0 + 6,
    )?;

    // Ordinary next-seq append after on-chain revoke → Reject (not a backdated rewrite).
    let late = author_member_write(
        &history,
        &member,
        &grant,
        &DataWrite {
            database_id: birth.database_id,
            namespace: "demo",
            record_key: "row",
            value: b"after-revoke",
            authored_at_ms: t0 + 7,
        },
    )?;
    match must_reject(&mut history, late)? {
        RejectReason::Unauthorized(msg) if msg.contains("cutoff") || msg.contains("role") => {}
        other => {
            return Err(format!(
                "expected ordinary post-revoke Unauthorized, got {other:?}"
            ))
        }
    }

    Ok("off-chain revoke ignored; ordinary next-seq append after on-chain revoke Reject".into())
}

fn accept_member_once(
    history: &mut VerifiedHistory,
    birth_entry: &mutation_log::SignedEntry,
    grant: &mutation_log::SignedEntry,
    member: &Device,
    database_id: mutation_log::DatabaseId,
    authored_at_ms: u64,
) -> Result<mutation_log::SignedEntry, String> {
    let mut member_hist = VerifiedHistory::new(database_id);
    for e in [birth_entry, grant] {
        match member_hist.accept_entry(e.clone()) {
            AcceptOutcome::Accept { .. } => {}
            other => return Err(format!("member bootstrap: {other:?}")),
        }
    }
    let written = member_write(
        &mut member_hist,
        member,
        grant,
        &DataWrite {
            database_id,
            namespace: "demo",
            record_key: "row",
            value: b"ok",
            authored_at_ms,
        },
    )?;
    must_accept(history, written.clone())?;
    Ok(written)
}

fn outsider_revoke(
    outsider: &Device,
    member: &Device,
    database_id: mutation_log::DatabaseId,
    member_entry: &mutation_log::SignedEntry,
    authored_at_ms: u64,
) -> Result<mutation_log::SignedEntry, String> {
    let cutoff = RevocationCutoff {
        device_id: member.device_id,
        writer_seq: member_entry.header().writer_seq,
        entry_hash: entry_hash(member_entry).map_err(|e| e.to_string())?,
    };
    let ctx = outsider.author_ctx(database_id, None, Vec::new(), Vec::new(), authored_at_ms);
    let body = grant_reduce(
        mutation_log::Grantee::Member(member.node_key),
        Role::Revoked,
        vec![cutoff],
    );
    author_grant(&ctx, &outsider.signing_key, body).map_err(|e| e.to_string())
}

fn author_owner_revoke_unaccepted(
    history: &VerifiedHistory,
    owner: &Device,
    database_id: mutation_log::DatabaseId,
    member: mutation_log::NodeKey,
    member_entry: &mutation_log::SignedEntry,
    authored_at_ms: u64,
) -> Result<mutation_log::SignedEntry, String> {
    let cutoff = RevocationCutoff {
        device_id: member_entry.header().device_id,
        writer_seq: member_entry.header().writer_seq,
        entry_hash: entry_hash(member_entry).map_err(|e| e.to_string())?,
    };
    let head = stream_head_of(history, owner);
    let ctx = owner.author_ctx(database_id, head, Vec::new(), Vec::new(), authored_at_ms);
    let body = grant_reduce(
        mutation_log::Grantee::Member(member),
        Role::Revoked,
        vec![cutoff],
    );
    author_grant(&ctx, &owner.signing_key, body).map_err(|e| e.to_string())
}
