//! B. Write grant → write → revoke with cutoff → post-cutoff AcceptEntry Reject.
//! Also: new device_id after revoke → cutoff 0 → Reject.

use mutation_log::{
    entry_hash, DeviceId, RejectReason, RevocationCutoff, SignedEntry, VerifiedHistory,
};
use uuid::Uuid;

use crate::device::{
    author_member_write, bootstrap_owner, member_write, must_reject, now_ms, owner_grant_write,
    owner_revoke, DataWrite, DbBirth, Device,
};
use crate::relay::{RelayClient, RelayHandle};

pub async fn run(relay: &RelayHandle) -> Result<String, String> {
    let owner = Device::generate();
    let member = Device::generate();
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
    let member_entry = accept_member_write(
        &mut history,
        &birth_entry,
        &grant,
        &member,
        birth.database_id,
        t0 + 2,
    )?;

    let cutoff = RevocationCutoff {
        device_id: member.device_id,
        writer_seq: member_entry.header().writer_seq,
        entry_hash: entry_hash(&member_entry).map_err(|e| e.to_string())?,
    };
    let revoke = owner_revoke(
        &mut history,
        &owner,
        birth.database_id,
        member.node_key,
        cutoff,
        t0 + 3,
    )?;

    let mut member_view = VerifiedHistory::new(birth.database_id);
    for e in [&birth_entry, &grant, &member_entry, &revoke] {
        match member_view.accept_entry(e.clone()) {
            mutation_log::AcceptOutcome::Accept { .. } => {}
            other => return Err(format!("member view accept: {other:?}")),
        }
    }
    reject_post_cutoff(
        &mut history,
        &member_view,
        &member,
        &grant,
        birth.database_id,
        t0 + 4,
        relay,
    )
    .await?;
    reject_new_device_cutoff(
        &mut history,
        &member_view,
        &member,
        &grant,
        birth.database_id,
        t0 + 5,
    )?;
    Ok("post-cutoff + new-device cutoff-0 Reject(Unauthorized); relay stored grant-blind".into())
}

fn accept_member_write(
    history: &mut VerifiedHistory,
    birth_entry: &SignedEntry,
    grant: &SignedEntry,
    member: &Device,
    database_id: mutation_log::DatabaseId,
    authored_at_ms: u64,
) -> Result<SignedEntry, String> {
    let mut member_hist = VerifiedHistory::new(database_id);
    for e in [birth_entry, grant] {
        match member_hist.accept_entry(e.clone()) {
            mutation_log::AcceptOutcome::Accept { .. } => {}
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
    match history.accept_entry(written.clone()) {
        mutation_log::AcceptOutcome::Accept { .. } => {}
        other => return Err(format!("owner accept member write: {other:?}")),
    }
    Ok(written)
}

async fn reject_post_cutoff(
    history: &mut VerifiedHistory,
    member_view: &VerifiedHistory,
    member: &Device,
    grant: &SignedEntry,
    database_id: mutation_log::DatabaseId,
    authored_at_ms: u64,
    relay: &RelayHandle,
) -> Result<(), String> {
    let post = author_member_write(
        member_view,
        member,
        grant,
        &DataWrite {
            database_id,
            namespace: "demo",
            record_key: "row",
            value: b"late",
            authored_at_ms,
        },
    )?;
    let client = RelayClient::new(&relay.base_url, member);
    client
        .append(&database_id.to_hex(), std::slice::from_ref(&post))
        .await?;
    expect_cutoff(must_reject(history, post)?)
}

fn reject_new_device_cutoff(
    history: &mut VerifiedHistory,
    member_view: &VerifiedHistory,
    member: &Device,
    grant: &SignedEntry,
    database_id: mutation_log::DatabaseId,
    authored_at_ms: u64,
) -> Result<(), String> {
    let member_new = Device {
        signing_key: member.signing_key.clone(),
        node_key: member.node_key,
        device_id: DeviceId::from_uuid(Uuid::new_v4()),
    };
    let entry = author_member_write(
        member_view,
        &member_new,
        grant,
        &DataWrite {
            database_id,
            namespace: "demo",
            record_key: "row",
            value: b"new-device",
            authored_at_ms,
        },
    )?;
    expect_cutoff(must_reject(history, entry)?)
}

fn expect_cutoff(reason: RejectReason) -> Result<(), String> {
    match reason {
        RejectReason::Unauthorized(msg) if msg.contains("cutoff") => Ok(()),
        other => Err(format!("expected cutoff Unauthorized, got {other:?}")),
    }
}
