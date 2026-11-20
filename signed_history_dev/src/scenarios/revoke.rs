//! B. Write grant → write → revoke with cutoff → post-cutoff AcceptEntry Reject.

use mutation_log::{entry_hash, RevocationCutoff, VerifiedHistory};

use crate::device::{
    author_member_write, bootstrap_owner, member_write, must_reject, now_ms, owner_grant_write,
    owner_revoke, DataWrite, DbBirth, Device,
};
use crate::relay::{RelayClient, RelayHandle};

pub async fn run(relay: &RelayHandle) -> Result<String, String> {
    let owner = Device::generate();
    let member = Device::generate();
    let birth = DbBirth::generate();
    let t0 = now_ms();
    let (mut history, birth_entry) = bootstrap_owner(&owner, &birth, t0)?;
    let grant = owner_grant_write(
        &mut history,
        &owner,
        birth.database_id,
        member.node_key,
        t0 + 1,
    )?;
    let member_entry = {
        let mut member_hist = VerifiedHistory::new(birth.database_id);
        for e in [&birth_entry, &grant] {
            match member_hist.accept_entry(e.clone()) {
                mutation_log::AcceptOutcome::Accept { .. } => {}
                other => return Err(format!("member bootstrap: {other:?}")),
            }
        }
        let written = member_write(
            &mut member_hist,
            &member,
            &grant,
            DataWrite {
                database_id: birth.database_id,
                namespace: "demo",
                record_key: "row",
                value: b"ok",
                authored_at_ms: t0 + 2,
            },
        )?;
        match history.accept_entry(written.clone()) {
            mutation_log::AcceptOutcome::Accept { .. } => {}
            other => return Err(format!("owner accept member write: {other:?}")),
        }
        written
    };

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
    let post = author_member_write(
        &member_view,
        &member,
        &grant,
        DataWrite {
            database_id: birth.database_id,
            namespace: "demo",
            record_key: "row",
            value: b"late",
            authored_at_ms: t0 + 4,
        },
    )?;
    let client = RelayClient::new(&relay.base_url, &member);
    let db = birth.database_id.to_hex();
    client.append(&db, &[post.clone()]).await?;
    let reason = must_reject(&mut history, post)?;
    match reason {
        mutation_log::RejectReason::Unauthorized(msg) if msg.contains("cutoff") => {}
        other => return Err(format!("expected cutoff Unauthorized, got {other:?}")),
    }
    Ok("post-cutoff member write Reject(Unauthorized); relay stored grant-blind".into())
}
