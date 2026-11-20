//! E. Node without grant: relay 403 on append/read; AcceptEntry still rejects the entry.

use mutation_log::{AcceptOutcome, VerifiedHistory};

use crate::device::{
    author_member_write, bootstrap_owner, must_reject, now_ms, owner_grant_write, DataWrite,
    DbBirth, Device,
};
use crate::relay::{RelayClient, RelayHandle};

pub async fn run(relay: &RelayHandle) -> Result<String, String> {
    let owner = Device::generate();
    let member = Device::generate();
    let stranger = Device::generate();
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

    let forged = author_member_write(
        &history,
        &stranger,
        &grant,
        &DataWrite {
            database_id: birth.database_id,
            namespace: "demo",
            record_key: "sneak",
            value: b"nope",
            authored_at_ms: t0 + 2,
        },
    )?;

    let db = birth.database_id.to_hex();
    RelayClient::new(&relay.base_url, &owner)
        .append(&db, &[birth_entry.clone(), grant.clone()])
        .await?;
    let client = RelayClient::new(&relay.base_url, &stranger);
    match client.append(&db, std::slice::from_ref(&forged)).await {
        Err(e) if e.contains("HTTP 403") && e.contains("forbidden") => {}
        other => return Err(format!("relay must 403 non-grantee append: {other:?}")),
    }
    match client
        .read(&db, &owner.node_key_hex(), &owner.device_id_str(), 1, 90)
        .await
    {
        Err(e) if e.contains("HTTP 403") && e.contains("forbidden") => {}
        other => return Err(format!("relay must 403 non-grantee read: {other:?}")),
    }

    let reason = must_reject(&mut history, forged.clone())?;
    match &reason {
        mutation_log::RejectReason::Unauthorized(msg) if msg.contains("non-grantee") => {}
        other => return Err(format!("expected Unauthorized(non-grantee), got {other:?}")),
    }

    let mut receiver = VerifiedHistory::new(birth.database_id);
    for e in [birth_entry, grant] {
        match receiver.accept_entry(e) {
            AcceptOutcome::Accept { .. } => {}
            other => return Err(format!("receiver setup: {other:?}")),
        }
    }
    match receiver.accept_entry(forged) {
        AcceptOutcome::Reject {
            reason: mutation_log::RejectReason::Unauthorized(msg),
        } if msg.contains("non-grantee") => {}
        other => {
            return Err(format!(
                "receiver expected non-grantee reject, got {other:?}"
            ))
        }
    }
    Ok(
        "relay 403 for non-grantee append+read; AcceptEntry Reject Unauthorized(non-grantee)"
            .into(),
    )
}
