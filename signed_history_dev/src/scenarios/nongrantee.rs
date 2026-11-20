//! E. Node without grant presents a direct data entry → AcceptEntry Reject.

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

    let forged = author_member_write(
        &history,
        &stranger,
        &grant,
        DataWrite {
            database_id: birth.database_id,
            namespace: "demo",
            record_key: "sneak",
            value: b"nope",
            authored_at_ms: t0 + 2,
        },
    )?;

    let client = RelayClient::new(&relay.base_url, &stranger);
    let db = birth.database_id.to_hex();
    client.append(&db, &[forged.clone()]).await?;

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
    let pulled = client
        .read(
            &db,
            &stranger.node_key_hex(),
            &stranger.device_id_str(),
            1,
            90,
        )
        .await?;
    if pulled.is_empty() {
        return Err("relay did not store non-grantee entry".into());
    }
    for e in pulled {
        match receiver.accept_entry(e) {
            AcceptOutcome::Reject {
                reason: mutation_log::RejectReason::Unauthorized(msg),
            } if msg.contains("non-grantee") => {}
            other => {
                return Err(format!(
                    "receiver expected non-grantee reject, got {other:?}"
                ))
            }
        }
    }
    Ok("AcceptEntry Reject Unauthorized(non-grantee); relay stored grant-blind".into())
}
