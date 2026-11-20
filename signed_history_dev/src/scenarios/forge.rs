//! A. Unsigned / tampered signature → AcceptEntry Reject (relay may store).

use mutation_log::{author_data, data_record, StreamKind, VerifiedHistory};

use crate::device::{
    bootstrap_owner, must_reject, now_ms, owner_write, stream_head_of, tamper_signature, DataWrite,
    DbBirth, Device,
};
use crate::relay::{RelayClient, RelayHandle};

pub async fn run(relay: &RelayHandle) -> Result<String, String> {
    let owner = Device::generate();
    let birth = DbBirth::generate();
    let t0 = now_ms();
    let (mut history, birth_entry) = bootstrap_owner(&owner, &birth, t0)?;
    let good = owner_write(
        &mut history,
        &owner,
        DataWrite {
            database_id: birth.database_id,
            namespace: "demo",
            record_key: "k1",
            value: b"v1",
            authored_at_ms: t0 + 1,
        },
    )?;

    let mut bad = good.clone();
    tamper_signature(&mut bad);
    let client = RelayClient::new(&relay.base_url, &owner);
    let db = birth.database_id.to_hex();
    client
        .append(&db, &[birth_entry.clone(), bad.clone()])
        .await?;
    let mut receiver = VerifiedHistory::new(birth.database_id);
    match receiver.accept_entry(birth_entry) {
        mutation_log::AcceptOutcome::Accept { .. } => {}
        other => return Err(format!("birth accept failed: {other:?}")),
    }
    let reason = must_reject(&mut receiver, bad)?;
    if !matches!(reason, mutation_log::RejectReason::Signature(_)) {
        return Err(format!("expected Signature reject, got {reason:?}"));
    }

    let head = stream_head_of(&history, &owner);
    let ctx = owner.author_ctx(birth.database_id, head, Vec::new(), Vec::new(), t0 + 2);
    let mut unsigned = author_data(
        &ctx,
        &owner.signing_key,
        data_record(StreamKind::Direct, "demo", "k2", b"x"),
    )
    .map_err(|e| e.to_string())?;
    unsigned.header_mut().signature = "A".repeat(86);
    let reason2 = must_reject(&mut history, unsigned)?;
    if !matches!(reason2, mutation_log::RejectReason::Signature(_)) {
        return Err(format!(
            "expected Signature reject for bad sig, got {reason2:?}"
        ));
    }

    Ok("AcceptEntry rejected tampered+forged signatures; relay stored bytes grant-blind".into())
}
