//! E. Node without grant: relay 403 on append/read; AcceptEntry rejects both
//! its proposal and its direct entry.

use mutation_log::{AcceptOutcome, RejectReason, SignedEntry, StreamKind, VerifiedHistory};

use super::fixture::{Fixture, KEY, NAMESPACE};
use super::revoke::expect_forbidden;
use crate::device::{DataWrite, Device};
use crate::proposal::author_member_entry;
use crate::relay::{RelayClient, RelayHandle};

pub async fn run(relay: &RelayHandle) -> Result<String, String> {
    let fx = Fixture::new()?;
    let stranger = Device::generate();
    let db = fx.birth.database_id.to_hex();
    let mut entries = Vec::with_capacity(2);
    for stream in [StreamKind::Proposal, StreamKind::Direct] {
        let write = DataWrite {
            database_id: fx.birth.database_id,
            namespace: NAMESPACE,
            record_key: KEY,
            value: b"nope",
            authored_at_ms: fx.clock + 1,
        };
        // The stranger cites the member's grant; it still holds none.
        entries.push(author_member_entry(
            &fx.owner_hist,
            &stranger,
            stream,
            Some(&fx.grant),
            &write,
        )?);
    }

    RelayClient::new(&relay.base_url, &fx.owner)
        .append(&db, &[fx.birth_entry.clone(), fx.grant.clone()])
        .await?;
    let client = RelayClient::new(&relay.base_url, &stranger);
    for entry in &entries {
        expect_forbidden(&client, &db, entry).await?;
    }
    let owner = &fx.owner;
    match client
        .read(&db, &owner.node_key_hex(), &owner.device_id_str(), 1, 90)
        .await
    {
        Err(e) if e.contains("HTTP 403") && e.contains("forbidden") => {}
        other => return Err(format!("relay must 403 non-grantee read: {other:?}")),
    }

    let mut receiver = VerifiedHistory::new(fx.birth.database_id);
    for e in [&fx.birth_entry, &fx.grant] {
        match receiver.accept_entry(e.clone()) {
            AcceptOutcome::Accept { .. } => {}
            other => return Err(format!("receiver setup: {other:?}")),
        }
    }
    for (entry, want) in entries.into_iter().zip([
        "proposal requires submit or write grant",
        "must cite exactly one lease",
    ]) {
        expect_reject(&mut receiver, entry, want)?;
    }
    Ok("relay 403 for non-grantee append+read; AcceptEntry rejects its proposal and its direct entry".into())
}

fn expect_reject(
    receiver: &mut VerifiedHistory,
    entry: SignedEntry,
    want: &str,
) -> Result<(), String> {
    match receiver.accept_entry(entry) {
        AcceptOutcome::Reject {
            reason: RejectReason::Unauthorized(msg) | RejectReason::Malformed(msg),
        } if msg.contains(want) => Ok(()),
        other => Err(format!("expected a rejection with {want}, got {other:?}")),
    }
}
