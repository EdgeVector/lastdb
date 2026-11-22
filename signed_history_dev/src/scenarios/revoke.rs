//! B. Write grant → proposal → incorporation / not accepted → on-chain revoke.
//!
//! A member with a write grant signs proposals. A proposal joins the member's
//! chain and changes no record. A member-signed direct entry is rejected with
//! or without a grant. Only the publisher's incorporation entry changes the
//! record. After the revoke, the member's next proposal is rejected, on the
//! same device and on a new one, and the relay returns 403 for its append.

use mutation_log::{SignedEntry, StreamKind};

use super::fixture::{Fixture, KEY, NAMESPACE};
use crate::device::{random_device_id, DataWrite, Device};
use crate::proposal::author_member_entry;
use crate::relay::{RelayClient, RelayHandle};

const DIRECT: &str = "only the publisher signs direct entries";

pub async fn run(relay: &RelayHandle) -> Result<String, String> {
    let mut fx = Fixture::new()?;
    let db = fx.birth.database_id.to_hex();
    let owner_client = RelayClient::new(&relay.base_url, &fx.owner);
    let member_client = RelayClient::new(&relay.base_url, &fx.member);
    owner_client
        .append(&db, &[fx.birth_entry.clone(), fx.grant.clone()])
        .await?;

    let accepted = fx.propose(KEY, b"ok")?;
    fx.expect_record(None)?;
    fx.expect_decision(&accepted, false, false)?;
    member_client
        .append(&db, std::slice::from_ref(&accepted))
        .await?;

    reject_direct(&mut fx, &db, &member_client).await?;

    let incorporation = fx.incorporate(&accepted, KEY, b"ok")?;
    fx.expect_record(Some(b"ok"))?;
    fx.expect_decision(&accepted, true, false)?;

    let refused = fx.propose(KEY, b"refused")?;
    let decline = fx.decline(&refused)?;
    fx.expect_record(Some(b"ok"))?;
    fx.expect_decision(&refused, false, true)?;
    member_client
        .append(&db, std::slice::from_ref(&refused))
        .await?;

    let revoke = fx.revoke()?;
    owner_client
        .append(&db, &[incorporation, decline, revoke])
        .await?;
    reject_after_revoke(&mut fx, &db, relay).await?;
    fx.expect_record(Some(b"ok"))?;
    Ok("proposal joins the chain and changes no record; member direct entry rejected; record changes only by publisher incorporation; not accepted proposal leaves it unchanged; after revoke proposals are rejected and relay 403s the append".into())
}

/// A member direct entry is rejected by AcceptEntry and by the relay.
async fn reject_direct(
    fx: &mut Fixture,
    db: &str,
    member_client: &RelayClient,
) -> Result<(), String> {
    for cite_grant in [true, false] {
        let direct = fx.author(StreamKind::Direct, cite_grant, KEY, b"sneak")?;
        fx.expect_unauthorized(direct.clone(), DIRECT)?;
        expect_forbidden(member_client, db, &direct).await?;
    }
    fx.expect_record(None)
}

async fn reject_after_revoke(
    fx: &mut Fixture,
    db: &str,
    relay: &RelayHandle,
) -> Result<(), String> {
    let late = fx.author(StreamKind::Proposal, true, KEY, b"late")?;
    fx.expect_unauthorized(late.clone(), "proposal requires")?;
    let client = RelayClient::new(&relay.base_url, &fx.member);
    expect_forbidden(&client, db, &late).await?;

    let new_device = Device {
        signing_key: fx.member.signing_key.clone(),
        node_key: fx.member.node_key,
        device_id: random_device_id(),
    };
    let other = author_member_entry(
        &fx.member_hist,
        &new_device,
        StreamKind::Proposal,
        Some(&fx.grant),
        &DataWrite {
            database_id: fx.birth.database_id,
            namespace: NAMESPACE,
            record_key: KEY,
            value: b"new-device",
            authored_at_ms: fx.clock + 1,
        },
    )?;
    fx.expect_unauthorized(other, "proposal requires")
}

pub(crate) async fn expect_forbidden(
    client: &RelayClient,
    db: &str,
    entry: &SignedEntry,
) -> Result<(), String> {
    match client.append(db, std::slice::from_ref(entry)).await {
        Err(e) if e.contains("HTTP 403") && e.contains("forbidden") => Ok(()),
        other => Err(format!("relay must 403 this append: {other:?}")),
    }
}
