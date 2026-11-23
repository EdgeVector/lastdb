//! B. Write grant → proposal → incorporation / not accepted → on-chain revoke.
//!
//! A member with a write grant signs proposals. A proposal joins the member's
//! chain and changes no record. A member-signed direct entry is rejected with
//! or without a grant. Only the publisher's incorporation entry changes the
//! record. After the revoke, the member's next proposal is rejected, on the
//! same device and on a new one, and the relay returns 403 for its append.

use mutation_log::{SignedEntry, StreamKind};

use super::fixture::{Fixture, KEY, NAMESPACE};
use crate::device::{must_accept, random_device_id, stream_head_of, Device};
use crate::relay::{RelayClient, RelayHandle};

const DIRECT: &str = "must cite exactly one lease";

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

    reject_direct(&mut fx)?;

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

/// A member direct entry that names no lease is rejected by AcceptEntry. The
/// relay would store it from a `write` member: it does not read leases.
fn reject_direct(fx: &mut Fixture) -> Result<(), String> {
    for cite_grant in [true, false] {
        let direct = fx.author(StreamKind::Direct, cite_grant, KEY, b"sneak")?;
        fx.expect_malformed(direct, DIRECT)?;
    }
    fx.expect_record(None)
}

async fn reject_after_revoke(
    fx: &mut Fixture,
    db: &str,
    relay: &RelayHandle,
) -> Result<(), String> {
    fx.clock += 1;
    let citing_all = author_citing_window(
        &fx.member_hist,
        &fx.member,
        fx.birth.database_id,
        fx.clock,
        KEY,
        b"honest-late",
    )?;
    fx.expect_unauthorized(citing_all, "proposal requires")?;

    let late = fx.author(StreamKind::Proposal, true, KEY, b"late")?;
    let client = RelayClient::new(&relay.base_url, &fx.member);
    expect_forbidden(&client, db, &late).await?;
    must_accept(&mut fx.member_hist, late.clone())?;
    must_accept(&mut fx.owner_hist, late.clone())?;
    fx.expect_record(Some(b"ok"))?;
    if fx
        .owner_hist
        .grants()
        .role_of(&fx.member.node_key)
        .is_some()
    {
        return Err("revoked member still holds a current role".into());
    }
    fx.decline(&late)?;
    fx.expect_decision(&late, false, true)?;

    let new_device = Device {
        signing_key: fx.member.signing_key.clone(),
        node_key: fx.member.node_key,
        device_id: random_device_id(),
    };
    fx.clock += 1;
    let other = author_citing_window(
        &fx.member_hist,
        &new_device,
        fx.birth.database_id,
        fx.clock,
        KEY,
        b"new-device",
    )?;
    fx.expect_unauthorized(other, "proposal requires")
}

fn author_citing_window(
    history: &mutation_log::VerifiedHistory,
    member: &Device,
    database_id: mutation_log::DatabaseId,
    authored_at_ms: u64,
    key: &str,
    value: &[u8],
) -> Result<SignedEntry, String> {
    let (seen, lamports): (Vec<_>, Vec<_>) = history.publisher_grants().into_iter().unzip();
    let head = stream_head_of(history, member);
    let ctx = member.author_ctx(database_id, head, seen, lamports, authored_at_ms);
    let body = mutation_log::data_record(StreamKind::Proposal, NAMESPACE, key, value);
    mutation_log::author_data(&ctx, &member.signing_key, body).map_err(|e| e.to_string())
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
