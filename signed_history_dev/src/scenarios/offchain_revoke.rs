//! Off-chain (side-channel) revoke must not change AcceptEntry's outcome.
//! A revoke signed by a non-publisher is rejected. A publisher revoke that
//! never enters the history changes nothing: the member's proposal is still
//! accepted and the publisher still incorporates it. After a real on-chain
//! revoke the member's next proposal is rejected and the record is unchanged.

use mutation_log::{author_grant, grant_reduce, Grantee, Role, SignedEntry, StreamKind};

use super::fixture::{Fixture, KEY};
use crate::device::{must_accept, stream_head_of, Device};

pub fn run() -> Result<String, String> {
    let mut fx = Fixture::new()?;
    let outsider = Device::generate();

    let first = fx.propose(KEY, b"ok")?;
    fx.incorporate(&first, KEY, b"ok")?;
    fx.expect_record(Some(b"ok"))?;

    // Side-channel: a non-publisher "revoke" offered to AcceptEntry is rejected.
    let fake = author_revoke(&fx, &outsider)?;
    fx.expect_unauthorized(fake, "publisher")?;

    // Side-channel: a real publisher revoke that never enters the history is
    // ignored. The member's proposal is still accepted and incorporated.
    let _withheld = author_revoke(&fx, &fx.owner)?;
    let still = fx.propose(KEY, b"still-granted")?;
    fx.expect_record(Some(b"ok"))?;
    fx.incorporate(&still, KEY, b"still-granted")?;
    fx.expect_decision(&still, true, false)?;
    fx.expect_record(Some(b"still-granted"))?;

    // On-chain revoke: citing the member's grant window is rejected, and even a
    // stale-cited proposal leaves the record unchanged with no current grant.
    fx.revoke()?;
    let (seen, lamports): (Vec<_>, Vec<_>) = fx.member_hist.publisher_grants().into_iter().unzip();
    let head = stream_head_of(&fx.member_hist, &fx.member);
    fx.clock += 1;
    let ctx = fx
        .member
        .author_ctx(fx.birth.database_id, head, seen, lamports, fx.clock);
    let body = mutation_log::data_record(
        StreamKind::Proposal,
        super::fixture::NAMESPACE,
        KEY,
        b"after-revoke",
    );
    let honest_late =
        mutation_log::author_data(&ctx, &fx.member.signing_key, body).map_err(|e| e.to_string())?;
    fx.expect_unauthorized(honest_late, "proposal requires")?;

    let stale = fx.author(StreamKind::Proposal, true, KEY, b"stale-grant")?;
    must_accept(&mut fx.member_hist, stale.clone())?;
    must_accept(&mut fx.owner_hist, stale.clone())?;
    if fx
        .owner_hist
        .grants()
        .role_of(&fx.member.node_key)
        .is_some()
    {
        return Err("on-chain revoke left the member with an active grant".into());
    }
    fx.decline(&stale)?;
    fx.expect_decision(&stale, false, true)?;
    fx.expect_record(Some(b"still-granted"))?;

    Ok("non-publisher revoke rejected; withheld revoke ignored (proposal accepted and incorporated); after on-chain revoke the proposal is rejected or declined and the record is unchanged".into())
}

/// A revoke of the fixture's member, signed by `signer`, not accepted.
fn author_revoke(fx: &Fixture, signer: &Device) -> Result<SignedEntry, String> {
    let head = stream_head_of(&fx.owner_hist, signer);
    let ctx = signer.author_ctx(
        fx.birth.database_id,
        head,
        Vec::new(),
        Vec::new(),
        fx.clock + 1,
    );
    let body = grant_reduce(
        Grantee::Member(fx.member.node_key),
        Role::Revoked,
        Vec::new(),
    );
    author_grant(&ctx, &signer.signing_key, body).map_err(|e| e.to_string())
}
