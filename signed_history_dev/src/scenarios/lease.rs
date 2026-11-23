//! Write lease scenarios: the counting rule of `mutation_log`, with no relay.
//!
//! Each scenario checks one rule of `docs/designs/write-leases.md`; the last one
//! checks that nodes that see the same entries in different orders hold the same
//! records.

use mutation_log::{
    author_lease, entry_hash, EntryHash, LeaseBody, RejectReason, Role, SignedEntry,
    VerifiedHistory,
};

use super::lease_world::{
    closes_at, deliver, deliver_all, expect, expect_excluded, expect_unauthorized, fresh, Member,
    World, HOUR_MS,
};
use crate::device::{must_accept, must_reject};

/// A `write` member signs a direct entry under a lease that covers it, and it
/// counts at once on the publisher and on another node.
pub fn covered_write_counts() -> Result<String, String> {
    let mut world = World::new()?;
    let mut member = Member::new();
    world.grant(member.key(), Role::Write)?;
    let lease = world.lease()?;
    let entry = member.direct(&mut world, Some(&lease), "k", b"v1")?;
    must_accept(&mut world.hist, entry.clone())?;
    expect(&world.hist, "publisher", "k", Some(b"v1"))?;
    let mut other = fresh(&world);
    deliver_all(&mut other, &world.log)?;
    expect(&other, "other node", "k", Some(b"v1"))?;
    expect_excluded(&other, "other node", &entry, false)?;
    let latest = world.hist.latest_lease().ok_or("no latest lease")?;
    if !world.hist.lease_covers(&latest.entry_hash, &member.key()) {
        return Err("lease_covers says the write member is not covered".into());
    }
    Ok("a covered write member's direct entry counted on both nodes with no accept step".into())
}

/// A member that does not hold `write` as of the lease does not join the
/// stream, and a lease needs the publisher and an end after its expiry.
pub fn uncovered_write_refused() -> Result<String, String> {
    let mut world = World::new()?;
    let mut submit = Member::new();
    let mut late = Member::new();
    world.grant(submit.key(), Role::Submit)?;
    let lease = world.lease()?;
    world.grant(late.key(), Role::Write)?;
    let entry = submit.direct(&mut world, Some(&lease), "k", b"from submit")?;
    expect_unauthorized(&mut world.hist, entry, "write grant")?;
    let entry = late.direct(&mut world, Some(&lease), "k", b"from late writer")?;
    expect_unauthorized(&mut world.hist, entry, "write grant")?;
    let mut nameless = Member::new();
    world.grant(nameless.key(), Role::Write)?;
    let entry = nameless.direct(&mut world, None, "k", b"names no lease")?;
    match must_reject(&mut world.hist, entry)? {
        RejectReason::Malformed(_) => {}
        other => return Err(format!("expected Malformed for no lease, got {other:?}")),
    }
    expect(&world.hist, "publisher", "k", None)?;
    // The next lease covers the writer added after the first one.
    let second = world.lease()?;
    let mut late = late.second_device();
    let entry = late.direct(&mut world, Some(&second), "k", b"covered now")?;
    must_accept(&mut world.hist, entry)?;
    expect(&world.hist, "publisher", "k", Some(b"covered now"))?;
    // A lease signed by a member, and a lease that closes before it expires.
    let forged = {
        let at = world.advance(1);
        let ctx = late
            .dev
            .author_ctx(world.database_id, late.head, Vec::new(), Vec::new(), at);
        let body = LeaseBody {
            expires_at_ms: at + HOUR_MS,
            closes_at_ms: at + 2 * HOUR_MS,
            closed: Vec::new(),
        };
        author_lease(&ctx, &late.dev.signing_key, body).map_err(|e| e.to_string())?
    };
    expect_unauthorized(&mut world.hist, forged, "publisher")?;
    let now = world.clock;
    let bad = world.build_lease(now + HOUR_MS, now + HOUR_MS)?;
    match must_reject(&mut world.hist, bad)? {
        RejectReason::Malformed(_) => {}
        other => {
            return Err(format!(
                "expected Malformed for closes <= expires, got {other:?}"
            ))
        }
    }
    Ok("submit, late and nameless direct entries refused; the next lease covers the late writer; a member lease and an inverted lease refused".into())
}

/// The scenario's world after a closing: member stream `a` wrote five entries
/// under the lease and the publisher had two of them when it signed the
/// closing; stream `b` (a second device) wrote one the publisher had not seen.
struct Closing {
    world: World,
    entries: Vec<SignedEntry>,
    stray: SignedEntry,
    closing: SignedEntry,
    before: VerifiedHistory,
}

fn closing_world() -> Result<Closing, String> {
    let mut world = World::new()?;
    let mut a = Member::new();
    let mut b = a.second_device();
    world.grant(a.key(), Role::Write)?;
    let lease = world.lease()?;
    world.write("r2", b"base")?;
    let entries = vec![
        a.direct(&mut world, Some(&lease), "ok", b"1")?,
        a.direct(&mut world, Some(&lease), "r", b"m1")?,
        a.direct(&mut world, Some(&lease), "r", b"m2")?,
        a.direct(&mut world, Some(&lease), "r2", b"m3")?,
        a.direct(&mut world, Some(&lease), "late", b"m4")?,
    ];
    let stray = b.direct(&mut world, Some(&lease), "dev2", b"from the second device")?;
    // The publisher holds the first two entries of stream a and nothing of stream b.
    for entry in &entries[..2] {
        must_accept(&mut world.hist, entry.clone())?;
    }
    // Another node saw every entry before the closing.
    let mut before = fresh(&world);
    deliver_all(&mut before, &world.log)?;
    expect(&before, "node before closing", "r", Some(b"m2"))?;
    expect(&before, "node before closing", "late", Some(b"m4"))?;
    expect(
        &before,
        "node before closing",
        "dev2",
        Some(b"from the second device"),
    )?;
    world.wait_until(closes_at(&lease)?);
    let closing = world.lease()?;
    Ok(Closing {
        world,
        entries,
        stray,
        closing,
        before,
    })
}

/// Closing keeps the entries at or before the closing point, excludes the ones
/// past it, and gives the records they wrote back to what the counting entries say.
pub fn closing_point() -> Result<String, String> {
    let mut c = closing_world()?;
    match &c.closing {
        SignedEntry::Lease { body, .. }
            if body.closed.len() == 1 && body.closed[0].writer_seq == 2 => {}
        other => {
            return Err(format!(
                "the closing entry lists the wrong streams: {other:?}"
            ))
        }
    }
    // Entries that reach the publisher after the closing: past the point, they do not count.
    for entry in &c.entries[2..] {
        deliver(&mut c.world.hist, entry)?;
    }
    deliver(&mut c.before, &c.closing)?;
    for (who, history) in [
        ("publisher", &c.world.hist),
        ("node that saw it all before", &c.before),
    ] {
        expect(history, who, "ok", Some(b"1"))?;
        expect(history, who, "r", Some(b"m1"))?;
        expect(history, who, "r2", Some(b"base"))?;
        expect(history, who, "late", None)?;
        expect_excluded(history, who, &c.entries[1], false)?;
        for entry in &c.entries[2..] {
            expect_excluded(history, who, entry, true)?;
        }
    }
    Ok("entries at or before the closing point stay; the three past it are excluded; r returns to its counting write m1, r2 to the publisher's base, late disappears".into())
}

/// A stream the closing entry does not list loses every entry that names the lease.
pub fn unlisted_stream_loses_all() -> Result<String, String> {
    let mut c = closing_world()?;
    deliver(&mut c.world.hist, &c.stray)?;
    deliver(&mut c.before, &c.closing)?;
    for (who, history) in [
        ("publisher", &c.world.hist),
        ("node that saw it before", &c.before),
    ] {
        expect(history, who, "dev2", None)?;
        expect_excluded(history, who, &c.stray, true)?;
    }
    Ok("the second device's entry, which the closing did not list, counted nowhere".into())
}

/// Nodes that see the same entries in different orders hold the same records
/// and the same excluded entries.
pub fn order_converges() -> Result<String, String> {
    let mut c = closing_world()?;
    for entry in c.entries[2..].iter().chain(std::iter::once(&c.stray)) {
        deliver(&mut c.world.hist, entry)?;
    }
    let all = c.world.log.clone();
    let mut orders: Vec<(&str, Vec<SignedEntry>)> = vec![("as signed", all.clone())];
    let mut reversed = all.clone();
    reversed.reverse();
    orders.push(("reversed", reversed));
    let mut rotated = all.clone();
    rotated.rotate_left(all.len() / 2);
    orders.push(("rotated", rotated));
    let mut closing_first: Vec<SignedEntry> = vec![c.closing.clone()];
    closing_first.extend(
        all.iter()
            .filter(|e| entry_hash(e).ok() != entry_hash(&c.closing).ok())
            .cloned(),
    );
    orders.push(("closing first", closing_first));
    let want_excluded = excluded_set(&c.world.hist);
    for (name, order) in &orders {
        let mut node = fresh(&c.world);
        deliver_all(&mut node, order)?;
        if node.pending_len() != 0 {
            return Err(format!(
                "{name}: {} entries still pending",
                node.pending_len()
            ));
        }
        if node.records() != c.world.hist.records() {
            return Err(format!("{name}: records differ from the publisher's"));
        }
        if excluded_set(&node) != want_excluded {
            return Err(format!(
                "{name}: excluded entries differ from the publisher's"
            ));
        }
    }
    if want_excluded.len() != 4 {
        return Err(format!(
            "expected 4 excluded entries, found {}",
            want_excluded.len()
        ));
    }
    Ok(format!(
        "{} orders gave the publisher's records and its {} excluded entries",
        orders.len(),
        want_excluded.len()
    ))
}

fn excluded_set(history: &VerifiedHistory) -> Vec<EntryHash> {
    let mut hashes: Vec<EntryHash> = history
        .excluded_entries()
        .iter()
        .filter_map(|entry| entry_hash(entry).ok())
        .collect();
    hashes.sort();
    hashes
}
