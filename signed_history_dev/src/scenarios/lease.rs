//! Write lease scenarios: the counting rule of `mutation_log`, with no relay.
//!
//! A publisher, members and several nodes, all in memory. Each scenario checks
//! one rule of `docs/designs/write-leases.md`; the last one checks that nodes
//! that see the same entries in different orders hold the same records.

use mutation_log::{
    author_data, author_grant, author_lease, data_record, entry_hash, grant_raise, AcceptOutcome,
    DatabaseId, EntryHash, Grantee, LeaseBody, NodeKey, RejectReason, Role, SignedEntry,
    StreamHead, StreamKind, VerifiedHistory,
};

use crate::device::{
    bootstrap_owner, must_accept, must_reject, now_ms, owner_write, random_device_id,
    stream_head_of, DataWrite, DbBirth, Device,
};

const NS: &str = "demo";
const HOUR_MS: u64 = 3_600_000;

/// The publisher, its history, and every entry any party signed, in signing order.
struct World {
    owner: Device,
    database_id: DatabaseId,
    hist: VerifiedHistory,
    log: Vec<SignedEntry>,
    clock: u64,
}

/// One device stream of a member. It tracks its own head; it holds no history.
struct Member {
    dev: Device,
    head: Option<StreamHead>,
}

impl Member {
    fn new() -> Self {
        Self {
            dev: Device::generate(),
            head: None,
        }
    }

    /// A second device of the same member node.
    fn second_device(&self) -> Self {
        Self {
            dev: Device {
                signing_key: self.dev.signing_key.clone(),
                node_key: self.dev.node_key,
                device_id: random_device_id(),
            },
            head: None,
        }
    }

    fn key(&self) -> NodeKey {
        self.dev.node_key
    }

    /// Sign a direct entry that cites `lease` (or none), and log it. No history
    /// takes it yet: the caller delivers it where the scenario wants it.
    fn direct(
        &mut self,
        world: &mut World,
        lease: Option<&SignedEntry>,
        key: &str,
        value: &[u8],
    ) -> Result<SignedEntry, String> {
        let at = world.advance(1);
        let (seen, lamports) = match lease {
            Some(lease) => (
                vec![entry_hash(lease).map_err(|e| e.to_string())?],
                vec![lease.header().lamport],
            ),
            None => (Vec::new(), Vec::new()),
        };
        let ctx = self
            .dev
            .author_ctx(world.database_id, self.head, seen, lamports, at);
        let body = data_record(StreamKind::Direct, NS, key, value);
        let entry = author_data(&ctx, &self.dev.signing_key, body).map_err(|e| e.to_string())?;
        self.head = Some(StreamHead {
            entry_hash: entry_hash(&entry).map_err(|e| e.to_string())?,
            writer_seq: entry.header().writer_seq,
            lamport: entry.header().lamport,
        });
        world.log.push(entry.clone());
        Ok(entry)
    }
}

impl World {
    fn new() -> Result<Self, String> {
        let owner = Device::generate();
        let birth = DbBirth::generate(owner.node_key);
        let t0 = now_ms();
        let (hist, birth_entry) = bootstrap_owner(&owner, &birth, t0)?;
        Ok(Self {
            owner,
            database_id: birth.database_id,
            hist,
            log: vec![birth_entry],
            clock: t0,
        })
    }

    fn advance(&mut self, ms: u64) -> u64 {
        self.clock += ms;
        self.clock
    }

    fn wait_until(&mut self, time_ms: u64) {
        self.clock = self.clock.max(time_ms);
    }

    fn grant(&mut self, member: NodeKey, role: Role) -> Result<(), String> {
        let at = self.advance(1);
        let head = stream_head_of(&self.hist, &self.owner);
        let ctx = self
            .owner
            .author_ctx(self.database_id, head, Vec::new(), Vec::new(), at);
        let body = grant_raise(Grantee::Member(member), role);
        let entry = author_grant(&ctx, &self.owner.signing_key, body).map_err(|e| e.to_string())?;
        must_accept(&mut self.hist, entry.clone())?;
        self.log.push(entry);
        Ok(())
    }

    /// Sign a lease without taking it into any history.
    fn build_lease(
        &mut self,
        expires_at_ms: u64,
        closes_at_ms: u64,
    ) -> Result<SignedEntry, String> {
        let at = self.advance(1);
        let head = stream_head_of(&self.hist, &self.owner);
        let ctx = self
            .owner
            .author_ctx(self.database_id, head, Vec::new(), Vec::new(), at);
        let body = LeaseBody {
            expires_at_ms,
            closes_at_ms,
            closed: self.hist.closing_streams(at),
        };
        author_lease(&ctx, &self.owner.signing_key, body).map_err(|e| e.to_string())
    }

    /// The publisher signs a lease that expires an hour from now and closes
    /// an hour after that, and takes it into its history. It closes the earlier
    /// leases whose closing time has passed.
    fn lease(&mut self) -> Result<SignedEntry, String> {
        let now = self.clock;
        let entry = self.build_lease(now + HOUR_MS, now + 2 * HOUR_MS)?;
        must_accept(&mut self.hist, entry.clone())?;
        self.log.push(entry.clone());
        Ok(entry)
    }

    /// The publisher writes a record directly.
    fn write(&mut self, key: &str, value: &[u8]) -> Result<SignedEntry, String> {
        let at = self.advance(1);
        let entry = owner_write(
            &mut self.hist,
            &self.owner,
            &DataWrite {
                database_id: self.database_id,
                namespace: NS,
                record_key: key,
                value,
                authored_at_ms: at,
            },
        )?;
        self.log.push(entry.clone());
        Ok(entry)
    }
}

fn closes_at(lease: &SignedEntry) -> Result<u64, String> {
    match lease {
        SignedEntry::Lease { body, .. } => Ok(body.closes_at_ms),
        _ => Err("not a lease entry".into()),
    }
}

/// Take an entry into a history; waiting for a dependency is fine, a rejection is not.
fn deliver(history: &mut VerifiedHistory, entry: &SignedEntry) -> Result<(), String> {
    match history.accept_entry(entry.clone()) {
        AcceptOutcome::Reject { reason } => Err(format!("reject {reason:?}")),
        _ => Ok(()),
    }
}

fn deliver_all(history: &mut VerifiedHistory, entries: &[SignedEntry]) -> Result<(), String> {
    for entry in entries {
        deliver(history, entry)?;
    }
    Ok(())
}

fn fresh(world: &World) -> VerifiedHistory {
    VerifiedHistory::new(world.database_id)
}

fn value(history: &VerifiedHistory, key: &str) -> Option<Vec<u8>> {
    history
        .records()
        .get(&(NS.to_owned(), key.to_owned()))
        .map(|record| record.value.clone())
}

fn expect(
    history: &VerifiedHistory,
    who: &str,
    key: &str,
    want: Option<&[u8]>,
) -> Result<(), String> {
    let have = value(history, key);
    if have.as_deref() == want {
        Ok(())
    } else {
        Err(format!(
            "{who}: record {key} expected {want:?}, got {have:?}"
        ))
    }
}

fn expect_unauthorized(
    history: &mut VerifiedHistory,
    entry: SignedEntry,
    want: &str,
) -> Result<(), String> {
    match must_reject(history, entry)? {
        RejectReason::Unauthorized(msg) if msg.contains(want) => Ok(()),
        other => Err(format!("expected Unauthorized({want}), got {other:?}")),
    }
}

fn expect_excluded(
    history: &VerifiedHistory,
    who: &str,
    entry: &SignedEntry,
    want: bool,
) -> Result<(), String> {
    let hash = entry_hash(entry).map_err(|e| e.to_string())?;
    if history.is_excluded(&hash) == want {
        Ok(())
    } else {
        Err(format!("{who}: excluded of an entry expected {want}"))
    }
}

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
