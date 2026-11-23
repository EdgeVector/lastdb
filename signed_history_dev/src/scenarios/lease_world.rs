//! The world of the lease scenarios: a publisher, members and nodes, in memory.

use mutation_log::{
    author_data, author_grant, author_lease, data_record, entry_hash, grant_raise, AcceptOutcome,
    DatabaseId, Grantee, LeaseBody, NodeKey, RejectReason, Role, SignedEntry, StreamHead,
    StreamKind, VerifiedHistory,
};

use crate::device::{
    bootstrap_owner, must_accept, must_reject, now_ms, owner_write, random_device_id,
    stream_head_of, DataWrite, DbBirth, Device,
};

pub(super) const NS: &str = "demo";
pub(super) const HOUR_MS: u64 = 3_600_000;

/// The publisher, its history, and every entry any party signed, in signing order.
pub(super) struct World {
    pub(super) owner: Device,
    pub(super) database_id: DatabaseId,
    pub(super) hist: VerifiedHistory,
    pub(super) log: Vec<SignedEntry>,
    pub(super) clock: u64,
}

/// One device stream of a member. It tracks its own head; it holds no history.
pub(super) struct Member {
    pub(super) dev: Device,
    pub(super) head: Option<StreamHead>,
}

impl Member {
    pub(super) fn new() -> Self {
        Self {
            dev: Device::generate(),
            head: None,
        }
    }

    /// A second device of the same member node.
    pub(super) fn second_device(&self) -> Self {
        Self {
            dev: Device {
                signing_key: self.dev.signing_key.clone(),
                node_key: self.dev.node_key,
                device_id: random_device_id(),
            },
            head: None,
        }
    }

    pub(super) fn key(&self) -> NodeKey {
        self.dev.node_key
    }

    /// Sign a direct entry that cites `lease` (or none), and log it. No history
    /// takes it yet: the caller delivers it where the scenario wants it.
    pub(super) fn direct(
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
    pub(super) fn new() -> Result<Self, String> {
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

    pub(super) fn advance(&mut self, ms: u64) -> u64 {
        self.clock += ms;
        self.clock
    }

    pub(super) fn wait_until(&mut self, time_ms: u64) {
        self.clock = self.clock.max(time_ms);
    }

    pub(super) fn grant(&mut self, member: NodeKey, role: Role) -> Result<(), String> {
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
    pub(super) fn build_lease(
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
    pub(super) fn lease(&mut self) -> Result<SignedEntry, String> {
        let now = self.clock;
        let entry = self.build_lease(now + HOUR_MS, now + 2 * HOUR_MS)?;
        must_accept(&mut self.hist, entry.clone())?;
        self.log.push(entry.clone());
        Ok(entry)
    }

    /// The publisher writes a record directly.
    pub(super) fn write(&mut self, key: &str, value: &[u8]) -> Result<SignedEntry, String> {
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

pub(super) fn closes_at(lease: &SignedEntry) -> Result<u64, String> {
    match lease {
        SignedEntry::Lease { body, .. } => Ok(body.closes_at_ms),
        _ => Err("not a lease entry".into()),
    }
}

/// Take an entry into a history; waiting for a dependency is fine, a rejection is not.
pub(super) fn deliver(history: &mut VerifiedHistory, entry: &SignedEntry) -> Result<(), String> {
    match history.accept_entry(entry.clone()) {
        AcceptOutcome::Reject { reason } => Err(format!("reject {reason:?}")),
        _ => Ok(()),
    }
}

pub(super) fn deliver_all(
    history: &mut VerifiedHistory,
    entries: &[SignedEntry],
) -> Result<(), String> {
    for entry in entries {
        deliver(history, entry)?;
    }
    Ok(())
}

pub(super) fn fresh(world: &World) -> VerifiedHistory {
    VerifiedHistory::new(world.database_id)
}

pub(super) fn value(history: &VerifiedHistory, key: &str) -> Option<Vec<u8>> {
    history
        .records()
        .get(&(NS.to_owned(), key.to_owned()))
        .map(|record| record.value.clone())
}

pub(super) fn expect(
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

pub(super) fn expect_unauthorized(
    history: &mut VerifiedHistory,
    entry: SignedEntry,
    want: &str,
) -> Result<(), String> {
    match must_reject(history, entry)? {
        RejectReason::Unauthorized(msg) if msg.contains(want) => Ok(()),
        other => Err(format!("expected Unauthorized({want}), got {other:?}")),
    }
}

pub(super) fn expect_excluded(
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
