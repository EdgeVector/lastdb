//! Shared setup: a publisher, one member with a `write` grant, and the two
//! histories (the publisher's and the member's) that the scenarios drive.

use mutation_log::{RejectReason, SignedEntry, StreamKind, VerifiedHistory};

use crate::device::{
    bootstrap_owner, must_accept, must_reject, now_ms, owner_grant_write, owner_revoke, DataWrite,
    DbBirth, Device,
};
use crate::proposal::{
    author_member_entry, declined, incorporated, owner_decline, owner_incorporate,
};

pub const NAMESPACE: &str = "demo";
pub const KEY: &str = "row";

pub struct Fixture {
    pub owner: Device,
    pub member: Device,
    pub birth: DbBirth,
    pub birth_entry: SignedEntry,
    pub grant: SignedEntry,
    pub owner_hist: VerifiedHistory,
    pub member_hist: VerifiedHistory,
    pub clock: u64,
}

impl Fixture {
    pub fn new() -> Result<Self, String> {
        let owner = Device::generate();
        let member = Device::generate();
        let birth = DbBirth::generate(owner.node_key);
        let t0 = now_ms();
        let (mut owner_hist, birth_entry) = bootstrap_owner(&owner, &birth, t0)?;
        let grant = owner_grant_write(
            &mut owner_hist,
            &owner,
            birth.database_id,
            member.node_key,
            t0 + 1,
        )?;
        let mut member_hist = VerifiedHistory::new(birth.database_id);
        must_accept(&mut member_hist, birth_entry.clone())?;
        must_accept(&mut member_hist, grant.clone())?;
        Ok(Self {
            owner,
            member,
            birth,
            birth_entry,
            grant,
            owner_hist,
            member_hist,
            clock: t0 + 2,
        })
    }

    fn write<'a>(&mut self, key: &'a str, value: &'a [u8]) -> DataWrite<'a> {
        self.clock += 1;
        DataWrite {
            database_id: self.birth.database_id,
            namespace: NAMESPACE,
            record_key: key,
            value,
            authored_at_ms: self.clock,
        }
    }

    /// The member signs a proposal citing its grant. Both histories accept it.
    pub fn propose(&mut self, key: &str, value: &[u8]) -> Result<SignedEntry, String> {
        let entry = self.author(StreamKind::Proposal, true, key, value)?;
        must_accept(&mut self.member_hist, entry.clone())?;
        must_accept(&mut self.owner_hist, entry.clone())?;
        Ok(entry)
    }

    /// Author a member entry from the member's current view, not accepted.
    pub fn author(
        &mut self,
        stream: StreamKind,
        cite_grant: bool,
        key: &str,
        value: &[u8],
    ) -> Result<SignedEntry, String> {
        let write = self.write(key, value);
        let grant = cite_grant.then_some(&self.grant);
        author_member_entry(&self.member_hist, &self.member, stream, grant, &write)
    }

    /// The publisher's history must reject `entry` as `Unauthorized(want)`.
    pub fn expect_unauthorized(&mut self, entry: SignedEntry, want: &str) -> Result<(), String> {
        match must_reject(&mut self.owner_hist, entry)? {
            RejectReason::Unauthorized(msg) if msg.contains(want) => Ok(()),
            other => Err(format!("expected Unauthorized({want}), got {other:?}")),
        }
    }

    /// The publisher incorporates the proposal; the member sees it too.
    pub fn incorporate(
        &mut self,
        proposal: &SignedEntry,
        key: &str,
        value: &[u8],
    ) -> Result<SignedEntry, String> {
        let write = self.write(key, value);
        let entry = owner_incorporate(&mut self.owner_hist, &self.owner, proposal, &write)?;
        must_accept(&mut self.member_hist, entry.clone())?;
        Ok(entry)
    }

    /// The publisher declines the proposal; the member sees it too.
    pub fn decline(&mut self, proposal: &SignedEntry) -> Result<SignedEntry, String> {
        self.clock += 1;
        let entry = owner_decline(&mut self.owner_hist, &self.owner, proposal, self.clock)?;
        must_accept(&mut self.member_hist, entry.clone())?;
        Ok(entry)
    }

    /// The publisher revokes the member; the member sees it too.
    pub fn revoke(&mut self) -> Result<SignedEntry, String> {
        self.clock += 1;
        let entry = owner_revoke(
            &mut self.owner_hist,
            &self.owner,
            self.birth.database_id,
            self.member.node_key,
            self.clock,
        )?;
        must_accept(&mut self.member_hist, entry.clone())?;
        Ok(entry)
    }

    /// The publisher history holds `want` at the demo record (`None`: absent).
    pub fn expect_record(&self, want: Option<&[u8]>) -> Result<(), String> {
        let key = (NAMESPACE.to_owned(), KEY.to_owned());
        let have = self.owner_hist.records().get(&key).map(|r| r.value.clone());
        if have.as_deref() == want {
            Ok(())
        } else {
            Err(format!("record expected {want:?}, got {have:?}"))
        }
    }

    /// The proposal's decision state in the publisher history.
    pub fn expect_decision(
        &self,
        proposal: &SignedEntry,
        want_incorporated: bool,
        want_declined: bool,
    ) -> Result<(), String> {
        let inc = incorporated(&self.owner_hist, proposal);
        let dec = declined(&self.owner_hist, proposal)?;
        if inc == want_incorporated && dec == want_declined {
            Ok(())
        } else {
            Err(format!(
                "proposal incorporated={inc} declined={dec}, expected {want_incorporated}/{want_declined}"
            ))
        }
    }
}
