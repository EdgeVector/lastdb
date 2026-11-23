//! Grant and revoke against the relay: a `write` member signs a direct entry
//! under the publisher's lease and proposes, the publisher incorporates the
//! proposal, then revokes the member, and the relay refuses the member's next
//! `log.append` and `blob.put` with HTTP 403. The relay stores the publisher's
//! `lease` entry and reads it back. It refuses a `lease` entry from a member and
//! a direct entry from a `submit` member. The member's proposal stays stored and the
//! publisher's incorporation is what puts its record in the history. A
//! never-granted node key gets 403 on signed `log.append` and on `log.read` of a
//! non-public database.

use bytes::Bytes;
use mutation_log::{
    author_data, author_lease, data_record, entry_hash, AcceptOutcome, AuthorCtx, DatabaseId,
    DeviceId, Grantee, Hex32, LeaseBody, NodeKey, Role, SignedEntry, StreamKind, VerifiedHistory,
};
use sha2::{Digest, Sha256};
use signed_history_materialize::encode_string_field;

use crate::chain::Chain;
use crate::identity::{new_device_id, now_ms, Actor};
use crate::relay::RelayClient;

/// FoldDB schema (history namespace) for records proposed by the member.
pub const MEMBER_NAMESPACE: &str = "relay_e2e_member";
/// Record key of the member's one incorporated proposal.
pub const MEMBER_KEY: &str = "from-member";
/// Text of the member's incorporated proposal.
pub const MEMBER_TEXT: &str = "proposed by a write member and incorporated by the publisher";

/// Where the database lives: relay, publisher and the publisher's first device stream.
pub struct Scene<'a> {
    pub http: &'a reqwest::Client,
    pub relay_url: &'a str,
    pub database_id: DatabaseId,
    pub owner: NodeKey,
    pub root_device: DeviceId,
}

impl Scene<'_> {
    fn client(&self, actor: &Actor, device: DeviceId) -> RelayClient {
        RelayClient::new(self.relay_url, self.http, actor, self.database_id, device)
    }
}

/// A node holding a publisher `write` grant, signing through its own device stream.
pub struct Member {
    actor: Actor,
    chain: Chain,
    grant: SignedEntry,
    proposal: Option<SignedEntry>,
}

impl Member {
    pub const fn node_key(&self) -> NodeKey {
        self.actor.node_key
    }

    /// The proposal the member deposited; the publisher incorporates it.
    pub fn proposal(&self) -> Result<&SignedEntry, String> {
        self.proposal
            .as_ref()
            .ok_or_else(|| "member has not proposed yet".to_owned())
    }

    /// Author a data entry on `stream` that cites the member's grant. It does
    /// not move the chain: the caller advances it when the entry is stored.
    fn author(
        &self,
        database_id: DatabaseId,
        stream: StreamKind,
        record_key: &str,
        text: &str,
    ) -> Result<SignedEntry, String> {
        let grant = self.grant.header();
        let grant_hash = entry_hash(&self.grant).map_err(|e| e.to_string())?;
        let ctx = AuthorCtx {
            database_id,
            node_key: self.actor.node_key,
            device_id: self.chain.device_id,
            stream_head: self.chain.head(),
            seen_heads: vec![grant_hash],
            seen_lamports: vec![grant.lamport],
            authored_at_ms: now_ms(),
        };
        let value = encode_string_field(text);
        let body = data_record(stream, MEMBER_NAMESPACE, record_key, &value);
        author_data(&ctx, &self.actor.signing_key, body).map_err(|e| e.to_string())
    }
}

/// The member reads the publisher's root stream, verifies it with AcceptEntry,
/// finds its own `write` grant and the publisher's lease (the relay read the
/// lease entry back), signs one direct entry under the lease and appends it,
/// and appends one proposal. The relay refuses a `lease` entry from the member
/// and a direct entry from a `submit` member.
pub async fn propose_as_member(
    scene: &Scene<'_>,
    actor: Actor,
    submit: &Actor,
) -> Result<Member, String> {
    let device = new_device_id();
    let client = scene.client(&actor, device);
    let page = client
        .read(&scene.owner.to_hex(), &scene.root_device.to_hex(), 1)
        .await?;
    let mut view = VerifiedHistory::new(scene.database_id);
    for entry in &page.entries {
        if let AcceptOutcome::Reject { reason } = view.accept_entry(entry.clone()) {
            return Err(format!("member rejected a publisher entry: {reason:?}"));
        }
    }
    if view.grants().role_of(&actor.node_key) != Some(Role::Write) {
        return Err("member holds no write grant in the publisher history".into());
    }
    let lease = page
        .entries
        .iter()
        .find(|entry| matches!(entry, SignedEntry::Lease { .. }))
        .cloned()
        .ok_or("the relay did not read back the publisher's lease entry")?;
    let lease_hash = entry_hash(&lease).map_err(|e| e.to_string())?;
    if !view.lease_covers(&lease_hash, &actor.node_key) {
        return Err("the publisher's lease does not cover the write member".into());
    }
    let grant = page
        .entries
        .into_iter()
        .find(|entry| names_member(entry, actor.node_key))
        .ok_or("publisher history has no grant entry for the member")?;
    let mut member = Member {
        actor,
        chain: Chain::new(device),
        grant,
        proposal: None,
    };
    let submit_device = new_device_id();
    let sneaked = direct_under(
        submit,
        &Chain::new(submit_device),
        scene.database_id,
        &lease,
    )?;
    refused(
        "submit member",
        "log.append of a direct entry",
        scene
            .client(submit, submit_device)
            .append(std::slice::from_ref(&sneaked))
            .await,
    )?;
    let forged = lease_by(&member.actor, &member.chain, scene.database_id)?;
    refused(
        "write member",
        "log.append of a lease entry",
        client.append(std::slice::from_ref(&forged)).await,
    )?;
    let direct = direct_under(&member.actor, &member.chain, scene.database_id, &lease)?;
    client.append(std::slice::from_ref(&direct)).await?;
    member.chain.advance(&direct)?;
    let first = member.author(
        scene.database_id,
        StreamKind::Proposal,
        MEMBER_KEY,
        MEMBER_TEXT,
    )?;
    client.append(std::slice::from_ref(&first)).await?;
    member.chain.advance(&first)?;
    member.proposal = Some(first);
    Ok(member)
}

/// A direct entry that writes the member's record and cites `lease`.
fn direct_under(
    actor: &Actor,
    chain: &Chain,
    database_id: DatabaseId,
    lease: &SignedEntry,
) -> Result<SignedEntry, String> {
    let ctx = AuthorCtx {
        database_id,
        node_key: actor.node_key,
        device_id: chain.device_id,
        stream_head: chain.head(),
        seen_heads: vec![entry_hash(lease).map_err(|e| e.to_string())?],
        seen_lamports: vec![lease.header().lamport],
        authored_at_ms: now_ms(),
    };
    let value = encode_string_field(MEMBER_TEXT);
    let body = data_record(StreamKind::Direct, MEMBER_NAMESPACE, MEMBER_KEY, &value);
    author_data(&ctx, &actor.signing_key, body).map_err(|e| e.to_string())
}

/// A lease entry signed by a member, which the relay must refuse.
fn lease_by(actor: &Actor, chain: &Chain, database_id: DatabaseId) -> Result<SignedEntry, String> {
    let now = now_ms();
    let ctx = AuthorCtx {
        database_id,
        node_key: actor.node_key,
        device_id: chain.device_id,
        stream_head: chain.head(),
        seen_heads: Vec::new(),
        seen_lamports: Vec::new(),
        authored_at_ms: now,
    };
    let body = LeaseBody {
        expires_at_ms: now + 1_000,
        closes_at_ms: now + 2_000,
        closed: Vec::new(),
    };
    author_lease(&ctx, &actor.signing_key, body).map_err(|e| e.to_string())
}

fn names_member(entry: &SignedEntry, member: NodeKey) -> bool {
    matches!(
        entry,
        SignedEntry::Grant { body, .. }
            if body.grantee == Grantee::Member(member) && body.role == Role::Write
    )
}

/// After the revoke is stored, a new proposal `log.append` and `blob.put` by the member must get 403.
pub async fn expect_revoked(scene: &Scene<'_>, member: &Member) -> Result<(), String> {
    let client = scene.client(&member.actor, member.chain.device_id);
    let late = member.author(
        scene.database_id,
        StreamKind::Proposal,
        "late",
        "proposed after the revoke",
    )?;
    refused(
        "revoked member",
        "log.append",
        client.append(std::slice::from_ref(&late)).await,
    )?;
    let probe = Bytes::from_static(b"blob from a revoked member");
    let digest: [u8; 32] = Sha256::digest(&probe).into();
    refused(
        "revoked member",
        "blob.put",
        client
            .blob_put(&Hex32::from_bytes(digest).to_hex(), probe)
            .await,
    )
}

/// A real node key that was never granted must get 403 on signed append and on
/// read of this non-public database. Returns `403` when both calls are refused.
pub async fn expect_nongrantee(scene: &Scene<'_>) -> Result<u16, String> {
    let stranger = Actor::generate();
    let device = new_device_id();
    let client = scene.client(&stranger, device);
    let mut chain = Chain::new(device);
    let probe = stranger_append(&stranger, &mut chain, scene.database_id)?;
    refused(
        "nongrantee",
        "log.append",
        client.append(std::slice::from_ref(&probe)).await,
    )?;
    refused(
        "nongrantee",
        "log.read",
        client
            .read(&scene.owner.to_hex(), &scene.root_device.to_hex(), 1)
            .await
            .map(|_| ()),
    )?;
    Ok(403)
}

fn stranger_append(
    actor: &Actor,
    chain: &mut Chain,
    database_id: DatabaseId,
) -> Result<SignedEntry, String> {
    let ctx = AuthorCtx {
        database_id,
        node_key: actor.node_key,
        device_id: chain.device_id,
        stream_head: chain.head(),
        seen_heads: Vec::new(),
        seen_lamports: Vec::new(),
        authored_at_ms: now_ms(),
    };
    let value = encode_string_field("nongrantee probe");
    let body = data_record(StreamKind::Direct, "relay_e2e_nongrantee", "probe", &value);
    let entry = author_data(&ctx, &actor.signing_key, body).map_err(|e| e.to_string())?;
    chain.advance(&entry)?;
    Ok(entry)
}

fn refused(who: &str, call: &str, outcome: Result<(), String>) -> Result<(), String> {
    match outcome {
        Ok(()) => Err(format!("relay accepted {call} from a {who}")),
        Err(error) if http_status(&error) == Some(403) => Ok(()),
        Err(error) => Err(format!(
            "{who} {call} failed with something other than 403: {error}"
        )),
    }
}

pub(crate) fn http_status(error: &str) -> Option<u16> {
    let rest = error.split("HTTP ").nth(1)?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}
