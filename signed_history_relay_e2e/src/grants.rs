//! Grant and revoke against the relay: a `write` member appends, the publisher
//! revokes it with a cutoff, and the relay then refuses the member's next
//! `log.append` and `blob.put` with HTTP 403. The member's earlier entry stays
//! stored and is judged by the cutoff on restore.

use bytes::Bytes;
use mutation_log::{
    author_data, data_record, entry_hash, AcceptOutcome, AuthorCtx, DatabaseId, DeviceId, Grantee,
    Hex32, NodeKey, RevocationCutoff, Role, SignedEntry, StreamKind, VerifiedHistory,
};
use sha2::{Digest, Sha256};
use signed_history_materialize::encode_string_field;

use crate::chain::Chain;
use crate::identity::{new_device_id, now_ms, Actor};
use crate::relay::RelayClient;

/// FoldDB schema (history namespace) for records written by the member.
pub const MEMBER_NAMESPACE: &str = "relay_e2e_member";
/// Record key of the member's one accepted write.
pub const MEMBER_KEY: &str = "from-member";
/// Text of the member's accepted write.
pub const MEMBER_TEXT: &str = "written by a write member before its revoke";

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

/// A node holding a publisher `write` grant, writing through its own device stream.
pub struct Member {
    actor: Actor,
    chain: Chain,
    grant: SignedEntry,
    first: Option<SignedEntry>,
}

impl Member {
    pub const fn node_key(&self) -> NodeKey {
        self.actor.node_key
    }

    /// Cutoff that keeps the member's accepted write and drops anything after it.
    pub fn cutoff(&self) -> Result<RevocationCutoff, String> {
        let first = self.first.as_ref().ok_or("member has not written yet")?;
        Ok(RevocationCutoff {
            device_id: self.chain.device_id,
            writer_seq: first.header().writer_seq,
            entry_hash: entry_hash(first).map_err(|e| e.to_string())?,
        })
    }

    fn write(
        &mut self,
        database_id: DatabaseId,
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
        let mut body = data_record(StreamKind::Direct, MEMBER_NAMESPACE, record_key, &value);
        body.grant_at_device = Some(grant.device_id);
        body.grant_at_seq = Some(grant.writer_seq);
        let entry = author_data(&ctx, &self.actor.signing_key, body).map_err(|e| e.to_string())?;
        self.chain.advance(&entry)?;
        Ok(entry)
    }
}

/// The member reads the publisher's root stream, verifies it with AcceptEntry,
/// finds its own `write` grant, then appends one record bound to that grant.
pub async fn write_as_member(scene: &Scene<'_>, actor: Actor) -> Result<Member, String> {
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
    let grant = page
        .entries
        .into_iter()
        .find(|entry| names_member(entry, actor.node_key))
        .ok_or("publisher history has no grant entry for the member")?;
    let mut member = Member {
        actor,
        chain: Chain::new(device),
        grant,
        first: None,
    };
    let first = member.write(scene.database_id, MEMBER_KEY, MEMBER_TEXT)?;
    client.append(std::slice::from_ref(&first)).await?;
    member.first = Some(first);
    Ok(member)
}

fn names_member(entry: &SignedEntry, member: NodeKey) -> bool {
    matches!(
        entry,
        SignedEntry::Grant { body, .. }
            if body.grantee == Grantee::Member(member) && body.role == Role::Write
    )
}

/// After the revoke is stored, a new `log.append` and `blob.put` by the member must get 403.
pub async fn expect_revoked(scene: &Scene<'_>, member: &mut Member) -> Result<(), String> {
    let client = scene.client(&member.actor, member.chain.device_id);
    let late = member.write(scene.database_id, "late", "written after the revoke")?;
    refused(
        "log.append",
        client.append(std::slice::from_ref(&late)).await,
    )?;
    let probe = Bytes::from_static(b"blob from a revoked member");
    let digest: [u8; 32] = Sha256::digest(&probe).into();
    refused(
        "blob.put",
        client
            .blob_put(&Hex32::from_bytes(digest).to_hex(), probe)
            .await,
    )
}

fn refused(call: &str, outcome: Result<(), String>) -> Result<(), String> {
    match outcome {
        Ok(()) => Err(format!("relay accepted {call} from a revoked member")),
        Err(error) if error.contains("HTTP 403") => Ok(()),
        Err(error) => Err(format!(
            "revoked member {call} failed with something other than 403: {error}"
        )),
    }
}
