//! Authoring of the publisher's device streams (birth, grants, revoke, bundles, heads, and the
//! cleanup's snapshot).

use app_identity_crypto::SigningKey;
use mutation_log::{
    author_birth, author_bundle, author_data, author_grant, author_heads, author_lease,
    author_snapshot, decode_signature_b64url, entry_hash, grant_raise, grant_reduce, AuthorCtx,
    BundleBody, DataBody, DatabaseId, DeviceHead, DeviceId, EncodeError, Grantee, Hex32,
    IncorporatedFrom, LeaseBody, NodeKey, Role, SignedEntry, SnapshotBody, StreamHead, StreamKind,
};

use sha2::{Digest, Sha256};

use crate::identity::{now_ms, Actor, DbBirth};

/// Authoring state of one device stream.
pub struct Chain {
    pub device_id: DeviceId,
    head: Option<StreamHead>,
}

impl Chain {
    pub const fn new(device_id: DeviceId) -> Self {
        Self {
            device_id,
            head: None,
        }
    }

    /// Current stream head, `None` before the first entry.
    pub const fn head(&self) -> Option<StreamHead> {
        self.head
    }

    /// Make `entry` the new tip after authoring it.
    pub fn advance(&mut self, entry: &SignedEntry) -> Result<(), String> {
        let hash = entry_hash(entry).map_err(|e| e.to_string())?;
        let header = entry.header();
        self.head = Some(StreamHead {
            entry_hash: hash,
            writer_seq: header.writer_seq,
            lamport: header.lamport,
        });
        Ok(())
    }

    /// Tip of the stream as cited by a `heads` entry; `None` before the first entry.
    pub fn tip(&self) -> Option<DeviceHead> {
        self.head.map(|head| DeviceHead {
            device_id: self.device_id,
            writer_seq: head.writer_seq,
            entry_hash: head.entry_hash,
        })
    }
}

/// The database publisher: one node key writing through several device streams.
pub struct Publisher {
    pub actor: Actor,
    pub database_id: DatabaseId,
}

impl Publisher {
    fn author(
        &self,
        chain: &mut Chain,
        sign: impl FnOnce(&AuthorCtx, &SigningKey) -> Result<SignedEntry, EncodeError>,
    ) -> Result<SignedEntry, String> {
        let ctx = AuthorCtx {
            database_id: self.database_id,
            node_key: self.actor.node_key,
            device_id: chain.device_id,
            stream_head: chain.head,
            seen_heads: Vec::new(),
            seen_lamports: Vec::new(),
            authored_at_ms: now_ms(),
        };
        let entry = sign(&ctx, &self.actor.signing_key).map_err(|e| e.to_string())?;
        chain.advance(&entry)?;
        Ok(entry)
    }

    pub fn birth(&self, chain: &mut Chain, birth: &DbBirth) -> Result<SignedEntry, String> {
        self.author(chain, |ctx, key| {
            author_birth(
                ctx,
                key,
                self.actor.node_key,
                birth.db_salt,
                birth.birth_signature.clone(),
            )
        })
    }

    pub fn grant_read(&self, chain: &mut Chain, member: NodeKey) -> Result<SignedEntry, String> {
        self.author(chain, |ctx, key| {
            author_grant(ctx, key, grant_raise(Grantee::Member(member), Role::Read))
        })
    }

    pub fn grant_write(&self, chain: &mut Chain, member: NodeKey) -> Result<SignedEntry, String> {
        self.author(chain, |ctx, key| {
            author_grant(ctx, key, grant_raise(Grantee::Member(member), Role::Write))
        })
    }

    pub fn grant_submit(&self, chain: &mut Chain, member: NodeKey) -> Result<SignedEntry, String> {
        self.author(chain, |ctx, key| {
            author_grant(ctx, key, grant_raise(Grantee::Member(member), Role::Submit))
        })
    }

    /// A write lease for the whole database. The relay stores it without reading it.
    pub fn lease(
        &self,
        chain: &mut Chain,
        expires_at_ms: u64,
        closes_at_ms: u64,
    ) -> Result<SignedEntry, String> {
        self.author(chain, |ctx, key| {
            let body = LeaseBody {
                expires_at_ms,
                closes_at_ms,
                closed: Vec::new(),
            };
            author_lease(ctx, key, body)
        })
    }

    /// Revoke `member`. The entry format carries cutoffs; nothing reads them.
    pub fn revoke(&self, chain: &mut Chain, member: NodeKey) -> Result<SignedEntry, String> {
        self.author(chain, |ctx, key| {
            let body = grant_reduce(Grantee::Member(member), Role::Revoked, Vec::new());
            author_grant(ctx, key, body)
        })
    }

    /// Incorporate a member's proposal: a direct entry that carries the proposal's
    /// payload, cites it in `seen_heads` and names it in `incorporated_from`.
    pub fn incorporate(
        &self,
        chain: &mut Chain,
        proposal: &SignedEntry,
    ) -> Result<SignedEntry, String> {
        let SignedEntry::Data { body: from, .. } = proposal else {
            return Err("only a data entry can be incorporated".into());
        };
        let header = proposal.header();
        let sig = decode_signature_b64url(&header.signature).map_err(|e| e.to_string())?;
        let digest: [u8; 32] = Sha256::digest(sig).into();
        let body = DataBody {
            stream: StreamKind::Direct,
            namespace: from.namespace.clone(),
            payload: from.payload.clone(),
            file: None,
            incorporated_from: Some(IncorporatedFrom {
                node_key: header.node_key,
                device_id: header.device_id,
                writer_seq: header.writer_seq,
                signature_sha256: Hex32::from_bytes(digest),
            }),
            grant_at_device: None,
            grant_at_seq: None,
        };
        let proposal_hash = entry_hash(proposal).map_err(|e| e.to_string())?;
        self.author(chain, |ctx, key| {
            let mut ctx = ctx.clone();
            ctx.seen_heads.push(proposal_hash);
            ctx.seen_lamports.push(header.lamport);
            author_data(&ctx, key, body)
        })
    }

    pub fn bundle(&self, chain: &mut Chain, body: BundleBody) -> Result<SignedEntry, String> {
        self.author(chain, |ctx, key| author_bundle(ctx, key, body))
    }

    pub fn heads(&self, chain: &mut Chain, tips: Vec<DeviceHead>) -> Result<SignedEntry, String> {
        self.author(chain, |ctx, key| author_heads(ctx, key, tips))
    }

    /// The `snapshot` entry that `blob/drop` names (see `cleanup.rs`).
    pub fn snapshot(&self, chain: &mut Chain, body: SnapshotBody) -> Result<SignedEntry, String> {
        self.author(chain, |ctx, key| author_snapshot(ctx, key, body))
    }
}
