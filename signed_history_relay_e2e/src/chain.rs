//! Authoring of the publisher's device streams (birth, read grant, bundles, heads).

use app_identity_crypto::SigningKey;
use mutation_log::{
    author_birth, author_bundle, author_grant, author_heads, entry_hash, grant_raise, AuthorCtx,
    BundleBody, DatabaseId, DeviceHead, DeviceId, EncodeError, Grantee, NodeKey, Role, SignedEntry,
    StreamHead,
};

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
        let hash = entry_hash(&entry).map_err(|e| e.to_string())?;
        let header = entry.header();
        chain.head = Some(StreamHead {
            entry_hash: hash,
            writer_seq: header.writer_seq,
            lamport: header.lamport,
        });
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

    pub fn bundle(&self, chain: &mut Chain, body: BundleBody) -> Result<SignedEntry, String> {
        self.author(chain, |ctx, key| author_bundle(ctx, key, body))
    }

    pub fn heads(&self, chain: &mut Chain, tips: Vec<DeviceHead>) -> Result<SignedEntry, String> {
        self.author(chain, |ctx, key| author_heads(ctx, key, tips))
    }
}
