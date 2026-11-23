//! The author of the personal database's entries: it seals and signs a write
//! when the store commits it.
//!
//! The store of the personal database captures every write for the signed log.
//! It makes each one, in the transaction that commits it, the next entry of this
//! device's stream, and asks this author for the two things it cannot do itself:
//! seal bytes with the database read key, and sign the entry with the node key.
//! The entry is final from the commit on. The sender sends its bytes.
//!
//! Everything here comes from the node seed and the device id, so the host can
//! make the author before it opens the store.

use std::path::Path;

use fold_db::storage::disk_backup::accepted::{CaptureAuthor, LocalEntry, SignedLocal};
use mutation_log::{
    author_data, entry_hash, AuthorCtx, DataBody, DatabaseId, EntryHash, Hex32, MutationFileRef,
    StreamHead, StreamKind, SEALED_NAMESPACE,
};

use crate::author_identity::AuthorIdentity;
use crate::personal_seal::Sealer;

/// Seals and signs the entries of this device's stream of the personal database.
pub(crate) struct PersonalAuthor {
    database: DatabaseId,
    identity: AuthorIdentity,
    device_hex: String,
    sealer: Sealer,
    /// Every entry of the personal database cites the birth.
    birth_hash: EntryHash,
    birth_order: u64,
}

impl PersonalAuthor {
    /// The author for the home with this node seed. The first call makes the device id.
    pub(crate) fn of_home(home: &Path, node_seed: &[u8; 32]) -> Result<Self, String> {
        let identity = AuthorIdentity::of_home(home, node_seed)?;
        let database_id = crate::personal_identity::database_id(node_seed);
        let (_, birth, birth_hash) = crate::database_birth::personal(node_seed, &identity)?;
        Ok(Self {
            database: DatabaseId::from_hex(&database_id)
                .map_err(|_| "database id is not hex".to_owned())?,
            device_hex: identity.device_id.to_hex(),
            sealer: Sealer::for_node_seed(node_seed)?,
            birth_order: birth.position.lamport,
            birth_hash,
            identity,
        })
    }
}

impl CaptureAuthor for PersonalAuthor {
    fn device(&self) -> &str {
        &self.device_hex
    }

    fn seal_file(&self, seq: u64, plain: &[u8]) -> Result<Vec<u8>, String> {
        self.sealer
            .seal(self.identity.node_key, self.identity.device_id, seq, plain)
    }

    fn sign(
        &self,
        previous: Option<&LocalEntry>,
        order: u64,
        body: &[u8],
        file_sha256: Option<&str>,
    ) -> Result<SignedLocal, String> {
        let head = match previous {
            Some(held) => Some(StreamHead {
                entry_hash: EntryHash::from_hex(&held.hash)
                    .map_err(|_| "the entry before this one has a bad hash".to_owned())?,
                writer_seq: held.seq,
                lamport: held.order,
            }),
            None => None,
        };
        let seq = previous.map_or(1, |held| held.seq + 1);
        let file = match file_sha256 {
            Some(sha256) => Some(MutationFileRef {
                file_handle: "file".to_owned(),
                sha256: Hex32::from_hex(sha256)
                    .map_err(|_| "a file digest is not hex".to_owned())?,
            }),
            None => None,
        };
        let author = AuthorCtx {
            database_id: self.database,
            node_key: self.identity.node_key,
            device_id: self.identity.device_id,
            stream_head: head,
            seen_heads: vec![self.birth_hash],
            seen_lamports: vec![self.birth_order],
            authored_at_ms: order,
        };
        let data = DataBody {
            stream: StreamKind::Direct,
            namespace: SEALED_NAMESPACE.to_owned(),
            payload: self.sealer.seal(
                self.identity.node_key,
                self.identity.device_id,
                seq,
                body,
            )?,
            file,
            incorporated_from: None,
            grant_at_device: None,
            grant_at_seq: None,
        };
        let entry = author_data(&author, &self.identity.signing_key, data)
            .map_err(|error| error.to_string())?;
        // The store stamped the write with this order value. An entry that could
        // only be signed under a higher one would disagree with its own record.
        if entry.header().lamport != order {
            return Err("the order value of a write is not above the entry before it".into());
        }
        let hash = entry_hash(&entry).map_err(|error| error.to_string())?;
        Ok(SignedLocal {
            entry: serde_json::to_vec(&entry).map_err(|_| "cannot encode a signed entry")?,
            hash: hash.to_hex(),
        })
    }
}
