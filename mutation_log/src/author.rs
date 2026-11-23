//! Author helpers: assign chain fields, sign, and return a `SignedEntry`.

use app_identity_crypto::SigningKey;

use crate::accept::StreamHead;
use crate::entry::{
    BirthBody, BlobDeleteBody, BundleBody, DataBody, DeviceHead, EntryHeader, GrantBody, HeadsBody,
    LeaseBody, RelayUrl, RelaysBody, SchemaBody, SignedEntry, SnapshotBody,
};
use crate::history_util::next_lamport;
use crate::ids::{DatabaseId, DeviceId, EntryHash, Hex32, NodeKey};
use crate::kinds::{Grantee, Role, StreamKind};
use crate::wire::{encode_signature_b64url, EncodeError};
use crate::{encode, wire};

/// Inputs shared by every author helper.
#[derive(Clone, Debug)]
pub struct AuthorCtx {
    /// Database id.
    pub database_id: DatabaseId,
    /// Writer public key (must match `signing_key`).
    pub node_key: NodeKey,
    /// Device stream id.
    pub device_id: DeviceId,
    /// Current stream head (`None` for seq 1).
    pub stream_head: Option<StreamHead>,
    /// Seen head hashes (will be sorted in signed bytes).
    pub seen_heads: Vec<EntryHash>,
    /// Lamports of those seen heads (same order as `seen_heads`).
    pub seen_lamports: Vec<u64>,
    /// The time the write was made, in milliseconds (caller-supplied; no clock
    /// in this crate). It is the entry's order value unless the stream or a
    /// cited entry is already above it.
    pub authored_at_ms: u64,
}

impl AuthorCtx {
    fn header_unsigned(&self) -> EntryHeader {
        let (writer_seq, prev_hash, prev_lamport) = match self.stream_head {
            None => (1, EntryHash::ZERO, 0),
            Some(h) => (h.writer_seq + 1, h.entry_hash, h.lamport),
        };
        // The larger of the clock and one more than the highest value seen, so
        // an entry made after another is always above it, and a value stays a
        // time in milliseconds.
        let lamport =
            next_lamport(prev_lamport, self.seen_lamports.iter().copied()).max(self.authored_at_ms);
        EntryHeader {
            database_id: self.database_id,
            node_key: self.node_key,
            device_id: self.device_id,
            writer_seq,
            prev_hash,
            authored_at_ms: self.authored_at_ms,
            lamport,
            seen_heads: self.seen_heads.clone(),
            signature: String::new(),
        }
    }
}

/// Sign `entry` in place with `signing_key` (fills `signature`).
pub fn sign_entry(entry: &mut SignedEntry, signing_key: &SigningKey) -> Result<(), EncodeError> {
    let bytes = encode::signed_bytes(entry)?;
    let sig = app_identity_crypto::sign(signing_key, &bytes);
    entry.header_mut().signature = encode_signature_b64url(&sig);
    Ok(())
}

/// Author a birth entry (`writer_seq=1`).
pub fn author_birth(
    ctx: &AuthorCtx,
    signing_key: &SigningKey,
    owner: NodeKey,
    db_salt: Hex32,
    birth_signature: String,
) -> Result<SignedEntry, EncodeError> {
    let mut entry = SignedEntry::Birth {
        header: ctx.header_unsigned(),
        namespace: "birth".into(),
        body: BirthBody {
            owner,
            db_salt,
            birth_signature,
        },
    };
    sign_entry(&mut entry, signing_key)?;
    Ok(entry)
}

/// Author a signed bundle entry (blob must already be on the relay).
pub fn author_bundle(
    ctx: &AuthorCtx,
    signing_key: &SigningKey,
    body: BundleBody,
) -> Result<SignedEntry, EncodeError> {
    let mut entry = SignedEntry::Bundle {
        header: ctx.header_unsigned(),
        namespace: "bundle".into(),
        body,
    };
    sign_entry(&mut entry, signing_key)?;
    Ok(entry)
}

/// Author a direct or proposal data entry.
pub fn author_data(
    ctx: &AuthorCtx,
    signing_key: &SigningKey,
    body: DataBody,
) -> Result<SignedEntry, EncodeError> {
    let mut entry = SignedEntry::Data {
        header: ctx.header_unsigned(),
        body,
    };
    sign_entry(&mut entry, signing_key)?;
    Ok(entry)
}

/// Author a grant / revoke entry.
pub fn author_grant(
    ctx: &AuthorCtx,
    signing_key: &SigningKey,
    body: GrantBody,
) -> Result<SignedEntry, EncodeError> {
    let mut entry = SignedEntry::Grant {
        header: ctx.header_unsigned(),
        namespace: "grant".into(),
        body,
    };
    sign_entry(&mut entry, signing_key)?;
    Ok(entry)
}

/// Author a schema declaration entry.
pub fn author_schema(
    ctx: &AuthorCtx,
    signing_key: &SigningKey,
    body: SchemaBody,
) -> Result<SignedEntry, EncodeError> {
    let mut entry = SignedEntry::Schema {
        header: ctx.header_unsigned(),
        body,
    };
    sign_entry(&mut entry, signing_key)?;
    Ok(entry)
}

/// Author a blob_delete entry.
pub fn author_blob_delete(
    ctx: &AuthorCtx,
    signing_key: &SigningKey,
    sha256: Hex32,
) -> Result<SignedEntry, EncodeError> {
    let mut entry = SignedEntry::BlobDelete {
        header: ctx.header_unsigned(),
        namespace: "blob_delete".into(),
        body: BlobDeleteBody { sha256 },
    };
    sign_entry(&mut entry, signing_key)?;
    Ok(entry)
}

/// Author a heads attestation.
pub fn author_heads(
    ctx: &AuthorCtx,
    signing_key: &SigningKey,
    publisher_heads: Vec<DeviceHead>,
) -> Result<SignedEntry, EncodeError> {
    let mut entry = SignedEntry::Heads {
        header: ctx.header_unsigned(),
        namespace: "heads".into(),
        body: HeadsBody { publisher_heads },
    };
    sign_entry(&mut entry, signing_key)?;
    Ok(entry)
}

/// Author a snapshot entry. The positions are sorted by device id.
pub fn author_snapshot(
    ctx: &AuthorCtx,
    signing_key: &SigningKey,
    mut body: SnapshotBody,
) -> Result<SignedEntry, EncodeError> {
    body.through
        .sort_by_key(|position| position.device_id.to_bytes());
    let mut entry = SignedEntry::Snapshot {
        header: ctx.header_unsigned(),
        namespace: "snapshot".into(),
        body,
    };
    sign_entry(&mut entry, signing_key)?;
    Ok(entry)
}

/// Author a lease entry. The closed streams are sorted by node key and device id.
pub fn author_lease(
    ctx: &AuthorCtx,
    signing_key: &SigningKey,
    mut body: LeaseBody,
) -> Result<SignedEntry, EncodeError> {
    body.closed
        .sort_by_key(|stream| (stream.node_key, stream.device_id));
    let mut entry = SignedEntry::Lease {
        header: ctx.header_unsigned(),
        namespace: "lease".into(),
        body,
    };
    sign_entry(&mut entry, signing_key)?;
    Ok(entry)
}

/// Author a relays list entry.
pub fn author_relays(
    ctx: &AuthorCtx,
    signing_key: &SigningKey,
    urls: Vec<String>,
) -> Result<SignedEntry, EncodeError> {
    let relays = urls.into_iter().map(|url| RelayUrl { url }).collect();
    let mut entry = SignedEntry::Relays {
        header: ctx.header_unsigned(),
        namespace: "relays".into(),
        body: RelaysBody { relays },
    };
    sign_entry(&mut entry, signing_key)?;
    Ok(entry)
}

/// Convenience: build a grant body with no cutoffs.
#[must_use]
pub fn grant_raise(grantee: Grantee, role: Role) -> GrantBody {
    GrantBody {
        grantee,
        role,
        cutoffs: Vec::new(),
    }
}

/// Convenience: build a reduction/revoke body with the cutoffs the wire format carries.
#[must_use]
pub fn grant_reduce(
    grantee: Grantee,
    role: Role,
    cutoffs: Vec<crate::entry::RevocationCutoff>,
) -> GrantBody {
    GrantBody {
        grantee,
        role,
        cutoffs,
    }
}

/// Build a data body for a whole-record put/tombstone.
#[must_use]
pub fn data_record(
    stream: StreamKind,
    namespace: impl Into<String>,
    record_key: &str,
    value: &[u8],
) -> DataBody {
    DataBody {
        stream,
        namespace: namespace.into(),
        payload: crate::payload::encode_record_payload(record_key, value),
        file: None,
        incorporated_from: None,
        grant_at_device: None,
        grant_at_seq: None,
    }
}

/// Hash of an entry (after signature is filled).
pub fn hash_of(entry: &SignedEntry) -> Result<EntryHash, EncodeError> {
    wire::entry_hash(entry)
}
