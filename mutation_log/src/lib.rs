//! Pure signed-history engine for LastDB cloud API v3.
//!
//! This crate implements local **`AcceptEntry(E, S) → accept | pending | reject`**
//! over Ed25519-signed device streams. Relays are untrusted transport; acceptance
//! and authorization are node-side only.
//!
//! # Signed bytes
//!
//! See [`encode`] for the exact binary layout. Common header (big-endian):
//! domain UTF-8 + `0x00`, then `database_id`(32) · `node_key`(32) ·
//! `device_id`(32) · `writer_seq`(u64) · `prev_hash`(32) · `authored_at_ms`(u64) ·
//! `lamport`(u64) · `seen_heads` (u16 + hashes sorted ascending).
//!
//! Domains: birth=`lastdb-birth-v1`, data=`lastdb-mutation-v1`,
//! bundle=`lastdb-bundle-v1`, grant=`lastdb-grant-v1`,
//! schema=`lastdb-schema-v1`, blob_delete=`lastdb-blob-delete-v1`,
//! heads=`lastdb-heads-v1`, relays=`lastdb-relays-v1`,
//! snapshot=`lastdb-snapshot-v1`, lease=`lastdb-lease-v1`.
//!
//! Bundle entries stay **pending** until [`VerifiedHistory::accept_bundle_with_changes`]
//! (or metadata-only via [`accept_entry`]) after the encrypted blob is verified off-crate.
//!
//! Wire `signature`: base64url, no padding, 86 characters. Hex ids: 64 lowercase.
//! `device_id`: 64 lowercase hex (32 bytes, same as `node_key`). `entry_hash = SHA-256(signed_bytes ‖ signature_raw_64)`.
//!
//! # Who signs what
//!
//! The publisher signs direct data, bundles and `lease` entries. A member with
//! a `submit` or `write` grant signs proposals (`stream = proposal`), which
//! change nothing until the publisher signs a direct entry that cites one in
//! `incorporated_from`, or a `heads` entry that records it as not accepted.
//! A member that held `write` as of a `lease` entry also signs direct entries
//! that cite that lease in `seen_heads`; see [`VerifiedHistory::latest_lease`]
//! and `docs/designs/write-leases.md` for when such an entry counts.
//! A grant entry still carries a `cutoffs` field because the wire format
//! requires it; nothing reads it.
//!
//! # Record LWW payload
//!
//! See [`payload`]: `u16 record_key_len` + key + `value_bytes` (empty = tombstone).
//! Whole-record LWW by `(lamport, node_key, device_id)`. The order value
//! `lamport` is a time in milliseconds set when the write was made. Equal-lamport grants:
//! `revoked` wins.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]
#![warn(missing_docs)]

mod accept;
mod auth;
mod author;
mod bootstrap;
mod bundle;
mod decode;
mod encode;
mod entry;
mod grants;
mod history;
mod history_bundle;
mod history_util;
mod ids;
mod kinds;
mod lease_body;
mod named_blobs;
mod payload;
mod record_key;
mod sealed;
mod wire;

pub use accept::{
    birth_proof_bytes, evaluate, position_of, stream_id_of, verify_birth_signature, AcceptContext,
    AcceptOutcome, PendingReason, RejectReason, StreamHead, LAMPORT_LIMIT,
};
pub use author::{
    author_birth, author_blob_delete, author_bundle, author_data, author_grant, author_heads,
    author_lease, author_relays, author_schema, author_snapshot, data_record, grant_raise,
    grant_reduce, hash_of, sign_entry, AuthorCtx,
};
pub use bootstrap::{
    bootstrap_from_records, BootstrapError, BootstrapGrant, BootstrapParams, BootstrapRecord,
    BootstrapResult, BootstrapSchema,
};
pub use bundle::{
    validate_bundle_body, validate_bundle_changes, BundleChange, BUNDLE_BLOB_SIZE_MAX,
    BUNDLE_CHANGE_COUNT_MAX,
};
pub use decode::{decode_bundle_body, decode_bundle_signed_bytes, DecodeError};
pub use encode::signed_bytes;
pub use entry::{
    BirthBody, BlobDeleteBody, BundleBody, ClosedStream, DataBody, DeviceHead, EntryHeader,
    GrantBody, HeadsBody, IncorporatedFrom, LeaseBody, MutationFileRef, RelayUrl, RelaysBody,
    RevocationCutoff, SchemaBody, SignedEntry, SnapshotBody, SnapshotThrough,
};
pub use grants::{AcceptedMeta, GrantState};
pub use history::{EquivocationEvidence, GrantTip, LeaseView, RejectEvidence, VerifiedHistory};
pub use history_util::{grant_body_of, next_lamport};
pub use ids::{DatabaseId, DeviceId, EntryHash, Hex32, IdError, NodeKey, StreamId};
pub use kinds::{domain, EntryKind, Grantee, Role, StreamKind};
pub use named_blobs::named_blobs;
pub use payload::{
    decode_record_payload, encode_record_payload, MaterializedRecord, PayloadError, RecordPayload,
    RecordPosition,
};
pub use record_key::{join_record_key, split_record_key, RANGE_SEPARATOR};
pub use sealed::{
    decode_bundle_changes, decode_named, encode_bundle_changes, encode_named, SealOpener,
    SEALED_NAMESPACE,
};
pub use wire::{
    decode_signature_b64url, encode_signature_b64url, entry_hash, verify_entry_signature,
    EncodeError, VerifyError,
};
