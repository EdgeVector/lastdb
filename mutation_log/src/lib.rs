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
//! blob_delete=`lastdb-blob-delete-v1`, heads=`lastdb-heads-v1`,
//! relays=`lastdb-relays-v1`.
//!
//! Bundle entries stay **pending** until [`VerifiedHistory::accept_bundle_with_changes`]
//! (or metadata-only via [`accept_entry`]) after the encrypted blob is verified off-crate.
//!
//! Wire `signature`: base64url, no padding, 86 characters. Hex ids: 64 lowercase.
//! `device_id`: 64 lowercase hex (32 bytes, same as `node_key`). `entry_hash = SHA-256(signed_bytes ‖ signature_raw_64)`.
//!
//! # Record LWW payload
//!
//! See [`payload`]: `u16 record_key_len` + key + `value_bytes` (empty = tombstone).
//! Whole-record LWW by `(lamport, node_key, device_id)`. Equal-lamport grants:
//! `revoked` wins. Cutoffs only when lowering/revoking.
//!
//! No snapshots, checkpoints, dual path, or feature flags.

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
mod outbox;
mod payload;
mod wire;

pub use accept::{
    birth_proof_bytes, position_of, stream_id_of, verify_birth_signature, AcceptOutcome,
    PendingReason, RejectReason, StreamHead,
};
pub use author::{
    author_birth, author_blob_delete, author_bundle, author_data, author_grant, author_heads,
    author_relays, data_record, grant_raise, grant_reduce, hash_of, sign_entry, AuthorCtx,
};
pub use bootstrap::{
    bootstrap_from_records, BootstrapError, BootstrapGrant, BootstrapParams, BootstrapRecord,
    BootstrapResult,
};
pub use bundle::{
    validate_bundle_body, validate_bundle_changes, BundleChange, BUNDLE_BLOB_SIZE_MAX,
    BUNDLE_CHANGE_COUNT_MAX,
};
pub use decode::{decode_bundle_body, decode_bundle_signed_bytes, DecodeError};
pub use encode::signed_bytes;
pub use entry::{
    BirthBody, BlobDeleteBody, BundleBody, DataBody, DeviceHead, EntryHeader, GrantBody, HeadsBody,
    IncorporatedFrom, MutationFileRef, RelayUrl, RelaysBody, RevocationCutoff, SignedEntry,
};
pub use grants::{past_cutoff, AcceptedMeta, CutoffTip, GrantState};
pub use history::{EquivocationEvidence, GrantTip, RejectEvidence, VerifiedHistory};
pub use history_util::{grant_body_of, next_lamport};
pub use ids::{DatabaseId, DeviceId, EntryHash, Hex32, IdError, NodeKey, StreamId};
pub use kinds::{domain, EntryKind, Grantee, Role, StreamKind};
pub use outbox::Outbox;
pub use payload::{
    decode_record_payload, encode_record_payload, MaterializedRecord, PayloadError, RecordPayload,
    RecordPosition,
};
pub use wire::{
    decode_signature_b64url, encode_signature_b64url, entry_hash, verify_entry_signature,
    EncodeError, VerifyError,
};
