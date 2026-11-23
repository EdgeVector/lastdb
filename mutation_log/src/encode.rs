//! Binary encoding of `signed_bytes(E)`.
//!
//! # Common header (big-endian)
//!
//! `Domain UTF-8` + `0x00`, then:
//! `database_id`(32) · `node_key`(32) · `device_id`(32, Hex32) ·
//! `writer_seq`(u64) · `prev_hash`(32) · `authored_at_ms`(u64) ·
//! `lamport`(u64) · `seen_heads`(u16 count + `entry_hash`(32)× sorted ascending).
//!
//! # Kind bodies
//!
//! - **birth:** `owner`(32) · `db_salt`(32) · `birth_signature` raw 64
//!   (`birth_signature` verifies `owner ‖ db_salt` under `database_id`)
//! - **data:** `stream` u8 (0=direct,1=proposal) · `namespace`(u16+utf8) ·
//!   `payload`(u32+bytes) · `flags` u8 (bit0=file, bit1=incorporated_from,
//!   bit2=grant_at) · optionals in that bit order
//! - **grant:** grantee tag (0=any / 1+node_key32) · `role` u8 ·
//!   cutoffs (`u16` count + `device_id`(32)·`writer_seq`(u64)·`entry_hash`(32)×)
//! - **schema:** `namespace`(u16+utf8) · `definition`(u32+bytes, UTF-8 JSON)
//! - **blob_delete:** `sha256`(32)
//! - **heads:** tips (`u16` + `device_id`(32)·`writer_seq`(u64)·`entry_hash`(32)×)
//! - **snapshot:** `manifest_sha256`(32) · `manifest_bytes`(u64) · through (`u16` count ≤ 1024,
//!   strictly ascending by `device_id`, each `device_id`(32)·`writer_seq`(u64)·`entry_hash`(32)·`order`(u64))
//! - **lease:** `expires_at_ms`(u64) · `closes_at_ms`(u64) · closed (`u16` count,
//!   strictly ascending by `node_key` then `device_id`, each
//!   `node_key`(32)·`device_id`(32)·`writer_seq`(u64))
//! - **relays:** urls (`u16` + per url `u16` len + utf8)
//! - **bundle:** `blob_sha256`(32) · `blob_size`(u64) · `change_count`(u32) ·
//!   `flags` u8 (bit2=`grant_at`) · grant optionals

use crate::entry::{
    BundleBody, DataBody, DeviceHead, GrantBody, HeadsBody, IncorporatedFrom, LeaseBody,
    MutationFileRef, RelaysBody, RevocationCutoff, SchemaBody, SignedEntry, SnapshotBody,
};
use crate::ids::Hex32;
use crate::kinds::Grantee;
use crate::wire::{decode_signature_b64url, EncodeError};

/// Build the exact bytes covered by `E.signature`.
pub fn signed_bytes(entry: &SignedEntry) -> Result<Vec<u8>, EncodeError> {
    let mut buf = Vec::with_capacity(256);
    let h = entry.header();
    let domain = entry.kind().domain();
    buf.extend_from_slice(domain.as_bytes());
    buf.push(0);
    buf.extend_from_slice(h.database_id.as_bytes());
    buf.extend_from_slice(h.node_key.as_bytes());
    buf.extend_from_slice(&h.device_id.to_bytes());
    put_u64(&mut buf, h.writer_seq);
    buf.extend_from_slice(h.prev_hash.as_bytes());
    put_u64(&mut buf, h.authored_at_ms);
    put_u64(&mut buf, h.lamport);
    encode_seen_heads(&mut buf, &h.seen_heads)?;
    match entry {
        SignedEntry::Birth { body, .. } => encode_birth(&mut buf, body)?,
        SignedEntry::Data { body, .. } => encode_data(&mut buf, body)?,
        SignedEntry::Bundle { body, .. } => encode_bundle(&mut buf, body)?,
        SignedEntry::Grant { body, .. } => encode_grant(&mut buf, body)?,
        SignedEntry::Schema { body, .. } => encode_schema(&mut buf, body)?,
        SignedEntry::BlobDelete { body, .. } => {
            buf.extend_from_slice(body.sha256.as_bytes());
        }
        SignedEntry::Heads { body, .. } => encode_heads(&mut buf, body)?,
        SignedEntry::Relays { body, .. } => encode_relays(&mut buf, body)?,
        SignedEntry::Snapshot { body, .. } => encode_snapshot(&mut buf, body)?,
        SignedEntry::Lease { body, .. } => encode_lease(&mut buf, body)?,
    }
    Ok(buf)
}

fn encode_schema(buf: &mut Vec<u8>, body: &SchemaBody) -> Result<(), EncodeError> {
    put_str_u16(buf, &body.namespace)?;
    put_bytes_u32(buf, &body.definition)
}

fn encode_seen_heads(buf: &mut Vec<u8>, heads: &[Hex32]) -> Result<(), EncodeError> {
    if heads.len() > u16::MAX as usize {
        return Err(EncodeError::TooManySeenHeads);
    }
    let mut sorted = heads.to_vec();
    sorted.sort();
    if sorted.windows(2).any(|w| w[0] == w[1]) {
        return Err(EncodeError::DuplicateSeenHead);
    }
    put_u16(buf, sorted.len() as u16);
    for h in &sorted {
        buf.extend_from_slice(h.as_bytes());
    }
    Ok(())
}

fn encode_birth(buf: &mut Vec<u8>, body: &crate::entry::BirthBody) -> Result<(), EncodeError> {
    buf.extend_from_slice(body.owner.as_bytes());
    buf.extend_from_slice(body.db_salt.as_bytes());
    let sig = decode_signature_b64url(&body.birth_signature)?;
    buf.extend_from_slice(&sig);
    Ok(())
}

fn encode_bundle(buf: &mut Vec<u8>, body: &BundleBody) -> Result<(), EncodeError> {
    buf.extend_from_slice(body.blob_sha256.as_bytes());
    put_u64(buf, body.blob_size);
    put_u32(buf, body.change_count);
    let mut flags = 0u8;
    if body.grant_at_device.is_some() || body.grant_at_seq.is_some() {
        flags |= 0x04;
    }
    buf.push(flags);
    if flags & 0x04 != 0 {
        let device = body.grant_at_device.ok_or(EncodeError::GrantAtIncomplete)?;
        let seq = body.grant_at_seq.ok_or(EncodeError::GrantAtIncomplete)?;
        buf.extend_from_slice(&device.to_bytes());
        put_u64(buf, seq);
    }
    Ok(())
}

fn encode_data(buf: &mut Vec<u8>, body: &DataBody) -> Result<(), EncodeError> {
    buf.push(body.stream.wire_byte());
    put_str_u16(buf, &body.namespace)?;
    put_bytes_u32(buf, &body.payload)?;
    let mut flags = 0u8;
    if body.file.is_some() {
        flags |= 0x01;
    }
    if body.incorporated_from.is_some() {
        flags |= 0x02;
    }
    if body.grant_at_device.is_some() || body.grant_at_seq.is_some() {
        flags |= 0x04;
    }
    buf.push(flags);
    if let Some(file) = &body.file {
        encode_file(buf, file)?;
    }
    if let Some(inc) = &body.incorporated_from {
        encode_incorporated(buf, inc);
    }
    if flags & 0x04 != 0 {
        let device = body.grant_at_device.ok_or(EncodeError::GrantAtIncomplete)?;
        let seq = body.grant_at_seq.ok_or(EncodeError::GrantAtIncomplete)?;
        buf.extend_from_slice(&device.to_bytes());
        put_u64(buf, seq);
    }
    Ok(())
}

fn encode_file(buf: &mut Vec<u8>, file: &MutationFileRef) -> Result<(), EncodeError> {
    put_str_u16(buf, &file.file_handle)?;
    buf.extend_from_slice(file.sha256.as_bytes());
    Ok(())
}

fn encode_incorporated(buf: &mut Vec<u8>, inc: &IncorporatedFrom) {
    buf.extend_from_slice(inc.node_key.as_bytes());
    buf.extend_from_slice(&inc.device_id.to_bytes());
    put_u64(buf, inc.writer_seq);
    buf.extend_from_slice(inc.signature_sha256.as_bytes());
}

fn encode_grant(buf: &mut Vec<u8>, body: &GrantBody) -> Result<(), EncodeError> {
    buf.push(body.grantee.tag());
    if let Grantee::Member(key) = body.grantee {
        buf.extend_from_slice(key.as_bytes());
    }
    buf.push(body.role.wire_byte());
    encode_cutoffs(buf, &body.cutoffs)
}

fn encode_cutoffs(buf: &mut Vec<u8>, cutoffs: &[RevocationCutoff]) -> Result<(), EncodeError> {
    if cutoffs.len() > 256 {
        return Err(EncodeError::TooManyCutoffs);
    }
    put_u16(buf, cutoffs.len() as u16);
    for c in cutoffs {
        buf.extend_from_slice(&c.device_id.to_bytes());
        put_u64(buf, c.writer_seq);
        buf.extend_from_slice(c.entry_hash.as_bytes());
    }
    Ok(())
}

fn encode_heads(buf: &mut Vec<u8>, body: &HeadsBody) -> Result<(), EncodeError> {
    if body.publisher_heads.is_empty() || body.publisher_heads.len() > 256 {
        return Err(EncodeError::BadHeadsCount);
    }
    put_u16(buf, body.publisher_heads.len() as u16);
    for tip in &body.publisher_heads {
        encode_device_head(buf, tip);
    }
    Ok(())
}

fn encode_device_head(buf: &mut Vec<u8>, tip: &DeviceHead) {
    buf.extend_from_slice(&tip.device_id.to_bytes());
    put_u64(buf, tip.writer_seq);
    buf.extend_from_slice(tip.entry_hash.as_bytes());
}

fn encode_snapshot(buf: &mut Vec<u8>, body: &SnapshotBody) -> Result<(), EncodeError> {
    let through = &body.through;
    let ascending = through
        .windows(2)
        .all(|pair| pair[0].device_id.to_bytes() < pair[1].device_id.to_bytes());
    if through.len() > 1024 || !ascending {
        return Err(EncodeError::BadSnapshotThrough);
    }
    buf.extend_from_slice(body.manifest_sha256.as_bytes());
    put_u64(buf, body.manifest_bytes);
    put_u16(buf, through.len() as u16);
    for position in through {
        buf.extend_from_slice(&position.device_id.to_bytes());
        put_u64(buf, position.writer_seq);
        buf.extend_from_slice(position.entry_hash.as_bytes());
        put_u64(buf, position.order);
    }
    Ok(())
}

fn encode_lease(buf: &mut Vec<u8>, body: &LeaseBody) -> Result<(), EncodeError> {
    let closed = &body.closed;
    let ascending = closed
        .windows(2)
        .all(|pair| (pair[0].node_key, pair[0].device_id) < (pair[1].node_key, pair[1].device_id));
    if closed.len() > u16::MAX as usize || !ascending {
        return Err(EncodeError::BadLeaseClosed);
    }
    put_u64(buf, body.expires_at_ms);
    put_u64(buf, body.closes_at_ms);
    put_u16(buf, closed.len() as u16);
    for stream in closed {
        buf.extend_from_slice(stream.node_key.as_bytes());
        buf.extend_from_slice(&stream.device_id.to_bytes());
        put_u64(buf, stream.writer_seq);
    }
    Ok(())
}

fn encode_relays(buf: &mut Vec<u8>, body: &RelaysBody) -> Result<(), EncodeError> {
    if body.relays.is_empty() || body.relays.len() > 32 {
        return Err(EncodeError::BadRelaysCount);
    }
    put_u16(buf, body.relays.len() as u16);
    for r in &body.relays {
        put_str_u16(buf, &r.url)?;
    }
    Ok(())
}

fn put_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn put_str_u16(buf: &mut Vec<u8>, s: &str) -> Result<(), EncodeError> {
    if s.len() > u16::MAX as usize {
        return Err(EncodeError::StringTooLong);
    }
    put_u16(buf, s.len() as u16);
    buf.extend_from_slice(s.as_bytes());
    Ok(())
}

fn put_bytes_u32(buf: &mut Vec<u8>, bytes: &[u8]) -> Result<(), EncodeError> {
    if bytes.len() > u32::MAX as usize {
        return Err(EncodeError::PayloadTooLong);
    }
    buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(bytes);
    Ok(())
}
