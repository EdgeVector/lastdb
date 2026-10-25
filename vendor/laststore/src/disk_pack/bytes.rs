use super::corrupt;
use crate::Result;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(super) fn u16_at(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(bytes[at..at + 2].try_into().expect("fixed field"))
}

pub(super) fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("fixed field"))
}

pub(super) fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("fixed field"))
}

pub(super) fn uuid_at(bytes: &[u8], at: usize) -> Uuid {
    Uuid::from_bytes(bytes[at..at + 16].try_into().expect("fixed field"))
}

pub(super) fn put16(bytes: &mut [u8], at: usize, value: u16) {
    bytes[at..at + 2].copy_from_slice(&value.to_le_bytes());
}

pub(super) fn put32(bytes: &mut [u8], at: usize, value: u32) {
    bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

pub(super) fn put64(bytes: &mut [u8], at: usize, value: u64) {
    bytes[at..at + 8].copy_from_slice(&value.to_le_bytes());
}

pub(super) fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(super) fn checksum(bytes: &[u8], at: usize) -> [u8; 32] {
    let _timing = crate::commit_timing::stage("format_checksum_us");
    let mut hash = Sha256::new();
    hash.update(&bytes[..at]);
    hash.update([0; 32]);
    hash.update(&bytes[at + 32..]);
    hash.finalize().into()
}

pub(super) fn verify_checksum(bytes: &[u8], at: usize) -> Result<()> {
    if checksum(bytes, at) != bytes[at..at + 32] {
        return Err(corrupt("checksum mismatch"));
    }
    Ok(())
}

pub(super) fn zero(bytes: &[u8]) -> Result<()> {
    let _timing = crate::commit_timing::stage("format_zero_us");
    crate::commit_timing::count("format_zero_bytes", bytes.len() as u64);
    if bytes.iter().any(|byte| *byte != 0) {
        return Err(corrupt("nonzero reserved bytes"));
    }
    Ok(())
}

pub(super) fn nonnil(id: Uuid) -> Result<Uuid> {
    if id.is_nil() {
        return Err(corrupt("nil object identity"));
    }
    Ok(id)
}
