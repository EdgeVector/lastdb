use super::super::bytes::*;
use super::super::corrupt;
use crate::Result;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Marker {
    pub(super) operations: u32,
    pub(super) sequence: u64,
    pub(super) previous: u64,
    pub(super) stream: Uuid,
    pub(super) logical_bytes: u64,
    pub(super) accepted_ms: u64,
    pub(super) batch_digest: [u8; 32],
}

impl Marker {
    pub(super) fn encode(&self, commit: bool) -> Result<[u8; 128]> {
        self.validate()?;
        if !commit && self.batch_digest != [0; 32] {
            return Err(corrupt("BEGIN contains a batch digest"));
        }
        let mut bytes = [0; 128];
        bytes[..8].copy_from_slice(if commit { b"LDJCM1\0\0" } else { b"LDJBG1\0\0" });
        put16(&mut bytes, 8, 1);
        put16(&mut bytes, 10, 128);
        put32(&mut bytes, 12, self.operations);
        put64(&mut bytes, 16, self.sequence);
        put64(&mut bytes, 24, self.previous);
        bytes[32..48].copy_from_slice(self.stream.as_bytes());
        put64(&mut bytes, 48, self.logical_bytes);
        put64(&mut bytes, 56, self.accepted_ms);
        bytes[64..96].copy_from_slice(&self.batch_digest);
        let sum = checksum(&bytes, 96);
        bytes[96..128].copy_from_slice(&sum);
        Ok(bytes)
    }

    pub(super) fn decode(bytes: &[u8; 128], commit: bool) -> Result<Self> {
        let magic = if commit { b"LDJCM1\0\0" } else { b"LDJBG1\0\0" };
        if &bytes[..8] != magic || u16_at(bytes, 8) != 1 || u16_at(bytes, 10) != 128 {
            return Err(corrupt("journal marker format"));
        }
        verify_checksum(bytes, 96)?;
        let marker = Self {
            operations: u32_at(bytes, 12),
            sequence: u64_at(bytes, 16),
            previous: u64_at(bytes, 24),
            stream: nonnil(uuid_at(bytes, 32))?,
            logical_bytes: u64_at(bytes, 48),
            accepted_ms: u64_at(bytes, 56),
            batch_digest: bytes[64..96].try_into().expect("marker digest"),
        };
        marker.validate()?;
        if !commit && marker.batch_digest != [0; 32] {
            return Err(corrupt("BEGIN contains a batch digest"));
        }
        Ok(marker)
    }

    fn validate(&self) -> Result<()> {
        nonnil(self.stream)?;
        if self.operations == 0
            || self.operations as usize > super::MAX_OPERATIONS
            || self.previous.checked_add(1) != Some(self.sequence)
            || !(256..=super::MAX_BATCH_BYTES).contains(&self.logical_bytes)
        {
            return Err(corrupt("journal marker count, sequence, or byte limit"));
        }
        Ok(())
    }
}
