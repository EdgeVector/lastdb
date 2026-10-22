use super::bytes::*;
use super::{corrupt, Location, MutationVersion, ReadCall, BLOCK_BYTES, KEY_BYTES};
use crate::read_receipt::TemporaryBytes;
use crate::Result;

pub(super) fn record_header(
    collection: &str,
    key: &str,
    body_bytes: u64,
    version: MutationVersion,
) -> Result<[u8; 96]> {
    if key.len() > KEY_BYTES || body_bytes > lastdb_storage_policy::native_v1::RECORD_BODY_MAX_BYTES
    {
        return Err(corrupt("record exceeds legacy-compatible limits"));
    }
    let mut bytes = [0; 96];
    bytes[..8].copy_from_slice(b"LDREC1\0\0");
    put16(&mut bytes, 8, 1);
    bytes[10] = 1;
    put32(&mut bytes, 12, key.len() as u32);
    put64(&mut bytes, 16, body_bytes);
    put64(&mut bytes, 24, version.batch);
    put32(&mut bytes, 32, version.operation);
    put32(&mut bytes, 36, 96);
    bytes[40..72].copy_from_slice(&digest(key.as_bytes()));
    bytes[72..88].copy_from_slice(&digest(collection.as_bytes())[..16]);
    Ok(bytes)
}

impl ReadCall {
    pub(super) fn read_record(
        &mut self,
        collection: &str,
        key: &str,
        location: &Location,
    ) -> Result<Vec<u8>> {
        if let Some(object) = self.object_metadata(location)? {
            let end = location
                .first_block
                .checked_mul(BLOCK_BYTES as u64)
                .and_then(|n| n.checked_add(u64::from(location.first_position)))
                .and_then(|n| n.checked_add(location.record_bytes))
                .ok_or_else(|| corrupt("record object offset overflow"))?;
            if end > object.logical_bytes {
                return Err(corrupt("record outside published object length"));
            }
        }
        let length = usize::try_from(location.record_bytes).map_err(|_| super::resource())?;
        self.charge(location.record_bytes)?;
        let mut record = Vec::new();
        record
            .try_reserve_exact(length)
            .map_err(|_| super::resource())?;
        let _charge = TemporaryBytes::new(record.capacity());
        for index in 0..location.blocks {
            let ordinal = location
                .first_block
                .checked_add(u64::from(index))
                .ok_or_else(|| corrupt("record block overflow"))?;
            let block = self.read_block(location, ordinal)?;
            let start = if index == 0 {
                location.first_position as usize
            } else {
                0
            };
            if start >= block.bytes.len() {
                return Err(corrupt("record starts outside decoded block"));
            }
            let take = (length - record.len()).min(block.bytes.len() - start);
            if record.len() + take < length && block.bytes.len() != BLOCK_BYTES {
                return Err(corrupt("short block before record end"));
            }
            record.extend_from_slice(&block.bytes[start..start + take]);
        }
        if record.len() != length || digest(&record) != location.digest {
            return Err(corrupt("record extent length or digest mismatch"));
        }
        let body_bytes = location
            .record_bytes
            .checked_sub(96 + key.len() as u64)
            .ok_or_else(|| corrupt("record shorter than complete key"))?;
        let expected = record_header(collection, key, body_bytes, location.version)?;
        if record[..96] != expected || record[96..96 + key.len()] != *key.as_bytes() {
            return Err(corrupt(
                "record full key, collection, or mutation version mismatch",
            ));
        }
        // Return the existing stored body, including any owner encryption bytes.
        record.drain(..96 + key.len());
        Ok(record)
    }
}
