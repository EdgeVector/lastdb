use super::bytes::*;
use super::{corrupt, Location, ReadCall, Source, BLOCK_BYTES};
use crate::read_receipt_io::{self as measured, ReadClass};
use crate::Result;
use std::path::Path;
use uuid::Uuid;

pub(super) fn decode(
    bytes: &[u8],
    object: Uuid,
    sequence: u64,
    ordinal: u64,
    expected: usize,
) -> Result<Vec<u8>> {
    measured::decode(
        || decode_inner(bytes, object, sequence, ordinal, expected),
        |output| (output.len(), output.capacity()),
    )
}

fn decode_inner(
    bytes: &[u8],
    object: Uuid,
    sequence: u64,
    ordinal: u64,
    expected: usize,
) -> Result<Vec<u8>> {
    if bytes.len() < 80
        || &bytes[..8] != b"LDBLK1\0\0"
        || u16_at(bytes, 8) != 1
        || u32_at(bytes, 12) as usize != bytes.len()
        || u64_at(bytes, 16) != ordinal
        || u32_at(bytes, 24) as usize != expected
        || !(1..=BLOCK_BYTES).contains(&expected)
        || uuid_at(bytes, 32) != object
        || u64_at(bytes, 48) != sequence
    {
        return Err(corrupt("block frame identity or length"));
    }
    zero(&bytes[68..80])?;
    if bytes[11] != 0 || u64_at(bytes, 56) != 0 {
        return Err(corrupt(
            "encrypted disk-pack frames require the unavailable key-epoch adapter",
        ));
    }
    let stored = u32_at(bytes, 28) as usize;
    if stored == 0 || stored > BLOCK_BYTES || stored + 80 != bytes.len() {
        return Err(corrupt("stored block length"));
    }
    let mut crc = crc32fast::Hasher::new();
    crc.update(&bytes[..64]);
    crc.update(&[0; 4]);
    crc.update(&bytes[68..]);
    if crc.finalize() != u32_at(bytes, 64) {
        return Err(corrupt("block CRC mismatch"));
    }
    let payload = &bytes[80..];
    let output = match bytes[10] {
        0 if stored == expected => payload.to_vec(),
        1 if stored < expected => {
            let size = zstd::zstd_safe::find_frame_compressed_size(payload)
                .map_err(|_| corrupt("invalid independent zstd frame"))?;
            if size != payload.len() {
                return Err(corrupt("trailing compressed frame bytes"));
            }
            let mut decoder = zstd::bulk::Decompressor::new()?;
            decoder.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(12))?;
            decoder
                .decompress(payload, expected)
                .map_err(|_| corrupt("block decompression failed"))?
        }
        _ => return Err(corrupt("unsupported block compression or length")),
    };
    if output.len() != expected {
        return Err(corrupt("decoded block length mismatch"));
    }
    Ok(output)
}

pub(super) fn encode(payload: &[u8], object: Uuid, sequence: u64, ordinal: u64) -> Result<Vec<u8>> {
    if payload.is_empty() || payload.len() > BLOCK_BYTES {
        return Err(corrupt("block encode length"));
    }
    let compressed = zstd::bulk::compress(payload, 3)?;
    let (compression, stored) = if compressed.len() < payload.len() {
        (1, compressed.as_slice())
    } else {
        (0, payload)
    };
    let mut bytes = vec![0; 80 + stored.len()];
    bytes[..8].copy_from_slice(b"LDBLK1\0\0");
    put16(&mut bytes, 8, 1);
    bytes[10] = compression;
    let length = bytes.len() as u32;
    put32(&mut bytes, 12, length);
    put64(&mut bytes, 16, ordinal);
    put32(&mut bytes, 24, payload.len() as u32);
    put32(&mut bytes, 28, stored.len() as u32);
    bytes[32..48].copy_from_slice(object.as_bytes());
    put64(&mut bytes, 48, sequence);
    bytes[80..].copy_from_slice(stored);
    let crc = crc32fast::hash(&bytes);
    put32(&mut bytes, 64, crc);
    Ok(bytes)
}

impl ReadCall {
    pub(super) fn verify_part(
        &mut self,
        path: &Path,
        location: &Location,
        part: u32,
        ordinal: u64,
    ) -> Result<()> {
        if let Some((start, end)) = self.parts.get(path) {
            if ordinal < *start || ordinal >= *end {
                return Err(corrupt("block outside named part"));
            }
            return Ok(());
        }
        let bytes = self.read_extent(path, 0, 128, false, ReadClass::FormatProbe)?;
        let bytes = &bytes.bytes;
        let kind = if location.source == Source::Pack {
            1
        } else {
            2
        };
        let sequence = if location.source == Source::Pack {
            0
        } else {
            location.version.batch
        };
        if &bytes[..8] != b"LDPART1\0"
            || u16_at(bytes, 8) != 1
            || bytes[10] != kind
            || bytes[11] != 0
            || u32_at(bytes, 12) != 128
            || uuid_at(bytes, 16) != location.object
            || u64_at(bytes, 32) != sequence
            || u32_at(bytes, 40) != part
            || !(128..=super::PART_BYTES).contains(&u64::from(u32_at(bytes, 44)))
            || u64_at(bytes, 64) != 0
            || u64_at(bytes, 72) > self.current.generation
            || u64_at(bytes, 72) == 0
        {
            return Err(corrupt("part identity or header"));
        }
        zero(&bytes[112..])?;
        verify_checksum(bytes, 80)?;
        let start = u64_at(bytes, 48);
        let end = start
            .checked_add(u64_at(bytes, 56))
            .ok_or_else(|| corrupt("part block overflow"))?;
        if let Some(object) = self.object_metadata(location)? {
            if u64::from(part) >= object.data_parts
                || end > object.blocks
                || u64_at(bytes, 72) != object.generation
            {
                return Err(corrupt("part outside complete object bounds"));
            }
        }
        if ordinal < start || ordinal >= end {
            return Err(corrupt("block outside named part"));
        }
        let metadata = measured::status(|| std::fs::metadata(path))?;
        if metadata.len() != u64::from(u32_at(bytes, 44)) {
            return Err(corrupt("part physical length mismatch"));
        }
        self.parts.insert(path.to_path_buf(), (start, end));
        Ok(())
    }
}
