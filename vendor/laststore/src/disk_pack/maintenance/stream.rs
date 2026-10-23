use crate::disk_pack::record::record_header;
use crate::disk_pack::{artifact, corrupt, Location, ReadCall, BLOCK_BYTES};
use crate::Result;
use sha2::{Digest, Sha256};
use std::io::{self, Read};

/// Verify an unchanged record while the pack writer copies at most 64 KiB at once.
pub(super) struct RecordBody<'a> {
    call: &'a mut ReadCall,
    location: Location,
    position: u64,
    hash: Sha256,
    block: Option<u64>,
}

impl<'a> RecordBody<'a> {
    pub(super) fn open(
        call: &'a mut ReadCall,
        collection: &str,
        key: &str,
        location: Location,
    ) -> Result<Self> {
        let body_bytes = location
            .record_bytes
            .checked_sub(96 + key.len() as u64)
            .ok_or_else(|| corrupt("compaction record is shorter than its key"))?;
        let end = location
            .first_block
            .checked_mul(BLOCK_BYTES as u64)
            .and_then(|n| n.checked_add(u64::from(location.first_position)))
            .and_then(|n| n.checked_add(location.record_bytes))
            .ok_or_else(|| corrupt("compaction record offset overflow"))?;
        if call
            .object_metadata(&location)?
            .map_or(true, |object| end > object.logical_bytes)
        {
            return Err(corrupt("compaction record outside its object"));
        }
        let mut stream = Self {
            call,
            location,
            position: 0,
            hash: Sha256::new(),
            block: None,
        };
        let mut prefix = vec![0; 96 + key.len()];
        stream.read_exact(&mut prefix)?;
        if prefix[..96] != record_header(collection, key, body_bytes, stream.location.version)?
            || prefix[96..] != *key.as_bytes()
        {
            return Err(corrupt("compaction record identity or mutation version"));
        }
        Ok(stream)
    }

    pub(super) fn body_bytes(&self) -> u64 {
        self.location.record_bytes - self.position
    }
}

impl Read for RecordBody<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() || self.position == self.location.record_bytes {
            return Ok(0);
        }
        let absolute = u64::from(self.location.first_position) + self.position;
        let ordinal = self.location.first_block + absolute / BLOCK_BYTES as u64;
        // A record can exceed one read call's block/byte budget. The stream
        // resets the finite I/O budget at each block boundary, so the block
        // cache never grows with the record. The initial call can reuse the
        // caller's bounded caches and metadata checks.
        if self.block.is_some_and(|previous| previous != ordinal) {
            *self.call = artifact::metadata_call(&self.call.root, self.call.current.clone());
        }
        self.block = Some(ordinal);
        let block = self
            .call
            .read_block(&self.location, ordinal)
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "compaction record body at {}/{} bytes: {error}",
                        self.position, self.location.record_bytes
                    ),
                )
            })?;
        let start = (absolute % BLOCK_BYTES as u64) as usize;
        if start >= block.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "short compaction source block",
            ));
        }
        let count = out
            .len()
            .min(block.bytes.len() - start)
            .min((self.location.record_bytes - self.position) as usize);
        if self.position + ((block.bytes.len() - start) as u64) < self.location.record_bytes
            && block.bytes.len() != BLOCK_BYTES
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "short block before compaction record end",
            ));
        }
        out[..count].copy_from_slice(&block.bytes[start..start + count]);
        self.hash.update(&out[..count]);
        self.position += count as u64;
        let actual: [u8; 32] = self.hash.clone().finalize().into();
        if self.position == self.location.record_bytes && actual != self.location.digest {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compaction source record digest differs",
            ));
        }
        Ok(count)
    }
}
