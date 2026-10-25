use super::super::{invalid, StorageResult};
use super::{ClientBackupStream, Coordinate, Export, RecordSegment};
use sha2::Digest;
use std::io::Read;

impl Export<'_> {
    pub(super) fn body(
        &mut self,
        kind: ClientBackupStream,
        coordinate: &Coordinate,
        input: &mut dyn Read,
        bytes: u64,
    ) -> StorageResult<()> {
        let mut offset = 0;
        let mut buffer = [0; 65536];
        let quote = serde_json::to_vec(&coordinate)
            .map_err(|_| invalid("source coordinate encoding"))?
            .len()
            + 128;
        while offset < bytes {
            let slot = Self::slot(kind);
            let stream = &self.streams[slot];
            if stream.segments.len() >= lastdb_storage_policy::backup_transport::MAX_PACK_MEMBERS
                || stream.coordinate_bytes + quote > super::super::store::RECORD_MAX as usize / 2
            {
                self.flush(kind)?;
            }
            let left = lastdb_storage_policy::backup_transport::OBJECT_CHUNK_BYTES
                - self.streams[slot].object_bytes;
            let count = left.min(bytes - offset);
            self.streams[slot].segments.push(RecordSegment {
                coordinate: coordinate.clone(),
                offset,
                bytes: count,
            });
            self.streams[slot].coordinate_bytes += quote;
            let mut remaining = count;
            while remaining > 0 {
                let take = remaining.min(buffer.len() as u64) as usize;
                input.read_exact(&mut buffer[..take])?;
                let stream = &mut self.streams[slot];
                stream.hash.update(&buffer[..take]);
                stream.object_hash.update(&buffer[..take]);
                stream.bytes = stream
                    .bytes
                    .checked_add(take as u64)
                    .ok_or_else(|| invalid("record stream byte overflow"))?;
                stream.object_bytes += take as u64;
                self.record_hash.update(&buffer[..take]);
                remaining -= take as u64;
            }
            offset += count;
            if self.streams[slot].object_bytes
                == lastdb_storage_policy::backup_transport::OBJECT_CHUNK_BYTES
            {
                self.flush(kind)?;
            }
        }
        if input.read(&mut buffer[..1])? != 0 {
            return Err(invalid("record body exceeds captured length"));
        }
        Ok(())
    }
    pub(super) fn encrypted(
        &mut self,
        kind: ClientBackupStream,
        mut bytes: &[u8],
    ) -> StorageResult<()> {
        let slot = Self::slot(kind);
        let maximum = lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize;
        while !bytes.is_empty() {
            let count = bytes.len().min(maximum - self.streams[slot].plain.len());
            let stream = &mut self.streams[slot];
            stream.plain.extend_from_slice(&bytes[..count]);
            stream.hash.update(&bytes[..count]);
            stream.bytes = stream
                .bytes
                .checked_add(count as u64)
                .ok_or_else(|| invalid("metadata stream byte overflow"))?;
            bytes = &bytes[count..];
            if self.streams[slot].plain.len() == maximum {
                self.flush(kind)?;
            }
        }
        Ok(())
    }
}
