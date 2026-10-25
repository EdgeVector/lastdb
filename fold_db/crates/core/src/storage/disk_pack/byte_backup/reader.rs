use super::{
    invalid,
    model::{ObjectRecord, Segment},
    store, ByteBackupCapture, Inner, StorageResult,
};
use laststore::disk_pack::backup::export::SnapshotReader;
use sha2::{Digest as _, Sha256};
use std::fs::File;
use std::io::{self, Cursor, Read};
use std::path::Path;
use std::sync::{atomic::Ordering, Arc};

impl ByteBackupCapture {
    pub fn reader(&self, index: u64) -> StorageResult<ByteBackupReader> {
        let state = self.lock()?;
        if !state.ready
            || state.terminal.is_some()
            || state.local_cancelled
            || state.released
            || self.inner.source.is_none()
        {
            return Err(invalid("byte source reader phase"));
        }
        self.require_export(&state)?;
        if self.inner.readers.load(Ordering::Acquire)
            >= lastdb_storage_policy::BACKUP_RESOURCES.concurrent_streams
        {
            return Err(invalid("byte source stream admission"));
        }
        let object = self.inner.object(&state, index)?;
        let payload = object
            .ciphertext
            .as_ref()
            .map(|name| store::input(&self.inner.store.root.join(name)))
            .transpose()?;
        if payload.as_ref().is_some_and(|file| {
            file.metadata()
                .map_or(true, |meta| meta.len() != object.input.object.bytes.get())
        }) {
            return Err(invalid("byte source encrypted metadata length"));
        }
        self.inner.readers.fetch_add(1, Ordering::AcqRel);
        Ok(ByteBackupReader {
            inner: Arc::clone(&self.inner),
            object,
            payload,
            reader: None,
            record_coordinate: None,
            position: 0,
            remaining: 0,
            bytes: 0,
            hash: Sha256::new(),
            failed: false,
        })
    }
}
/// A reader keeps the generation and session admission until Drop. At most one
/// original native handle is open per reader, including a concatenated pack.
pub struct ByteBackupReader {
    inner: Arc<Inner>,
    object: ObjectRecord,
    payload: Option<File>,
    reader: Option<SnapshotReader>,
    record_coordinate: Option<(String, String)>,
    position: usize,
    remaining: u64,
    bytes: u64,
    hash: Sha256,
    failed: bool,
}
impl ByteBackupReader {
    pub fn input(&self) -> &cloud_backup_protocol::v2::transfer::TransferInput {
        &self.object.input
    }
    fn open_segment(&mut self, part: &Segment) -> io::Result<()> {
        let source = self
            .inner
            .source
            .as_ref()
            .ok_or_else(|| io::Error::other("byte source unavailable"))?;
        let mut reader = self
            .inner
            .home
            .reader(source, Path::new(part.path.as_str()))
            .map_err(|_| io::Error::other("byte source generation read"))?;
        reader.seek_to(part.offset)?;
        self.reader = Some(reader);
        self.remaining = part.bytes;
        Ok(())
    }
    fn next(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.failed {
            return Err(io::Error::other("byte source reader failed"));
        }
        if buffer.is_empty() {
            return Ok(0);
        }
        let maximum = buffer
            .len()
            .min(lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize);
        let count = if let Some(payload) = &mut self.payload {
            payload.read(&mut buffer[..maximum])?
        } else if !self.object.record_segments.is_empty() {
            self.record_bytes(&mut buffer[..maximum])?
        } else {
            loop {
                if self.position == self.object.segments.len() {
                    break 0;
                }
                if self.reader.is_none() {
                    self.open_segment(&self.object.segments[self.position].clone())?;
                }
                if self.remaining == 0 {
                    self.reader = None;
                    self.position += 1;
                    continue;
                }
                let maximum = maximum.min(self.remaining as usize);
                let reader = self
                    .reader
                    .as_mut()
                    .ok_or_else(|| io::Error::other("byte native reader missing"))?;
                let count = reader.read(&mut buffer[..maximum])?;
                if count == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "byte native source truncated",
                    ));
                }
                self.remaining -= count as u64;
                break count;
            }
        };
        self.bytes = self
            .bytes
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::other("byte source stream overflow"))?;
        self.hash.update(&buffer[..count]);
        let expected = self.object.input.object.bytes.get();
        if self.bytes > expected || (count == 0 && self.bytes != expected) {
            return Err(io::Error::other("byte source exact stream length"));
        }
        if self.bytes == expected
            && crate::hex::hex_lower(self.hash.clone().finalize())
                != self.object.input.object.sha256.as_str()
        {
            return Err(io::Error::other("byte source stream checksum"));
        }
        Ok(count)
    }
    fn record_bytes(&mut self, output: &mut [u8]) -> io::Result<usize> {
        loop {
            let Some(part) = self.object.record_segments.get(self.position) else {
                return Ok(0);
            };
            if self.record_coordinate.is_none() {
                let coordinate = part
                    .coordinate
                    .load(&self.inner)
                    .map_err(|_| io::Error::other("stored source coordinate read"))?;
                let destination = if coordinate.0 == "cas_blobs" {
                    cloud_backup_protocol::v2::Destination::B2
                } else {
                    cloud_backup_protocol::v2::Destination::R2
                };
                if self.object.input.object.destination != Some(destination) {
                    return Err(io::Error::other("stored source destination differs"));
                }
                self.record_coordinate = Some(coordinate);
                self.remaining = part.bytes;
            }
            if self.remaining == 0 {
                self.record_coordinate = None;
                self.position += 1;
                continue;
            }
            let count = self.remaining.min(output.len() as u64) as usize;
            let offset = part.offset + part.bytes - self.remaining;
            let source = self
                .inner
                .source
                .as_ref()
                .ok_or_else(|| io::Error::other("stored source missing"))?;
            let (collection, key) = self
                .record_coordinate
                .as_ref()
                .ok_or_else(|| io::Error::other("stored source coordinate missing"))?;
            let mut call = self
                .inner
                .home
                .read(source, record_lookup_budget(collection, key)?)
                .map_err(|_| io::Error::other("stored source held read"))?;
            let mut target = Cursor::new(&mut output[..count]);
            let copied = call
                .copy_body_range_to(collection, key, offset..offset + count as u64, &mut target)
                .map_err(|_| io::Error::other("stored source range verification"))?
                .ok_or_else(|| io::Error::other("stored source body missing"))?;
            if copied.body_bytes != count as u64 || target.position() != count as u64 {
                return Err(io::Error::other("stored source range length"));
            }
            self.remaining -= count as u64;
            return Ok(count);
        }
    }
}

fn record_lookup_budget(
    collection: &str,
    key: &str,
) -> io::Result<laststore::disk_pack::ReadBudget> {
    let coordinate = collection
        .len()
        .checked_add(key.len())
        .ok_or_else(|| io::Error::other("stored coordinate size overflow"))?
        as u64;
    let bytes = lastdb_storage_policy::BACKUP_RESOURCES.decoded_values_bytes;
    let page = lastdb_storage_policy::native_v1::PAGE_BYTES as u64;
    // Two pages per page-sized key span conservatively include overflow framing.
    // The ordinary path allowance remains available for directory/locator pages.
    let overflow = coordinate
        .div_ceil(page)
        .checked_mul(2)
        .and_then(|pages| {
            pages.checked_add(lastdb_storage_policy::backup_decode::NATIVE_STREAM_PAGES)
        })
        .ok_or_else(|| io::Error::other("stored coordinate page admission overflow"))?;
    Ok(laststore::disk_pack::ReadBudget {
        pages: overflow.min(bytes / page),
        blocks: lastdb_storage_policy::backup_decode::NATIVE_STREAM_BLOCKS,
        bytes,
        keys: 1,
    })
}

impl Read for ByteBackupReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let result = self.next(buffer);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}
impl Drop for ByteBackupReader {
    fn drop(&mut self) {
        self.reader = None;
        self.payload = None;
        self.record_coordinate = None;
        self.inner.readers.fetch_sub(1, Ordering::AcqRel);
    }
}
