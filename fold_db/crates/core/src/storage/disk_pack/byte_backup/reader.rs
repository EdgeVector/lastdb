use super::{
    invalid,
    model::{ObjectRecord, Segment},
    store, ByteBackupCapture, Inner, StorageResult,
};
use sha2::{Digest as _, Sha256};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{atomic::Ordering, Arc};

impl ByteBackupCapture {
    pub fn reader(&self, index: u64) -> StorageResult<ByteBackupReader> {
        let state = self.lock()?;
        if !state.ready
            || state.terminal.is_some()
            || state.local_cancelled
            || state.released
            || !self.inner.available()
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
            position: 0,
            remaining: 0,
            bytes: 0,
            hash: Sha256::new(),
            failed: false,
        })
    }
}
/// A reader keeps the held run files and session admission until Drop. At most
/// one original native handle is open per reader, including a concatenated pack.
pub struct ByteBackupReader {
    inner: Arc<Inner>,
    object: ObjectRecord,
    payload: Option<File>,
    reader: Option<File>,
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
        if self
            .inner
            .destination(Path::new(part.path.as_str()))
            .map_err(|_| io::Error::other("byte source destination"))?
            != self.object.input.object.destination
        {
            return Err(io::Error::other("byte source destination differs"));
        }
        let mut reader = self
            .inner
            .file_reader(Path::new(part.path.as_str()))
            .map_err(|_| io::Error::other("byte source generation read"))?;
        reader.seek(SeekFrom::Start(part.offset))?;
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
            && lastdb_model::hex::hex_lower(self.hash.clone().finalize())
                != self.object.input.object.sha256.as_str()
        {
            return Err(io::Error::other("byte source stream checksum"));
        }
        Ok(count)
    }
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
        self.inner.readers.fetch_sub(1, Ordering::AcqRel);
    }
}
