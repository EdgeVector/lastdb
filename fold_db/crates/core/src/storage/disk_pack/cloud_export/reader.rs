use super::catalog::ExportObject;
use super::store::hex;
use super::{invalid, ExportInner, NativeCloudExport, StorageResult};
use laststore::disk_pack::backup::export::SnapshotReader;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

impl NativeCloudExport {
    /// Open the saved object, never a caller-supplied path or changed input.
    /// At most four readers use this process session. Terminal state stops new
    /// readers; existing readers must stop before source release.
    pub fn open_object(
        &self,
        page_index: u32,
        object_index: usize,
    ) -> StorageResult<NativeCloudObjectReader> {
        let record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("native export state lock failed"))?;
        record.require_ready()?;
        let descriptor = record
            .pages
            .get(page_index as usize)
            .ok_or_else(|| invalid("native export page does not exist"))?;
        let page = self.inner.store.read_page(&record.begin.job, descriptor)?;
        let object = page
            .objects
            .into_iter()
            .nth(object_index)
            .ok_or_else(|| invalid("native export object does not exist"))?;
        if self.inner.readers.load(Ordering::Acquire)
            >= cloud_backup_protocol::limits::CONCURRENT_STREAMS_MAX
        {
            return Err(invalid("native export concurrent stream limit reached"));
        }
        if self.inner.source.is_none() {
            return Err(invalid("native export source is unavailable"));
        }
        self.inner.readers.fetch_add(1, Ordering::AcqRel);
        Ok(NativeCloudObjectReader {
            inner: Arc::clone(&self.inner),
            object,
            position: 0,
            reader: None,
            file_hash: Sha256::new(),
            object_hash: Sha256::new(),
            file_bytes: 0,
            total_bytes: 0,
            failed: false,
        })
    }
}

/// A bounded stream of the exact saved bytes. Each original member and the
/// complete object are hashed again. A retry reopens this same frozen input.
pub struct NativeCloudObjectReader {
    inner: Arc<ExportInner>,
    object: ExportObject,
    position: usize,
    reader: Option<SnapshotReader>,
    file_hash: Sha256,
    object_hash: Sha256,
    file_bytes: u64,
    total_bytes: u64,
    failed: bool,
}

impl NativeCloudObjectReader {
    pub fn input(&self) -> &cloud_backup_protocol::transfer::TransferInput {
        &self.object.input
    }

    pub fn input_sha256(&self) -> &cloud_backup_protocol::Digest {
        &self.object.input_sha256
    }

    fn read_next(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.failed {
            return Err(std::io::Error::other("native export stream failed"));
        }
        if buffer.is_empty() {
            return Ok(0);
        }
        while self.position < self.object.files.len() {
            let file = &self.object.files[self.position];
            if self.reader.is_none() {
                let source = self
                    .inner
                    .source
                    .as_ref()
                    .ok_or_else(|| std::io::Error::other("native export source unavailable"))?;
                self.reader = Some(
                    source
                        .reader(
                            Path::new(file.path.as_str()),
                            &self.inner.runtime.read_lease,
                        )
                        .map_err(|error| std::io::Error::other(error.to_string()))?,
                );
            }
            let maximum = buffer
                .len()
                .min(cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize);
            let read = self
                .reader
                .as_mut()
                .expect("opened retained source reader")
                .read(&mut buffer[..maximum])?;
            if read == 0 {
                if self.file_bytes != file.bytes.get()
                    || hex(self.file_hash.clone().finalize().into()) != file.sha256
                {
                    return Err(std::io::Error::other("native export member bytes changed"));
                }
                self.position += 1;
                self.reader = None;
                self.file_bytes = 0;
                self.file_hash = Sha256::new();
                continue;
            }
            self.file_bytes += read as u64;
            self.total_bytes += read as u64;
            self.file_hash.update(&buffer[..read]);
            self.object_hash.update(&buffer[..read]);
            if self.file_bytes == file.bytes.get()
                && hex(self.file_hash.clone().finalize().into()) != file.sha256
            {
                return Err(std::io::Error::other(
                    "native export member checksum changed",
                ));
            }
            if self.total_bytes == self.object.input.object.bytes.get()
                && hex(self.object_hash.clone().finalize().into())
                    != self.object.input.object.sha256
            {
                return Err(std::io::Error::other(
                    "native export object checksum changed",
                ));
            }
            return Ok(read);
        }
        if self.total_bytes != self.object.input.object.bytes.get() {
            return Err(std::io::Error::other("native export object truncated"));
        }
        Ok(0)
    }
}

impl Read for NativeCloudObjectReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let result = self.read_next(buffer);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}

impl Drop for NativeCloudObjectReader {
    fn drop(&mut self) {
        // The active source hold drops before the session's reader count.
        self.reader = None;
        self.inner.readers.fetch_sub(1, Ordering::AcqRel);
    }
}
