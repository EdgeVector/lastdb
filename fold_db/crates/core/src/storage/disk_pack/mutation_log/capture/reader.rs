use super::*;
use std::io::{self, Read, Seek, SeekFrom};

pub struct MutationLogReader {
    inner: Arc<Inner>,
    file: File,
    remaining: u64,
}
impl MutationLogCapture {
    pub fn reader(
        &self,
        index: usize,
        offset: u64,
        bytes: u64,
    ) -> StorageResult<MutationLogReader> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| invalid("mutation capture state owner"))?;
        if !state.ready
            || state.local_cancelled
            || state.terminal.is_some()
            || state.released
            || self.inner.readers.load(Ordering::Acquire)
                >= lastdb_storage_policy::BACKUP_RESOURCES.concurrent_streams
        {
            return Err(invalid("mutation capture reader admission"));
        }
        let selected = state
            .files
            .get(index)
            .ok_or_else(|| invalid("mutation capture file number"))?;
        if bytes == 0
            || offset
                .checked_add(bytes)
                .is_none_or(|end| end > selected.bytes)
        {
            return Err(invalid("mutation capture reader extent"));
        }
        let mut file = input(
            &self
                .inner
                .directory
                .join(format!("{}.ldrun", selected.run_id.as_str())),
            selected.bytes,
        )?;
        file.seek(SeekFrom::Start(offset))?;
        self.inner.readers.fetch_add(1, Ordering::AcqRel);
        Ok(MutationLogReader {
            inner: Arc::clone(&self.inner),
            file,
            remaining: bytes,
        })
    }
}
impl Read for MutationLogReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 || buffer.is_empty() {
            return Ok(0);
        }
        let maximum = self
            .remaining
            .min(buffer.len() as u64)
            .min(lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes)
            as usize;
        let count = self.file.read(&mut buffer[..maximum])?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "frozen mutation log file ends early",
            ));
        }
        self.remaining -= count as u64;
        Ok(count)
    }
}
impl Drop for MutationLogReader {
    fn drop(&mut self) {
        self.inner.readers.fetch_sub(1, Ordering::AcqRel);
    }
}
