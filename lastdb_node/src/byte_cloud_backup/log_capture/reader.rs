use super::*;
use fold_db::storage::disk_pack::mutation_log::MutationLogReader;
use std::collections::VecDeque;
use std::io::{self, Cursor};

pub(super) fn open(capture: &LogCapture, index: u64) -> Result<Box<dyn Read + Send>> {
    let record = capture.record(index)?;
    if let Some(name) = record.payload {
        let bytes = capture.store.raw(&name)?.ok_or(BackupError::Missing)?;
        if bytes.len() as u64 != record.input.object.bytes.get()
            || bytes_sha256(&bytes) != record.input.object.sha256
        {
            return Err(BackupError::Identity);
        }
        return Ok(Box::new(Cursor::new(bytes)));
    }
    Ok(Box::new(Reader {
        core: Arc::clone(&capture.core),
        remaining: record.segments.into(),
        active: None,
    }))
}
struct Reader {
    core: Arc<MutationLogCapture>,
    remaining: VecDeque<model::Segment>,
    active: Option<MutationLogReader>,
}
impl Read for Reader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            if let Some(reader) = &mut self.active {
                let count = reader.read(buffer)?;
                if count > 0 {
                    return Ok(count);
                }
                self.active = None;
            }
            let Some(segment) = self.remaining.pop_front() else {
                return Ok(0);
            };
            self.active = Some(
                self.core
                    .reader(segment.file, segment.offset, segment.bytes)
                    .map_err(|_| io::Error::other("frozen mutation source unavailable"))?,
            );
        }
    }
}
