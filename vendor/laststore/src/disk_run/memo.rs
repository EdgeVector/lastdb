//! Bounded per-read index-block memoization for repeated point lookups.

use super::reader::RunReader;
use super::IndexBlock;
use crate::frame::DecodeScratch;
use crate::Result;
use std::mem::size_of;
use std::sync::Arc;
use uuid::Uuid;

const MAX_MEMO_ENTRIES: usize = 64;
const MAX_MEMO_BYTES: u64 = 256 * 1024;

struct MemoEntry {
    run_id: Uuid,
    index_num: usize,
    block: Arc<IndexBlock>,
    bytes: u64,
}

pub(crate) struct MetadataMemo {
    entries: Vec<MemoEntry>,
    pub(crate) retained_bytes: u64,
    max_bytes: u64,
}

impl MetadataMemo {
    pub(crate) fn new(budget: u64) -> Self {
        Self {
            entries: Vec::new(),
            retained_bytes: 0,
            max_bytes: MAX_MEMO_BYTES.min(budget / 16),
        }
    }

    pub(crate) fn read(
        &mut self,
        reader: &RunReader,
        index_num: usize,
        scratch: &mut DecodeScratch,
    ) -> Result<Arc<IndexBlock>> {
        let run_id = reader.footer.run_id;
        if let Some(entry) = self
            .entries
            .iter()
            .find(|e| e.run_id == run_id && e.index_num == index_num)
        {
            return Ok(Arc::clone(&entry.block));
        }
        let block = Arc::new(reader.read_index(index_num, Some(scratch), None)?);
        let item_sz = size_of::<MemoEntry>();
        let bytes = (size_of::<IndexBlock>() + 2 * size_of::<usize>()) as u64 + block.footprint();
        let cap_ok =
            self.entries.capacity() > 0 || self.entries.try_reserve_exact(MAX_MEMO_ENTRIES).is_ok();
        if bytes.saturating_add((MAX_MEMO_ENTRIES * item_sz) as u64) <= self.max_bytes && cap_ok {
            if self.retained_bytes == 0 {
                self.retained_bytes = (self.entries.capacity() * item_sz) as u64;
            }
            while self.entries.len() >= MAX_MEMO_ENTRIES
                || self.retained_bytes.saturating_add(bytes) > self.max_bytes
            {
                self.retained_bytes -= self.entries.remove(0).bytes;
            }
            self.entries.push(MemoEntry {
                run_id,
                index_num,
                block: Arc::clone(&block),
                bytes,
            });
            self.retained_bytes += bytes;
        }
        Ok(block)
    }
}
