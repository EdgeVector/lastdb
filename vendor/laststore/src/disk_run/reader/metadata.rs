//! Authenticated metadata reuse belongs to one held read call, never the home.

use super::*;

const MAX_ENTRIES: usize = 64;
const MAX_BYTES: u64 = 256 * 1024;

struct Entry {
    run: Uuid,
    number: usize,
    block: Arc<IndexBlock>,
    bytes: u64,
}

pub(in crate::disk_run) struct MetadataMemo {
    entries: Vec<Entry>,
    bytes: u64,
    maximum: u64,
}

impl MetadataMemo {
    pub(in crate::disk_run) fn new(budget: u64) -> Self {
        Self {
            entries: Vec::new(),
            bytes: 0,
            // Optional metadata cannot consume the call's whole byte allowance.
            maximum: MAX_BYTES.min(budget / 16),
        }
    }

    pub(in crate::disk_run) fn retained_bytes(&self) -> u64 {
        self.bytes
    }

    pub(super) fn read(
        &mut self,
        reader: &RunReader,
        number: usize,
        scratch: &mut DecodeScratch,
    ) -> Result<Arc<IndexBlock>> {
        if let Some(entry) = self
            .entries
            .iter()
            .find(|entry| entry.run == reader.run_id() && entry.number == number)
        {
            crate::read_receipt::update(|r| {
                r.metadata_decode_reuses = r.metadata_decode_reuses.saturating_add(1);
            });
            return Ok(Arc::clone(&entry.block));
        }
        let block = Arc::new(reader.read_index_with(number, Some(scratch))?);
        self.retain(reader.run_id(), number, &block);
        Ok(block)
    }

    fn retain(&mut self, run: Uuid, number: usize, block: &Arc<IndexBlock>) {
        let bytes = block.memory_bytes();
        let slots = MAX_ENTRIES * std::mem::size_of::<Entry>();
        if bytes.saturating_add(slots as u64) > self.maximum {
            return;
        }
        if self.entries.capacity() == 0 {
            if self.entries.try_reserve_exact(MAX_ENTRIES).is_err() {
                return;
            }
            self.bytes = (self.entries.capacity() * std::mem::size_of::<Entry>()) as u64;
        }
        while !self.entries.is_empty()
            && (self.entries.len() == MAX_ENTRIES
                || self.bytes.saturating_add(bytes) > self.maximum)
        {
            self.bytes -= self.entries.remove(0).bytes;
        }
        if self.bytes.saturating_add(bytes) > self.maximum {
            return;
        }
        self.entries.push(Entry {
            run,
            number,
            block: Arc::clone(block),
            bytes,
        });
        self.bytes += bytes;
    }
}

impl IndexBlock {
    fn memory_bytes(&self) -> u64 {
        (std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<usize>()
            + self.blocks.capacity() * std::mem::size_of::<BlockRef>()
            + self.filter_bytes.capacity()
            + self
                .blocks
                .iter()
                .map(|entry| entry.first_key.capacity())
                .sum::<usize>()) as u64
    }
}
