//! Authenticate one complete mutation run without allocating its value bodies.
use super::*;
use crate::disk_run::RunSummary;
use std::path::Path;

impl RunReader {
    pub(crate) fn verified_summary(&self, path: &Path) -> Result<RunSummary> {
        let admission = lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes;
        let mut summary = RunSummary {
            path: path.to_owned(),
            run_id: self.run_id(),
            records: 0,
            tombstones: 0,
            record_bytes: 0,
            tombstone_bytes: 0,
            file_bytes: std::fs::metadata(path)?.len(),
            first_key: None,
            last_key: None,
        };
        let mut blocks = 0u64;
        for number in 0..self.index_count() {
            let index = self.stream_index(number, admission)?;
            let retained = self.metadata_bytes()
                + (index.blocks.capacity() * std::mem::size_of::<BlockRef>()) as u64
                + index
                    .blocks
                    .iter()
                    .map(|r| r.first_key.capacity() as u64)
                    .sum::<u64>()
                + index.filter_bytes.capacity() as u64
                + summary
                    .last_key
                    .as_ref()
                    .map_or(0, |key| key.capacity() as u64);
            for reference in &index.blocks {
                let mut block = Block::new(self, reference, admission / 4, retained)?;
                while let Some(value) = block.checked_next(reference)? {
                    if summary
                        .last_key
                        .as_ref()
                        .is_some_and(|old| old >= &block.key)
                    {
                        return Err(corrupt("imported mutation run key order"));
                    }
                    summary.records = summary
                        .records
                        .checked_add(1)
                        .ok_or(Error::ReadBudgetExceeded)?;
                    summary.record_bytes = summary
                        .record_bytes
                        .checked_add(block.key.len() as u64)
                        .and_then(|n| n.checked_add(block.remaining))
                        .ok_or(Error::ReadBudgetExceeded)?;
                    if !value {
                        summary.tombstones += 1;
                        summary.tombstone_bytes += block.key.len() as u64;
                    }
                    if summary.first_key.is_none() {
                        summary.first_key = Some(block.key.clone());
                    }
                    summary.last_key = Some(block.key.clone());
                }
                blocks += 1;
            }
        }
        if summary.records != self.records()
            || blocks != u64::from(self.footer.data_blocks)
            || summary.first_key.as_deref() != self.first_key()
            || summary.last_key.as_deref() != self.last_key()
        {
            return Err(corrupt("imported mutation run complete summary"));
        }
        Ok(summary)
    }
}
