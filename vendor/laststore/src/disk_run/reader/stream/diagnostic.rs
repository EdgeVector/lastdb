//! Checkpoint-only record lengths, without decoded bodies or a key inventory.
use super::*;
use crate::disk_run::codec::{get_len, get_slice};
use std::path::Path;

/// One transient merge head. Value bytes include the stored-value tag.
pub(crate) type SummaryRecord = (Vec<u8>, Option<u64>);

/// One bounded index and payload block per input run, independent of run length.
pub(crate) struct SummaryCursor {
    reader: RunReader,
    allowance: u64,
    index: Option<super::super::IndexBlock>,
    next_index: usize,
    next_block: usize,
    payload: Vec<u8>,
    at: usize,
    key: Vec<u8>,
    first_key: Option<Vec<u8>>,
    seen: bool,
}

impl SummaryCursor {
    pub(crate) fn open(path: &Path, key: &[u8; 32], allowance: u64) -> Result<Self> {
        let reserved = lastdb_storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .saturating_sub(allowance + 4 * 1024 * 1024);
        let reader = RunReader::open_bounded_with_reserve(path, key, allowance / 8, reserved)?;
        if u64::from(reader.footer.max_payload) > allowance / 8 {
            return Err(Error::ReadBudgetExceeded);
        }
        Ok(Self {
            reader,
            allowance,
            index: None,
            next_index: 0,
            next_block: 0,
            payload: Vec::new(),
            at: 0,
            key: Vec::new(),
            first_key: None,
            seen: false,
        })
    }

    pub(crate) fn next_record(&mut self) -> Result<Option<SummaryRecord>> {
        if self.at == self.payload.len() && !self.load_block()? {
            return Ok(None);
        }
        let shared = get_len(&self.payload, &mut self.at, self.key.len())?;
        let suffix = get_len(&self.payload, &mut self.at, MAX_RUN_KEY_BYTES - shared)?;
        let length = shared + suffix;
        if (length.max(self.key.capacity()) as u64).saturating_mul(2) > self.allowance / 4 {
            return Err(Error::ReadBudgetExceeded);
        }
        let suffix = get_slice(&self.payload, &mut self.at, suffix)?;
        let ascending = !self.seen || compare_key(&self.key, shared, suffix).is_lt();
        if !ascending || length == 0 {
            return Err(corrupt("diagnostic record order"));
        }
        self.key.truncate(shared);
        self.key
            .try_reserve_exact(length.saturating_sub(self.key.len()))
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.key.extend_from_slice(suffix);
        if self.first_key.take().is_some_and(|first| first != self.key) {
            return Err(corrupt("diagnostic block first key"));
        }
        self.seen = true;
        let tag = get_len(&self.payload, &mut self.at, MAX_RUN_VALUE_BYTES + 1)?;
        let bytes = tag.checked_sub(1);
        if let Some(bytes) = bytes {
            let value = get_slice(&self.payload, &mut self.at, bytes)?;
            if !matches!(
                value.first(),
                Some(&crate::disk_run::value::RAW | &crate::disk_run::value::COMPACT_JSON)
            ) {
                return Err(corrupt("diagnostic stored value tag"));
            }
        }
        Ok(Some((self.key.clone(), bytes.map(|n| n as u64))))
    }

    fn load_block(&mut self) -> Result<bool> {
        self.payload = Vec::new();
        loop {
            if self
                .index
                .as_ref()
                .is_none_or(|index| self.next_block == index.blocks.len())
            {
                self.index = None;
                if self.next_index == self.reader.index_count() {
                    return Ok(false);
                }
                self.index = Some(
                    self.reader
                        .stream_index(self.next_index, self.allowance / 4)?,
                );
                self.next_index += 1;
                self.next_block = 0;
            }
            let index = self.index.as_ref().expect("diagnostic index opened");
            let Some(reference) = index.blocks.get(self.next_block) else {
                continue;
            };
            if self.seen && reference.first_key <= self.key {
                return Err(corrupt("diagnostic cross-block key order"));
            }
            if reference.first_key.len() as u64 > self.allowance / 8 {
                return Err(Error::ReadBudgetExceeded);
            }
            self.next_block += 1;
            self.payload = self.reader.read_metadata_frame(
                reference.offset,
                reference.len,
                reference.number,
                self.reader.block_limit(),
                self.allowance / 8,
            )?;
            self.at = 0;
            self.key.clear();
            self.seen = false;
            self.first_key = Some(reference.first_key.clone());
            if self.payload.is_empty() {
                return Err(corrupt("diagnostic empty block"));
            }
            return Ok(true);
        }
    }
}

fn compare_key(before: &[u8], shared: usize, suffix: &[u8]) -> Ordering {
    before[shared..].cmp(suffix)
}
