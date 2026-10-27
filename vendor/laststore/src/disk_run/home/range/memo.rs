//! Optional authenticated next-key heads. Values keep the existing read path.
use super::RunRead;
use crate::disk_run::MAX_RUN_KEY_BYTES;
use crate::{Error, Result};

struct Head {
    slot: usize,
    key: Option<Vec<u8>>,
    pending: bool,
}

pub(super) enum Selection {
    Key(Vec<u8>),
    Exhausted,
    Fallback,
}

pub(super) struct HeadMemo {
    heads: Vec<Head>,
    bytes: u64,
    maximum: u64,
}

impl HeadMemo {
    pub(super) fn new(
        owner: &RunRead,
        lower: &[u8],
        upper: Option<&[u8]>,
        retained: u64,
    ) -> Option<Self> {
        // Keep room for the next returned head and the selected-key copy before
        // either transient allocation can become a retained head.
        let available = lastdb_storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(owner.shared_metadata_bytes.max(owner.retained_bytes()))?
            .checked_sub(owner.costs.budget_bytes)?
            .checked_sub(retained)?;
        let maximum = lastdb_storage_policy::backup_decode::PROOF_KEY_HEAD_BYTES
            .min(available)
            .checked_sub(2 * MAX_RUN_KEY_BYTES as u64)?;
        let count = owner
            .runs
            .iter()
            .filter(|(run, _)| run.meta.overlaps(lower, upper))
            .count();
        let bytes = count.checked_mul(std::mem::size_of::<Head>())? as u64;
        if bytes > maximum {
            return None;
        }
        let mut heads = Vec::new();
        heads.try_reserve_exact(count).ok()?;
        let bytes = (heads.capacity() * std::mem::size_of::<Head>()) as u64;
        if bytes > maximum {
            return None;
        }
        for (slot, (run, _)) in owner.runs.iter().enumerate() {
            if run.meta.overlaps(lower, upper) {
                heads.push(Head {
                    slot,
                    key: None,
                    pending: true,
                });
            }
        }
        Some(Self {
            heads,
            bytes,
            maximum,
        })
    }

    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(super) fn next(
        &mut self,
        owner: &mut RunRead,
        lower: &[u8],
        upper: Option<&[u8]>,
        exclusive: bool,
        retained: u64,
    ) -> Result<Selection> {
        for head in &mut self.heads {
            if !head.pending {
                continue;
            }
            let key = match owner.streamed_next_key_at(
                head.slot,
                lower,
                upper,
                exclusive,
                retained.saturating_add(self.bytes),
            ) {
                Ok(key) => key,
                Err(Error::ReadBudgetExceeded) => return Ok(Selection::Fallback),
                Err(error) => return Err(error),
            };
            let bytes = key.as_ref().map_or(0, |key| key.capacity() as u64);
            let Some(next) = self.bytes.checked_add(bytes) else {
                return Ok(Selection::Fallback);
            };
            if next > self.maximum {
                return Ok(Selection::Fallback);
            }
            self.bytes = next;
            head.key = key;
            head.pending = false;
        }
        let Some(key) = self.heads.iter().filter_map(|head| head.key.as_ref()).min() else {
            return Ok(Selection::Exhausted);
        };
        let mut selected = Vec::new();
        if selected.try_reserve_exact(key.len()).is_err() || selected.capacity() > MAX_RUN_KEY_BYTES
        {
            return Ok(Selection::Fallback);
        }
        selected.extend_from_slice(key);
        Ok(Selection::Key(selected))
    }

    pub(super) fn consume(&mut self, key: &[u8]) {
        for head in &mut self.heads {
            if head.key.as_deref() == Some(key) {
                if let Some(previous) = head.key.take() {
                    self.bytes -= previous.capacity() as u64;
                }
                head.pending = true;
            }
        }
    }
}
