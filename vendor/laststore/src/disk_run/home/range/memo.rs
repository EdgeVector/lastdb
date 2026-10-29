//! Optional key heads and authenticated forward block cursors share one allowance.
use super::RunRead;
use crate::disk_run::reader::stream::{ProofBlock, ProofBounds, ProofHead};
use crate::disk_run::MAX_RUN_KEY_BYTES;
use crate::{Error, Result};

struct Head {
    slot: usize,
    key: Option<Vec<u8>>,
    pending: bool,
    block: Option<Box<ProofBlock>>,
}

pub(super) enum Selection {
    Key(Vec<u8>, usize),
    Exhausted,
    Fallback,
}

pub(super) enum CachedValue {
    Unavailable,
    Value(Option<Vec<u8>>),
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
                    block: None,
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
        let bounds = ProofBounds {
            lower,
            upper,
            exclusive,
        };
        for index in 0..self.heads.len() {
            if !self.heads[index].pending {
                continue;
            }
            if !self.advance(owner, index, bounds, retained)? {
                return Ok(Selection::Fallback);
            }
        }
        // Original slots are newest first. The tuple's slot breaks an equal-key
        // tie explicitly, so the value read can use this exact winning run.
        let Some((key, slot)) = self
            .heads
            .iter()
            .filter_map(|head| head.key.as_ref().map(|key| (key, head.slot)))
            .min()
        else {
            return Ok(Selection::Exhausted);
        };
        let mut selected = Vec::new();
        if selected.try_reserve_exact(key.len()).is_err() || selected.capacity() > MAX_RUN_KEY_BYTES
        {
            return Ok(Selection::Fallback);
        }
        selected.extend_from_slice(key);
        Ok(Selection::Key(selected, slot))
    }

    fn advance(
        &mut self,
        owner: &mut RunRead,
        index: usize,
        bounds: ProofBounds<'_>,
        retained: u64,
    ) -> Result<bool> {
        let prior = self.heads[index].block.take();
        if let Some(block) = &prior {
            self.bytes -= block.memory_bytes();
        }
        let cached = match prior {
            Some(mut block) => match block.next_key(bounds) {
                Ok(Some(key)) => Some(ProofHead { key, block: *block }),
                Ok(None) | Err(Error::ReadBudgetExceeded) => None,
                Err(error) => return Err(error),
            },
            None => None,
        };
        let cached = match cached {
            Some(cached) => Some(cached),
            None if self
                .heads
                .iter()
                .filter(|head| head.block.is_some())
                .count()
                < lastdb_storage_policy::backup_decode::PROOF_BLOCK_CURSOR_HANDLES =>
            {
                match owner.proof_head_at(
                    self.heads[index].slot,
                    bounds,
                    self.maximum.saturating_sub(self.bytes),
                    retained.saturating_add(self.bytes),
                ) {
                    Ok(cached) => cached,
                    Err(Error::ReadBudgetExceeded) => None,
                    Err(error) => return Err(error),
                }
            }
            None => None,
        };
        let key = if let Some(cached) = cached {
            self.bytes = self.bytes.saturating_add(cached.block.memory_bytes());
            self.heads[index].block = Some(Box::new(cached.block));
            Some(cached.key)
        } else {
            match owner.streamed_next_key_at(
                self.heads[index].slot,
                bounds.lower,
                bounds.upper,
                bounds.exclusive,
                retained.saturating_add(self.bytes),
            ) {
                Ok(key) => key,
                Err(Error::ReadBudgetExceeded) => return Ok(false),
                Err(error) => return Err(error),
            }
        };
        self.bytes = self
            .bytes
            .saturating_add(key.as_ref().map_or(0, |key| key.capacity() as u64));
        if self.bytes > self.maximum {
            return Ok(false);
        }
        self.heads[index].key = key;
        self.heads[index].pending = false;
        Ok(true)
    }

    pub(super) fn value(
        &self,
        owner: &mut RunRead,
        slot: usize,
        collection: &str,
        key: &str,
        maximum: u64,
    ) -> Result<CachedValue> {
        let block = self
            .heads
            .iter()
            .find(|head| head.slot == slot)
            .and_then(|head| head.block.as_deref());
        let Some(block) = block else {
            return Ok(CachedValue::Unavailable);
        };
        owner
            .cached_block_value(block, collection, key, maximum)
            .map(CachedValue::Value)
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
