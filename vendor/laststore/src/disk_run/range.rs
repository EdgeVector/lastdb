//! Ordered iteration over the records of one run inside key bounds.

use super::reader::{IndexBlock, RunReader};
use super::Found;
use crate::Result;
use std::ops::Bound;

/// Records of one run inside key bounds, in key order. It reads one index
/// block and one data block at a time and starts at the block that holds
/// the lower bound, so a bounded range never reads the whole run.
pub struct RunRange<'a> {
    reader: &'a RunReader,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    next_index: usize,
    index: Option<IndexBlock>,
    next_block: usize,
    positioned: bool,
    current: std::vec::IntoIter<(Vec<u8>, Found)>,
    done: bool,
}

impl<'a> RunRange<'a> {
    pub(super) fn new(reader: &'a RunReader, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Self {
        let next_index = match lower {
            Bound::Included(key) | Bound::Excluded(key) => reader.index_for(key).unwrap_or(0),
            Bound::Unbounded => 0,
        };
        Self {
            reader,
            lower: owned(lower),
            upper: owned(upper),
            next_index,
            index: None,
            next_block: 0,
            positioned: false,
            current: Vec::new().into_iter(),
            done: false,
        }
    }

    fn below_lower(&self, key: &[u8]) -> bool {
        match &self.lower {
            Bound::Included(lower) => key < lower.as_slice(),
            Bound::Excluded(lower) => key <= lower.as_slice(),
            Bound::Unbounded => false,
        }
    }

    fn above_upper(&self, key: &[u8]) -> bool {
        match &self.upper {
            Bound::Included(upper) => key > upper.as_slice(),
            Bound::Excluded(upper) => key >= upper.as_slice(),
            Bound::Unbounded => false,
        }
    }

    /// Loads the next data block into `current`. `false` means the run or
    /// the bounds are used up.
    fn load_next_block(&mut self) -> Result<bool> {
        loop {
            let exhausted = match &self.index {
                Some(index) => self.next_block >= index.blocks.len(),
                None => true,
            };
            if exhausted {
                if self.next_index >= self.reader.index_count() {
                    return Ok(false);
                }
                let index = self.reader.read_index(self.next_index)?;
                self.next_index += 1;
                self.next_block = self.first_block_in(&index);
                self.index = Some(index);
                continue;
            }
            let index = self.index.as_ref().expect("index loaded");
            let block = &index.blocks[self.next_block];
            if self.above_upper(&block.first_key) {
                return Ok(false);
            }
            let records = self.reader.read_data(block)?;
            self.next_block += 1;
            self.current = records.into_iter();
            return Ok(true);
        }
    }

    /// The first index block skips the data blocks wholly below the lower
    /// bound. Later index blocks start at their first data block.
    fn first_block_in(&mut self, index: &IndexBlock) -> usize {
        if self.positioned {
            return 0;
        }
        self.positioned = true;
        match &self.lower {
            Bound::Included(key) | Bound::Excluded(key) => index
                .blocks
                .partition_point(|b| b.first_key.as_slice() <= key.as_slice())
                .saturating_sub(1),
            Bound::Unbounded => 0,
        }
    }
}

fn owned(bound: Bound<&[u8]>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(key) => Bound::Included(key.to_vec()),
        Bound::Excluded(key) => Bound::Excluded(key.to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl Iterator for RunRange<'_> {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.done {
            if let Some((key, found)) = self.current.next() {
                if self.below_lower(&key) {
                    continue;
                }
                if self.above_upper(&key) {
                    self.done = true;
                    return None;
                }
                return Some(Ok((key, found)));
            }
            match self.load_next_block() {
                Ok(true) => {}
                Ok(false) => self.done = true,
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            }
        }
        None
    }
}
