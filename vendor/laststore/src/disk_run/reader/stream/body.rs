//! Bounded materialization and streaming of one authenticated stored value.
use super::Block;
use crate::{Error, Result};
use std::io::{Read, Write};

pub(crate) struct StreamBody {
    pub(super) block: Block,
    pub(super) bytes: u64,
    pub(super) memory_available: u64,
}
impl StreamBody {
    pub(crate) fn allocation_limit(&self) -> u64 {
        self.memory_available
    }
    pub(crate) fn into_vec(self, maximum: u64) -> Result<Vec<u8>> {
        if self.bytes > maximum {
            return Err(Error::ReadBudgetExceeded);
        }
        let mut out = ExactVec {
            bytes: Vec::new(),
            maximum,
        };
        out.bytes
            .try_reserve_exact(self.bytes as usize)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.copy(&mut out, maximum)?;
        Ok(out.bytes)
    }
    pub(crate) fn copy(mut self, out: &mut dyn Write, maximum: u64) -> Result<u64> {
        if self.bytes > maximum {
            return Err(Error::ReadBudgetExceeded);
        }
        copy_exact(&mut self.block.input, out, self.bytes)?;
        self.block.remaining = 0;
        self.block.finish()?;
        Ok(self.bytes)
    }
}
struct ExactVec {
    bytes: Vec<u8>,
    maximum: u64,
}
impl Write for ExactVec {
    fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
        let length = self
            .bytes
            .len()
            .checked_add(input.len())
            .ok_or_else(|| std::io::Error::other("stream value size overflow"))?;
        if length as u64 > self.maximum {
            return Err(std::io::Error::other("stream value allowance"));
        }
        self.bytes
            .try_reserve_exact(length - self.bytes.len())
            .map_err(|_| std::io::Error::other("stream value allocation"))?;
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(super) fn copy_exact(input: &mut dyn Read, out: &mut dyn Write, mut bytes: u64) -> Result<()> {
    let mut buffer = [0u8; 65536];
    while bytes != 0 {
        let take = bytes.min(buffer.len() as u64) as usize;
        input.read_exact(&mut buffer[..take])?;
        out.write_all(&buffer[..take])?;
        bytes -= take as u64;
    }
    Ok(())
}
