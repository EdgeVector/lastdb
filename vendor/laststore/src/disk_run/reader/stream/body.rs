//! Bounded materialization and streaming of one authenticated stored value.
use super::Block;
use crate::disk_run::{codec::corrupt, value};
use crate::{Error, Result};
use std::io::{Read, Write};

pub(crate) struct StreamBody {
    pub(super) block: Block,
    pub(super) bytes: u64,
    pub(super) memory_available: u64,
    pub(super) tag: Option<u8>,
}
impl StreamBody {
    pub(crate) fn allocation_limit(&self) -> u64 {
        self.memory_available
    }
    pub(crate) fn into_vec(mut self, maximum: u64) -> Result<Vec<u8>> {
        self.read_tag()?;
        if self.tag == Some(value::RAW) && self.bytes > maximum {
            return Err(Error::ReadBudgetExceeded);
        }
        if self.tag == Some(value::COMPACT_JSON)
            && self.bytes.saturating_mul(6) > self.memory_available
        {
            return Err(Error::ReadBudgetExceeded);
        }
        let initial = self.bytes.min(maximum);
        let mut out = ExactVec {
            bytes: Vec::new(),
            maximum,
        };
        out.bytes
            .try_reserve_exact(initial as usize)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.copy(&mut out, maximum)?;
        Ok(out.bytes)
    }
    pub(crate) fn copy(mut self, out: &mut dyn Write, maximum: u64) -> Result<u64> {
        self.read_tag()?;
        let tag = self
            .tag
            .ok_or_else(|| corrupt("stored value tag missing"))?;
        let bytes = match tag {
            value::RAW => {
                if self.bytes > maximum {
                    return Err(Error::ReadBudgetExceeded);
                }
                copy_exact(&mut self.block.input, out, self.bytes)?;
                self.bytes
            }
            value::COMPACT_JSON => {
                let admission = lastdb_storage_policy::BACKUP_RESOURCES
                    .decoded_values_bytes
                    .min(self.memory_available);
                if self.bytes.saturating_mul(6) > admission {
                    return Err(Error::ReadBudgetExceeded);
                }
                let mut stored = Vec::with_capacity(self.bytes as usize + 1);
                stored.push(tag);
                (&mut self.block.input)
                    .take(self.bytes)
                    .read_to_end(&mut stored)?;
                if stored.len() as u64 != self.bytes + 1 {
                    return Err(corrupt("stored compact value ends early"));
                }
                let decoded = value::decode(stored, maximum.min(admission / 4) as usize)?;
                out.write_all(&decoded)?;
                decoded.len() as u64
            }
            _ => return Err(corrupt("unknown stored value tag")),
        };
        self.block.remaining = 0;
        self.block.finish()?;
        Ok(bytes)
    }
    fn read_tag(&mut self) -> Result<()> {
        if self.tag.is_none() {
            if self.bytes == 0 {
                return Err(corrupt("stored value is empty"));
            }
            let mut tag = [0u8; 1];
            self.block.input.read_exact(&mut tag)?;
            self.bytes -= 1;
            if !matches!(tag[0], value::RAW | value::COMPACT_JSON) {
                return Err(corrupt("unknown stored value tag"));
            }
            self.tag = Some(tag[0]);
        }
        Ok(())
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
