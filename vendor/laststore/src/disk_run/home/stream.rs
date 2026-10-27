//! Bounded backup reads. Ordinary owner reads keep their existing fast path.
use super::super::keys::{run_key, storage_key};
use super::RunRead;
use crate::disk_run::reader::stream::SelectedBody;
use crate::disk_run::RunReader;
use crate::{Error, Result};
use sha2::{Digest, Sha256};
use std::io::{self, Write};

/// Exact decoded stored bytes copied without a file-sized allocation.
pub struct CopiedRunBody {
    /// Complete body length, excluding the internal stored-value tag.
    pub body_bytes: u64,
    /// SHA-256 of those exact bytes.
    pub body_sha256: [u8; 32],
}
struct Output<'a> {
    out: &'a mut dyn Write,
    hash: Sha256,
    bytes: u64,
}
impl Write for Output<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.out.write_all(bytes)?;
        self.hash.update(bytes);
        self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("run body size overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}
impl RunRead {
    /// Stream one normal-read body under this exact root. A tombstone returns None.
    /// Large RAW values use fixed buffers and no independent body-size cap.
    pub fn copy_body_to(
        &mut self,
        collection: &str,
        id: &[u8],
        output: &mut dyn Write,
    ) -> Result<Option<CopiedRunBody>> {
        self.bounded_streams = true;
        self.examine(1)?;
        self.charge((collection.len() + id.len()) as u64)?;
        let wanted = self.compound_key(collection, id)?;
        let admission = self.stream_admission(wanted.len())?;
        for slot in 0..self.runs.len() {
            if !self.runs[slot].0.meta.covers(&wanted) {
                continue;
            }
            let reserved = self.shared_metadata_bytes.max(self.retained_bytes())
                + self.costs.budget_bytes
                + wanted.len() as u64;
            let reader = RunReader::open_bounded_with_reserve(
                &self.runs[slot].0.path,
                &self.key,
                admission,
                reserved,
            )?;
            self.costs.runs_opened += 1;
            match reader.stream_body(
                &wanted,
                admission,
                self.shared_metadata_bytes.max(self.retained_bytes())
                    + self.costs.budget_bytes
                    + wanted.len() as u64,
            )? {
                Some(SelectedBody::Deleted) => return Ok(None),
                Some(SelectedBody::Value(body)) => {
                    let mut out = Output {
                        out: output,
                        hash: Sha256::new(),
                        bytes: 0,
                    };
                    let copied = body.copy(&mut out, u64::MAX)?;
                    if copied != out.bytes {
                        return Err(crate::disk_run::codec::corrupt("copied run body length"));
                    }
                    return Ok(Some(CopiedRunBody {
                        body_bytes: copied,
                        body_sha256: out.hash.finalize().into(),
                    }));
                }
                None => {}
            }
        }
        Ok(None)
    }
    fn stream_admission(&self, extra: usize) -> Result<u64> {
        let logical = self
            .budget
            .bytes
            .checked_sub(self.costs.budget_bytes)
            .ok_or(Error::ReadBudgetExceeded)?;
        let global = lastdb_storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(self.shared_metadata_bytes.max(self.retained_bytes()))
            .and_then(|n| n.checked_sub(self.costs.budget_bytes))
            .and_then(|n| n.checked_sub(extra as u64))
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok(logical
            .min(global)
            .min(lastdb_storage_policy::BACKUP_RESOURCES.decoded_values_bytes))
    }
    fn compound_key(&self, collection: &str, key: &[u8]) -> Result<Vec<u8>> {
        let length = collection
            .len()
            .checked_add(key.len())
            .and_then(|n| n.checked_add(2))
            .ok_or(Error::ReadBudgetExceeded)?;
        let frame_buffers = 2 * lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes + 16;
        let needed = (length as u64)
            .checked_add(frame_buffers)
            .ok_or(Error::ReadBudgetExceeded)?;
        let needed = usize::try_from(needed).map_err(|_| Error::ReadBudgetExceeded)?;
        self.stream_admission(needed)?;
        let wanted = run_key(collection, key)?;
        let actual = (wanted.capacity() as u64)
            .checked_add(frame_buffers)
            .ok_or(Error::ReadBudgetExceeded)?;
        self.stream_admission(usize::try_from(actual).map_err(|_| Error::ReadBudgetExceeded)?)?;
        Ok(wanted)
    }
    pub(in crate::disk_run::home) fn streamed_value(
        &mut self,
        collection: &str,
        key: &str,
        maximum: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.examine(1)?;
        self.charge((collection.len() + key.len()) as u64)?;
        let wanted = self.compound_key(collection, key.as_bytes())?;
        let admission = self.stream_admission(wanted.len())?;
        for slot in 0..self.runs.len() {
            if !self.runs[slot].0.meta.covers(&wanted) {
                continue;
            }
            let reserved = self.shared_metadata_bytes.max(self.retained_bytes())
                + self.costs.budget_bytes
                + wanted.len() as u64;
            let reader = RunReader::open_bounded_with_reserve(
                &self.runs[slot].0.path,
                &self.key,
                admission,
                reserved,
            )?;
            self.costs.runs_opened += 1;
            match reader.stream_body(
                &wanted,
                admission,
                self.shared_metadata_bytes.max(self.retained_bytes())
                    + self.costs.budget_bytes
                    + wanted.len() as u64,
            )? {
                Some(SelectedBody::Deleted) => return Ok(None),
                Some(SelectedBody::Value(body)) => {
                    let maximum = maximum.min(admission).min(body.allocation_limit());
                    let value = body.into_vec(maximum)?;
                    self.charge(value.len() as u64)?;
                    return Ok(Some(value));
                }
                None => {}
            }
        }
        Ok(None)
    }
    pub(in crate::disk_run::home) fn streamed_next_key(
        &mut self,
        collection: &str,
        lower: &[u8],
        upper: Option<&[u8]>,
        exclusive: bool,
    ) -> Result<Option<String>> {
        let mut selected: Option<Vec<u8>> = None;
        for slot in 0..self.runs.len() {
            let end = selected.as_deref().or(upper);
            if let Some(next) = self.streamed_next_key_at(
                slot,
                lower,
                end,
                exclusive,
                selected.as_ref().map_or(0, |key| key.capacity() as u64),
            )? {
                selected = Some(next);
            }
        }
        selected
            .map(|key| storage_key(collection, &key))
            .transpose()
    }

    /// Select one authenticated key without reopening unrelated input runs.
    /// `extra_retained` includes the caller's complete retained key-head cache.
    pub(in crate::disk_run::home) fn streamed_next_key_at(
        &mut self,
        slot: usize,
        lower: &[u8],
        upper: Option<&[u8]>,
        exclusive: bool,
        extra_retained: u64,
    ) -> Result<Option<Vec<u8>>> {
        if !self.runs[slot].0.meta.overlaps(lower, upper) {
            return Ok(None);
        }
        let coordinates = lower
            .len()
            .checked_add(upper.map_or(0, <[u8]>::len))
            .ok_or(Error::ReadBudgetExceeded)?;
        let extra = usize::try_from(extra_retained)
            .ok()
            .and_then(|n| n.checked_add(coordinates))
            .ok_or(Error::ReadBudgetExceeded)?;
        let admission = self.stream_admission(extra)?;
        let reserved = self
            .shared_metadata_bytes
            .max(self.retained_bytes())
            .checked_add(self.costs.budget_bytes)
            .and_then(|n| n.checked_add(extra as u64))
            .ok_or(Error::ReadBudgetExceeded)?;
        let reader = RunReader::open_bounded_with_reserve(
            &self.runs[slot].0.path,
            &self.key,
            admission,
            reserved,
        )?;
        self.costs.runs_opened += 1;
        reader.stream_key(lower, upper, exclusive, admission, reserved)
    }
}
