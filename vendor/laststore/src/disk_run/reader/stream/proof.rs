//! Optional proof-only authenticated block storage. No format or ordinary-read change.
#[path = "proof/load.rs"]
mod load;
#[path = "proof/records.rs"]
mod records;

use super::{corrupt, RunReader};
use crate::disk_run::value;
use crate::{Error, Result};
use std::fs::{File, Metadata};
use std::ops::Range;
use std::os::unix::fs::MetadataExt;

#[derive(Clone, Copy)]
pub(crate) struct ProofBounds<'a> {
    pub lower: &'a [u8],
    pub upper: Option<&'a [u8]>,
    pub exclusive: bool,
}

pub(crate) struct ProofHead {
    pub key: Vec<u8>,
    pub block: ProofBlock,
}

/// The file descriptor and original frame identity remain bound to these bytes.
pub(crate) struct ProofBlock {
    file: File,
    identity: Metadata,
    payload: Vec<u8>,
    at: usize,
    key: Vec<u8>,
    body: Option<Range<usize>>,
    stream_workspace: u64,
}

impl ProofBlock {
    pub(crate) fn memory_bytes(&self) -> u64 {
        (std::mem::size_of::<Self>() + self.payload.capacity() + self.key.capacity()) as u64
    }

    pub(crate) fn stream_workspace(&self) -> u64 {
        self.stream_workspace
    }

    pub(crate) fn next_key(&mut self, bounds: ProofBounds<'_>) -> Result<Option<Vec<u8>>> {
        self.check()?;
        while self.at < self.payload.len() {
            self.body = records::next(&self.payload, &mut self.at, &mut self.key)?;
            if bounds
                .upper
                .is_some_and(|upper| self.key.as_slice() >= upper)
            {
                self.check()?;
                return Ok(None);
            }
            if self.key.as_slice() > bounds.lower
                || !bounds.exclusive && self.key.as_slice() == bounds.lower
            {
                let mut key = Vec::new();
                key.try_reserve_exact(self.key.len())
                    .map_err(|_| Error::ReadBudgetExceeded)?;
                key.extend_from_slice(&self.key);
                self.check()?;
                return Ok(Some(key));
            }
        }
        self.check()?;
        Ok(None)
    }

    pub(crate) fn value(
        &self,
        wanted: &[u8],
        maximum: u64,
        admission: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.check()?;
        if self.key != wanted {
            return Err(corrupt("proof cached key differs"));
        }
        let Some(body) = &self.body else {
            return Ok(None);
        };
        let stored = &self.payload[body.clone()];
        let bytes = stored.len().saturating_sub(1) as u64;
        let limit = match stored.first() {
            Some(&value::RAW) => {
                if bytes > maximum.min(admission) {
                    return Err(Error::ReadBudgetExceeded);
                }
                let mut result = Vec::new();
                result
                    .try_reserve_exact(bytes as usize)
                    .map_err(|_| Error::ReadBudgetExceeded)?;
                if result.capacity() as u64 > admission {
                    return Err(Error::ReadBudgetExceeded);
                }
                result.extend_from_slice(&stored[1..]);
                self.check()?;
                return Ok(Some(result));
            }
            Some(&value::COMPACT_JSON) => {
                // Keep the existing stream decoder's six-times workspace and
                // one-quarter output admission, even though this cache needs less.
                let decoded = lastdb_storage_policy::BACKUP_RESOURCES
                    .decoded_values_bytes
                    .min(admission);
                if bytes.saturating_mul(6) > decoded {
                    return Err(Error::ReadBudgetExceeded);
                }
                maximum.min(decoded / 4)
            }
            _ => return Err(corrupt("unknown stored value tag")),
        };
        let mut copy = Vec::new();
        copy.try_reserve_exact(stored.len())
            .map_err(|_| Error::ReadBudgetExceeded)?;
        copy.extend_from_slice(stored);
        let result = match value::decode(
            copy,
            usize::try_from(limit).map_err(|_| Error::ReadBudgetExceeded)?,
        ) {
            // A cache can reduce only this output allowance. Retry without it
            // before classifying that one limit failure as malformed storage.
            Err(Error::Corrupt(detail))
                if limit < maximum && detail == "disk run: compact value exceeds its limit" =>
            {
                return Err(Error::ReadBudgetExceeded);
            }
            result => result?,
        };
        self.check()?;
        Ok(Some(result))
    }

    fn check(&self) -> Result<()> {
        let current = crate::read_receipt_io::status(|| self.file.metadata())?;
        if !current.is_file()
            || current.dev() != self.identity.dev()
            || current.ino() != self.identity.ino()
            || current.len() != self.identity.len()
            || current.mtime() != self.identity.mtime()
            || current.mtime_nsec() != self.identity.mtime_nsec()
        {
            return Err(corrupt("proof cached file changed"));
        }
        Ok(())
    }
}
