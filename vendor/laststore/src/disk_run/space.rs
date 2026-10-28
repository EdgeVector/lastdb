//! Incremental replacement-file admission. No quota covers the whole base.

use crate::space::{reserve_space, SpacePolicy, SpaceReservation};
use crate::{Error, Result};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Retain a publication allowance while run blocks become physical file bytes.
const PUBLICATION_BYTES: u64 = 4096;
/// Observe capacity once per MiB on the normal path, not once per record/block.
const OUTPUT_CHUNK_BYTES: u64 = 1 << 20;

#[derive(Clone)]
pub(crate) struct MergeSpace(Arc<Mutex<Pending>>);

struct Pending {
    policy: SpacePolicy,
    ticket: SpaceReservation,
    outputs: Vec<PathBuf>,
    keep_outputs: bool,
}

impl MergeSpace {
    pub(crate) fn new(root: &Path, policy: &SpacePolicy) -> Result<Self> {
        let ticket = reserve_space(root, policy, PUBLICATION_BYTES)?;
        Ok(Self(Arc::new(Mutex::new(Pending {
            policy: policy.clone(),
            ticket,
            outputs: Vec::new(),
            keep_outputs: false,
        }))))
    }

    /// Admit the exact encoded block before its write, with bounded spare quota.
    pub(crate) fn admit_run(&self, bytes: u64) -> Result<()> {
        let mut pending = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let required = bytes.checked_add(PUBLICATION_BYTES).ok_or_else(overflow)?;
        if required <= pending.ticket.bytes() {
            return Ok(());
        }
        let chunk = pending
            .ticket
            .bytes()
            .checked_add(OUTPUT_CHUNK_BYTES)
            .ok_or_else(overflow)?
            .max(required);
        let extra = pending
            .ticket
            .reserve_extension(&pending.policy, chunk)
            .or_else(|error| match error {
                Error::Io(io)
                    if io.raw_os_error() == Some(rustix::io::Errno::NOSPC.raw_os_error()) =>
                {
                    pending.ticket.reserve_extension(&pending.policy, required)
                }
                error => Err(error),
            })?;
        pending.ticket.absorb(extra);
        Ok(())
    }

    /// Written run bytes no longer consume unwritten quota. Concurrent admission
    /// sees either the reservation or the physical file, including overlap races.
    pub(crate) fn materialize(&self, bytes: u64) -> Result<()> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .ticket
            .materialize(bytes)
    }

    /// CURRENT's exact encoded bytes stay reserved through its directory barrier.
    pub(crate) fn admit_current(&self, bytes: u64) -> Result<()> {
        let mut pending = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if bytes > pending.ticket.bytes() {
            let extra = pending.ticket.reserve_extension(&pending.policy, bytes)?;
            pending.ticket.absorb(extra);
        }
        Ok(())
    }

    /// Register after rename, before its barrier can fail.
    pub(crate) fn output(&self, path: &Path) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .outputs
            .push(path.to_path_buf());
    }

    pub(crate) fn keep_outputs(&self) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keep_outputs = true;
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if !self.keep_outputs {
            for path in &self.outputs {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

fn overflow() -> Error {
    Error::WriteCapacity("disk run: replacement reservation overflow".into())
}
