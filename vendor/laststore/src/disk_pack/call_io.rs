//! Bounded file ownership for one immutable read generation.

use super::call::Buffer;
use super::{corrupt, ReadCall, PART_BYTES};
use crate::read_receipt_io::{self as measured, MeasuredFile, ReadClass};
use crate::Result;
use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// An ordinary read call never retains descriptors beyond the call or grows with key count.
const MAX_READ_HANDLES: usize = 32;

struct OpenPart {
    path: PathBuf,
    file: MeasuredFile,
    length: u64,
}

pub(super) struct CallFiles {
    // Front is most recently used; every access needs at most `capacity` comparisons.
    parts: VecDeque<OpenPart>,
    capacity: usize,
}

impl Default for CallFiles {
    fn default() -> Self {
        Self::with_capacity(MAX_READ_HANDLES)
    }
}

impl CallFiles {
    /// A pool that keeps at most `capacity` descriptors (at least one). Maintenance
    /// that walks the whole home asks for more than a read call uses.
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            parts: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    fn get(&mut self, path: &Path) -> Result<(&mut OpenPart, bool, u64)> {
        // Byte comparison: `Path` equality walks every component, which costs
        // more than the descriptor it saves once the pool holds dozens of parts.
        let reused = if let Some(at) = self
            .parts
            .iter()
            .position(|part| part.path.as_os_str() == path.as_os_str())
        {
            let mut part = self.parts.remove(at).expect("known call-local handle");
            // Internal writer calls can also address fresh private parts as
            // they grow. Reuse the descriptor, never a stale length bound.
            part.length = part.file.metadata()?.len();
            self.parts.push_front(part);
            true
        } else {
            // Close before open so even a replacement never exceeds the cap.
            if self.parts.len() >= self.capacity {
                self.parts.pop_back();
            }
            let file = measured::open(path)?;
            let length = file.metadata()?.len();
            self.parts.push_front(OpenPart {
                path: path.to_path_buf(),
                file,
                length,
            });
            false
        };
        let peak = self.parts.len() as u64;
        crate::read_receipt::update(|receipt| {
            receipt.pack_file_handles_peak = receipt.pack_file_handles_peak.max(peak);
            receipt.pack_file_handle_reuses += u64::from(reused);
        });
        Ok((
            self.parts.front_mut().expect("opened call-local handle"),
            reused,
            peak,
        ))
    }
}

impl ReadCall {
    pub(super) fn read_extent(
        &mut self,
        path: &Path,
        offset: u64,
        length: usize,
        page_part: bool,
        class: ReadClass,
    ) -> Result<Buffer> {
        self.charge(length as u64)?;
        let (part, reused, peak) = self.files.get(path)?;
        self.costs.file_handle_reuses += u64::from(reused);
        self.costs.file_handles_peak = self.costs.file_handles_peak.max(peak);
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| corrupt("file offset overflow"))?;
        if end > part.length
            || part.length > PART_BYTES
            || (page_part && part.length % super::PAGE_BYTES as u64 != 0)
        {
            return Err(crate::Error::Corrupt(format!(
                "disk-pack v1: extent outside immutable part: {} offset {offset} length {length} \
                 file length {}",
                path.display(),
                part.length
            )));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| super::resource())?;
        bytes.resize(length, 0);
        let mut buffer = Buffer::from_vec(bytes);
        part.file.seek(SeekFrom::Start(offset))?;
        let _class = class.enter();
        part.file.read_exact(&mut buffer.bytes)?;
        self.costs.file_bytes = self.costs.file_bytes.saturating_add(length as u64);
        Ok(buffer)
    }
}
