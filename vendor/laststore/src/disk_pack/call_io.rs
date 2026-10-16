//! Bounded file ownership for one immutable read generation.

use super::call::Buffer;
use super::{corrupt, ReadCall, PART_BYTES};
use crate::read_receipt_io::{self as measured, MeasuredFile, ReadClass};
use crate::Result;
use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// The pool never retains descriptors beyond the read call or grows with key count.
const MAX_READ_HANDLES: usize = 32;

struct OpenPart {
    path: PathBuf,
    file: MeasuredFile,
    length: u64,
}

#[derive(Default)]
pub(super) struct CallFiles {
    // Front is most recently used; every access needs at most 32 comparisons.
    parts: VecDeque<OpenPart>,
}

impl CallFiles {
    fn get(&mut self, path: &Path) -> Result<(&mut OpenPart, bool)> {
        let reused = if let Some(at) = self.parts.iter().position(|part| part.path == path) {
            let part = self.parts.remove(at).expect("known call-local handle");
            self.parts.push_front(part);
            true
        } else {
            // Close before open so even a replacement never exceeds the cap.
            if self.parts.len() == MAX_READ_HANDLES {
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
        let (part, reused) = self.files.get(path)?;
        self.costs.file_handle_reuses += u64::from(reused);
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| corrupt("file offset overflow"))?;
        if end > part.length
            || part.length > PART_BYTES
            || (page_part && part.length % super::PAGE_BYTES as u64 != 0)
        {
            return Err(corrupt("extent outside immutable part"));
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
        self.costs.file_handles_peak = self
            .costs
            .file_handles_peak
            .max(self.files.parts.len() as u64);
        Ok(buffer)
    }
}
