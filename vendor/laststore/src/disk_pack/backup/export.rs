//! Destination-free export of one durably retained native snapshot.
//!
//! The caller owns terminal confirmation and explicit reference release. Drop
//! never releases the durable reference. No complete backup copy is created.

use super::files;
use crate::disk_pack::home::{DiskHome, HomeRead};
use crate::disk_pack::lifetime::{
    DurableReference, GenerationHold, ReferencePurpose, WriterReadLease,
};
use crate::disk_pack::{corrupt, Current, ReadBudget, PART_BYTES};
use crate::Result;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const MAX_FILES: usize = lastdb_storage_policy::native_v1::MAX_BACKUP_FILES;

/// One original immutable file, or an exact generated snapshot selector.
#[derive(Clone, Debug)]
pub struct SnapshotFile {
    /// Exact path relative to the native home, including snapshot selectors.
    pub path: PathBuf,
    /// Complete stored file length, at most one native part.
    pub bytes: u64,
}

/// A complete file inventory bound to one durable cloud-recovery reference.
pub struct SnapshotExport {
    reference: DurableReference,
    files: Vec<SnapshotFile>,
    current_bytes: [u8; 4096],
    bytes: u64,
}

impl SnapshotExport {
    /// Verify the exact retained generation. The live CURRENT is never selected.
    pub fn open(reference: DurableReference) -> Result<Self> {
        if reference.info().purpose != ReferencePurpose::CloudRecovery
            || reference.info().expiry_ms != 0
        {
            return Err(corrupt(
                "snapshot export requires a non-expiring cloud reference",
            ));
        }
        let current_bytes = reference.current().encode()?;
        let inventory = reference.inventory()?;
        let mut entries: Vec<_> = inventory
            .files
            .into_iter()
            .map(|file| SnapshotFile {
                path: file.path,
                bytes: file.bytes,
            })
            .collect();
        entries.push(SnapshotFile {
            path: "CURRENT".into(),
            bytes: 4096,
        });
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        if entries.len() > MAX_FILES {
            return Err(crate::disk_pack::resource());
        }
        let mut bytes = 0u64;
        let mut previous = None;
        for file in &entries {
            files::validate_path(&file.path)?;
            if previous == Some(&file.path) || file.bytes == 0 || file.bytes > PART_BYTES {
                return Err(corrupt("snapshot export inventory bounds"));
            }
            bytes = bytes
                .checked_add(file.bytes)
                .ok_or_else(|| corrupt("snapshot size overflow"))?;
            previous = Some(&file.path);
        }
        Ok(Self {
            reference,
            files: entries,
            current_bytes,
            bytes,
        })
    }

    /// Original selected native root, independent of the live CURRENT.
    pub fn current(&self) -> &Current {
        self.reference.current()
    }

    /// Complete, sorted, unique native file inventory.
    pub fn files(&self) -> &[SnapshotFile] {
        &self.files
    }

    /// Sum of every original file and the exact CURRENT selector.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Exact durable source reference owned by this export.
    pub fn reference_id(&self) -> uuid::Uuid {
        self.reference.info().identity
    }

    /// Read the exact retained root with the home's bounded native reader.
    /// A restarted writer can admit this reference below its ordinary read floor.
    pub fn read_retained(
        &self,
        home: &DiskHome,
        budget: ReadBudget,
        lease: &WriterReadLease,
    ) -> Result<HomeRead> {
        home.read_retained(&self.reference, budget, lease)
    }

    /// A writer-owned reader retains only its generation. It does not hold the
    /// global filesystem deletion lock during a slow transfer.
    pub fn reader(&self, path: &Path, lease: &WriterReadLease) -> Result<SnapshotReader> {
        let position = self
            .files
            .binary_search_by(|file| file.path.as_path().cmp(path))
            .map_err(|_| corrupt("snapshot export path is not in its inventory"))?;
        let selected = &self.files[position];
        let hold = GenerationHold::from_reference(lease, &self.reference)?;
        let input = match path.to_str() {
            Some("CURRENT") => Input::Memory(Cursor::new(self.current_bytes.to_vec())),
            _ => {
                let file = files::open(self.reference.home_path(), path)?;
                if file.metadata()?.len() != selected.bytes {
                    return Err(corrupt("snapshot export file length changed"));
                }
                Input::File(file)
            }
        };
        Ok(SnapshotReader {
            input,
            remaining: selected.bytes,
            bytes: selected.bytes,
            _hold: hold,
        })
    }
}

enum Input {
    File(File),
    Memory(Cursor<Vec<u8>>),
}

/// An opaque bounded original-file stream with an active generation hold.
pub struct SnapshotReader {
    input: Input,
    remaining: u64,
    bytes: u64,
    _hold: GenerationHold,
}

impl SnapshotReader {
    /// Select an offset within the same held immutable file without reading its prefix.
    pub fn seek_to(&mut self, offset: u64) -> std::io::Result<()> {
        if offset > self.bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "snapshot offset exceeds file",
            ));
        }
        match &mut self.input {
            Input::File(file) => {
                if file.metadata()?.len() != self.bytes {
                    return Err(std::io::Error::other("snapshot export file length changed"));
                }
                file.seek(SeekFrom::Start(offset))?;
            }
            Input::Memory(cursor) => {
                cursor.seek(SeekFrom::Start(offset))?;
            }
        }
        self.remaining = self.bytes - offset;
        Ok(())
    }
}

impl Read for SnapshotReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            if let Input::File(file) = &self.input {
                if file.metadata()?.len() != self.bytes {
                    return Err(std::io::Error::other("snapshot export file length changed"));
                }
            }
            return Ok(0);
        }
        let count = buffer.len().min(self.remaining.min(1024 * 1024) as usize);
        let read = match &mut self.input {
            Input::File(file) => file.read(&mut buffer[..count])?,
            Input::Memory(cursor) => cursor.read(&mut buffer[..count])?,
        };
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "snapshot export file truncated",
            ));
        }
        self.remaining -= read as u64;
        Ok(read)
    }
}
