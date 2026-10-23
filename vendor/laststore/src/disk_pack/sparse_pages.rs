use super::page_write::{encode_page, PageFields};
use super::tree_output::TreePages;
use super::{corrupt, TreeKind, TreeRoot};
use crate::{durability, Result};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Only fresh, private parts. Published parts are never opened for writes.
pub(super) struct SparsePages {
    protection: super::private_files::OutputFiles,
    directory: PathBuf,
    object: Uuid,
    kind: TreeKind,
    generation: u64,
    next: u64,
    file: Option<File>,
    pub(super) written_pages: u64,
    part_numbers: Vec<u32>,
}

impl SparsePages {
    pub(super) fn protected(
        root: &Path,
        tree: &TreeRoot,
        generation: u64,
        protection: super::private_files::OutputFiles,
    ) -> Result<Self> {
        tree.validate()?;
        let directory = root
            .join(tree.kind.directory())
            .join(tree.object.to_string());
        if generation == 0 || !directory.is_dir() {
            return Err(corrupt("tree edit namespace or generation"));
        }
        Ok(Self {
            protection: {
                protection.protect_directory(&directory)?;
                protection
            },
            directory,
            object: tree.object,
            kind: tree.kind,
            generation,
            next: 0,
            file: None,
            written_pages: 0,
            part_numbers: Vec::new(),
        })
    }

    pub(super) fn check_tree(&self, tree: &TreeRoot) -> Result<()> {
        if self.object != tree.object || self.kind != tree.kind {
            return Err(corrupt("shared edit allocator differs from tree identity"));
        }
        Ok(())
    }

    /// Close the private descriptor between trees; finish still owns its barrier.
    pub(super) fn park(&mut self) {
        crate::commit_timing::count("metadata_writer_parks", u64::from(self.file.is_some()));
        drop(self.file.take());
    }

    fn reopen_part(&mut self) -> Result<()> {
        if self.file.is_none() && self.next % crate::disk_pack::PAGES_PER_PART != 0 {
            let part = self.next / crate::disk_pack::PAGES_PER_PART;
            let file = OpenOptions::new()
                .append(true)
                .open(self.directory.join(format!("part-{part:08x}")))?;
            if file.metadata()?.len()
                != self.next % crate::disk_pack::PAGES_PER_PART * super::PAGE_BYTES as u64
            {
                return Err(corrupt("private shared metadata part length changed"));
            }
            self.file = Some(file);
            crate::commit_timing::count("metadata_writer_reopens", 1);
        }
        Ok(())
    }

    fn allocate(&mut self) -> Result<()> {
        self.sync_part()?;
        self.part_numbers
            .try_reserve(1)
            .map_err(|_| super::resource())?;
        // Exact create-new attempts replace a directory walk and never reuse an identity.
        for _ in 0..8 {
            let part = u32::from_le_bytes(
                Uuid::new_v4().as_bytes()[..4]
                    .try_into()
                    .expect("UUID prefix"),
            );
            if part == 0 {
                continue;
            }
            let path = self.directory.join(format!("part-{part:08x}"));
            match self.protection.create_new(&path) {
                Ok(file) => {
                    self.file = Some(file);
                    self.next = u64::from(part) * crate::disk_pack::PAGES_PER_PART;
                    self.part_numbers.push(part);
                    return Ok(());
                }
                Err(crate::Error::Io(error))
                    if error.kind() == std::io::ErrorKind::AlreadyExists =>
                {
                    continue
                }
                Err(crate::Error::WriteCapacity(reason))
                    if reason == super::private_files::ACTIVE_DELETION =>
                {
                    continue
                }
                Err(error) => return Err(error),
            }
        }
        Err(crate::Error::Config(
            "disk-pack metadata part allocation exhausted its bounded attempts".into(),
        ))
    }

    fn sync_part(&mut self) -> Result<()> {
        self.reopen_part()?;
        if let Some(file) = self.file.take() {
            durability::sync_dirty_file(&file)?;
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<Vec<u32>> {
        self.sync_part()?;
        durability::sync_directory(&File::open(&self.directory)?)?;
        Ok(self.part_numbers)
    }
}

impl TreePages for SparsePages {
    fn reserve(&mut self, count: usize) -> Result<u64> {
        if !(1..=crate::disk_pack::PAGES_PER_PART as usize).contains(&count) {
            return Err(corrupt("invalid contiguous metadata reservation"));
        }
        if (self.file.is_none() && self.next % crate::disk_pack::PAGES_PER_PART == 0)
            || self.next % crate::disk_pack::PAGES_PER_PART + count as u64
                > crate::disk_pack::PAGES_PER_PART
        {
            self.allocate()?;
        } else {
            self.reopen_part()?;
        }
        Ok(self.next)
    }

    fn append(&mut self, fields: PageFields, payload: &[u8]) -> Result<u64> {
        self.reserve(1)?;
        let number = self.next;
        let bytes = encode_page(self.object, self.generation, number, fields, payload)?;
        self.file
            .as_mut()
            .expect("private metadata part")
            .write_all(&bytes)?;
        self.written_pages += 1;
        self.next += 1;
        if self.next % crate::disk_pack::PAGES_PER_PART == 0 {
            self.sync_part()?;
        }
        Ok(number)
    }
}
