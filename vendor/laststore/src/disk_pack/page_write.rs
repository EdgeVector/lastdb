use super::bytes::*;
use super::{corrupt, HEADER_BYTES, PAGE_BYTES, PAGE_PAYLOAD};
use crate::{durability, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub(super) fn directory(path: &Path) -> Result<()> {
    let _timing = crate::commit_timing::stage("metadata_namespace_create_us");
    fs::create_dir(path)?;
    durability::sync_directory(&File::open(path)?)?;
    if let Some(parent) = path.parent() {
        durability::sync_directory(&File::open(parent)?)?;
    }
    Ok(())
}

pub(super) struct PageFields {
    pub(super) kind: u8,
    pub(super) level: u8,
    pub(super) count: u16,
    pub(super) width: u16,
    pub(super) left: u64,
    pub(super) next: u64,
    pub(super) auxiliary: u64,
}

pub(super) struct PageWriter {
    protection: super::private_files::OutputFiles,
    directory: PathBuf,
    object: Uuid,
    generation: u64,
    next: u64,
    file: Option<File>,
}

impl PageWriter {
    pub(super) fn new(path: PathBuf, object: Uuid, generation: u64) -> Result<Self> {
        Self::protected(path, object, generation, Default::default())
    }

    pub(super) fn protected(
        path: PathBuf,
        object: Uuid,
        generation: u64,
        protection: super::private_files::OutputFiles,
    ) -> Result<Self> {
        nonnil(object)?;
        if generation == 0 {
            return Err(corrupt("zero creation generation"));
        }
        protection.protect_directory(&path)?;
        directory(&path)?;
        Ok(Self {
            protection,
            directory: path,
            object,
            generation,
            next: 0,
            file: None,
        })
    }

    pub(super) fn next_page(&self) -> u64 {
        self.next
    }

    pub(super) fn object(&self) -> Uuid {
        self.object
    }

    pub(super) fn append(&mut self, fields: PageFields, payload: &[u8]) -> Result<u64> {
        if payload.len() > PAGE_PAYLOAD {
            return Err(corrupt("page write payload too large"));
        }
        let part = u32::try_from(self.next / crate::disk_pack::PAGES_PER_PART)
            .map_err(|_| corrupt("metadata part exhaustion"))?;
        if self.next % crate::disk_pack::PAGES_PER_PART == 0 {
            self.sync_part()?;
            self.file = Some(
                self.protection
                    .create_new(&self.directory.join(format!("part-{part:08x}")))?,
            );
        }
        let bytes = encode_page(self.object, self.generation, self.next, fields, payload)?;
        let write = crate::commit_timing::stage("metadata_page_write_us");
        self.file
            .as_mut()
            .expect("open metadata part")
            .write_all(&bytes)?;
        drop(write);
        super::page_usage::written(&bytes);
        let number = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| corrupt("page number exhaustion"))?;
        Ok(number)
    }

    fn sync_part(&mut self) -> Result<()> {
        if let Some(file) = self.file.take() {
            let _timing = crate::commit_timing::stage("metadata_part_sync_us");
            durability::sync_dirty_file(&file)?;
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<()> {
        self.sync_part()?;
        let _timing = crate::commit_timing::stage("metadata_directory_sync_us");
        durability::sync_directory(&File::open(&self.directory)?)
    }

    /// Finalize private map headers before any root references them.
    /// No published metadata part is modified by this method.
    pub(super) fn finish_block_map(mut self, logical_bytes: u64, data_parts: u64) -> Result<()> {
        let _timing = crate::commit_timing::stage("metadata_block_map_finish_us");
        drop(self.file.take());
        for part in 0..self.next.div_ceil(crate::disk_pack::PAGES_PER_PART) {
            let part_number = u32::try_from(part).map_err(|_| corrupt("map part exhaustion"))?;
            let path = self.directory.join(format!("part-{part_number:08x}"));
            let mut file = OpenOptions::new().read(true).write(true).open(path)?;
            let pages = (self.next - part * crate::disk_pack::PAGES_PER_PART)
                .min(crate::disk_pack::PAGES_PER_PART);
            if file.metadata()?.len() != pages * PAGE_BYTES as u64 {
                return Err(corrupt("private map part length"));
            }
            let mut bytes = vec![0; PAGE_BYTES];
            for index in 0..pages {
                let position = index * PAGE_BYTES as u64;
                file.seek(SeekFrom::Start(position))?;
                file.read_exact(&mut bytes)?;
                if &bytes[..8] != b"LDPAGE1\0"
                    || bytes[10] != 5
                    || bytes[11] != 0
                    || uuid_at(&bytes, 24) != self.object
                    || u64_at(&bytes, 16) != self.generation
                    || u64_at(&bytes, 40) != part * crate::disk_pack::PAGES_PER_PART + index
                {
                    return Err(corrupt("private block-map identity"));
                }
                verify_checksum(&bytes, 80)?;
                bytes[11] = 1;
                put64(&mut bytes, 112, logical_bytes);
                put64(&mut bytes, 120, data_parts);
                let sum = checksum(&bytes, 80);
                bytes[80..112].copy_from_slice(&sum);
                file.seek(SeekFrom::Start(position))?;
                file.write_all(&bytes)?;
                super::page_usage::map_rewritten(&bytes);
            }
            crate::commit_timing::during("metadata_part_sync_us", || {
                durability::sync_dirty_file(&file)
            })?;
        }
        let _directory_timing = crate::commit_timing::stage("metadata_directory_sync_us");
        durability::sync_directory(&File::open(&self.directory)?)
    }
}

pub(super) fn encode_page(
    object: Uuid,
    generation: u64,
    number: u64,
    fields: PageFields,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let _timing = crate::commit_timing::stage("metadata_page_encode_us");
    if payload.len() > PAGE_PAYLOAD {
        return Err(corrupt("page write payload too large"));
    }
    let mut bytes = vec![0; PAGE_BYTES];
    bytes[..8].copy_from_slice(b"LDPAGE1\0");
    put16(&mut bytes, 8, 1);
    bytes[10] = fields.kind;
    put16(&mut bytes, 12, HEADER_BYTES as u16);
    bytes[14] = fields.level;
    put64(&mut bytes, 16, generation);
    bytes[24..40].copy_from_slice(object.as_bytes());
    put64(&mut bytes, 40, number);
    put32(&mut bytes, 48, payload.len() as u32);
    put16(&mut bytes, 52, fields.count);
    put16(&mut bytes, 54, fields.width);
    put64(&mut bytes, 56, fields.left);
    put64(&mut bytes, 64, fields.next);
    put64(&mut bytes, 72, fields.auxiliary);
    bytes[HEADER_BYTES..HEADER_BYTES + payload.len()].copy_from_slice(payload);
    let sum = checksum(&bytes, 80);
    bytes[80..112].copy_from_slice(&sum);
    Ok(bytes)
}
