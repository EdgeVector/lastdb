use super::bytes::*;
use super::page_write::{directory, PageFields, PageWriter};
use super::{corrupt, Location, MutationVersion, Source, BLOCK_BYTES, MAP_ENTRIES, PART_BYTES};
use crate::{durability, Result};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub(super) struct PackWriter {
    object: Uuid,
    generation: u64,
    directory: PathBuf,
    map: PageWriter,
    map_entries: Vec<u8>,
    block: Vec<u8>,
    ordinal: u64,
    part: u32,
    part_start: u64,
    part_bytes: u32,
    file: Option<File>,
    source: Source,
    sequence: u64,
    logical_bytes: u64,
    physical_bytes: u64,
}

pub(super) struct WrittenData {
    pub(super) object: Uuid,
    pub(super) sequence: u64,
    pub(super) data_parts: u64,
    pub(super) map_pages: u64,
    pub(super) logical_bytes: u64,
    pub(super) physical_bytes: u64,
}

impl PackWriter {
    pub(super) fn new(root: &Path, generation: u64) -> Result<Self> {
        let object = Uuid::new_v4();
        let path = root.join("packs").join(object.to_string());
        Self::at(path, object, generation, Source::Pack, 0)
    }

    pub(super) fn journal(
        root: &Path,
        stream: Uuid,
        sequence: u64,
        generation: u64,
    ) -> Result<Self> {
        let path = root
            .join("journal")
            .join(stream.to_string())
            .join(format!("batch-{sequence:016x}"));
        Self::at(path, stream, generation, Source::Journal, sequence)
    }

    fn at(
        path: PathBuf,
        object: Uuid,
        generation: u64,
        source: Source,
        sequence: u64,
    ) -> Result<Self> {
        directory(&path)?;
        let map = PageWriter::new(path.join("map"), object, generation)?;
        Ok(Self {
            object,
            generation,
            directory: path,
            map,
            map_entries: Vec::new(),
            block: Vec::with_capacity(BLOCK_BYTES),
            ordinal: 0,
            part: 0,
            part_start: 0,
            part_bytes: 128,
            file: None,
            source,
            sequence,
            logical_bytes: 0,
            physical_bytes: 0,
        })
    }

    pub(super) fn put(
        &mut self,
        collection: &str,
        key: &str,
        version: MutationVersion,
        body_bytes: u64,
        body: &mut dyn Read,
    ) -> Result<Location> {
        let header = super::record::record_header(collection, key, body_bytes, version)?;
        let record_bytes = 96u64
            .checked_add(key.len() as u64)
            .and_then(|n| n.checked_add(body_bytes))
            .ok_or_else(|| corrupt("record length overflow"))?;
        let first_block = self.ordinal;
        let first_position = self.block.len() as u32;
        let mut hash = Sha256::new();
        self.append(&header, &mut hash)?;
        self.append(key.as_bytes(), &mut hash)?;
        let mut remaining = body_bytes;
        let mut buffer = [0; 65_536];
        while remaining > 0 {
            let count = remaining.min(buffer.len() as u64) as usize;
            body.read_exact(&mut buffer[..count])?;
            self.append(&buffer[..count], &mut hash)?;
            remaining -= count as u64;
        }
        let blocks = (record_bytes + u64::from(first_position)).div_ceil(BLOCK_BYTES as u64);
        Ok(Location {
            source: self.source,
            version,
            record_bytes,
            object: self.object,
            first_block,
            first_position,
            blocks: u32::try_from(blocks)
                .map_err(|_| corrupt("record block count exceeds format"))?,
            digest: hash.finalize().into(),
        })
    }

    fn append(&mut self, bytes: &[u8], hash: &mut Sha256) -> Result<()> {
        hash.update(bytes);
        self.append_raw(bytes)
    }

    pub(super) fn position(&self) -> u64 {
        self.logical_bytes
    }

    pub(super) fn append_raw(&mut self, mut bytes: &[u8]) -> Result<()> {
        self.logical_bytes = self
            .logical_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| corrupt("data stream length overflow"))?;
        while !bytes.is_empty() {
            let take = bytes.len().min(BLOCK_BYTES - self.block.len());
            self.block.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.block.len() == BLOCK_BYTES {
                self.flush_block()?;
            }
        }
        Ok(())
    }

    fn open_part(&mut self) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.directory.join(format!("part-{:08x}", self.part)))?;
        file.write_all(&[0; 128])?;
        self.file = Some(file);
        self.part_bytes = 128;
        self.part_start = self.ordinal;
        Ok(())
    }

    fn finish_part(&mut self) -> Result<()> {
        let Some(mut file) = self.file.take() else {
            return Ok(());
        };
        let mut header = [0; 128];
        header[..8].copy_from_slice(b"LDPART1\0");
        put16(&mut header, 8, 1);
        header[10] = if self.source == Source::Pack { 1 } else { 2 };
        put32(&mut header, 12, 128);
        header[16..32].copy_from_slice(self.object.as_bytes());
        put64(&mut header, 32, self.sequence);
        put32(&mut header, 40, self.part);
        put32(&mut header, 44, self.part_bytes);
        put64(&mut header, 48, self.part_start);
        put64(&mut header, 56, self.ordinal - self.part_start);
        put64(&mut header, 72, self.generation);
        let sum = checksum(&header, 80);
        header[80..112].copy_from_slice(&sum);
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header)?;
        durability::sync_dirty_file(&file)?;
        self.physical_bytes = self
            .physical_bytes
            .checked_add(u64::from(self.part_bytes))
            .ok_or_else(|| corrupt("physical byte count overflow"))?;
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let frame = super::frame::encode(&self.block, self.object, self.sequence, self.ordinal)?;
        if u64::from(self.part_bytes) + frame.len() as u64 > PART_BYTES {
            self.finish_part()?;
            self.part = self
                .part
                .checked_add(1)
                .ok_or_else(|| corrupt("pack part exhaustion"))?;
        }
        if self.file.is_none() {
            self.open_part()?;
        }
        self.file
            .as_mut()
            .expect("open pack part")
            .write_all(&frame)?;
        self.map_entries.extend_from_slice(&self.part.to_le_bytes());
        self.map_entries
            .extend_from_slice(&self.part_bytes.to_le_bytes());
        self.map_entries
            .extend_from_slice(&(frame.len() as u32).to_le_bytes());
        self.map_entries
            .extend_from_slice(&(self.block.len() as u32).to_le_bytes());
        self.map_entries.extend_from_slice(&[0; 8]);
        self.part_bytes += frame.len() as u32;
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or_else(|| corrupt("block ordinal exhaustion"))?;
        self.block.clear();
        if self.map_entries.len() / 24 == MAP_ENTRIES as usize {
            self.flush_map()?;
        }
        Ok(())
    }

    fn flush_map(&mut self) -> Result<()> {
        if self.map_entries.is_empty() {
            return Ok(());
        }
        let first = self
            .map
            .next_page()
            .checked_mul(MAP_ENTRIES)
            .ok_or_else(|| corrupt("block map ordinal exhaustion"))?;
        self.map.append(
            PageFields {
                kind: 5,
                level: 0,
                count: (self.map_entries.len() / 24) as u16,
                width: 24,
                left: 0,
                next: 0,
                auxiliary: first,
            },
            &self.map_entries,
        )?;
        self.map_entries.clear();
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<WrittenData> {
        self.flush_block()?;
        self.flush_map()?;
        self.finish_part()?;
        let map_bytes = self
            .map
            .next_page()
            .checked_mul(super::PAGE_BYTES as u64)
            .ok_or_else(|| corrupt("map byte count overflow"))?;
        let map_pages = self.map.next_page();
        let data_parts = if self.ordinal == 0 {
            0
        } else {
            u64::from(self.part) + 1
        };
        self.map.finish_block_map(self.logical_bytes, data_parts)?;
        durability::sync_directory(&File::open(&self.directory)?)?;
        Ok(WrittenData {
            object: self.object,
            sequence: self.sequence,
            data_parts,
            map_pages,
            logical_bytes: self.logical_bytes,
            physical_bytes: self
                .physical_bytes
                .checked_add(map_bytes)
                .ok_or_else(|| corrupt("data byte count overflow"))?,
        })
    }
}
