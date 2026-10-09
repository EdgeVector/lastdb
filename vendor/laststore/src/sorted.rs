//! Immutable sorted records in independently readable 4 KiB blocks.
#![allow(dead_code)] // LastStore write-path wiring is a follow-up card.
//!
//! The footer holds physical block locations and sparse key fences. A record
//! can cross block boundaries. Keys-only cursors skip its body by logical
//! offset, including large bodies; they never decode the intervening blocks.
//! Only a cursor owns a transient decoded block. A resident segment owns no
//! record bodies or decoded blocks.

use crate::frame::{self, FrameHeader};
use crate::sorted_file;
use crate::{segfmt, Error, Result};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"LSSORT1\0";
const INDEX_MAGIC: &[u8; 8] = b"LSBIDX1\0";
const END_MAGIC: &[u8; 8] = b"LSSEND1\0";
const HEADER_LEN: usize = 52;
const TRAILER_LEN: usize = 32;
pub(crate) const BLOCK_BYTES: usize = 4096;
const MAX_FOOTER_BYTES: u64 = 64 * 1024 * 1024;

fn corrupt(message: &str) -> Error {
    Error::Corrupt(format!("sorted segment: {message}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Header {
    pub chunk_uuid: Uuid,
    pub shard: u16,
    pub end_csn: u64,
    pub encrypted: bool,
    /// This file is a complete group snapshot. Recovery can ignore all older
    /// files, including files a crash left between publication and unlink.
    pub complete: bool,
    /// Exact group residue after this seal, before any newer tail writes.
    pub group_live_bytes: u64,
    pub group_dead_bytes: u64,
}

impl Header {
    fn bytes(self) -> [u8; HEADER_LEN] {
        let mut bytes = [0; HEADER_LEN];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8] = u8::from(self.encrypted);
        bytes[9] = u8::from(self.complete);
        bytes[10..12].copy_from_slice(&self.shard.to_le_bytes());
        bytes[12..28].copy_from_slice(self.chunk_uuid.as_bytes());
        bytes[28..36].copy_from_slice(&self.end_csn.to_le_bytes());
        bytes[36..44].copy_from_slice(&self.group_live_bytes.to_le_bytes());
        bytes[44..52].copy_from_slice(&self.group_dead_bytes.to_le_bytes());
        bytes
    }

    fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != HEADER_LEN || &bytes[..8] != MAGIC || bytes[8] > 1 || bytes[9] > 1 {
            return Err(corrupt("invalid header"));
        }
        Ok(Self {
            chunk_uuid: Uuid::from_slice(&bytes[12..28]).map_err(|_| corrupt("invalid UUID"))?,
            shard: u16::from_le_bytes(bytes[10..12].try_into().unwrap()),
            end_csn: u64::from_le_bytes(bytes[28..36].try_into().unwrap()),
            encrypted: bytes[8] == 1,
            complete: bytes[9] == 1,
            group_live_bytes: u64::from_le_bytes(bytes[36..44].try_into().unwrap()),
            group_dead_bytes: u64::from_le_bytes(bytes[44..52].try_into().unwrap()),
        })
    }

    fn frame(self, counter: u64) -> FrameHeader {
        FrameHeader {
            chunk_uuid: self.chunk_uuid,
            shard: self.shard,
            start_csn: self.end_csn,
            counter,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Record {
    /// None is a tombstone. It must hide older segments until a complete merge.
    pub body: Option<Vec<u8>>,
}

/// A rewrite source exposes keys and body lengths before any body bytes.
/// The writer requests at most one block at a time, even for large records.
pub(crate) trait RecordSource {
    fn next_record(&mut self) -> Result<Option<RecordHead>>;
    fn read_body(&mut self, destination: &mut [u8]) -> Result<()>;
}

pub(crate) struct RecordHead {
    pub key: String,
    pub body_len: Option<usize>,
}

/// A large append streams the caller's existing bytes without a second body buffer.
pub(crate) struct BorrowedRecord<'a> {
    pub head: Option<RecordHead>,
    pub body: &'a [u8],
}

impl RecordSource for BorrowedRecord<'_> {
    fn next_record(&mut self) -> Result<Option<RecordHead>> {
        Ok(self.head.take())
    }

    fn read_body(&mut self, destination: &mut [u8]) -> Result<()> {
        self.body.read_exact(destination)?;
        Ok(())
    }
}

#[derive(Debug)]
struct Block {
    offset: u64,
    encoded_len: u32,
    crc: u32,
}

#[derive(Debug)]
struct Fence {
    first_record: u64,
    upper: Vec<u8>,
}

struct FenceRef<'a> {
    first_record: u64,
    upper: &'a [u8],
}

#[derive(Debug)]
pub(crate) struct Segment {
    path: PathBuf,
    pub header: Header,
    logical_len: u64,
    pub records: u64,
    /// Keep the authenticated footer in its packed representation. Blocks
    /// have fixed-width entries; fences need only offsets for binary search.
    /// In particular, no resident allocation is made for an individual key.
    footer: Vec<u8>,
    block_offset: usize,
    fence_offsets: Vec<u32>,
}

/// Return a boundary >= the left key and < the right key. A shortened boundary
/// need not be UTF-8; key navigation compares bytes.
fn separator(left: &[u8], right: &[u8]) -> Vec<u8> {
    let common = left.iter().zip(right).take_while(|(a, b)| a == b).count();
    if common < left.len() && common < right.len() && left[common] < u8::MAX {
        let next = left[common] + 1;
        if next < right[common] {
            let mut value = left[..=common].to_vec();
            value[common] = next;
            return value;
        }
    }
    left.to_vec()
}

fn encode_payload(
    header: Header,
    key: Option<&[u8; 32]>,
    counter: u64,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    if header.encrypted {
        frame::encode_frame(
            key.ok_or_else(|| corrupt("missing data key"))?,
            header.frame(counter),
            bytes,
        )
    } else {
        Ok(bytes.to_vec())
    }
}

fn decode_payload(
    header: Header,
    key: Option<&[u8; 32]>,
    counter: u64,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    if header.encrypted {
        let decoded = frame::decode_frame_bounded(
            key.ok_or_else(|| corrupt("missing data key"))?,
            bytes,
            BLOCK_BYTES,
        )?;
        if decoded.header != header.frame(counter) {
            return Err(corrupt("frame identity differs from its segment"));
        }
        Ok(decoded.payload)
    } else {
        Ok(bytes.to_vec())
    }
}

struct Writer<'a> {
    file: sorted_file::Output,
    header: Header,
    key: Option<&'a [u8; 32]>,
    block: Vec<u8>,
    blocks: Vec<Block>,
    logical_len: u64,
    footer_bytes: usize,
}

impl Writer<'_> {
    fn charge_footer(&mut self, bytes: usize) -> Result<()> {
        self.footer_bytes = self
            .footer_bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes as u64 <= MAX_FOOTER_BYTES)
            .ok_or_else(|| corrupt("footer exceeds format limit"))?;
        Ok(())
    }
    fn push(&mut self, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            let take = (BLOCK_BYTES - self.block.len()).min(bytes.len());
            self.block.extend_from_slice(&bytes[..take]);
            self.logical_len = self
                .logical_len
                .checked_add(take as u64)
                .ok_or_else(|| corrupt("logical length overflow"))?;
            bytes = &bytes[take..];
            if self.block.len() == BLOCK_BYTES {
                self.flush_block()?;
            }
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        self.charge_footer(16)?;
        let encoded = encode_payload(self.header, self.key, self.blocks.len() as u64, &self.block)?;
        let offset = self.file.position();
        self.file.write_all(&encoded)?;
        self.blocks.push(Block {
            offset,
            encoded_len: encoded.len() as u32,
            crc: crc32fast::hash(&encoded),
        });
        self.block.clear();
        Ok(())
    }
}

impl Segment {
    /// Publish an already validated staged append without a trailing fallible
    /// sync. The caller records its inverse before it syncs the directory.
    pub(crate) fn publish_as(&mut self, destination: PathBuf) -> Result<PathBuf> {
        std::fs::hard_link(&self.path, &destination)?;
        Ok(std::mem::replace(&mut self.path, destination))
    }

    pub(crate) fn recognizes(path: &Path) -> Result<bool> {
        let mut file = sorted_file::Input::open(path)?;
        let mut magic = [0; 8];
        if file.len() < magic.len() as u64 {
            return Ok(false);
        }
        file.read_exact(&mut magic)?;
        Ok(&magic == MAGIC)
    }

    /// Identify an incoming numbered sorted unit before packaging dispatch.
    /// Authentication and complete format validation still occur on install.
    pub(crate) fn recognizes_bytes(bytes: &[u8]) -> bool {
        bytes.starts_with(MAGIC) || sorted_file::is_manifest(bytes)
    }

    pub(crate) fn write_from(
        path: &Path,
        header: Header,
        key: Option<&[u8; 32]>,
        records: &mut impl RecordSource,
    ) -> Result<Self> {
        if path.exists() {
            return Err(corrupt("destination already exists"));
        }
        if header.encrypted != key.is_some() {
            return Err(corrupt("packaging and data key differ"));
        }
        {
            let mut writer = Writer {
                file: sorted_file::Output::new(path, header.chunk_uuid)?,
                header,
                key,
                block: Vec::with_capacity(BLOCK_BYTES),
                blocks: Vec::new(),
                logical_len: 0,
                footer_bytes: INDEX_MAGIC.len() + HEADER_LEN + 24,
            };
            writer.file.write_all(&header.bytes())?;
            let mut fences: Vec<Fence> = Vec::new();
            let mut previous: Option<String> = None;
            let mut last_start_block = None;
            let mut count = 0u64;
            let mut scratch = [0; BLOCK_BYTES];
            while let Some(record) = records.next_record()? {
                if record.key.len() > segfmt::MAX_ID_LEN
                    || record
                        .body_len
                        .is_some_and(|length| length > segfmt::MAX_BODY_LEN)
                {
                    return Err(corrupt("record exceeds the existing key or body limit"));
                }
                if previous
                    .as_ref()
                    .is_some_and(|p| p.as_bytes() >= record.key.as_bytes())
                {
                    return Err(corrupt("records are not in unique ascending key order"));
                }
                let start_block = writer.logical_len / BLOCK_BYTES as u64;
                if last_start_block != Some(start_block) {
                    if let Some(last) = fences.last_mut() {
                        last.upper =
                            separator(previous.as_ref().unwrap().as_bytes(), record.key.as_bytes());
                        writer.charge_footer(last.upper.len())?;
                    }
                    writer.charge_footer(12)?;
                    fences.push(Fence {
                        first_record: writer.logical_len,
                        upper: Vec::new(),
                    });
                    last_start_block = Some(start_block);
                }
                let opcode = if record.body_len.is_some() {
                    segfmt::OP_PUT
                } else {
                    segfmt::OP_DEL
                };
                writer.push(&[opcode])?;
                writer.push(&(record.key.len() as u16).to_le_bytes())?;
                writer.push(record.key.as_bytes())?;
                if let Some(length) = record.body_len {
                    writer.push(&(length as u32).to_le_bytes())?;
                    let mut remaining = length;
                    while remaining > 0 {
                        let take = remaining.min(BLOCK_BYTES);
                        records.read_body(&mut scratch[..take])?;
                        writer.push(&scratch[..take])?;
                        remaining -= take;
                    }
                }
                previous = Some(record.key);
                count = count
                    .checked_add(1)
                    .ok_or_else(|| corrupt("record count overflow"))?;
            }
            if let Some(last) = fences.last_mut() {
                last.upper = previous.unwrap().into_bytes();
                writer.charge_footer(last.upper.len())?;
            }
            writer.flush_block()?;
            let mut footer = Vec::new();
            footer.extend_from_slice(INDEX_MAGIC);
            footer.extend_from_slice(&header.bytes());
            footer.extend_from_slice(&writer.logical_len.to_le_bytes());
            footer.extend_from_slice(&count.to_le_bytes());
            let block_count =
                u32::try_from(writer.blocks.len()).map_err(|_| corrupt("too many blocks"))?;
            let fence_count =
                u32::try_from(fences.len()).map_err(|_| corrupt("too many fences"))?;
            footer.extend_from_slice(&block_count.to_le_bytes());
            footer.extend_from_slice(&fence_count.to_le_bytes());
            for block in &writer.blocks {
                footer.extend_from_slice(&block.offset.to_le_bytes());
                footer.extend_from_slice(&block.encoded_len.to_le_bytes());
                footer.extend_from_slice(&block.crc.to_le_bytes());
            }
            for fence in &fences {
                footer.extend_from_slice(&fence.first_record.to_le_bytes());
                footer.extend_from_slice(&(fence.upper.len() as u32).to_le_bytes());
                footer.extend_from_slice(&fence.upper);
            }
            if footer.len() as u64 > MAX_FOOTER_BYTES {
                return Err(corrupt("footer exceeds format limit"));
            }
            let footer = encode_payload(header, key, writer.blocks.len() as u64, &footer)?;
            let offset = writer.file.position();
            writer.file.write_all(&footer)?;
            let mut trailer = [0; TRAILER_LEN];
            trailer[..8].copy_from_slice(END_MAGIC);
            trailer[8..16].copy_from_slice(&offset.to_le_bytes());
            trailer[16..24].copy_from_slice(&(footer.len() as u64).to_le_bytes());
            trailer[24..28].copy_from_slice(&crc32fast::hash(&footer).to_le_bytes());
            writer.file.write_all(&trailer)?;
            writer.file.publish()?;
            Self::open(path, key)
        }
    }

    pub(crate) fn open(path: &Path, key: Option<&[u8; 32]>) -> Result<Self> {
        let mut file = sorted_file::Input::open(path)?;
        let file_len = file.len();
        if file_len < (HEADER_LEN + TRAILER_LEN) as u64 {
            return Err(corrupt("short file"));
        }
        let mut raw_header = [0; HEADER_LEN];
        file.read_exact(&mut raw_header)?;
        let header = Header::parse(&raw_header)?;
        if file.uuid().is_some_and(|uuid| uuid != header.chunk_uuid) {
            return Err(corrupt("manifest and segment UUID differ"));
        }
        if header.encrypted != key.is_some() {
            return Err(corrupt("packaging and data key differ"));
        }
        file.seek(SeekFrom::End(-(TRAILER_LEN as i64)))?;
        let mut trailer = [0; TRAILER_LEN];
        file.read_exact(&mut trailer)?;
        if &trailer[..8] != END_MAGIC || trailer[28..] != [0; 4] {
            return Err(corrupt("invalid trailer"));
        }
        let offset = u64::from_le_bytes(trailer[8..16].try_into().unwrap());
        let length = u64::from_le_bytes(trailer[16..24].try_into().unwrap());
        if offset < HEADER_LEN as u64
            || length > MAX_FOOTER_BYTES + 128
            || offset
                .checked_add(length)
                .and_then(|n| n.checked_add(TRAILER_LEN as u64))
                != Some(file_len)
        {
            return Err(corrupt("footer bounds are invalid"));
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut encoded = vec![0; length as usize];
        file.read_exact(&mut encoded)?;
        if crc32fast::hash(&encoded) != u32::from_le_bytes(trailer[24..28].try_into().unwrap()) {
            return Err(corrupt("footer checksum mismatch"));
        }
        // The footer's frame counter is self-describing. Check it against the
        // parsed block count below, after authentication and bounded decode.
        let (footer, footer_counter) = if header.encrypted {
            let decoded =
                frame::decode_frame_bounded(key.unwrap(), &encoded, MAX_FOOTER_BYTES as usize)?;
            if decoded.header.chunk_uuid != header.chunk_uuid
                || decoded.header.shard != header.shard
                || decoded.header.start_csn != header.end_csn
            {
                return Err(corrupt("footer frame identity mismatch"));
            }
            (decoded.payload, Some(decoded.header.counter))
        } else {
            (encoded, None)
        };
        let mut decoder = Decoder(&footer);
        if decoder.take(8)? != INDEX_MAGIC || decoder.take(HEADER_LEN)? != raw_header {
            return Err(corrupt("footer header mismatch"));
        }
        let logical_len = decoder.u64()?;
        let records = decoder.u64()?;
        let block_count = decoder.u32()? as usize;
        let fence_count = decoder.u32()? as usize;
        if block_count as u64 != logical_len.div_ceil(BLOCK_BYTES as u64)
            || footer_counter.is_some_and(|counter| counter != block_count as u64)
            || block_count > decoder.0.len() / 16
            || fence_count > block_count
        {
            return Err(corrupt("invalid footer counts"));
        }
        let block_offset = footer.len() - decoder.0.len();
        let mut next_offset = HEADER_LEN as u64;
        for i in 0..block_count {
            let block = Block {
                offset: decoder.u64()?,
                encoded_len: decoder.u32()?,
                crc: decoder.u32()?,
            };
            let plain_len = (logical_len - i as u64 * BLOCK_BYTES as u64).min(BLOCK_BYTES as u64);
            if block.offset != next_offset
                || block.encoded_len == 0
                || block.encoded_len as usize > BLOCK_BYTES + 128
                || (!header.encrypted && block.encoded_len as u64 != plain_len)
            {
                return Err(corrupt("invalid block extent"));
            }
            next_offset = next_offset
                .checked_add(block.encoded_len as u64)
                .ok_or_else(|| corrupt("block extent overflow"))?;
            if next_offset > offset {
                return Err(corrupt("block overlaps footer"));
            }
        }
        if next_offset != offset {
            return Err(corrupt("gap before footer"));
        }
        let mut fence_offsets = Vec::with_capacity(fence_count);
        let mut previous: Option<FenceRef<'_>> = None;
        for _ in 0..fence_count {
            // MAX_FOOTER_BYTES bounds this offset well below u32::MAX.
            let fence_offset = (footer.len() - decoder.0.len()) as u32;
            let first_record = decoder.u64()?;
            let key_len = decoder.u32()? as usize;
            if key_len > segfmt::MAX_ID_LEN {
                return Err(corrupt("fence key exceeds limit"));
            }
            let upper = decoder.take(key_len)?;
            if first_record >= logical_len
                || previous.as_ref().is_some_and(|last| {
                    last.first_record / BLOCK_BYTES as u64 >= first_record / BLOCK_BYTES as u64
                        || last.upper >= upper
                })
                || (previous.is_none() && first_record != 0)
            {
                return Err(corrupt("invalid fence order"));
            }
            previous = Some(FenceRef {
                first_record,
                upper,
            });
            fence_offsets.push(fence_offset);
        }
        if !decoder.0.is_empty()
            || (records == 0) != (logical_len == 0)
            || records > logical_len / 3
            || (records > 0 && (fence_offsets.is_empty() || records < fence_count as u64))
        {
            return Err(corrupt("invalid record boundaries"));
        }
        Ok(Self {
            path: path.to_path_buf(),
            header,
            logical_len,
            records,
            footer,
            block_offset,
            fence_offsets,
        })
    }

    pub(crate) fn resident_bytes(&self) -> u64 {
        (std::mem::size_of::<Self>()
            + self.path.as_os_str().len()
            + self.footer.capacity()
            + self.fence_offsets.capacity() * std::mem::size_of::<u32>()) as u64
    }

    fn block(&self, index: usize) -> Block {
        let offset = self.block_offset + index * 16;
        let bytes = &self.footer[offset..offset + 16];
        Block {
            offset: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            encoded_len: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            crc: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
        }
    }

    fn fence_at(&self, offset: u32) -> FenceRef<'_> {
        let bytes = &self.footer[offset as usize..];
        let first_record = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let key_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        FenceRef {
            first_record,
            upper: &bytes[12..12 + key_len],
        }
    }

    fn fence(&self, index: usize) -> Option<FenceRef<'_>> {
        self.fence_offsets
            .get(index)
            .map(|&offset| self.fence_at(offset))
    }

    pub(crate) fn record_bytes(&self) -> u64 {
        self.logical_len
    }

    pub(crate) fn cursor<'a>(
        &'a self,
        key: Option<&'a [u8; 32]>,
        start: &[u8],
    ) -> Result<Cursor<'a>> {
        let fence = self
            .fence_offsets
            .partition_point(|&offset| self.fence_at(offset).upper < start);
        let offset = self
            .fence(fence)
            .map_or(self.logical_len, |f| f.first_record);
        Ok(Cursor {
            segment: self,
            key,
            file: sorted_file::Input::open(&self.path)?,
            offset,
            cached: None,
            data_blocks_read: 0,
        })
    }

    pub(crate) fn get(&self, key: Option<&[u8; 32]>, id: &str) -> Result<Option<Record>> {
        let mut cursor = self.cursor(key, id.as_bytes())?;
        while let Some(record) = cursor.next_key()? {
            match record.key.as_bytes().cmp(id.as_bytes()) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => {
                    return Ok(Some(Record {
                        body: record
                            .body
                            .map(|(offset, length)| cursor.read_bytes(offset, length))
                            .transpose()?,
                    }))
                }
            }
        }
        Ok(None)
    }

    /// Probe a key without reading its body, including a tombstone when present.
    pub(crate) fn find(&self, key: Option<&[u8; 32]>, id: &str) -> Result<Option<KeyRecord>> {
        let mut cursor = self.cursor(key, id.as_bytes())?;
        while let Some(record) = cursor.next_key()? {
            match record.key.as_str().cmp(id) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => return Ok(Some(record)),
            }
        }
        Ok(None)
    }

    /// Explicit maintenance proof of all records and blocks. Normal point and
    /// range reads deliberately do not run this pass. Bodies cross one fixed
    /// scratch block; no document body or complete key population is retained.
    pub(crate) fn verify(&self, key: Option<&[u8; 32]>) -> Result<()> {
        let mut cursor = self.cursor(key, b"")?;
        let mut previous: Option<String> = None;
        let mut count = 0u64;
        let mut fence = 0usize;
        let mut previous_start_block = None;
        let mut scratch = [0; BLOCK_BYTES];
        loop {
            let start = cursor.offset;
            let Some(record) = cursor.next_key()? else {
                break;
            };
            if previous
                .as_ref()
                .is_some_and(|previous| previous >= &record.key)
            {
                return Err(corrupt("record keys are not ascending"));
            }
            let start_block = start / BLOCK_BYTES as u64;
            if previous_start_block != Some(start_block) {
                if previous_start_block.is_some() {
                    if self.fence(fence).unwrap().upper >= record.key.as_bytes() {
                        return Err(corrupt("fence overlaps the next key block"));
                    }
                    fence += 1;
                }
                if self
                    .fence(fence)
                    .map_or(true, |fence| fence.first_record != start)
                {
                    return Err(corrupt("fence does not address its first record"));
                }
                previous_start_block = Some(start_block);
            }
            if record.key.as_bytes() > self.fence(fence).unwrap().upper {
                return Err(corrupt("fence excludes a key in its block"));
            }
            if let Some((mut offset, mut remaining)) = record.body {
                while remaining > 0 {
                    let take = remaining.min(BLOCK_BYTES);
                    cursor.read_into(offset, &mut scratch[..take])?;
                    offset += take as u64;
                    remaining -= take;
                }
            }
            previous = Some(record.key);
            count += 1;
        }
        if count != self.records || (count > 0 && fence + 1 != self.fence_offsets.len()) {
            return Err(corrupt("record or fence count differs from the footer"));
        }
        Ok(())
    }
}

struct Decoder<'a>(&'a [u8]);
impl<'a> Decoder<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        if length > self.0.len() {
            return Err(corrupt("short footer"));
        }
        let (head, tail) = self.0.split_at(length);
        self.0 = tail;
        Ok(head)
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

pub(crate) struct KeyRecord {
    pub key: String,
    /// Logical body position and length. None is a tombstone.
    pub body: Option<(u64, usize)>,
}

pub(crate) struct Cursor<'a> {
    segment: &'a Segment,
    key: Option<&'a [u8; 32]>,
    file: sorted_file::Input,
    offset: u64,
    cached: Option<(usize, Vec<u8>)>,
    pub data_blocks_read: u64,
}

impl Cursor<'_> {
    pub(crate) fn read_into(&mut self, mut offset: u64, mut destination: &mut [u8]) -> Result<()> {
        if offset
            .checked_add(destination.len() as u64)
            .map_or(true, |end| end > self.segment.logical_len)
        {
            return Err(corrupt("record exceeds logical data bounds"));
        }
        while !destination.is_empty() {
            let block_index = (offset / BLOCK_BYTES as u64) as usize;
            if self
                .cached
                .as_ref()
                .map_or(true, |(index, _)| *index != block_index)
            {
                let block = self.segment.block(block_index);
                self.file.seek(SeekFrom::Start(block.offset))?;
                let mut encoded = vec![0; block.encoded_len as usize];
                self.file.read_exact(&mut encoded)?;
                if crc32fast::hash(&encoded) != block.crc {
                    return Err(corrupt("block checksum mismatch"));
                }
                let decoded =
                    decode_payload(self.segment.header, self.key, block_index as u64, &encoded)?;
                let expected = (self.segment.logical_len - block_index as u64 * BLOCK_BYTES as u64)
                    .min(BLOCK_BYTES as u64);
                if decoded.len() as u64 != expected {
                    return Err(corrupt("decoded block length mismatch"));
                }
                self.cached = Some((block_index, decoded));
                self.data_blocks_read += 1;
            }
            let bytes = &self.cached.as_ref().unwrap().1;
            let start = (offset % BLOCK_BYTES as u64) as usize;
            let count = (bytes.len() - start).min(destination.len());
            destination[..count].copy_from_slice(&bytes[start..start + count]);
            destination = &mut destination[count..];
            offset += count as u64;
        }
        Ok(())
    }

    pub(crate) fn read_bytes(&mut self, offset: u64, length: usize) -> Result<Vec<u8>> {
        if offset
            .checked_add(length as u64)
            .map_or(true, |end| end > self.segment.logical_len)
        {
            return Err(corrupt("record exceeds logical data bounds"));
        }
        let mut bytes = vec![0; length];
        self.read_into(offset, &mut bytes)?;
        Ok(bytes)
    }

    pub(crate) fn next_key(&mut self) -> Result<Option<KeyRecord>> {
        if self.offset == self.segment.logical_len {
            return Ok(None);
        }
        let mut header = [0; 3];
        self.read_into(self.offset, &mut header)?;
        let key_len = u16::from_le_bytes(header[1..].try_into().unwrap()) as usize;
        let key_start = self.offset + 3;
        let key = String::from_utf8(self.read_bytes(key_start, key_len)?)
            .map_err(|_| corrupt("record key is not UTF-8"))?;
        let mut next = key_start + key_len as u64;
        let body = match header[0] {
            segfmt::OP_DEL => None,
            segfmt::OP_PUT => {
                let mut length = [0; 4];
                self.read_into(next, &mut length)?;
                let length = u32::from_le_bytes(length) as usize;
                next += 4;
                let offset = next;
                next = next
                    .checked_add(length as u64)
                    .ok_or_else(|| corrupt("record length overflow"))?;
                if next > self.segment.logical_len {
                    return Err(corrupt("record body exceeds segment"));
                }
                Some((offset, length))
            }
            _ => return Err(corrupt("invalid record opcode")),
        };
        self.offset = next;
        Ok(Some(KeyRecord { key, body }))
    }
}
