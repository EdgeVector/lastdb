use super::bytes::*;
use super::call::Buffer;
use super::{
    corrupt, resource, ReadCall, TreeKind, HEADER_BYTES, INTERNAL_ENTRIES, INTERNAL_WIDTH,
    LEAF_ENTRIES, LEAF_WIDTH, PAGE_BYTES, PAGE_PAYLOAD,
};
use crate::read_receipt_io::ReadClass;
use crate::Result;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct PageAddress {
    directory: std::path::PathBuf,
    object: Uuid,
    number: u64,
}

pub(super) struct Page {
    buffer: Buffer,
}

impl Page {
    pub(super) fn decode(
        buffer: Buffer,
        object: Uuid,
        number: u64,
        generation: u64,
    ) -> Result<Self> {
        let bytes = &buffer.bytes;
        if bytes.len() != PAGE_BYTES
            || &bytes[..8] != b"LDPAGE1\0"
            || u16_at(bytes, 8) != 1
            || bytes[11] != 0
            || u16_at(bytes, 12) != HEADER_BYTES as u16
            || bytes[15] != 0
            || u64_at(bytes, 16) == 0
            || u64_at(bytes, 16) > generation
            || uuid_at(bytes, 24) != object
            || u64_at(bytes, 40) != number
        {
            return Err(corrupt("page identity or header"));
        }
        verify_checksum(bytes, 80)?;
        zero(&bytes[112..128])?;
        let used = u32_at(bytes, 48) as usize;
        if used > PAGE_PAYLOAD {
            return Err(corrupt("page payload length"));
        }
        zero(&bytes[HEADER_BYTES + used..])?;
        let page = Self { buffer };
        page.validate_shape()?;
        Ok(page)
    }

    fn validate_shape(&self) -> Result<()> {
        let bytes = &self.buffer.bytes;
        let width = u16_at(bytes, 54) as usize;
        let used = self.payload().len();
        match self.kind() {
            1 if (1..8).contains(&self.level())
                && width == INTERNAL_WIDTH
                && (1..=INTERNAL_ENTRIES).contains(&self.count())
                && self.left() != 0
                && self.next() == 0
                && self.auxiliary() == 0 => {}
            2 | 3
                if self.level() == 0
                    && width == LEAF_WIDTH
                    && self.count() <= LEAF_ENTRIES
                    && self.left() == 0
                    && self.next() == 0
                    && self.auxiliary() == 0 => {}
            4 if self.level() == 0
                && width == 0
                && self.count() == 0
                && self.left() == 0
                && used > 0
                && (1..=65_407).contains(&self.auxiliary()) =>
            {
                return Ok(())
            }
            5 if self.level() == 0
                && width == 24
                && self.count() <= super::MAP_ENTRIES as usize
                && self.left() == 0
                && self.next() == 0 => {}
            _ => return Err(corrupt("unsupported page kind or shape")),
        }
        if self.count() * width != used {
            return Err(corrupt("page entry length"));
        }
        if self.kind() == 5 {
            self.validate_map()?;
        }
        if self.kind() == 1 || self.kind() == 2 || self.kind() == 3 {
            for index in 0..self.count() {
                validate_key_descriptor(self.entry(index))?;
                if index > 0 {
                    let previous = self.entry(index - 1);
                    let next = self.entry(index);
                    let common = (u16_at(previous, 4).min(u16_at(next, 4))) as usize;
                    if previous[8..8 + common] > next[8..8 + common] {
                        return Err(corrupt("descending inline key prefixes"));
                    }
                }
                if self.kind() == 1 && u64_at(self.entry(index), super::KEY_DESCRIPTOR) == 0 {
                    return Err(corrupt("zero child page"));
                }
            }
        }
        Ok(())
    }

    fn validate_map(&self) -> Result<()> {
        let mut previous: Option<(u32, u64)> = None;
        for entry in self.payload().chunks_exact(24) {
            let part = u32_at(entry, 0);
            let offset = u32_at(entry, 4);
            let stored = u32_at(entry, 8);
            let decoded = u32_at(entry, 12);
            let end = u64::from(offset) + u64::from(stored);
            if offset < 128
                || !(81..=4192).contains(&stored)
                || !(1..=4096).contains(&decoded)
                || end > super::PART_BYTES
                || u64_at(entry, 16) != 0
                || previous.is_some_and(|(last_part, last_end)| {
                    part < last_part || (part == last_part && u64::from(offset) < last_end)
                })
            {
                return Err(corrupt("overlapping or invalid block map extents"));
            }
            previous = Some((part, end));
        }
        Ok(())
    }

    pub(super) fn kind(&self) -> u8 {
        self.buffer.bytes[10]
    }
    pub(super) fn level(&self) -> u8 {
        self.buffer.bytes[14]
    }
    pub(super) fn count(&self) -> usize {
        u16_at(&self.buffer.bytes, 52) as usize
    }
    pub(super) fn left(&self) -> u64 {
        u64_at(&self.buffer.bytes, 56)
    }
    pub(super) fn next(&self) -> u64 {
        u64_at(&self.buffer.bytes, 64)
    }
    pub(super) fn auxiliary(&self) -> u64 {
        u64_at(&self.buffer.bytes, 72)
    }
    pub(super) fn payload(&self) -> &[u8] {
        &self.buffer.bytes[HEADER_BYTES..HEADER_BYTES + u32_at(&self.buffer.bytes, 48) as usize]
    }
    pub(super) fn entry(&self, index: usize) -> &[u8] {
        let width = u16_at(&self.buffer.bytes, 54) as usize;
        &self.payload()[index * width..(index + 1) * width]
    }
}

pub(super) fn validate_key_descriptor(bytes: &[u8]) -> Result<()> {
    let length = u32_at(bytes, 0) as usize;
    let inline = u16_at(bytes, 4) as usize;
    let overflow = length > 128;
    if length > super::KEY_BYTES
        || inline != length.min(128)
        || u16_at(bytes, 6) != u16::from(overflow)
        || (u64_at(bytes, 136) != 0) != overflow
    {
        return Err(corrupt("key descriptor"));
    }
    zero(&bytes[8 + inline..136])?;
    Ok(())
}

impl ReadCall {
    pub(super) fn tree_page(&mut self, tree: &super::TreeRoot, page: u64) -> Result<Arc<Page>> {
        let directory = self
            .root
            .join(tree.kind.directory())
            .join(tree.object.to_string());
        let value = self.page(&directory, tree.object, page)?;
        match value.kind() {
            1 => {}
            2 if tree.kind == TreeKind::Locator => {}
            3 if tree.kind == TreeKind::Directory => {}
            _ => return Err(corrupt("wrong tree page kind")),
        }
        Ok(value)
    }

    pub(super) fn map_page(&mut self, base: &Path, object: Uuid, number: u64) -> Result<Arc<Page>> {
        let page = self.page(&base.join("map"), object, number)?;
        if page.kind() != 5
            || page.auxiliary()
                != number
                    .checked_mul(super::MAP_ENTRIES)
                    .ok_or_else(|| corrupt("map ordinal overflow"))?
        {
            return Err(corrupt("block map identity"));
        }
        Ok(page)
    }

    pub(super) fn page(
        &mut self,
        directory: &Path,
        object: Uuid,
        number: u64,
    ) -> Result<Arc<Page>> {
        let address = PageAddress {
            directory: directory.to_path_buf(),
            object,
            number,
        };
        if let Some(page) = self.pages.get(&address) {
            self.costs.shared_pages += 1;
            return Ok(Arc::clone(page));
        }
        if self.pages.len() as u64 >= self.budget.pages {
            return Err(resource());
        }
        let part =
            u32::try_from(number / 512).map_err(|_| corrupt("page part exceeds format limit"))?;
        let path = directory.join(format!("part-{part:08x}"));
        let bytes = self.read_extent(
            &path,
            (number % 512) * PAGE_BYTES as u64,
            PAGE_BYTES,
            true,
            ReadClass::FormatProbe,
        )?;
        let page = Arc::new(Page::decode(
            bytes,
            object,
            number,
            self.current.generation,
        )?);
        match page.kind() {
            1 | 2 if directory.starts_with(self.root.join("locator")) => {
                self.costs.locator_pages += 1
            }
            1 | 3 if directory.starts_with(self.root.join("directory")) => {
                self.costs.directory_pages += 1
            }
            4 => self.costs.overflow_pages += 1,
            5 => self.costs.map_pages += 1,
            _ => return Err(corrupt("page kind outside its object namespace")),
        }
        self.pages.insert(address, Arc::clone(&page));
        Ok(page)
    }
}
