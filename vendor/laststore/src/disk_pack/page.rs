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
    pub(super) number: u64,
}

pub(super) struct Page {
    buffer: Buffer,
}

/// One checked immutable allocator template, retained only by one publication.
pub(super) struct EmptyLocator {
    root: std::path::PathBuf,
    tree: super::TreeRoot,
    page: Arc<Page>,
    current: super::Current,
}

impl Page {
    pub(super) fn decode(
        buffer: Buffer,
        object: Uuid,
        number: u64,
        generation: u64,
        features: u64,
    ) -> Result<Self> {
        let _timing = crate::commit_timing::stage("metadata_page_decode_us");
        let bytes = &buffer.bytes;
        if bytes.len() != PAGE_BYTES
            || &bytes[..8] != b"LDPAGE1\0"
            || u16_at(bytes, 8) != 1
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
        if super::packed_page::is_packed(bytes[10]) && features & super::FEATURE_PACKED_KEYS == 0 {
            return Err(corrupt("packed-key page requires CURRENT format feature"));
        }
        let bounded_map =
            bytes[10] == 5 && bytes[11] == 1 && features & super::FEATURE_OBJECT_METADATA != 0;
        if !bounded_map {
            zero(&bytes[112..128])?;
        }
        if bytes[11] != 0 && !bounded_map
            || (bytes[10] == 5 && features & super::FEATURE_OBJECT_METADATA != 0 && !bounded_map)
        {
            return Err(corrupt("unsupported page flags or missing object bounds"));
        }
        let used = u32_at(bytes, 48) as usize;
        if used > PAGE_PAYLOAD {
            return Err(corrupt("page payload length"));
        }
        zero(&bytes[HEADER_BYTES + used..])?;
        let page = Self { buffer };
        page.validate_shape(features)?;
        Ok(page)
    }

    fn validate_shape(&self, features: u64) -> Result<()> {
        let bytes = &self.buffer.bytes;
        let width = u16_at(bytes, 54) as usize;
        let used = self.payload().len();
        match self.kind() {
            super::packed_page::LEAF
                if self.level() == 0
                    && width == 0
                    && self.left() == 0
                    && self.next() == 0
                    && self.auxiliary() == 0 =>
            {
                return super::packed_page::validate(self);
            }
            super::packed_page::INTERNAL
                if (1..8).contains(&self.level())
                    && width == 0
                    && self.count() > 0
                    && self.left() != 0
                    && self.next() == 0
                    && self.auxiliary() == 0 =>
            {
                return super::packed_page::validate(self);
            }
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
                && (1..=(super::key_limits::key_byte_limit(features) - 128) as u64)
                    .contains(&self.auxiliary()) =>
            {
                return Ok(())
            }
            5 if self.level() == 0
                && width == 24
                && self.count() <= super::MAP_ENTRIES as usize
                && self.left() == 0
                && self.next() == 0 => {}
            11 if self.level() == 0
                && width == 80
                && self.count() <= 201
                && self.left() == 0
                && self.auxiliary() > 0
                && used >= 128 =>
            {
                return Ok(())
            }
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
                super::key_layout::validate_fixed(self.entry(index))?;
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
                || !(1..=super::BLOCK_BYTES as u32).contains(&decoded)
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
    pub(super) fn generation(&self) -> u64 {
        u64_at(&self.buffer.bytes, 16)
    }
    pub(super) fn object_bounds(&self) -> Option<(u64, u64)> {
        (self.kind() == 5 && self.buffer.bytes[11] == 1).then(|| {
            (
                u64_at(&self.buffer.bytes, 112),
                u64_at(&self.buffer.bytes, 120),
            )
        })
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
        if super::packed_page::is_packed(self.kind()) {
            return super::packed_page::entry(self.payload(), index);
        }
        let width = u16_at(&self.buffer.bytes, 54) as usize;
        &self.payload()[index * width..(index + 1) * width]
    }
}

impl ReadCall {
    pub(super) fn empty_locator_template(&mut self, object: Uuid) -> Result<Option<EmptyLocator>> {
        let tree = super::TreeRoot {
            object,
            page: 0,
            height: 1,
            kind: TreeKind::Locator,
        };
        let page = self.tree_page(&tree, 0)?;
        if page.level() != 0 || page.count() != 0 {
            return Ok(None);
        }
        Ok(Some(EmptyLocator {
            root: self.root.clone(),
            tree,
            page,
            current: self.current.clone(),
        }))
    }

    /// The selected empty page belongs to every new group's own read budget.
    /// All other pages and validation records remain local to that group's call.
    pub(super) fn reuse_empty_locator(
        &mut self,
        template: &EmptyLocator,
    ) -> Result<super::TreeRoot> {
        if self.root != template.root || self.current != template.current {
            return Err(corrupt(
                "empty locator template belongs to another publication",
            ));
        }
        let address = PageAddress {
            directory: self
                .root
                .join("locator")
                .join(template.tree.object.to_string()),
            object: template.tree.object,
            number: 0,
        };
        if !self.pages.contains_key(&address) {
            if self.pages.len() as u64 >= self.budget.pages {
                return Err(resource());
            }
            self.pages.insert(address, Arc::clone(&template.page));
            crate::commit_timing::count("empty_locator_template_reuses", 1);
        }
        Ok(template.tree.clone())
    }

    pub(super) fn tree_page(&mut self, tree: &super::TreeRoot, page: u64) -> Result<Arc<Page>> {
        let directory = self
            .root
            .join(tree.kind.directory())
            .join(tree.object.to_string());
        let value = self.page(&directory, tree.object, page)?;
        match value.kind() {
            1 if tree.kind == TreeKind::Directory => {}
            super::packed_page::INTERNAL if tree.kind == TreeKind::Locator => {}
            super::packed_page::LEAF if tree.kind == TreeKind::Locator => {}
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
        let part = u32::try_from(number / crate::disk_pack::PAGES_PER_PART)
            .map_err(|_| corrupt("page part exceeds format limit"))?;
        let path = directory.join(format!("part-{part:08x}"));
        let bytes = self.read_extent(
            &path,
            (number % crate::disk_pack::PAGES_PER_PART) * PAGE_BYTES as u64,
            PAGE_BYTES,
            true,
            ReadClass::FormatProbe,
        )?;
        let page = Arc::new(Page::decode(
            bytes,
            object,
            number,
            self.current.generation,
            self.current.required_features,
        )?);
        match page.kind() {
            super::packed_page::LEAF | super::packed_page::INTERNAL
                if directory.starts_with(self.root.join("locator")) =>
            {
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
