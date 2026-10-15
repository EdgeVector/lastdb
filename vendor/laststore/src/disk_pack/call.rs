use super::bytes::u32_at;
use super::page::{Page, PageAddress};
use super::{corrupt, resource, Current, DirectoryEntry, Location, Source, BLOCK_BYTES, KEY_BYTES};
use crate::read_receipt::TemporaryBytes;
use crate::read_receipt_io::{self as measured, ReadClass};
use crate::Result;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

/// Finite limits shared by all work in one read call. Zero is never unlimited.
#[derive(Clone, Copy, Debug)]
pub struct ReadBudget {
    /// Maximum distinct metadata pages read.
    pub pages: u64,
    /// Maximum distinct payload blocks decoded.
    pub blocks: u64,
    /// Maximum cumulative file bytes, decoded bytes, and returned body bytes.
    pub bytes: u64,
    /// Maximum exact keys or range entries examined.
    pub keys: u64,
}

impl ReadBudget {
    /// Check positive limits and the artifact reader's aggregate safety ceilings.
    pub fn validate(self) -> Result<()> {
        if self.pages == 0
            || self.blocks == 0
            || self.bytes == 0
            || self.keys == 0
            || self.pages > 4096
            || self.blocks > 16_384
            || self.bytes > 256 * 1024 * 1024
            || self.keys > 10_000
        {
            return Err(crate::Error::Config(
                "disk-pack v1 budget requires 1..4096 pages, 1..16384 blocks, 1..256 MiB, and 1..10000 keys".into(),
            ));
        }
        Ok(())
    }
}

/// Costs specific to bounded disk-pack reads; ordinary I/O also enters DiskReadReceipt.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadCosts {
    /// Directory tree pages read.
    pub directory_pages: u64,
    /// Locator tree pages read.
    pub locator_pages: u64,
    /// Long-key overflow pages read.
    pub overflow_pages: u64,
    /// Payload block-map pages read.
    pub map_pages: u64,
    /// Distinct independently decoded payload blocks.
    pub payload_blocks: u64,
    /// Metadata page reads avoided by call-local sharing.
    pub shared_pages: u64,
    /// Payload block reads avoided by call-local sharing.
    pub shared_blocks: u64,
    /// Full file extents requested, including CURRENT and part headers.
    pub file_bytes: u64,
    /// Bytes admitted against the aggregate call limit.
    pub budget_bytes: u64,
    /// Exact keys or ordered entries examined, including tombstones.
    pub examined_keys: u64,
}

pub(super) struct Buffer {
    pub(super) bytes: Vec<u8>,
    _charge: TemporaryBytes,
}

impl Buffer {
    pub(super) fn from_vec(bytes: Vec<u8>) -> Self {
        let charge = TemporaryBytes::new(bytes.capacity());
        Self {
            bytes,
            _charge: charge,
        }
    }
}

/// One immutable committed generation with strictly call-local page/block reuse.
/// Dropping this value releases every file buffer and decoded storage object.
pub struct ReadCall {
    pub(super) root: PathBuf,
    pub(super) current: Current,
    pub(super) budget: ReadBudget,
    pub(super) costs: ReadCosts,
    pub(super) pages: HashMap<PageAddress, Arc<Page>>,
    pub(super) blocks: HashMap<(Uuid, u64, u64), Arc<Buffer>>,
    pub(super) parts: HashMap<PathBuf, (u64, u64)>,
}

impl ReadCall {
    /// Capture CURRENT once. Expected codecs come from the held home descriptor.
    /// The caller must retain this generation's files for the entire call.
    pub fn open(
        root: &Path,
        placement_codec: u16,
        key_codec: u16,
        budget: ReadBudget,
    ) -> Result<Self> {
        budget.validate()?;
        let mut file = measured::open(root.join("CURRENT"))?;
        if file.metadata()?.len() != 4096 {
            return Err(corrupt("CURRENT length"));
        }
        if budget.bytes < 4096 {
            return Err(resource());
        }
        let mut bytes = [0; 4096];
        let _class = ReadClass::FormatProbe.enter();
        file.read_exact(&mut bytes)?;
        let current = Current::decode(&bytes)?;
        if current.placement_codec != placement_codec || current.key_codec != key_codec {
            return Err(corrupt("CURRENT codec differs from held home descriptor"));
        }
        Ok(Self {
            root: root.to_path_buf(),
            current,
            budget,
            costs: ReadCosts {
                file_bytes: 4096,
                budget_bytes: 4096,
                ..ReadCosts::default()
            },
            pages: HashMap::new(),
            blocks: HashMap::new(),
            parts: HashMap::new(),
        })
    }

    /// The exact root captured at call start.
    pub fn current(&self) -> &Current {
        &self.current
    }

    /// Costs accumulated so far, including successful work before an error.
    pub fn costs(&self) -> ReadCosts {
        self.costs
    }

    /// Resolve one exact storage key in an explicitly addressed group.
    /// The integration must compute this group with the held placement codec.
    pub fn get(&mut self, collection: &str, group: u16, key: &str) -> Result<Option<Vec<u8>>> {
        self.examine()?;
        if key.len() > KEY_BYTES {
            return Err(corrupt("storage key exceeds format limit"));
        }
        let directory_key = Self::directory_key(collection, group)?;
        let directory = self.current.directory.clone();
        let Some(value) = self.find(&directory, &directory_key)? else {
            return Ok(None);
        };
        let entry = DirectoryEntry::decode(&value)?;
        if entry.group != group || entry.converted_frontier > self.current.frontier {
            return Err(corrupt("directory coordinate or frontier mismatch"));
        }
        let Some(value) = self.find(&entry.locator, key.as_bytes())? else {
            return Ok(None);
        };
        let location = Location::decode(&value)?;
        if location.version.batch > self.current.frontier {
            return Err(corrupt("record newer than committed frontier"));
        }
        if location.source == Source::Tombstone {
            return Ok(None);
        }
        self.read_record(collection, key, &location).map(Some)
    }

    /// Resolve all supplied exact coordinates in order with one shared call cache.
    /// Repeated keys share payload blocks. An error never becomes an absent item.
    pub fn get_many(&mut self, keys: &[(&str, u16, &str)]) -> Result<Vec<Option<Vec<u8>>>> {
        let count = u64::try_from(keys.len()).map_err(|_| resource())?;
        if count > self.budget.keys.saturating_sub(self.costs.examined_keys) {
            return Err(resource());
        }
        keys.iter()
            .map(|(collection, group, key)| self.get(collection, *group, key))
            .collect()
    }

    pub(super) fn directory_key(collection: &str, group: u16) -> Result<Vec<u8>> {
        if collection.len() > KEY_BYTES - 4 || group >= 1024 {
            return Err(corrupt("directory coordinate exceeds format limit"));
        }
        let mut key = Vec::new();
        key.try_reserve_exact(collection.len() + 4)
            .map_err(|_| resource())?;
        key.extend_from_slice(&(collection.len() as u16).to_le_bytes());
        key.extend_from_slice(collection.as_bytes());
        key.extend_from_slice(&group.to_le_bytes());
        Ok(key)
    }

    pub(super) fn charge(&mut self, bytes: u64) -> Result<()> {
        let next = self
            .costs
            .budget_bytes
            .checked_add(bytes)
            .ok_or_else(resource)?;
        if next > self.budget.bytes {
            return Err(resource());
        }
        self.costs.budget_bytes = next;
        Ok(())
    }

    pub(super) fn examine(&mut self) -> Result<()> {
        if self.costs.examined_keys >= self.budget.keys {
            return Err(resource());
        }
        self.costs.examined_keys += 1;
        Ok(())
    }

    pub(super) fn read_extent(
        &mut self,
        path: &Path,
        offset: u64,
        length: usize,
        page_part: bool,
        class: ReadClass,
    ) -> Result<Buffer> {
        self.charge(length as u64)?;
        let mut file = measured::open(path)?;
        let file_bytes = file.metadata()?.len();
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| corrupt("file offset overflow"))?;
        if end > file_bytes
            || file_bytes > super::PART_BYTES
            || (page_part && file_bytes % super::PAGE_BYTES as u64 != 0)
        {
            return Err(corrupt("extent outside immutable part"));
        }
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(|_| resource())?;
        bytes.resize(length, 0);
        let mut buffer = Buffer::from_vec(bytes);
        file.seek(SeekFrom::Start(offset))?;
        let _class = class.enter();
        file.read_exact(&mut buffer.bytes)?;
        self.costs.file_bytes = self.costs.file_bytes.saturating_add(length as u64);
        Ok(buffer)
    }

    pub(super) fn data_path(&self, location: &Location) -> PathBuf {
        match location.source {
            Source::Pack => self.root.join("packs").join(location.object.to_string()),
            Source::Journal => self
                .root
                .join("journal")
                .join(location.object.to_string())
                .join(format!("batch-{:016x}", location.version.batch)),
            Source::Tombstone => unreachable!("tombstone has no data path"),
        }
    }

    pub(super) fn read_block(&mut self, location: &Location, ordinal: u64) -> Result<Arc<Buffer>> {
        let sequence = if location.source == Source::Journal {
            location.version.batch
        } else {
            0
        };
        let identity = (location.object, sequence, ordinal);
        if let Some(block) = self.blocks.get(&identity) {
            self.costs.shared_blocks += 1;
            return Ok(Arc::clone(block));
        }
        if self.costs.payload_blocks >= self.budget.blocks {
            return Err(resource());
        }
        let base = self.data_path(location);
        let map = self.map_page(&base, location.object, ordinal / super::MAP_ENTRIES)?;
        let index = (ordinal % super::MAP_ENTRIES) as usize;
        if index >= map.count() {
            return Err(corrupt("block outside map"));
        }
        let entry = &map.payload()[index * 24..index * 24 + 24];
        let part = u32_at(entry, 0);
        let offset = u32_at(entry, 4);
        let stored = u32_at(entry, 8) as usize;
        let decoded = u32_at(entry, 12) as usize;
        if u32_at(entry, 16) != 0
            || u32_at(entry, 20) != 0
            || offset < 128
            || !(81..=4192).contains(&stored)
            || !(1..=BLOCK_BYTES).contains(&decoded)
        {
            return Err(corrupt("invalid block map extent"));
        }
        let path = base.join(format!("part-{part:08x}"));
        self.verify_part(&path, location, part, ordinal)?;
        let frame = self.read_extent(&path, offset.into(), stored, false, ReadClass::Payload)?;
        self.charge(decoded as u64)?;
        let bytes =
            super::frame::decode(&frame.bytes, location.object, sequence, ordinal, decoded)?;
        let block = Arc::new(Buffer::from_vec(bytes));
        self.costs.payload_blocks += 1;
        self.blocks.insert(identity, Arc::clone(&block));
        Ok(block)
    }
}
