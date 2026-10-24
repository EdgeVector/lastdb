use super::page::{Page, PageAddress};
use super::{corrupt, resource, Current, DirectoryEntry, Location, Source};
use crate::read_receipt::TemporaryBytes;
use crate::read_receipt_io::{self as measured, ReadClass};
use crate::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
    /// Bytes from complete successful extents, including CURRENT and part headers.
    pub file_bytes: u64,
    /// Bytes admitted against ordinary limits or a separate exact-record copy allowance.
    pub budget_bytes: u64,
    /// Exact keys or ordered entries examined, including tombstones.
    pub examined_keys: u64,
    /// Largest descriptor pool within this read call; at most 32.
    pub file_handles_peak: u64,
    /// Extent reads that reuse a descriptor already owned by this call.
    pub file_handle_reuses: u64,
    /// Bounded physical payload reads after adjacent-frame coalescing.
    pub batch_read_runs: u64,
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
    pub(super) blocks: HashMap<super::block_plan::BlockId, Arc<Buffer>>,
    pub(super) parts: HashMap<PathBuf, (u64, u64)>,
    pub(super) files: super::call_io::CallFiles,
    pub(super) _generation_hold: Option<super::lifetime::GenerationHold>,
    pub(super) payload_class: ReadClass,
    pub(super) native_descriptor: Option<super::home::HomeDescriptor>,
}

impl ReadCall {
    pub(super) fn from_writer(
        root: &Path,
        current: Current,
        descriptor: super::home::HomeDescriptor,
        budget: ReadBudget,
        lease: &super::lifetime::WriterReadLease,
    ) -> Result<Self> {
        budget.validate_for_features(current.required_features)?;
        Self::from_writer_budget(root, current, descriptor, budget, lease)
    }

    pub(super) fn from_write_preparation(
        root: &Path,
        current: Current,
        descriptor: super::home::HomeDescriptor,
        lease: &super::lifetime::WriterReadLease,
    ) -> Result<Self> {
        let budget = ReadBudget {
            pages: ReadBudget::for_current(&current).pages,
            blocks: lastdb_storage_policy::native_read::EAGER_BLOCKS,
            bytes: lastdb_storage_policy::native_read::EAGER_WORK_BYTES,
            keys: 40_000,
        };
        Self::from_writer_budget(root, current, descriptor, budget, lease)
    }

    // Only the ordinary validated budget and the fixed preparation budget enter here.
    fn from_writer_budget(
        root: &Path,
        current: Current,
        descriptor: super::home::HomeDescriptor,
        budget: ReadBudget,
        lease: &super::lifetime::WriterReadLease,
    ) -> Result<Self> {
        current.encode()?;
        descriptor.check_current(&current)?;
        let hold = super::lifetime::GenerationHold::from_writer(lease, root, &current)?;
        Ok(Self {
            root: root.to_path_buf(),
            current,
            budget,
            costs: ReadCosts::default(),
            pages: HashMap::new(),
            blocks: HashMap::new(),
            parts: HashMap::new(),
            files: super::call_io::CallFiles::default(),
            payload_class: ReadClass::Payload,
            native_descriptor: Some(descriptor),
            _generation_hold: Some(hold),
        })
    }

    pub(super) fn from_committed(
        root: &Path,
        current: Current,
        descriptor: super::home::HomeDescriptor,
        budget: ReadBudget,
    ) -> Result<Self> {
        budget.validate_for_features(current.required_features)?;
        current.encode()?;
        descriptor.check_current(&current)?;
        if budget.bytes < 4352 {
            return Err(resource());
        }
        let hold = super::lifetime::GenerationHold::acquire(root, &current)?;
        Ok(Self {
            root: root.to_path_buf(),
            current,
            budget,
            costs: ReadCosts {
                file_bytes: 4352,
                budget_bytes: 4352,
                ..ReadCosts::default()
            },
            pages: HashMap::new(),
            blocks: HashMap::new(),
            parts: HashMap::new(),
            files: super::call_io::CallFiles::default(),
            payload_class: ReadClass::Payload,
            native_descriptor: Some(descriptor),
            _generation_hold: Some(hold),
        })
    }

    /// Capture CURRENT once. Expected codecs come from the held home descriptor.
    /// The caller must retain this generation's files for the entire call.
    pub fn open(
        root: &Path,
        placement_codec: u16,
        key_codec: u16,
        budget: ReadBudget,
    ) -> Result<Self> {
        // Reject impossible dimensions before the bounded CURRENT capture.
        budget.validate_for_features(super::FEATURE_EXTENDED_KEYS)?;
        let native = measured::try_exists(root.join(super::home::MARKER))?;
        let initial_bytes = if native {
            12_288
        } else {
            4096 + b"artifact-only=1\nformat=1\n".len() as u64
        };
        if budget.bytes < initial_bytes {
            return Err(resource());
        }
        let (current, hold) = super::lifetime::GenerationHold::capture(root)?;
        budget.validate_for_features(current.required_features)?;
        if current.placement_codec != placement_codec || current.key_codec != key_codec {
            return Err(corrupt("CURRENT codec differs from held home descriptor"));
        }
        let native_descriptor = if current.required_features & super::FEATURE_NATIVE_HOME != 0 {
            if budget.bytes < 8192 {
                return Err(resource());
            }
            if root.join("disk-pack-artifact-v1").try_exists()? {
                return Err(corrupt("native home contains an artifact marker"));
            }
            let descriptor = super::home::HomeDescriptor::read(root)?;
            descriptor.check_current(&current)?;
            Some(descriptor)
        } else {
            None
        };
        Ok(Self {
            root: root.to_path_buf(),
            current,
            budget,
            costs: ReadCosts {
                file_bytes: initial_bytes,
                budget_bytes: initial_bytes,
                ..ReadCosts::default()
            },
            pages: HashMap::new(),
            blocks: HashMap::new(),
            parts: HashMap::new(),
            files: super::call_io::CallFiles::default(),
            payload_class: ReadClass::Payload,
            native_descriptor,
            _generation_hold: Some(hold),
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
        let Some(location) = self.locate(collection, group, key)? else {
            return Ok(None);
        };
        self.read_record(collection, key, &location).map(Some)
    }

    pub(super) fn locate(
        &mut self,
        collection: &str,
        group: u16,
        key: &str,
    ) -> Result<Option<Location>> {
        self.examine()?;
        if key.len() > self.current.storage_key_byte_limit() {
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
        let location = Location::decode_for_current(&value, &self.current)?;
        if location.version.batch > self.current.frontier {
            return Err(corrupt("record newer than committed frontier"));
        }
        if location.source == Source::Tombstone {
            return Ok(None);
        }
        Ok(Some(location))
    }

    /// Resolve all supplied exact coordinates in order with one shared call cache.
    /// Repeated keys share payload blocks. An error never becomes an absent item.
    pub fn get_many(&mut self, keys: &[(&str, u16, &str)]) -> Result<Vec<Option<Vec<u8>>>> {
        let count = u64::try_from(keys.len()).map_err(|_| resource())?;
        if count > self.budget.keys.saturating_sub(self.costs.examined_keys) {
            return Err(resource());
        }
        self.batch_get(
            keys.iter()
                .map(|(collection, group, key)| (*collection, Ok(*group), *key)),
        )?
        .into_iter()
        .collect()
    }

    pub(super) fn directory_key(collection: &str, group: u16) -> Result<Vec<u8>> {
        if collection.len() > super::key_limits::COLLECTION_BYTES || group >= 1024 {
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

    pub(super) fn data_path(&self, location: &Location) -> PathBuf {
        match location.source {
            Source::Pack => self.root.join("packs").join(location.object.to_string()),
            Source::Tombstone => unreachable!("tombstone has no data path"),
        }
    }

    pub(super) fn read_block(&mut self, location: &Location, ordinal: u64) -> Result<Arc<Buffer>> {
        let identity = super::block_plan::block_id(location, ordinal);
        if let Some(block) = self.blocks.get(&identity) {
            self.costs.shared_blocks += 1;
            return Ok(Arc::clone(block));
        }
        if self.costs.payload_blocks >= self.budget.blocks {
            return Err(resource());
        }
        let plan = self.plan_block(location, ordinal)?;
        let frame = self.read_extent(
            &plan.path,
            plan.offset,
            plan.stored,
            false,
            self.payload_class,
        )?;
        self.decode_planned(&plan, &frame.bytes)
    }
}
