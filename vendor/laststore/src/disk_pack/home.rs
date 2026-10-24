//! Durable placement and key identity for a native pack home.
//! This API does not select a node backend or change the memory ack boundary.

use super::bytes::*;
use super::{corrupt, Current, ReadBudget, ReadCall};
use crate::read_receipt_io::{self as measured, ReadClass};
use crate::{durability, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

mod admission;
mod build;
mod maintenance;
mod merge;
mod mutation_quote;
mod read;
mod stream;
pub mod transfer;
mod write;

pub use admission::MutationReservation;
pub use build::{HomeBuilder, HomeGroupWriter};
pub use merge::PackMerge;
pub use mutation_quote::MutationQuote;
pub use read::HomeRead;
pub use stream::StreamedRecord;
pub use write::{HomeMutation, HomeSnapshot};

pub(super) const MARKER: &str = "laststore-pack-home-v1";
/// FNV-1a-64, 1024 groups, first-NUL-inclusive hash prefix, configured 1..16 fanout.
pub const PLACEMENT_CODEC: u16 = 0x0101;
/// Complete LastStore UTF-8 document id, compared by unsigned UTF-8 bytes.
/// Higher layers retain their own escaping, binary-id, blind-key, and range codecs.
pub const KEY_CODEC: u16 = 0x0101;

/// Deliberately unsupported by legacy version-1 layout parsers, before any data open.
pub const LEGACY_FENCE_BYTES: &[u8] = b"version=2\nlayout_mode=disk_pack_v1\n";

/// Create and sync the downgrade refusal in a new, explicitly selected destination.
pub fn write_legacy_fence(root: &Path) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(root.join("laststore-layout-v1"))?;
    file.write_all(LEGACY_FENCE_BYTES)?;
    durability::sync_dirty_file(&file)?;
    durability::sync_directory(&File::open(root)?)
}

/// Immutable home identity. Unsupported physical encryption is refused explicitly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HomeDescriptor {
    /// Unique home identity, independent of payload object identities.
    pub identity: Uuid,
    /// Power-of-two number of groups under one encoded hash, at most sixteen.
    pub hash_fanout: u16,
    /// Existing physical layout epoch retained by conversion.
    pub layout_epoch: u32,
}

impl HomeDescriptor {
    /// Retain supported legacy placement exactly; refuse physical encryption here.
    /// A separate conversion owner must hold the complete source boundary.
    pub fn from_layout(layout: &crate::LayoutDescriptor) -> Result<Self> {
        if layout.layout_mode != crate::LayoutMode::HashGroup
            || layout.shard_bits != 0
            || layout.hash_group_bits != 10
            || layout.hash_algo != crate::HashAlgo::Fnv1a64
            || layout.hash_group_key != crate::HashGroupKey::PartitionPrefix
            || layout.packaging != crate::PackagingMode::Plain
        {
            return Err(crate::Error::Config(
                "native pack conversion requires plain frames, FNV-1a-64, no high-bit shards, 1024 groups, and hash-prefix placement".into(),
            ));
        }
        let fanout = layout.hash_group_partition_fanout.try_into().map_err(|_| {
            crate::Error::Config("source hash fanout exceeds native pack support".into())
        })?;
        Self::new(fanout, layout.layout_epoch)
    }

    /// Allocate a new plain-frame home descriptor. Stored value bytes stay opaque.
    pub fn new(hash_fanout: u16, layout_epoch: u32) -> Result<Self> {
        let descriptor = Self {
            identity: Uuid::new_v4(),
            hash_fanout,
            layout_epoch,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn validate(&self) -> Result<()> {
        if self.identity.is_nil() || !self.hash_fanout.is_power_of_two() || self.hash_fanout > 16 {
            return Err(crate::Error::Config(
                "pack home requires a nonnil identity and power-of-two hash fanout in 1..16".into(),
            ));
        }
        Ok(())
    }

    /// Derive the exact group from the held codec, without any file discovery.
    pub fn group(&self, key: &str) -> Result<u16> {
        self.validate()?;
        if key.len() > super::KEY_BYTES {
            return Err(corrupt("native storage key exceeds format limit"));
        }
        let split = key.find('\0').map_or(key.len(), |at| at + 1);
        let base = crate::store::fnv1a64(&key.as_bytes()[..split]) & 1023;
        let offset =
            crate::store::fnv1a64(&key.as_bytes()[split..]) & u64::from(self.hash_fanout - 1);
        Ok(((base + offset) & 1023) as u16)
    }

    pub(super) fn groups(&self, prefix: &str) -> Result<Vec<u16>> {
        self.validate()?;
        if prefix.len() < 2
            || prefix.len() > super::KEY_BYTES
            || prefix.find('\0') != prefix.len().checked_sub(1)
        {
            return Err(crate::Error::Config(
                "native range requires one complete first-NUL-terminated encoded hash".into(),
            ));
        }
        let base = crate::store::fnv1a64(prefix.as_bytes()) & 1023;
        Ok((0..self.hash_fanout)
            .map(|offset| ((base + u64::from(offset)) & 1023) as u16)
            .collect())
    }

    pub(super) fn encode(&self) -> Result<[u8; 4096]> {
        self.validate()?;
        let mut bytes = [0; 4096];
        bytes[..8].copy_from_slice(b"LDHOME1\0");
        put16(&mut bytes, 8, 1);
        put16(&mut bytes, 10, 4096);
        put16(&mut bytes, 16, PLACEMENT_CODEC);
        put16(&mut bytes, 18, KEY_CODEC);
        bytes[20] = 0; // No high-bit shards.
        bytes[21] = 10;
        bytes[22] = 1; // FNV-1a-64.
        bytes[23] = 2; // First-NUL-inclusive prefix.
        put16(&mut bytes, 24, self.hash_fanout);
        put32(&mut bytes, 28, self.layout_epoch);
        bytes[32..48].copy_from_slice(self.identity.as_bytes());
        // Physical frame packaging 0 (plain). Body encryption remains opaque.
        let sum = checksum(&bytes, 128);
        bytes[128..160].copy_from_slice(&sum);
        Ok(bytes)
    }

    fn decode(bytes: &[u8; 4096]) -> Result<Self> {
        if &bytes[..8] != b"LDHOME1\0"
            || u16_at(bytes, 8) != 1
            || u16_at(bytes, 10) != 4096
            || u16_at(bytes, 16) != PLACEMENT_CODEC
            || u16_at(bytes, 18) != KEY_CODEC
            || bytes[20..24] != [0, 10, 1, 2]
        {
            return Err(corrupt("unsupported native home format or codec"));
        }
        zero(&bytes[12..16])?;
        zero(&bytes[26..28])?;
        zero(&bytes[48..128])?;
        zero(&bytes[160..])?;
        verify_checksum(bytes, 128)?;
        let value = Self {
            identity: nonnil(uuid_at(bytes, 32))?,
            hash_fanout: u16_at(bytes, 24),
            layout_epoch: u32_at(bytes, 28),
        };
        value.validate()?;
        Ok(value)
    }

    pub(super) fn read(root: &Path) -> Result<Self> {
        let _class = ReadClass::FormatProbe.enter();
        let mut file = measured::open(root.join(MARKER))?;
        if file.metadata()?.len() != 4096 {
            return Err(corrupt("native home descriptor length"));
        }
        let mut bytes = [0; 4096];
        file.read_exact(&mut bytes)?;
        Self::decode(&bytes)
    }

    pub(super) fn write(&self, root: &Path) -> Result<()> {
        write_legacy_fence(root)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(root.join(MARKER))?;
        file.write_all(&self.encode()?)?;
        durability::sync_dirty_file(&file)?;
        durability::sync_directory(&File::open(root)?)
    }

    pub(super) fn check_current(&self, current: &Current) -> Result<()> {
        self.validate()?;
        if current.required_features & super::FEATURE_NATIVE_HOME == 0
            || current.placement_codec != PLACEMENT_CODEC
            || current.key_codec != KEY_CODEC
            || current.home_digest != digest(&self.encode()?)
        {
            return Err(corrupt("native CURRENT differs from held home descriptor"));
        }
        Ok(())
    }
}

/// Explicit native pack root. No legacy home or artifact is adopted implicitly.
pub struct DiskHome {
    root: PathBuf,
    descriptor: HomeDescriptor,
}

impl DiskHome {
    /// Exact explicitly selected native root, for supported backup and maintenance.
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// Read the exact descriptor and root without opening a writer.
    pub fn open(root: &Path) -> Result<Self> {
        super::conversion_fence::refuse_pending(root)?;
        Self::open_conversion(root)
    }

    pub(crate) fn open_conversion(root: &Path) -> Result<Self> {
        let root = root.canonicalize()?;
        if root.join(super::conversion::REVERSE_FENCE).try_exists()? {
            return Err(crate::Error::Config("native source is sealed after explicit reverse conversion; use the new legacy destination".into()));
        }
        let descriptor = HomeDescriptor::read(&root)?;
        let current = super::artifact::current(&root)?;
        descriptor.check_current(&current)?;
        Ok(Self { root, descriptor })
    }

    /// The immutable placement/key identity held by this home handle.
    pub fn descriptor(&self) -> &HomeDescriptor {
        &self.descriptor
    }

    /// Use an already verified complete publication from this home's writer.
    /// The owner retains its generation files and publishes this root only after disk barriers.
    /// This avoids reopening CURRENT for a memory-only or request-shared read.
    pub fn read_committed(&self, current: Current, budget: ReadBudget) -> Result<HomeRead> {
        let call = ReadCall::from_committed(&self.root, current, self.descriptor.clone(), budget)?;
        Ok(HomeRead::new(self.descriptor.clone(), call))
    }

    /// Capture a reader under this home's already held exclusive writer lease.
    /// The guard performs no file reads and retains the writer until this call ends.
    pub fn read_from_writer(
        &self,
        current: Current,
        budget: ReadBudget,
        lease: &super::lifetime::WriterReadLease,
    ) -> Result<HomeRead> {
        let call =
            ReadCall::from_writer(&self.root, current, self.descriptor.clone(), budget, lease)?;
        Ok(HomeRead::new(self.descriptor.clone(), call))
    }

    /// Read an exact durable reference, including one below the writer floor.
    /// Reference validation and local registration exclude concurrent reclaim.
    /// The returned reader retains the writer, not the filesystem deletion lock.
    pub fn read_retained(
        &self,
        reference: &super::lifetime::DurableReference,
        budget: ReadBudget,
        lease: &super::lifetime::WriterReadLease,
    ) -> Result<HomeRead> {
        let call = ReadCall::from_reference(
            &self.root,
            self.descriptor.clone(),
            reference,
            budget,
            lease,
        )?;
        Ok(HomeRead::new(self.descriptor.clone(), call))
    }

    /// Prepare one atomic write under this home's exclusive writer lease.
    /// The call permits 40,000 examined keys; its page, block, and byte ceilings
    /// match an ordinary read. Each physical batch still permits 10,000 slots.
    pub fn read_for_write_preparation(
        &self,
        current: Current,
        lease: &super::lifetime::WriterReadLease,
    ) -> Result<HomeRead> {
        let call =
            ReadCall::from_write_preparation(&self.root, current, self.descriptor.clone(), lease)?;
        Ok(HomeRead::new(self.descriptor.clone(), call))
    }

    /// Capture one generation and one aggregate budget for all reads in this call.
    pub fn read(&self, budget: ReadBudget) -> Result<HomeRead> {
        let call = ReadCall::open(&self.root, PLACEMENT_CODEC, KEY_CODEC, budget)?;
        self.descriptor.check_current(call.current())?;
        if call.native_descriptor.as_ref() != Some(&self.descriptor) {
            return Err(corrupt("native home descriptor changed under held handle"));
        }
        Ok(HomeRead::new(self.descriptor.clone(), call))
    }
}
