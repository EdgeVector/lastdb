use super::bytes::*;
use super::{corrupt, TreeKind, TreeRoot, BLOCK_BYTES};
use crate::Result;
use uuid::Uuid;

/// Physical mutation order; logical atom and author identities remain unchanged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct MutationVersion {
    /// Complete batch sequence.
    pub batch: u64,
    /// Operation number within that batch.
    pub operation: u32,
}

/// Durable source of one committed locator entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// Direct extent within an immutable local pack.
    Pack,
    /// Versioned deletion with no payload extent.
    Tombstone,
}

/// One exact payload extent or versioned tombstone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Location {
    /// Durable extent kind.
    pub source: Source,
    /// Complete stored mutation version.
    pub version: MutationVersion,
    /// Header, full key, and body byte count.
    pub record_bytes: u64,
    /// Immutable pack UUID; nil for a tombstone.
    pub object: Uuid,
    /// First logical block ordinal.
    pub first_block: u64,
    /// Byte position in the first decoded block.
    pub first_position: u32,
    /// Exact number of consecutive blocks.
    pub blocks: u32,
    /// SHA-256 over the entire stored record.
    pub digest: [u8; 32],
}

impl Location {
    /// Decode an exact fixed-width locator. Cloud-only extents remain disabled.
    /// Without a held CURRENT this preserves the original operation ceiling.
    pub fn decode(bytes: &[u8; 128]) -> Result<Self> {
        Self::decode_with_limit(bytes, super::LEGACY_BATCH_OPERATIONS)
    }

    pub(super) fn decode_for_current(bytes: &[u8; 128], current: &super::Current) -> Result<Self> {
        Self::decode_with_limit(bytes, current.batch_operation_limit())
    }

    fn decode_with_limit(bytes: &[u8; 128], operation_limit: usize) -> Result<Self> {
        zero(&bytes[1..4])?;
        zero(&bytes[88..])?;
        let source = match bytes[0] {
            2 => Source::Pack,
            4 => Source::Tombstone,
            _ => return Err(corrupt("unsupported locator source")),
        };
        let value = Self {
            source,
            version: MutationVersion {
                batch: u64_at(bytes, 4),
                operation: u32_at(bytes, 12),
            },
            record_bytes: u64_at(bytes, 16),
            object: uuid_at(bytes, 24),
            first_block: u64_at(bytes, 40),
            first_position: u32_at(bytes, 48),
            blocks: u32_at(bytes, 52),
            digest: bytes[56..88].try_into().expect("digest field"),
        };
        value.validate(operation_limit)?;
        Ok(value)
    }

    /// Encode a validated locator entry.
    /// Without a held feature contract this preserves the original ceiling.
    pub fn encode(&self) -> Result<[u8; 128]> {
        self.encode_with_limit(super::LEGACY_BATCH_OPERATIONS)
    }

    pub(super) fn encode_for_features(&self, features: u64) -> Result<[u8; 128]> {
        self.encode_with_limit(super::current::batch_operation_limit(features))
    }

    fn encode_with_limit(&self, operation_limit: usize) -> Result<[u8; 128]> {
        self.validate(operation_limit)?;
        let mut bytes = [0; 128];
        bytes[0] = match self.source {
            Source::Pack => 2,
            Source::Tombstone => 4,
        };
        put64(&mut bytes, 4, self.version.batch);
        put32(&mut bytes, 12, self.version.operation);
        put64(&mut bytes, 16, self.record_bytes);
        bytes[24..40].copy_from_slice(self.object.as_bytes());
        put64(&mut bytes, 40, self.first_block);
        put32(&mut bytes, 48, self.first_position);
        put32(&mut bytes, 52, self.blocks);
        bytes[56..88].copy_from_slice(&self.digest);
        Ok(bytes)
    }

    fn validate(&self, operation_limit: usize) -> Result<()> {
        if self.version.operation as usize >= operation_limit {
            return Err(corrupt("mutation version outside complete-batch limits"));
        }
        if self.source == Source::Tombstone {
            if self.record_bytes != 0
                || !self.object.is_nil()
                || self.first_block != 0
                || self.first_position != 0
                || self.blocks != 0
                || self.digest != [0; 32]
            {
                return Err(corrupt("tombstone contains an extent"));
            }
            return Ok(());
        }
        nonnil(self.object)?;
        let length = self
            .record_bytes
            .checked_add(u64::from(self.first_position))
            .ok_or_else(|| corrupt("extent length overflow"))?;
        if self.record_bytes < 96
            || self.first_position as usize >= BLOCK_BYTES
            || length.div_ceil(BLOCK_BYTES as u64) != u64::from(self.blocks)
            || self
                .first_block
                .checked_add(u64::from(self.blocks))
                .is_none()
        {
            return Err(corrupt("invalid record extent"));
        }
        Ok(())
    }
}

/// A converted collection/group directory value. Bridge values are not activated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    /// Existing physical group, zero through 1023.
    pub group: u16,
    /// Complete immutable locator root.
    pub locator: TreeRoot,
    /// Complete conversion boundary included by the committed root.
    pub converted_frontier: u64,
}

impl DirectoryEntry {
    /// Decode a converted directory value; refuse unknown or bridge modes.
    pub fn decode(bytes: &[u8; 128]) -> Result<Self> {
        if bytes[0] != 2 {
            return Err(corrupt(
                "directory mode requires an unavailable bridge reader",
            ));
        }
        zero(&bytes[1..2])?;
        zero(&bytes[4..8])?;
        zero(&bytes[34..96])?;
        zero(&bytes[104..])?;
        let entry = Self {
            group: u16_at(bytes, 2),
            locator: TreeRoot {
                object: nonnil(uuid_at(bytes, 8))?,
                page: u64_at(bytes, 24),
                height: u16_at(bytes, 32)
                    .try_into()
                    .map_err(|_| corrupt("locator height"))?,
                kind: TreeKind::Locator,
            },
            converted_frontier: u64_at(bytes, 96),
        };
        entry.validate()?;
        Ok(entry)
    }

    /// Encode a validated converted directory value.
    pub fn encode(&self) -> Result<[u8; 128]> {
        self.validate()?;
        let mut bytes = [0; 128];
        bytes[0] = 2;
        put16(&mut bytes, 2, self.group);
        bytes[8..24].copy_from_slice(self.locator.object.as_bytes());
        put64(&mut bytes, 24, self.locator.page);
        put16(&mut bytes, 32, self.locator.height.into());
        put64(&mut bytes, 96, self.converted_frontier);
        Ok(bytes)
    }

    fn validate(&self) -> Result<()> {
        self.locator.validate()?;
        if self.group >= 1024 || self.locator.kind != TreeKind::Locator {
            return Err(corrupt("directory group or locator kind"));
        }
        Ok(())
    }
}
