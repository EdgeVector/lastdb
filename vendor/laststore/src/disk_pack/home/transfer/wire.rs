use super::super::DiskHome;
use super::TransferReceipt;
use crate::disk_pack::maintenance::StoredRecord;
use crate::disk_pack::{Current, MutationVersion, ReadBudget};
use crate::Result;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};

pub(super) const DIGEST_BYTES: usize = 32;
const METADATA_MAX: usize = 300_000;

// Wire feature 1024 selects a 32 MiB key; collection framing stays at 65,531.
// Keep these wire constants independent of the linked source reader version.
pub(super) fn record_metadata_max(features: u64) -> usize {
    let key = if features & 1024 != 0 {
        32 * 1024 * 1024
    } else {
        65_535
    };
    // JSON can escape each input byte as six bytes. Other fields are bounded.
    6 * (key + 65_531) + 1024
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    protocol: String,
    source_home: [u8; 16],
    source_generation: u64,
    pub(super) required_features: u64,
    pub(super) hash_fanout: u16,
    pub(super) layout_epoch: u32,
    pub(super) frontier: u64,
    placement_codec: u16,
    key_codec: u16,
}

impl Header {
    pub fn source_home_id(&self) -> [u8; 16] {
        self.source_home
    }

    pub fn source_generation(&self) -> u64 {
        self.source_generation
    }

    pub fn required_features(&self) -> u64 {
        self.required_features
    }

    pub fn frontier(&self) -> u64 {
        self.frontier
    }

    pub fn digest(&self) -> Result<[u8; DIGEST_BYTES]> {
        let bytes = serde_json::to_vec(self).map_err(|_| refuse("header encoding failed"))?;
        Ok(Sha256::digest(bytes).into())
    }

    pub fn new(home: &DiskHome, current: &Current) -> Self {
        Self {
            protocol: "lastdb.native-record-transfer.v1".into(),
            source_home: *home.descriptor().identity.as_bytes(),
            source_generation: current.generation,
            required_features: current.required_features,
            hash_fanout: home.descriptor().hash_fanout,
            layout_epoch: home.descriptor().layout_epoch,
            frontier: current.frontier,
            placement_codec: current.placement_codec,
            key_codec: current.key_codec,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.protocol != "lastdb.native-record-transfer.v1"
            || self.source_home == [0; 16]
            || self.source_generation == 0
            || self.placement_codec != super::super::PLACEMENT_CODEC
            || self.key_codec != super::super::KEY_CODEC
        {
            return Err(refuse("unsupported native transfer header"));
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Footer {
    pub(super) header_sha256: [u8; DIGEST_BYTES],
    pub(super) receipt: TransferReceipt,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub(super) collection: String,
    pub(super) group: u16,
    pub(super) key: String,
    batch: u64,
    operation: u32,
    pub(super) body_bytes: u64,
    pub(super) deleted: bool,
}

impl Record {
    pub fn collection(&self) -> &str {
        &self.collection
    }

    pub fn group(&self) -> u16 {
        self.group
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn body_bytes(&self) -> u64 {
        self.body_bytes
    }

    pub fn deleted(&self) -> bool {
        self.deleted
    }

    pub fn from_stored(record: &StoredRecord<'_>) -> Self {
        Self {
            collection: record.collection.into(),
            group: record.group,
            key: record.key.into(),
            batch: record.version.batch,
            operation: record.version.operation,
            body_bytes: record.body_bytes,
            deleted: record.body.is_none(),
        }
    }

    pub fn version(&self, frontier: u64) -> Result<MutationVersion> {
        if self.batch > frontier
            || (self.deleted && self.body_bytes != 0)
            || self.body_bytes > lastdb_storage_policy::native_v1::RECORD_BODY_MAX_BYTES
        {
            return Err(refuse("invalid native transfer record"));
        }
        Ok(MutationVersion {
            batch: self.batch,
            operation: self.operation,
        })
    }
}

pub(super) fn refuse(message: &'static str) -> crate::Error {
    crate::Error::Config(format!("native record transfer: {message}"))
}

pub(super) fn budget() -> ReadBudget {
    ReadBudget {
        pages: 4096,
        blocks: 16_384,
        bytes: 256 * 1024 * 1024,
        keys: 10_000,
    }
}

pub(super) fn write_metadata<T: Serialize>(output: &mut dyn Write, value: &T) -> Result<()> {
    write_metadata_bounded(output, value, METADATA_MAX)
}

pub(super) fn write_metadata_bounded<T: Serialize>(
    output: &mut dyn Write,
    value: &T,
    maximum: usize,
) -> Result<()> {
    let mut count = MetadataCount { bytes: 0, maximum };
    serde_json::to_writer(&mut count, value)
        .map_err(|_| refuse("metadata exceeds protocol bound"))?;
    let bytes = u32::try_from(count.bytes).map_err(|_| refuse("metadata length overflow"))?;
    output.write_all(&bytes.to_le_bytes())?;
    serde_json::to_writer(output, value).map_err(|_| refuse("metadata encoding failed"))?;
    Ok(())
}

pub(super) fn read_metadata<T: DeserializeOwned>(input: &mut dyn Read) -> Result<T> {
    read_metadata_bounded(input, METADATA_MAX)
}

fn read_metadata_bounded<T: DeserializeOwned>(input: &mut dyn Read, maximum: usize) -> Result<T> {
    let mut size = [0; 4];
    input.read_exact(&mut size)?;
    let size = u32::from_le_bytes(size) as usize;
    if size == 0 || size > maximum {
        return Err(refuse("metadata exceeds protocol bound"));
    }
    let mut bounded = input.take(size as u64);
    let value =
        serde_json::from_reader(&mut bounded).map_err(|_| refuse("invalid transfer metadata"))?;
    if bounded.limit() != 0 {
        return Err(refuse("truncated transfer metadata"));
    }
    Ok(value)
}

struct MetadataCount {
    bytes: usize,
    maximum: usize,
}
impl Write for MetadataCount {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|total| *total <= self.maximum)
            .ok_or_else(|| std::io::Error::other("transfer metadata bound"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Borrow the original key while serializing; an escaped key need not coexist
/// with a second complete key or an encoded JSON buffer.
pub(super) fn write_stored_prefix(
    output: &mut dyn Write,
    stored: &StoredRecord<'_>,
    features: u64,
) -> Result<()> {
    #[derive(Serialize)]
    struct Borrowed<'a> {
        collection: &'a str,
        group: u16,
        key: &'a str,
        batch: u64,
        operation: u32,
        body_bytes: u64,
        deleted: bool,
    }
    let record = Borrowed {
        collection: stored.collection,
        group: stored.group,
        key: stored.key,
        batch: stored.version.batch,
        operation: stored.version.operation,
        body_bytes: stored.body_bytes,
        deleted: stored.body.is_none(),
    };
    output.write_all(&[1])?;
    write_metadata_bounded(output, &record, record_metadata_max(features))
}

pub(super) struct DigestWrite<'a> {
    inner: &'a mut dyn Write,
    digest: Sha256,
}

impl<'a> DigestWrite<'a> {
    pub(super) fn new(inner: &'a mut dyn Write) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
        }
    }

    pub(super) fn finish(self) -> [u8; DIGEST_BYTES] {
        self.digest.finalize().into()
    }
}

impl Write for DigestWrite<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let count = self.inner.write(bytes)?;
        self.digest.update(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub(super) struct DigestRead<'a> {
    pub(super) inner: &'a mut dyn Read,
    digest: Sha256,
}

impl<'a> DigestRead<'a> {
    pub(super) fn new(inner: &'a mut dyn Read) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
        }
    }

    pub(super) fn next_record(&mut self, maximum: usize) -> Result<Option<Record>> {
        let mut tag = [0];
        self.inner.read_exact(&mut tag)?;
        match tag[0] {
            0 => Ok(None),
            1 => {
                self.digest.update(tag);
                read_metadata_bounded(self, maximum).map(Some)
            }
            _ => Err(refuse("invalid frame tag")),
        }
    }

    pub(super) fn finish(self) -> [u8; DIGEST_BYTES] {
        self.digest.finalize().into()
    }
}

impl Read for DigestRead<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let count = self.inner.read(bytes)?;
        self.digest.update(&bytes[..count]);
        Ok(count)
    }
}
