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

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Header {
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
    pub(super) fn digest(&self) -> Result<[u8; DIGEST_BYTES]> {
        let bytes = serde_json::to_vec(self).map_err(|_| refuse("header encoding failed"))?;
        Ok(Sha256::digest(bytes).into())
    }

    pub(super) fn new(home: &DiskHome, current: &Current) -> Self {
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

    pub(super) fn validate(&self) -> Result<()> {
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

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    pub(super) collection: String,
    pub(super) group: u16,
    pub(super) key: String,
    batch: u64,
    operation: u32,
    pub(super) body_bytes: u64,
    pub(super) deleted: bool,
}

impl Record {
    pub(super) fn from_stored(record: &StoredRecord<'_>) -> Self {
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

    pub(super) fn version(&self, frontier: u64) -> Result<MutationVersion> {
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
    let bytes = serde_json::to_vec(value).map_err(|_| refuse("metadata encoding failed"))?;
    if bytes.len() > METADATA_MAX {
        return Err(refuse("metadata exceeds protocol bound"));
    }
    output.write_all(&(bytes.len() as u32).to_le_bytes())?;
    output.write_all(&bytes)?;
    Ok(())
}

pub(super) fn read_metadata<T: DeserializeOwned>(input: &mut dyn Read) -> Result<T> {
    let mut size = [0; 4];
    input.read_exact(&mut size)?;
    let size = u32::from_le_bytes(size) as usize;
    if size == 0 || size > METADATA_MAX {
        return Err(refuse("metadata exceeds protocol bound"));
    }
    let mut bytes = vec![0; size];
    input.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes).map_err(|_| refuse("invalid transfer metadata"))
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

    pub(super) fn next_record(&mut self) -> Result<Option<Record>> {
        let mut tag = [0];
        self.inner.read_exact(&mut tag)?;
        match tag[0] {
            0 => Ok(None),
            1 => {
                self.digest.update(tag);
                read_metadata(self).map(Some)
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
