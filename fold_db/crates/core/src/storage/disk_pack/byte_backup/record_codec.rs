//! Client-owned streamed record recovery. The cloud never interprets this format.
use super::{invalid, StorageResult};
use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::{DecimalU64, Digest};
use laststore::disk_pack::home::transfer::{self, TransferHeader, TransferReceipt};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read};
use std::path::Path;

pub const CLIENT_STREAM_PLAINTEXT_MAX_BYTES: u64 =
    lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes;

/// Current native feature support belongs to the client. Keep the frozen V1
/// transport validator unchanged while accepting this reader's extended keys.
pub fn validate_client_native_root(root: &cloud_backup_protocol::NativeRoot) -> StorageResult<()> {
    let mut legacy = root.clone();
    legacy.required_features = DecimalU64::new(
        root.required_features.get() & !laststore::disk_pack::FEATURE_EXTENDED_KEYS,
    );
    legacy.validate().map_err(super::protocol)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientBackupStream {
    Metadata,
    R2Bodies,
    B2Bodies,
    Sequence,
}
impl ClientBackupStream {
    pub fn destination(self) -> Destination {
        if self == Self::B2Bodies {
            Destination::B2
        } else {
            Destination::R2
        }
    }
    pub fn encrypted(self) -> bool {
        matches!(self, Self::Metadata | Self::Sequence)
    }
    pub(super) fn tag(self) -> u8 {
        match self {
            Self::Metadata => 1,
            Self::R2Bodies => 2,
            Self::B2Bodies => 3,
            Self::Sequence => 4,
        }
    }
    pub(super) fn from_tag(tag: u8) -> StorageResult<Self> {
        match tag {
            1 => Ok(Self::Metadata),
            2 => Ok(Self::R2Bodies),
            3 => Ok(Self::B2Bodies),
            _ => Err(invalid("record sequence stream tag")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientStreamChunk {
    pub stream: ClientBackupStream,
    pub ordinal: u64,
    pub receipt_id: Digest,
    pub plaintext_bytes: DecimalU64,
}
impl ClientStreamChunk {
    pub fn validate(&self) -> StorageResult<()> {
        if self.plaintext_bytes.get() == 0 {
            return Err(invalid("record stream empty chunk"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientStreamSummary {
    pub stream: ClientBackupStream,
    pub chunks: u64,
    pub bytes: u64,
    pub sha256: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredRecordProfile {
    pub header: TransferHeader,
    pub receipt: TransferReceipt,
    pub streams: Vec<ClientStreamSummary>,
}
impl StoredRecordProfile {
    pub fn validate(&self) -> StorageResult<()> {
        self.header.validate().map_err(super::super::map)?;
        if self.streams.len() != 4 {
            return Err(invalid("record stream summary count"));
        }
        let kinds = [
            ClientBackupStream::Metadata,
            ClientBackupStream::R2Bodies,
            ClientBackupStream::B2Bodies,
            ClientBackupStream::Sequence,
        ];
        for (summary, expected) in self.streams.iter().zip(kinds) {
            if summary.stream != expected
                || (summary.chunks == 0) != (summary.bytes == 0)
                || (summary.bytes == 0
                    && summary.sha256 != cloud_backup_protocol::bytes_sha256(b""))
            {
                return Err(invalid("record stream summary identity"));
            }
        }
        if self.streams[0].bytes == 0
            || self.streams[3].bytes == 0
            || !self.streams[3].bytes.is_multiple_of(9)
        {
            return Err(invalid("record stream framing summary"));
        }
        self.receipt
            .records
            .checked_add(self.receipt.tombstones)
            .ok_or_else(|| invalid("record count overflow"))?;
        Ok(())
    }
    pub fn summary(&self, stream: ClientBackupStream) -> StorageResult<&ClientStreamSummary> {
        self.streams
            .iter()
            .find(|s| s.stream == stream)
            .ok_or_else(|| invalid("record stream summary missing"))
    }
}

/// The existing native importer verifies the complete ordered transfer digest.
/// The caller supplies a fresh staging root and installs it only after success.
pub fn import_record_backup(
    root: &Path,
    input: &mut dyn Read,
    profile: &StoredRecordProfile,
) -> StorageResult<TransferReceipt> {
    profile.validate()?;
    let mut prefix = Vec::new();
    transfer::write_header(&mut prefix, &profile.header).map_err(super::super::map)?;
    let mut actual = vec![0; prefix.len()];
    input.read_exact(&mut actual)?;
    if actual != prefix {
        return Err(invalid(
            "record transfer header differs from encrypted root",
        ));
    }
    let mut stream = Cursor::new(actual).chain(input);
    let receipt = transfer::import_streaming(root, &mut stream).map_err(super::super::map)?;
    if receipt != profile.receipt {
        return Err(invalid("imported stored record closure differs"));
    }
    Ok(receipt)
}
