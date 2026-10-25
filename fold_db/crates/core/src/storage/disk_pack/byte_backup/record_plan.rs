//! Separate stored bodies by destination without copying a source home.
mod coordinate;
mod data;
mod finish;
mod frozen;
use super::{
    invalid, model::State, ClientBackupStream, ClientStreamSummary, Inner, StorageResult,
    StoredRecordProfile,
};
pub(super) use coordinate::Coordinate;
use laststore::disk_pack::{
    home::transfer,
    maintenance::{self, StoredRecord},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{self, Write};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecordSegment {
    pub coordinate: Coordinate,
    pub offset: u64,
    pub bytes: u64,
}
struct Stream {
    kind: ClientBackupStream,
    bytes: u64,
    chunks: u64,
    hash: Sha256,
    plain: Vec<u8>,
    segments: Vec<RecordSegment>,
    object_bytes: u64,
    object_hash: Sha256,
    coordinate_bytes: usize,
}
impl Stream {
    fn new(kind: ClientBackupStream) -> Self {
        Self {
            kind,
            bytes: 0,
            chunks: 0,
            hash: Sha256::new(),
            plain: Vec::new(),
            segments: Vec::new(),
            object_bytes: 0,
            object_hash: Sha256::new(),
            coordinate_bytes: 0,
        }
    }
    fn summary(&self) -> StorageResult<ClientStreamSummary> {
        Ok(ClientStreamSummary {
            stream: self.kind,
            chunks: self.chunks,
            bytes: self.bytes,
            sha256: cloud_backup_protocol::Digest::try_from(crate::hex::hex_lower(
                self.hash.clone().finalize(),
            ))
            .map_err(super::protocol)?,
        })
    }
}
struct Export<'a> {
    inner: &'a Inner,
    state: &'a mut State,
    key: &'a [u8; 32],
    index: u64,
    streams: [Stream; 4],
    pending: Option<(ClientBackupStream, u64)>,
    record_hash: Sha256,
    receipt: transfer::TransferReceipt,
}
pub(super) fn build(inner: &Inner, state: &mut State, key: &[u8; 32]) -> StorageResult<()> {
    let source = inner
        .source
        .as_ref()
        .ok_or_else(|| invalid("stored record source missing"))?;
    let header = transfer::TransferHeader::new(inner.home.home(), source.current());
    let kinds = [
        ClientBackupStream::Metadata,
        ClientBackupStream::R2Bodies,
        ClientBackupStream::B2Bodies,
        ClientBackupStream::Sequence,
    ];
    state.metadata_pack_objects = 0;
    state.multi_file_pack_objects = 0;
    let mut export = Export {
        inner,
        state,
        key,
        index: 0,
        streams: kinds.map(Stream::new),
        pending: None,
        record_hash: Sha256::new(),
        receipt: transfer::TransferReceipt::default(),
    };
    transfer::write_header(
        &mut Metadata {
            export: &mut export,
            digest: false,
        },
        &header,
    )
    .map_err(super::super::map)?;
    maintenance::visit_generation_streaming(inner.home.home().path(), source.current(), |record| {
        export
            .record(record)
            .map_err(|error| laststore::Error::Config(error.to_string()))
    })
    .map_err(super::super::map)?;
    export.receipt.record_sha256 = export.record_hash.clone().finalize().into();
    let receipt = export.receipt.clone();
    transfer::write_footer(
        &mut Metadata {
            export: &mut export,
            digest: false,
        },
        &header,
        &receipt,
    )
    .map_err(super::super::map)?;
    export.finish_run()?;
    for kind in kinds {
        export.flush(kind)?;
    }
    let streams = export
        .streams
        .iter()
        .map(Stream::summary)
        .collect::<StorageResult<Vec<_>>>()?;
    export.state.record_profile = Some(StoredRecordProfile {
        header,
        receipt,
        streams,
    });
    let content = export.index;
    finish::metadata(inner, export.state, key, content)?;
    export.state.ready = true;
    export.state.validate()?;
    inner.store.write("job.json", export.state)
}
impl Export<'_> {
    fn slot(kind: ClientBackupStream) -> usize {
        kind.tag() as usize - 1
    }
    fn record(&mut self, mut record: StoredRecord<'_>) -> StorageResult<()> {
        self.state.max_coordinate_bytes = self
            .state
            .max_coordinate_bytes
            .max(record.collection.len() as u64 + record.key.len() as u64);
        let features = self.state.native_root.required_features.get();
        transfer::write_stored_record_prefix(
            &mut Metadata {
                export: self,
                digest: true,
            },
            &record,
            features,
        )
        .map_err(super::super::map)?;
        if let Some(body) = record.body.take() {
            let kind = if record.collection == "cas_blobs" {
                ClientBackupStream::B2Bodies
            } else {
                ClientBackupStream::R2Bodies
            };
            if record.body_bytes != 0 {
                let coordinate = Coordinate::save(self.inner, record.collection, record.key)?;
                self.add_run(kind, record.body_bytes)?;
                self.body(kind, coordinate, body, record.body_bytes)?;
            } else if body.read(&mut [0; 1])? != 0 {
                return Err(invalid("empty stored record body differs"));
            }
            self.receipt.records = self
                .receipt
                .records
                .checked_add(1)
                .ok_or_else(|| invalid("record export count overflow"))?;
        } else {
            self.receipt.tombstones = self
                .receipt
                .tombstones
                .checked_add(1)
                .ok_or_else(|| invalid("record export deletion count overflow"))?;
        }
        self.receipt.body_bytes = self
            .receipt
            .body_bytes
            .checked_add(record.body_bytes)
            .ok_or_else(|| invalid("record export byte sum overflow"))?;
        Ok(())
    }
    fn add_run(&mut self, kind: ClientBackupStream, bytes: u64) -> StorageResult<()> {
        if bytes == 0 {
            return Ok(());
        }
        if let Some((prior, length)) = &mut self.pending {
            if *prior == kind {
                *length = length
                    .checked_add(bytes)
                    .ok_or_else(|| invalid("record run overflow"))?;
                return Ok(());
            }
        }
        self.finish_run()?;
        self.pending = Some((kind, bytes));
        Ok(())
    }
    fn finish_run(&mut self) -> StorageResult<()> {
        if let Some((kind, bytes)) = self.pending.take() {
            let mut record = [0; 9];
            record[0] = kind.tag();
            record[1..].copy_from_slice(&bytes.to_le_bytes());
            self.encrypted(ClientBackupStream::Sequence, &record)?;
        }
        Ok(())
    }
}
struct Metadata<'a, 'b> {
    export: &'a mut Export<'b>,
    digest: bool,
}
impl Write for Metadata<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.export
            .add_run(ClientBackupStream::Metadata, bytes.len() as u64)
            .and_then(|()| self.export.encrypted(ClientBackupStream::Metadata, bytes))
            .map_err(|error| io::Error::other(error.to_string()))?;
        if self.digest {
            self.export.record_hash.update(bytes);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
