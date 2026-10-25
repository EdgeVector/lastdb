use super::super::{
    invalid, model::ObjectRecord, planner, protocol, ClientStreamChunk, StorageResult,
};
use super::{ClientBackupStream, Export};
use cloud_backup_protocol::{bytes_sha256, v2::Object, DecimalU64};
use sha2::{Digest, Sha256};

impl Export<'_> {
    pub(super) fn flush(&mut self, kind: ClientBackupStream) -> StorageResult<()> {
        let slot = Self::slot(kind);
        let stream = &mut self.streams[slot];
        let plain_bytes = if kind.encrypted() {
            stream.plain.len() as u64
        } else {
            stream.object_bytes
        };
        if plain_bytes == 0 {
            return Ok(());
        }
        let ciphertext = if kind.encrypted() {
            let name = format!("payload-{:08}.enc", self.index);
            let encrypted = if self.inner.store.exists(&name)? {
                let bytes = self.inner.store.raw(
                    &name,
                    lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes + 29,
                )?;
                let plain = zeroize::Zeroizing::new(
                    crate::crypto::decrypt_envelope(self.key, &bytes)
                        .map_err(|_| invalid("record metadata encryption"))?,
                );
                if plain.as_slice() != stream.plain.as_slice() {
                    return Err(invalid("record metadata changed"));
                }
                bytes
            } else {
                let bytes = crate::crypto::encrypt_envelope(self.key, &stream.plain)
                    .map_err(|_| invalid("record metadata encryption"))?;
                self.inner.store.write_raw(&name, &bytes)?;
                bytes
            };
            Some((name, bytes_sha256(&encrypted), encrypted.len() as u64))
        } else {
            None
        };
        let (hash, stored_bytes) = match &ciphertext {
            Some((_, digest, bytes)) => (digest.clone(), *bytes),
            None => (
                cloud_backup_protocol::Digest::try_from(crate::hex::hex_lower(
                    stream.object_hash.clone().finalize(),
                ))
                .map_err(protocol)?,
                stream.object_bytes,
            ),
        };
        let input = planner::input(
            self.inner,
            self.state,
            self.index,
            Object {
                sha256: hash,
                bytes: DecimalU64::new(stored_bytes),
                destination: Some(kind.destination()),
            },
        )?;
        let chunk = ClientStreamChunk {
            stream: kind,
            ordinal: stream.chunks,
            receipt_id: planner::expected_receipt(&input)?.receipt_id,
            plaintext_bytes: DecimalU64::new(plain_bytes),
        };
        let segments = std::mem::take(&mut stream.segments);
        let packed = segments.len() > 1;
        let object = ObjectRecord {
            input,
            segments: Vec::new(),
            native_files: Vec::new(),
            metadata_pack: packed,
            ciphertext: ciphertext.map(|(name, _, _)| name),
            receipt: None,
            record_segments: segments,
            client_stream: Some(chunk),
        };
        planner::save(self.inner, self.state, self.index, object)?;
        self.state.metadata_pack_objects += u64::from(packed);
        self.state.multi_file_pack_objects += u64::from(packed);
        self.index += 1;
        stream.chunks += 1;
        stream.plain.clear();
        stream.object_bytes = 0;
        stream.object_hash = Sha256::new();
        stream.coordinate_bytes = 0;
        Ok(())
    }
}
