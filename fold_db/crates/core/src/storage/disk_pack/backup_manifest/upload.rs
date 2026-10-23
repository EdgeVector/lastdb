use super::{invalid, protocol, store, ManifestAssembly, StorageResult};
use cloud_backup_protocol::transfer::{ObjectReceipt, TransferInput, TransferStatus, TransferView};
use cloud_backup_protocol::{
    bytes_sha256, DecimalU64, Digest, Id, ObjectIdentity, ObjectKind, Version,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct FrozenUpload {
    pub input: TransferInput,
    pub path: PathBuf,
    pub receipt: Option<ObjectReceipt>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct UploadRecord {
    pub input: TransferInput,
    pub input_sha256: Digest,
    pub plaintext_sha256: Digest,
    pub receipt: Option<ObjectReceipt>,
}
pub(super) fn object_name(index: usize) -> String {
    format!("manifest-{index:04}")
}

impl ManifestAssembly<'_> {
    pub(super) fn freeze_upload(
        &mut self,
        name: &str,
        kind: ObjectKind,
        plain: &[u8],
    ) -> StorageResult<FrozenUpload> {
        let metadata = format!("{name}.upload.json");
        let cipher = format!("{name}.enc");
        let plain_sha = bytes_sha256(plain);
        if self.store.exists(&metadata)? {
            let upload = self.load_upload(name, kind)?;
            if upload.plaintext_sha256 != plain_sha {
                return Err(invalid("frozen manifest plaintext changed"));
            }
        } else {
            let encrypted = if self.store.exists(&cipher)? {
                let bytes = self.store.raw(
                    &cipher,
                    cloud_backup_protocol::limits::MANIFEST_PAGE_ENVELOPE_MAX_BYTES,
                )?;
                let old = crate::crypto::decrypt_envelope(&self.key, &bytes)
                    .map_err(|_| invalid("saved manifest encryption"))?;
                if old != plain {
                    return Err(invalid("saved manifest plaintext differs"));
                }
                bytes
            } else {
                let bytes = crate::crypto::encrypt_envelope(&self.key, plain)
                    .map_err(|_| invalid("manifest encryption"))?;
                self.store.write_raw(self.capture, &cipher, &bytes)?;
                bytes
            };
            let input = TransferInput {
                version: Version::V1,
                request_id: Id::try_from(uuid::Uuid::new_v4().to_string()).map_err(protocol)?,
                job: self.state.selection.begin.job.clone(),
                object: ObjectIdentity {
                    sha256: bytes_sha256(&encrypted),
                    bytes: DecimalU64::new(encrypted.len() as u64),
                    kind,
                },
            };
            input.validate().map_err(protocol)?;
            let record = UploadRecord {
                input_sha256: input.input_sha256().map_err(protocol)?,
                input,
                plaintext_sha256: plain_sha,
                receipt: None,
            };
            self.store
                .write(self.capture, &metadata, &record, store::CONTROL_MAX)?;
        }
        self.frozen_upload(name, kind)
    }

    pub(super) fn load_upload(&self, name: &str, kind: ObjectKind) -> StorageResult<UploadRecord> {
        let record: UploadRecord = self
            .store
            .read(&format!("{name}.upload.json"), store::CONTROL_MAX)?;
        record.input.validate().map_err(protocol)?;
        if record.input.job != self.state.selection.begin.job
            || record.input.object.kind != kind
            || record.input_sha256 != record.input.input_sha256().map_err(protocol)?
        {
            return Err(invalid("saved manifest upload identity"));
        }
        let bytes = self.store.raw(
            &format!("{name}.enc"),
            cloud_backup_protocol::limits::MANIFEST_PAGE_ENVELOPE_MAX_BYTES,
        )?;
        if bytes.len() as u64 != record.input.object.bytes.get()
            || bytes_sha256(&bytes) != record.input.object.sha256
        {
            return Err(invalid("saved manifest ciphertext identity"));
        }
        if let Some(receipt) = &record.receipt {
            receipt.validate().map_err(protocol)?;
            if receipt.scope != record.input.job.stream.scope
                || receipt.object != record.input.object
            {
                return Err(invalid("saved manifest receipt identity"));
            }
        }
        Ok(record)
    }
    pub(super) fn frozen_upload(
        &self,
        name: &str,
        kind: ObjectKind,
    ) -> StorageResult<FrozenUpload> {
        let record = self.load_upload(name, kind)?;
        self.store.barrier(&format!("{name}.enc"))?;
        self.store.barrier(&format!("{name}.upload.json"))?;
        Ok(FrozenUpload {
            input: record.input,
            path: self.store.root.join(format!("{name}.enc")),
            receipt: record.receipt,
        })
    }
    pub(super) fn confirm_upload(
        &mut self,
        name: &str,
        kind: ObjectKind,
        response: &TransferView,
    ) -> StorageResult<()> {
        let mut record = self.load_upload(name, kind)?;
        response.validate_for(&record.input).map_err(protocol)?;
        let TransferStatus::Verified { receipt } = &response.state else {
            return Err(invalid("manifest upload remains unverified"));
        };
        if record.receipt.as_ref().is_some_and(|old| old != receipt) {
            return Err(invalid("manifest upload receipt changed"));
        }
        record.receipt = Some(receipt.clone());
        self.store.write(
            self.capture,
            &format!("{name}.upload.json"),
            &record,
            store::CONTROL_MAX,
        )
    }
}
