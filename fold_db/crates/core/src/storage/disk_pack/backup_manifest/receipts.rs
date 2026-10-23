use super::{content, invalid, protocol, store, ManifestAssembly, StorageResult};
use cloud_backup_protocol::transfer::ObjectReceipt;
use cloud_backup_protocol::{Digest, ObjectKind};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;

pub(super) const IDS_MAX: u64 = 1024 * 512;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ReceiptRecord {
    pub ordinal: u64,
    pub receipt: ObjectReceipt,
}
fn name(id: &Digest) -> String {
    format!("receipt-{}.json", id.as_str())
}

impl ManifestAssembly<'_> {
    pub(super) fn include_receipt(&mut self, receipt: &ObjectReceipt) -> StorageResult<()> {
        receipt.validate().map_err(protocol)?;
        if receipt.scope != self.state.selection.begin.job.stream.scope
            || receipt.object.kind == ObjectKind::ManifestIndex
        {
            return Err(invalid("manifest receipt scope or kind"));
        }
        let path = name(&receipt.receipt_id);
        let record = if self.store.exists(&path)? {
            let record: ReceiptRecord = self.store.read(&path, store::CONTROL_MAX)?;
            if record.receipt != *receipt || record.ordinal > self.state.unique_receipts {
                return Err(invalid("manifest receipt identity or ordinal"));
            }
            record
        } else {
            if self.state.unique_receipts == IDS_MAX {
                return Err(invalid("manifest receipt inventory exceeds wire capacity"));
            }
            let record = ReceiptRecord {
                ordinal: self.state.unique_receipts,
                receipt: receipt.clone(),
            };
            self.store
                .write(self.capture, &path, &record, store::CONTROL_MAX)?;
            record
        };
        let id = content::raw_digest(&receipt.receipt_id)?;
        if record.ordinal < self.state.unique_receipts {
            return self.verify_id_slot(record.ordinal, &id);
        }
        self.store.append_id(self.capture, record.ordinal, &id)?;
        self.state.unique_receipts += 1;
        self.save()
    }

    fn verify_id_slot(&self, ordinal: u64, expected: &[u8; 32]) -> StorageResult<()> {
        let mut file = store::input(&self.store.root.join("receipt-ids.bin"))?;
        file.seek(SeekFrom::Start(ordinal * 32))?;
        let mut actual = [0; 32];
        file.read_exact(&mut actual)?;
        if &actual != expected {
            return Err(invalid("manifest receipt ID slot differs"));
        }
        Ok(())
    }

    pub(super) fn collect_receipts(&mut self) -> StorageResult<Vec<[u8; 32]>> {
        for ordinal in 0..self.state.native_fed {
            self.include_receipt(&self.native_feed(ordinal)?.receipt)?;
        }
        for index in 0..self.state.selection.dependencies {
            if !self.store.exists(&content::external_name(index))? {
                continue;
            }
            for receipt in self.external_feed(index)?.receipts {
                self.include_receipt(&receipt)?;
            }
        }
        for index in 0..self.manifest_page_count() {
            let upload =
                self.load_upload(&super::upload::object_name(index), ObjectKind::ManifestPage)?;
            self.include_receipt(
                upload
                    .receipt
                    .as_ref()
                    .ok_or_else(|| invalid("manifest page upload is unverified"))?,
            )?;
        }
        self.admit_restore_closure()?;
        let mut file = store::input(&self.store.root.join("receipt-ids.bin"))?;
        if file.metadata()?.len() != self.state.unique_receipts * 32 {
            return Err(invalid("manifest receipt ID length"));
        }
        let mut ids = Vec::with_capacity(self.state.unique_receipts as usize);
        for _ in 0..self.state.unique_receipts {
            let mut id = [0; 32];
            file.read_exact(&mut id)?;
            ids.push(id);
        }
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("manifest receipt ID duplicate"));
        }
        Ok(ids)
    }

    pub(super) fn receipt(&self, id: &[u8; 32]) -> StorageResult<ObjectReceipt> {
        let digest = Digest::try_from(crate::hex::hex_lower(id)).map_err(protocol)?;
        let record: ReceiptRecord = self.store.read(&name(&digest), store::CONTROL_MAX)?;
        record.receipt.validate().map_err(protocol)?;
        if record.receipt.receipt_id != digest
            || record.ordinal >= self.state.unique_receipts
            || record.receipt.scope != self.state.selection.begin.job.stream.scope
        {
            return Err(invalid("manifest stored receipt identity"));
        }
        Ok(record.receipt)
    }

    fn admit_restore_closure(&self) -> StorageResult<()> {
        // Match current importer charges: 512 per receipt, 192 per unique use,
        // and 192 per native path or external pointer/kind. Pages reserve 4 MiB.
        let charge = self
            .state
            .unique_receipts
            .checked_mul(
                lastdb_storage_policy::backup_decode::RECEIPT_MEMORY_BYTES
                    + lastdb_storage_policy::backup_decode::IDENTITY_MEMORY_BYTES,
            )
            .and_then(|bytes| {
                self.state
                    .selection
                    .native_files
                    .checked_add(self.state.selection.external_dependencies)
                    .and_then(|paths| {
                        paths.checked_mul(
                            lastdb_storage_policy::backup_decode::IDENTITY_MEMORY_BYTES,
                        )
                    })
                    .and_then(|paths| bytes.checked_add(paths))
            })
            .ok_or_else(|| invalid("restore inventory charge overflow"))?;
        if charge
            > lastdb_storage_policy::BACKUP_RESOURCES.decoded_values_bytes
                - lastdb_storage_policy::backup_decode::JSON_PAGE_RESERVE_BYTES
        {
            return Err(invalid(
                "manifest closure exceeds current restore memory admission",
            ));
        }
        Ok(())
    }
}

impl super::Store {
    fn append_id(
        &mut self,
        capture: &super::NativeBackupCapture,
        ordinal: u64,
        id: &[u8; 32],
    ) -> StorageResult<()> {
        let path = self.root.join("receipt-ids.bin");
        let old = if path.try_exists()? {
            store::input(&path)?.metadata()?.len()
        } else {
            0
        };
        let offset = ordinal
            .checked_mul(32)
            .ok_or_else(|| invalid("receipt ID ordinal overflow"))?;
        if old < offset || old > offset + 32 {
            return Err(invalid("receipt ID append boundary"));
        }
        capture.admit_assembly_metadata(
            self.bytes()
                .checked_add(32)
                .ok_or_else(|| invalid("receipt ID spool overflow"))?,
        )?;
        let _space = capture.reserve_assembly_output(65536)?;
        let result = (|| {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path)?;
            file.seek(SeekFrom::Start(offset))?;
            if old == offset + 32 {
                let mut saved = [0; 32];
                file.read_exact(&mut saved)?;
                if &saved != id {
                    return Err(invalid("receipt ID retry changed"));
                }
            } else {
                file.set_len(offset)?;
                file.write_all(id)?;
            }
            file.sync_all()?;
            std::fs::File::open(&self.root)?.sync_all()?;
            Ok(())
        })();
        let current = store::input(&path)?.metadata()?.len();
        self.resize_stream(old, current)?;
        capture.track_assembly_metadata(self.bytes())?;
        result
    }
}
