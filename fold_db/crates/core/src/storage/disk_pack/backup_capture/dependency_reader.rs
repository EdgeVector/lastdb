use super::dependencies::{ready, selection, ActiveWork};
use super::dependency_state::FileCoverage;
use super::dependency_store::DependencyStore;
use super::{invalid, NativeBackupCapture, StorageResult};
use cloud_backup_protocol::transfer::TransferInput;
use sha2::{Digest as _, Sha256};
use std::fs::File;
use std::io::Read;

impl NativeBackupCapture {
    /// Read only a saved, unconfirmed backup-owned object. No external source
    /// request or native CAS conversion occurs through this reader.
    pub fn open_file_object(&self, index: u64) -> StorageResult<NativeBackupFileReader> {
        let active = ActiveWork::acquire(&self.inner)?;
        let (job, current) = selection(&self.inner)?;
        let store = DependencyStore::open(&self.inner.store, &job)?;
        let state = ready(&store, &job, &current)?;
        if index >= state.entries {
            return Err(invalid("backup dependency does not exist"));
        }
        let entry = store.entry(&job, index)?;
        let FileCoverage::BackupOwned {
            input,
            receipt: None,
            ..
        } = entry.coverage
        else {
            return Err(invalid("backup file has no unconfirmed frozen object"));
        };
        let record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("backup capture state lock failed"))?;
        record.require_ready()?;
        let file = super::store::regular(&store.blob_path(index))?;
        if file.metadata()?.len() != input.object.bytes.get() {
            return Err(invalid("backup file object length differs"));
        }
        Ok(NativeBackupFileReader {
            _active: active,
            file: Some(file),
            input: *input,
            read: 0,
            hash: Sha256::new(),
            failed: false,
        })
    }
}

pub struct NativeBackupFileReader {
    _active: ActiveWork,
    file: Option<File>,
    input: TransferInput,
    read: u64,
    hash: Sha256,
    failed: bool,
}

impl NativeBackupFileReader {
    pub fn input(&self) -> &TransferInput {
        &self.input
    }
}

impl Read for NativeBackupFileReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if self.failed {
            return Err(std::io::Error::other("backup file reader failed"));
        }
        if output.is_empty() {
            return Ok(0);
        }
        let maximum = output
            .len()
            .min(cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize);
        let count = match self
            .file
            .as_mut()
            .ok_or_else(|| std::io::Error::other("backup file reader closed"))?
            .read(&mut output[..maximum])
        {
            Ok(count) => count,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        self.read += count as u64;
        self.hash.update(&output[..count]);
        if self.read > self.input.object.bytes.get()
            || (count == 0 && self.read != self.input.object.bytes.get())
            || (self.read == self.input.object.bytes.get()
                && super::store::hex(self.hash.clone().finalize().into())
                    != self.input.object.sha256)
        {
            self.failed = true;
            return Err(std::io::Error::other("backup file object bytes differ"));
        }
        Ok(count)
    }
}

impl Drop for NativeBackupFileReader {
    fn drop(&mut self) {
        self.file = None;
    }
}
