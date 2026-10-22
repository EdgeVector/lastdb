use super::state::{BackupCapturePhase, ExportRecord};
use super::store::{hex, LocalStore};
use super::{invalid, protocol, PackRuntime, StorageResult};
use cloud_backup_protocol::limits::*;
use cloud_backup_protocol::transfer::{ObjectReceipt, TransferInput};
use cloud_backup_protocol::{
    BackupJob, DecimalU64, Digest, Id, NativeFileEntry, ObjectExtent, ObjectIdentity, ObjectKind,
    RelativePath, Version,
};
use laststore::disk_pack::backup::export::{SnapshotExport, SnapshotFile};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::io::Read;

const SMALL_FILE_BYTES: u64 = 64 * 1024;

/// Private original native-file identity and its position in one object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupCaptureFile {
    pub path: RelativePath,
    pub sha256: Digest,
    pub bytes: DecimalU64,
    pub offset: DecimalU64,
}

/// Frozen transfer input. Its files concatenate without a new physical format.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupCaptureObject {
    pub input: TransferInput,
    pub input_sha256: Digest,
    pub files: Vec<BackupCaptureFile>,
}

impl BackupCaptureObject {
    fn validate(&self, job: &BackupJob) -> StorageResult<()> {
        self.input.validate().map_err(protocol)?;
        if self.input.job != *job
            || self.input.input_sha256().map_err(protocol)? != self.input_sha256
            || self.files.is_empty()
            || self.files.len() > 512
            || !matches!(
                self.input.object.kind,
                ObjectKind::NativeFile | ObjectKind::MetadataPack
            )
            || (self.input.object.kind == ObjectKind::NativeFile && self.files.len() != 1)
        {
            return Err(invalid("backup capture object identity"));
        }
        let mut offset = 0;
        let mut previous = None;
        for file in &self.files {
            if file.bytes.get() == 0
                || file.bytes.get() > NATIVE_PART_MAX_BYTES
                || file.offset.get() != offset
                || previous.is_some_and(|path| path >= file.path.as_str())
            {
                return Err(invalid("backup capture file extent bounds"));
            }
            offset += file.bytes.get();
            previous = Some(file.path.as_str());
        }
        if offset != self.input.object.bytes.get()
            || (self.input.object.kind == ObjectKind::NativeFile
                && self.files[0].sha256 != self.input.object.sha256)
        {
            return Err(invalid("backup capture object length or digest"));
        }
        Ok(())
    }

    /// The caller obtains this receipt from the authoritative transfer result.
    /// A locally computed receipt alone does not prove cloud availability.
    pub fn native_entries(&self, receipt: &ObjectReceipt) -> StorageResult<Vec<NativeFileEntry>> {
        self.validate(&self.input.job)?;
        receipt.validate().map_err(protocol)?;
        if receipt.scope != self.input.job.stream.scope || receipt.object != self.input.object {
            return Err(invalid(
                "backup capture receipt does not select this exact object",
            ));
        }
        Ok(self
            .files
            .iter()
            .map(|file| NativeFileEntry {
                path: file.path.clone(),
                native_sha256: file.sha256.clone(),
                native_bytes: file.bytes,
                extent: ObjectExtent {
                    receipt_id: receipt.receipt_id.clone(),
                    offset: file.offset,
                    length: file.bytes,
                },
            })
            .collect())
    }
}

/// One bounded local catalogue page. It is not an encrypted cloud manifest.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupCapturePage {
    pub job: BackupJob,
    pub page_index: u32,
    pub objects: Vec<BackupCaptureObject>,
}

impl BackupCapturePage {
    pub(super) fn validate(&self) -> StorageResult<()> {
        self.job.validate().map_err(protocol)?;
        let files: usize = self.objects.iter().map(|object| object.files.len()).sum();
        if self.page_index as usize >= MANIFEST_PAGES_MAX
            || self.objects.is_empty()
            || self.objects.len() > 512
            || files > 512
        {
            return Err(invalid("backup capture page bounds"));
        }
        for object in &self.objects {
            object.validate(&self.job)?;
        }
        Ok(())
    }
}

pub(super) fn build(
    runtime: &PackRuntime,
    source: &SnapshotExport,
    store: &LocalStore,
    record: &mut ExportRecord,
) -> StorageResult<()> {
    if source.bytes() > BACKUP_STORED_BYTES_MAX || source.files().len() > NATIVE_FILES_MAX {
        return Err(invalid(
            "backup capture retained source exceeds its admission limit",
        ));
    }
    let quote = source
        .files()
        .iter()
        .try_fold(2 * 1024 * 1024u64, |total, file| {
            total
                .checked_add(file.path.as_os_str().len() as u64 + 1536)
                .ok_or_else(|| invalid("backup capture metadata quote overflow"))
        })?;
    if quote > DURABLE_LOCAL_SPOOL_MAX_BYTES {
        return Err(invalid("backup capture spool quote exceeds its limit"));
    }
    let _space = runtime
        .writer
        .lock()
        .map_err(|_| invalid("backup capture writer lock failed"))?
        .reserve_space(quote, &runtime.space_policy)
        .map_err(super::super::map)?;
    store.cleanup_pages(record)?;
    record.pages.clear();
    record.native_files = 0;
    record.native_bytes = 0;
    record.spool_bytes = 0;
    store.save(record)?;
    let mut page = BackupCapturePage {
        job: record.begin.job.clone(),
        page_index: 0,
        objects: Vec::new(),
    };
    let mut count = 0;
    let mut files = source.files().iter().peekable();
    while let Some(first) = files.next() {
        let mut members = vec![first];
        let mut bytes = first.bytes;
        if first.bytes < SMALL_FILE_BYTES {
            while let Some(next) = files.peek() {
                if next.bytes >= SMALL_FILE_BYTES
                    || members.len() >= 512
                    || bytes + next.bytes > NATIVE_PART_MAX_BYTES
                {
                    break;
                }
                bytes += next.bytes;
                members.push(files.next().expect("peeked native file"));
            }
        }
        if count + members.len() > 512 {
            save_page(store, record, &mut page)?;
            count = 0;
        }
        let object = make_object(runtime, source, &record.begin.job, &members)?;
        count += members.len();
        page.objects.push(object);
    }
    if !page.objects.is_empty() {
        save_page(store, record, &mut page)?;
    }
    if record.native_files != source.files().len() as u64 || record.native_bytes != source.bytes() {
        return Err(invalid(
            "backup capture catalogue does not cover the exact source inventory",
        ));
    }
    record.phase = BackupCapturePhase::Ready;
    store.save(record)
}

fn save_page(
    store: &LocalStore,
    record: &mut ExportRecord,
    page: &mut BackupCapturePage,
) -> StorageResult<()> {
    let descriptor = store.write_page(page)?;
    record.native_files += descriptor.files;
    record.native_bytes += page
        .objects
        .iter()
        .map(|object| object.input.object.bytes.get())
        .sum::<u64>();
    record.spool_bytes += descriptor.bytes.get();
    record.pages.push(descriptor);
    store.save(record)?;
    page.page_index += 1;
    page.objects.clear();
    Ok(())
}

fn make_object(
    runtime: &PackRuntime,
    source: &SnapshotExport,
    job: &BackupJob,
    members: &[&SnapshotFile],
) -> StorageResult<BackupCaptureObject> {
    let mut hash = Sha256::new();
    let mut files = Vec::with_capacity(members.len());
    let mut offset = 0;
    let mut buffer = [0u8; 65_536];
    for file in members {
        let mut reader = source
            .reader(&file.path, &runtime.read_lease)
            .map_err(super::super::map)?;
        let mut file_hash = Sha256::new();
        let mut bytes = 0;
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
            file_hash.update(&buffer[..read]);
            bytes += read as u64;
        }
        if bytes != file.bytes {
            return Err(invalid("backup capture file changed during its hash pass"));
        }
        let path = file
            .path
            .to_str()
            .ok_or_else(|| invalid("backup capture path is not UTF-8"))?;
        files.push(BackupCaptureFile {
            path: RelativePath::try_from(path.to_owned()).map_err(protocol)?,
            sha256: hex(file_hash.finalize().into()),
            bytes: DecimalU64::new(bytes),
            offset: DecimalU64::new(offset),
        });
        offset += bytes;
    }
    let input = TransferInput {
        version: Version::V1,
        request_id: Id::try_from(uuid::Uuid::new_v4().to_string()).map_err(protocol)?,
        job: job.clone(),
        object: ObjectIdentity {
            sha256: hex(hash.finalize().into()),
            bytes: DecimalU64::new(offset),
            kind: if members[0].bytes < SMALL_FILE_BYTES {
                ObjectKind::MetadataPack
            } else {
                ObjectKind::NativeFile
            },
        },
    };
    let object = BackupCaptureObject {
        input_sha256: input.input_sha256().map_err(protocol)?,
        input,
        files,
    };
    object.validate(job)?;
    Ok(object)
}
