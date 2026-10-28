use super::*;
use cloud_backup_protocol::v2::transfer::ObjectReceipt;
use cloud_backup_protocol::Digest;
use fold_db::storage::disk_pack::byte_backup::run_backup_destination;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct FileHash {
    pub done: u64,
    pub bytes: u64,
    pub expected: Digest,
    pub hash: Sha256,
}
pub(super) struct Span {
    pub file: usize,
    pub file_offset: u64,
    pub object_offset: u64,
    pub bytes: u64,
    destination: cloud_backup_protocol::v2::Destination,
}
pub(super) struct Plan {
    pub objects: Vec<ObjectReceipt>,
    pub files: Vec<FileHash>,
    pub spans: BTreeMap<Digest, Vec<Span>>,
}

pub(super) fn build(manifest: &manifest::Manifest, store: &Store) -> Result<Plan> {
    let mut plan = Plan {
        objects: Vec::new(),
        files: Vec::new(),
        spans: BTreeMap::new(),
    };
    let mut receipts = BTreeMap::new();
    let mut paths = BTreeSet::new();
    let mut total_bytes = 0u64;
    let mut metadata_bytes = 0u64;
    let mut descriptor_hash = Sha256::new();
    descriptor_hash.update(b"lastdb:stopped-byte-source-manifest:v1\0");
    if manifest.objects == 0
        || manifest.native_files == 0
        || manifest.objects
            > (cloud_backup_protocol::v2::limits::MAX_PAGES
                * cloud_backup_protocol::v2::limits::MAX_PAGE_ENTRIES) as u64
    {
        return Err(BackupError::Invalid);
    }
    for index in 0..manifest.objects {
        let object = manifest::object(store, index)?;
        object.input.validate()?;
        let raw = serde_json::to_vec(&object).map_err(|_| BackupError::Invalid)?;
        descriptor_hash.update(index.to_be_bytes());
        descriptor_hash.update((raw.len() as u64).to_be_bytes());
        descriptor_hash.update(&raw);
        metadata_bytes = metadata_bytes
            .checked_add(raw.len() as u64)
            .ok_or(BackupError::Limit)?;
        if metadata_bytes > lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes / 4 {
            return Err(BackupError::Limit);
        }
        if object.input.job != manifest.begin.job {
            return Err(BackupError::Identity);
        }
        let mut receipt = ObjectReceipt {
            version: Version::V2,
            receipt_id: object.input.object.sha256.clone(),
            scope: object.input.job.stream.scope.clone(),
            object: object.input.object,
        };
        receipt.receipt_id = receipt.identity_sha256()?;
        receipt.validate()?;
        match receipts.insert(receipt.receipt_id.clone(), receipt.clone()) {
            Some(previous) if previous != receipt => return Err(BackupError::Identity),
            None => plan.objects.push(receipt),
            _ => {}
        }
        for file in object.native_files {
            file.validate()?;
            let destination = run_backup_destination(file.path.as_str())?;
            if !paths.insert(file.path.as_str().to_owned())
                || plan.files.len()
                    >= fold_db::storage::disk_pack::byte_backup::MAX_CLIENT_PAGES
                        * fold_db::storage::disk_pack::byte_backup::MAX_PAGE_FILES
            {
                return Err(BackupError::Invalid);
            }
            let file_index = plan.files.len();
            total_bytes = total_bytes
                .checked_add(file.native_bytes.get())
                .ok_or(BackupError::Limit)?;
            plan.files.push(FileHash {
                done: 0,
                bytes: file.native_bytes.get(),
                expected: file.native_sha256,
                hash: Sha256::new(),
            });
            let mut file_offset = 0;
            for extent in file.extents {
                plan.spans.entry(extent.receipt_id).or_default().push(Span {
                    file: file_index,
                    file_offset,
                    object_offset: extent.offset.get(),
                    bytes: extent.length.get(),
                    destination,
                });
                file_offset += extent.length.get();
            }
        }
    }
    if fold_db::hex::hex_lower(descriptor_hash.finalize()) != manifest.descriptors_sha256.as_str()
        || plan.files.len() as u64 != manifest.native_files
        || total_bytes != manifest.native_bytes
    {
        return Err(BackupError::Identity);
    }
    for (id, spans) in &plan.spans {
        let receipt = receipts.get(id).ok_or(BackupError::Missing)?;
        for span in spans {
            if span
                .object_offset
                .checked_add(span.bytes)
                .is_none_or(|end| end > receipt.object.bytes.get())
                || receipt.object.destination != Some(span.destination)
            {
                return Err(BackupError::Identity);
            }
        }
    }
    Ok(plan)
}
