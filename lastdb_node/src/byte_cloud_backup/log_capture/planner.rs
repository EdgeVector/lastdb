//! Stream frozen mutation-log files into R2 transfer objects and encrypted client pages.
use super::{object_name, payload_name, LogCapture, ObjectRecord, MAX_OBJECTS};
use crate::byte_cloud_backup::{BackupError, Result};
use crate::byte_cloud_log::{LogKind, MutationLogPage, MutationLogRoot, VERSION};
use cloud_backup_protocol::v2::{transfer::TransferInput, Destination, Object, Version};
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Id, RecoveryDescriptor};
use fold_db::cloud_backup_plan::{
    expected_receipt, plan_files, ClientNativeFile, PlanFile, PlanSegment,
};
use fold_db::storage::disk_pack::byte_backup::{
    encode_metadata, ClientPageReference, MAX_PAGE_FILES,
};
use serde::Serialize;
use std::sync::Arc;

impl LogCapture {
    pub(super) fn build(&mut self, recovery: &RecoveryDescriptor, key: &[u8; 32]) -> Result<()> {
        let mut files: Vec<_> = self
            .frozen
            .files
            .iter()
            .enumerate()
            .map(|(id, file)| PlanFile {
                id,
                path: file.path.clone(),
                bytes: file.bytes,
                destination: Destination::R2,
            })
            .collect();
        files.sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
        let core = Arc::clone(&self.core);
        let scope = self.frozen.begin.job.stream.scope.clone();
        let mut native_files = Vec::with_capacity(files.len());
        let totals = plan_files(
            &files,
            &scope,
            |file| {
                core.reader(file.id, 0, file.bytes)
                    .map_err(BackupError::from)
            },
            |index, planned| {
                native_files.extend(planned.native_files);
                self.save_object(index, planned.object, planned.segments, None)
                    .map(|_| ())
            },
        )?;
        self.plan.packs = totals.packs;
        self.plan.multi_file_packs = totals.multi_file_packs;
        self.finish_metadata(recovery, key, totals.objects, native_files)?;
        self.plan.ready = true;
        self.store.write("log-plan.json", &self.plan)
    }

    fn finish_metadata(
        &mut self,
        recovery: &RecoveryDescriptor,
        key: &[u8; 32],
        mut index: u64,
        native_files: Vec<ClientNativeFile>,
    ) -> Result<()> {
        let mut page = MutationLogPage {
            version: VERSION,
            page_index: 0,
            begin_sha256: self.frozen.begin.input_sha256()?,
            descriptor_sha256: self.frozen.begin.descriptor.commitment()?,
            files: Vec::new(),
        };
        let mut pages = Vec::new();
        for file in native_files {
            if page.files.len() == MAX_PAGE_FILES {
                pages.push(self.freeze_page(key, index, &page)?);
                index += 1;
                page.page_index += 1;
                page.files.clear();
            }
            page.files.push(file);
        }
        if !page.files.is_empty() {
            pages.push(self.freeze_page(key, index, &page)?);
            index += 1;
        }
        let root = MutationLogRoot {
            version: VERSION,
            kind: LogKind::NativeMutationLog,
            begin_sha256: page.begin_sha256,
            descriptor_sha256: page.descriptor_sha256,
            recovery: recovery.clone(),
            base: self.plan.context.base.clone(),
            previous: self.plan.context.previous.clone(),
            anchor: self.frozen.anchor.clone(),
            from: self.frozen.from.clone(),
            through: self.frozen.through.clone(),
            entries: self.frozen.entries.clone(),
            native_file_count: self.plan.files,
            native_bytes: self.plan.native_bytes,
            pages,
        };
        root.validate_for(&self.frozen.begin)
            .map_err(|_| BackupError::Invalid)?;
        self.freeze(key, index, &root)?;
        if self.plan.objects != index + 1 {
            return Err(BackupError::Identity);
        }
        Ok(())
    }

    fn freeze_page(
        &mut self,
        key: &[u8; 32],
        index: u64,
        page: &MutationLogPage,
    ) -> Result<ClientPageReference> {
        page.validate_for(&self.frozen.begin)
            .map_err(|_| BackupError::Invalid)?;
        let object = self.freeze(key, index, page)?;
        Ok(ClientPageReference {
            page_index: page.page_index,
            receipt_id: expected_receipt(&object.input)?.receipt_id,
            sha256: object.input.object.sha256,
            bytes: object.input.object.bytes,
        })
    }

    fn freeze(
        &mut self,
        key: &[u8; 32],
        index: u64,
        value: &impl Serialize,
    ) -> Result<ObjectRecord> {
        let name = payload_name(index);
        let plain = encode_metadata(value).map_err(|_| BackupError::Limit)?;
        let encrypted = if let Some(bytes) = self.store.raw(&name)? {
            let old = zeroize::Zeroizing::new(
                fold_db::crypto::decrypt_envelope(key, &bytes)
                    .map_err(|_| BackupError::Identity)?,
            );
            if old.as_slice() != plain.as_slice() {
                return Err(BackupError::Identity);
            }
            bytes
        } else {
            let bytes =
                fold_db::crypto::encrypt_envelope(key, &plain).map_err(|_| BackupError::Invalid)?;
            self.store.write_raw(&name, &bytes)?;
            bytes
        };
        let object = Object {
            sha256: bytes_sha256(&encrypted),
            bytes: DecimalU64::new(encrypted.len() as u64),
            destination: Destination::R2,
        };
        self.save_object(index, object, Vec::new(), Some(name))
    }

    fn save_object(
        &mut self,
        index: u64,
        object: Object,
        segments: Vec<PlanSegment>,
        payload: Option<String>,
    ) -> Result<ObjectRecord> {
        if index >= MAX_OBJECTS {
            return Err(BackupError::Limit);
        }
        let name = object_name(index);
        let record = if let Some(prior) = self.store.read::<ObjectRecord>(&name)? {
            prior.validate(&self.frozen, index)?;
            if prior.input.object != object
                || prior.segments != segments
                || prior.payload != payload
            {
                return Err(BackupError::Identity);
            }
            prior
        } else {
            let input = TransferInput {
                version: Version::V2,
                request_id: Id::try_from(uuid::Uuid::new_v4().to_string())?,
                job: self.frozen.begin.job.clone(),
                object,
                blob: None,
            };
            input.validate()?;
            ObjectRecord {
                input,
                segments,
                payload,
                receipt: None,
            }
        };
        record.validate(&self.frozen, index)?;
        self.store.write(&name, &record)?;
        self.plan.objects = self.plan.objects.max(index + 1);
        self.budget(self.store.bytes())?;
        self.store.write("log-plan.json", &self.plan)?;
        Ok(record)
    }
}
