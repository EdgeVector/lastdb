use super::{invalid, protocol, store, ManifestAssembly, Phase, StorageResult};
use cloud_backup_protocol::publication::CommitInput;
use cloud_backup_protocol::transfer::TransferView;
use cloud_backup_protocol::{
    canonical_bytes, DecimalU64, ManifestPage, ManifestPageReference, NativeBackupManifest,
    ObjectKind, Version,
};

impl ManifestAssembly<'_> {
    pub fn freeze_manifest_index(&mut self) -> StorageResult<super::FrozenUpload> {
        if self.state.phase < Phase::References {
            return Err(invalid("public reference tree is not frozen"));
        }
        for index in 0..self.reference_count() {
            if !self.reference_confirmed(index)? {
                return Err(invalid("public reference metadata is unconfirmed"));
            }
        }
        let mut pages = Vec::with_capacity(self.manifest_page_count());
        for index in 0..self.manifest_page_count() {
            let upload =
                self.load_upload(&super::upload::object_name(index), ObjectKind::ManifestPage)?;
            let receipt = upload
                .receipt
                .ok_or_else(|| invalid("manifest page receipt is missing"))?;
            pages.push(ManifestPageReference {
                page_index: index as u32,
                receipt_id: receipt.receipt_id,
            });
        }
        let selection = &self.state.selection;
        let manifest = NativeBackupManifest {
            version: Version::V1,
            job: selection.begin.job.clone(),
            native_root: selection.begin.native_root.clone(),
            native_file_count: u32::try_from(selection.native_files)
                .map_err(|_| invalid("manifest native count"))?,
            external_dependency_count: DecimalU64::new(selection.external_dependencies),
            native_bytes: DecimalU64::new(selection.native_bytes),
            pages,
            object_references: self
                .state
                .reference_root
                .clone()
                .ok_or_else(|| invalid("manifest reference root"))?,
            recovery_commitment: selection.begin.recovery.commitment().map_err(protocol)?,
        };
        manifest.validate().map_err(protocol)?;
        for index in 0..self.manifest_page_count() {
            let upload =
                self.load_upload(&super::upload::object_name(index), ObjectKind::ManifestPage)?;
            manifest
                .validate_page_receipt(
                    index as u32,
                    upload
                        .receipt
                        .as_ref()
                        .ok_or_else(|| invalid("manifest page receipt"))?,
                )
                .map_err(protocol)?;
            let bytes = self.store.raw(
                &format!("{}.enc", super::upload::object_name(index)),
                cloud_backup_protocol::limits::MANIFEST_PAGE_ENVELOPE_MAX_BYTES,
            )?;
            let plain = crate::crypto::decrypt_envelope(&self.key, &bytes)
                .map_err(|_| invalid("manifest page encryption"))?;
            let page: ManifestPage =
                serde_json::from_slice(&plain).map_err(|_| invalid("manifest page plaintext"))?;
            manifest.validate_page(&page).map_err(protocol)?;
        }
        let frozen = self.freeze_upload(
            "manifest-index",
            ObjectKind::ManifestIndex,
            &canonical_bytes(&manifest).map_err(protocol)?,
        )?;
        if self.state.phase < Phase::Index {
            self.state.phase = Phase::Index;
            self.save()?;
        }
        if frozen.receipt.is_some() {
            self.freeze_commit()?;
        }
        Ok(frozen)
    }

    pub fn confirm_manifest_index(&mut self, response: &TransferView) -> StorageResult<()> {
        if self.state.phase < Phase::Index {
            return Err(invalid("manifest index is not frozen"));
        }
        self.confirm_upload("manifest-index", ObjectKind::ManifestIndex, response)?;
        self.freeze_commit()
    }
    pub fn commit_input(&self) -> StorageResult<CommitInput> {
        if self.state.phase != Phase::Complete {
            return Err(invalid("manifest assembly is not commit-ready"));
        }
        let commit: CommitInput = self.store.read("commit.json", store::CONTROL_MAX)?;
        commit
            .validate_for(&self.state.selection.begin)
            .map_err(protocol)?;
        let receipt = self
            .load_upload("manifest-index", ObjectKind::ManifestIndex)?
            .receipt
            .ok_or_else(|| invalid("manifest index is unverified"))?;
        if Some(&commit.reference_root) != self.state.reference_root.as_ref()
            || commit.encrypted_manifest_receipt_id != receipt.receipt_id
        {
            return Err(invalid("manifest commit differs from saved closure"));
        }
        self.store.barrier("commit.json")?;
        Ok(commit)
    }
    fn freeze_commit(&mut self) -> StorageResult<()> {
        let receipt = self
            .load_upload("manifest-index", ObjectKind::ManifestIndex)?
            .receipt
            .ok_or_else(|| invalid("manifest index is unverified"))?;
        let commit = CommitInput {
            version: Version::V1,
            job: self.state.selection.begin.job.clone(),
            job_input_sha256: self
                .state
                .selection
                .begin
                .input_sha256()
                .map_err(protocol)?,
            reference_root: self
                .state
                .reference_root
                .clone()
                .ok_or_else(|| invalid("manifest reference root"))?,
            encrypted_manifest_receipt_id: receipt.receipt_id,
        };
        commit
            .validate_for(&self.state.selection.begin)
            .map_err(protocol)?;
        self.store
            .immutable(self.capture, "commit.json", &commit, store::CONTROL_MAX)?;
        self.state.phase = Phase::Complete;
        self.save()
    }
}
