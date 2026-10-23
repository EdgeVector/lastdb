use super::{invalid, protocol, store, ManifestAssembly, Phase, StorageResult};
use cloud_backup_protocol::access::{
    ReferenceMetadata, ReferencePutReceipt, ReferencePutRequest, ReferencePutResult,
};
use cloud_backup_protocol::references::{ReferenceIndex, ReferencePage};
use cloud_backup_protocol::{canonical_bytes, Version};

fn request_name(index: usize) -> String {
    format!("reference-{index:04}.json")
}
fn ack_name(index: usize) -> String {
    format!("reference-{index:04}.ack.json")
}

impl ManifestAssembly<'_> {
    pub fn freeze_references(&mut self) -> StorageResult<()> {
        if self.state.phase >= Phase::References {
            return Ok(());
        }
        if self.state.phase != Phase::Pages {
            return Err(invalid("manifest pages are not frozen"));
        }
        self.cache = None;
        let ids = self.collect_receipts()?;
        let mut page = ReferencePage {
            version: Version::V1,
            scope: self.state.selection.begin.job.stream.scope.clone(),
            page_index: 0,
            objects: Vec::new(),
        };
        let mut descriptors = Vec::new();
        for id in ids {
            page.objects.push(self.receipt(&id)?);
            if !self.reference_fits(&page)? {
                let last = page
                    .objects
                    .pop()
                    .ok_or_else(|| invalid("reference page member"))?;
                descriptors.push(self.save_reference_page(&page)?);
                page.page_index += 1;
                page.objects.clear();
                page.objects.push(last);
                if !self.reference_fits(&page)? {
                    return Err(invalid("receipt exceeds reference request capacity"));
                }
            }
        }
        descriptors.push(self.save_reference_page(&page)?);
        let index = ReferenceIndex {
            version: Version::V1,
            scope: self.state.selection.begin.job.stream.scope.clone(),
            pages: descriptors,
        };
        index.validate().map_err(protocol)?;
        let root = index.root().map_err(protocol)?;
        let request = ReferencePutRequest {
            version: Version::V1,
            job: self.state.selection.begin.job.clone(),
            metadata: ReferenceMetadata::Index { index },
        };
        request.validate().map_err(protocol)?;
        self.store.immutable(
            self.capture,
            &request_name(page.page_index as usize + 1),
            &request,
            store::CONTROL_MAX,
        )?;
        self.state.reference_pages = page.page_index + 1;
        self.state.reference_root = Some(root);
        self.state.phase = Phase::References;
        self.save()
    }

    fn reference_fits(&self, page: &ReferencePage) -> StorageResult<bool> {
        if page.objects.is_empty()
            || page.objects.len() > cloud_backup_protocol::limits::MANIFEST_PAGE_ENTRIES_MAX
        {
            return Ok(false);
        }
        let request = ReferencePutRequest {
            version: Version::V1,
            job: self.state.selection.begin.job.clone(),
            metadata: ReferenceMetadata::Page { page: page.clone() },
        };
        Ok(canonical_bytes(page).map_err(protocol)?.len() as u64
            <= cloud_backup_protocol::limits::MAX_REFERENCE_PAGE_BYTES
            && canonical_bytes(&request).map_err(protocol)?.len() as u64
                <= cloud_backup_protocol::limits::REQUEST_JSON_MAX_BYTES)
    }
    fn save_reference_page(
        &mut self,
        page: &ReferencePage,
    ) -> StorageResult<cloud_backup_protocol::references::ReferencePageDescriptor> {
        page.validate().map_err(protocol)?;
        let descriptor = page.descriptor().map_err(protocol)?;
        let request = ReferencePutRequest {
            version: Version::V1,
            job: self.state.selection.begin.job.clone(),
            metadata: ReferenceMetadata::Page { page: page.clone() },
        };
        request.validate().map_err(protocol)?;
        self.store.immutable(
            self.capture,
            &request_name(page.page_index as usize),
            &request,
            store::CONTROL_MAX,
        )?;
        Ok(descriptor)
    }

    pub fn reference_request(&self, index: usize) -> StorageResult<ReferencePutRequest> {
        if index >= self.reference_count() {
            return Err(invalid("reference request index"));
        }
        let request: ReferencePutRequest =
            self.store.read(&request_name(index), store::CONTROL_MAX)?;
        request.validate().map_err(protocol)?;
        let right_kind = match &request.metadata {
            ReferenceMetadata::Page { page } => {
                index < self.state.reference_pages as usize && page.page_index as usize == index
            }
            ReferenceMetadata::Index { index: value } => {
                index == self.state.reference_pages as usize
                    && Some(value.root().map_err(protocol)?) == self.state.reference_root
            }
        };
        if request.job != self.state.selection.begin.job || !right_kind {
            return Err(invalid("reference request identity"));
        }
        self.store.barrier(&request_name(index))?;
        Ok(request)
    }
    pub fn reference_confirmed(&self, index: usize) -> StorageResult<bool> {
        let request = self.reference_request(index)?;
        if !self.store.exists(&ack_name(index))? {
            return Ok(false);
        }
        let receipt: ReferencePutReceipt = self.store.read(&ack_name(index), store::CONTROL_MAX)?;
        receipt.validate_for(&request).map_err(protocol)?;
        self.store.barrier(&ack_name(index))?;
        Ok(true)
    }
    pub fn confirm_reference(
        &mut self,
        index: usize,
        response: &ReferencePutResult,
    ) -> StorageResult<()> {
        let request = self.reference_request(index)?;
        response.validate_for(&request).map_err(protocol)?;
        let ReferencePutResult::Ok { receipt } = response else {
            return Err(invalid("reference metadata remains unconfirmed"));
        };
        self.store
            .immutable(self.capture, &ack_name(index), receipt, store::CONTROL_MAX)
    }
}
