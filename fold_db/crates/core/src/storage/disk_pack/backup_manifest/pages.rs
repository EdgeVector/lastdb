use super::{content, invalid, protocol, ManifestAssembly, Phase, StorageResult};
use cloud_backup_protocol::transfer::TransferView;
use cloud_backup_protocol::{
    canonical_bytes, ManifestPage, ManifestPageContext, ObjectKind, Version,
};

impl ManifestAssembly<'_> {
    pub(super) fn build_pages(&mut self) -> StorageResult<()> {
        let mut page = self.empty_page(0);
        for ordinal in 0..self.state.native_fed {
            for entry in self.native_feed(ordinal)?.entries {
                page.native_files.push(entry);
                if !fits(&page)? {
                    let last = page
                        .native_files
                        .pop()
                        .ok_or_else(|| invalid("manifest page entry"))?;
                    self.save_manifest_page(&page)?;
                    page = self.empty_page(page.context.page_index + 1);
                    page.native_files.push(last);
                    if !fits(&page)? {
                        return Err(invalid("native manifest entry exceeds page bound"));
                    }
                }
            }
        }
        for index in 0..self.state.selection.dependencies {
            if !self.store.exists(&content::external_name(index))? {
                continue;
            }
            page.external_dependencies
                .push(self.external_feed(index)?.dependency);
            if !fits(&page)? {
                let last = page
                    .external_dependencies
                    .pop()
                    .ok_or_else(|| invalid("manifest page dependency"))?;
                self.save_manifest_page(&page)?;
                page = self.empty_page(page.context.page_index + 1);
                page.external_dependencies.push(last);
                if !fits(&page)? {
                    return Err(invalid("external manifest dependency exceeds page bound"));
                }
            }
        }
        self.save_manifest_page(&page)?;
        self.state.manifest_pages = page.context.page_index + 1;
        Ok(())
    }
    fn empty_page(&self, index: u32) -> ManifestPage {
        ManifestPage {
            context: ManifestPageContext {
                version: Version::V1,
                job: self.state.selection.begin.job.clone(),
                current_sha256: self
                    .state
                    .selection
                    .begin
                    .native_root
                    .current_sha256
                    .clone(),
                page_index: index,
            },
            native_files: Vec::new(),
            external_dependencies: Vec::new(),
        }
    }
    fn save_manifest_page(&mut self, page: &ManifestPage) -> StorageResult<()> {
        page.validate().map_err(protocol)?;
        self.freeze_upload(
            &super::upload::object_name(page.context.page_index as usize),
            ObjectKind::ManifestPage,
            &canonical_bytes(page).map_err(protocol)?,
        )?;
        Ok(())
    }
    pub fn manifest_upload(&self, index: usize) -> StorageResult<super::FrozenUpload> {
        if self.state.phase < Phase::Pages || index >= self.manifest_page_count() {
            return Err(invalid("manifest page upload index"));
        }
        self.frozen_upload(&super::upload::object_name(index), ObjectKind::ManifestPage)
    }
    pub fn confirm_manifest_page(
        &mut self,
        index: usize,
        response: &TransferView,
    ) -> StorageResult<()> {
        if self.state.phase < Phase::Pages || index >= self.manifest_page_count() {
            return Err(invalid("manifest page confirmation index"));
        }
        self.confirm_upload(
            &super::upload::object_name(index),
            ObjectKind::ManifestPage,
            response,
        )
    }
}
fn fits(page: &ManifestPage) -> StorageResult<bool> {
    Ok(page.native_files.len() + page.external_dependencies.len()
        <= cloud_backup_protocol::limits::MANIFEST_PAGE_ENTRIES_MAX
        && canonical_bytes(page).map_err(protocol)?.len() as u64
            <= cloud_backup_protocol::limits::MANIFEST_PAGE_PLAINTEXT_MAX_BYTES)
}
