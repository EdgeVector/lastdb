use super::{invalid, AssemblySelection, StorageResult};
use cloud_backup_protocol::{Digest, ReferenceRoot};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) enum Phase {
    Content,
    Pages,
    References,
    Index,
    Complete,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    pub format: u16,
    pub selection: AssemblySelection,
    pub key_sha256: Digest,
    pub phase: Phase,
    pub native_fed: u64,
    pub external_fed: u64,
    pub external_next: u64,
    pub manifest_pages: u32,
    pub unique_receipts: u64,
    pub reference_pages: u32,
    pub reference_root: Option<ReferenceRoot>,
}
impl State {
    pub fn new(selection: AssemblySelection, key_sha256: Digest) -> Self {
        Self {
            format: 1,
            selection,
            key_sha256,
            phase: Phase::Content,
            native_fed: 0,
            external_fed: 0,
            external_next: 0,
            manifest_pages: 0,
            unique_receipts: 0,
            reference_pages: 0,
            reference_root: None,
        }
    }
    pub fn validate(&self) -> StorageResult<()> {
        self.selection.begin.validate().map_err(super::protocol)?;
        if self.format != 1
            || self.native_fed > self.selection.native_objects
            || self.external_fed > self.selection.external_dependencies
            || self.external_next > self.selection.dependencies
            || self.manifest_pages as usize > cloud_backup_protocol::limits::MANIFEST_PAGES_MAX
            || self.reference_pages as usize > cloud_backup_protocol::limits::MANIFEST_PAGES_MAX
            || self.unique_receipts > super::receipts::IDS_MAX
        {
            return Err(invalid("manifest assembly state bounds"));
        }
        if self.phase >= Phase::Pages
            && (self.manifest_pages == 0
                || self.native_fed != self.selection.native_objects
                || self.external_fed != self.selection.external_dependencies)
        {
            return Err(invalid("manifest assembly content completeness"));
        }
        if self.phase >= Phase::References
            && (self.reference_pages == 0 || self.reference_root.is_none())
        {
            return Err(invalid("manifest reference completeness"));
        }
        if let Some(root) = &self.reference_root {
            root.validate().map_err(super::protocol)?;
        }
        Ok(())
    }
}
