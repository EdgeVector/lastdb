use super::{invalid, protocol, store, ManifestAssembly, Phase, StorageResult};
use crate::storage::disk_pack::backup_capture::BackupCaptureObject;
use cloud_backup_protocol::transfer::{ObjectReceipt, TransferInput, TransferStatus, TransferView};
use cloud_backup_protocol::{Digest, ExternalFileDependency, NativeFileEntry, Version};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct NativeFeed {
    pub ordinal: u64,
    pub input: TransferInput,
    pub receipt: ObjectReceipt,
    pub entries: Vec<NativeFileEntry>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ExternalFeed {
    pub index: u64,
    pub dependency: ExternalFileDependency,
    pub receipts: Vec<ObjectReceipt>,
}
pub(super) fn native_name(index: u64) -> String {
    format!("native-{index:06}.json")
}
pub(super) fn external_name(index: u64) -> String {
    format!("external-{index:06}.json")
}

impl ManifestAssembly<'_> {
    pub fn native_transfer(&self, ordinal: u64) -> StorageResult<Option<TransferView>> {
        if ordinal >= self.state.native_fed {
            return Ok(None);
        }
        let feed = self.native_feed(ordinal)?;
        let view = TransferView {
            version: Version::V1,
            request_id: feed.input.request_id.clone(),
            input_sha256: feed.input.input_sha256().map_err(protocol)?,
            state: TransferStatus::Verified {
                receipt: feed.receipt,
            },
        };
        view.validate_for(&feed.input).map_err(protocol)?;
        Ok(Some(view))
    }

    pub fn feed_native(
        &mut self,
        ordinal: u64,
        object: &BackupCaptureObject,
        response: &TransferView,
    ) -> StorageResult<()> {
        response.validate_for(&object.input).map_err(protocol)?;
        let TransferStatus::Verified { receipt } = &response.state else {
            return Err(invalid(
                "manifest content needs authoritative Verified transfer",
            ));
        };
        if self.capture.assembly_native(ordinal, &mut self.cache)? != *object {
            return Err(invalid("manifest native feed differs from exact capture"));
        }
        let feed = NativeFeed {
            ordinal,
            input: object.input.clone(),
            receipt: receipt.clone(),
            entries: object.native_entries(receipt)?,
        };
        if ordinal > self.state.native_fed
            || (ordinal == self.state.native_fed && self.state.phase != Phase::Content)
        {
            return Err(invalid("manifest native feed order"));
        }
        self.store
            .immutable(self.capture, &native_name(ordinal), &feed, store::FEED_MAX)?;
        if ordinal == self.state.native_fed {
            self.state.native_fed += 1;
            self.save()?;
        }
        Ok(())
    }

    pub fn feed_external(
        &mut self,
        index: u64,
        dependency: &ExternalFileDependency,
        receipts: &[ObjectReceipt],
    ) -> StorageResult<()> {
        let Some((expected, expected_receipts)) = self.capture.assembly_external(index)? else {
            return Err(invalid(
                "native-covered file cannot enter external manifest",
            ));
        };
        if expected != *dependency || expected_receipts != receipts {
            return Err(invalid("manifest external feed differs from exact capture"));
        }
        let feed = ExternalFeed {
            index,
            dependency: dependency.clone(),
            receipts: receipts.to_vec(),
        };
        self.validate_external(&feed)?;
        if index < self.state.external_next {
            return self.store.immutable(
                self.capture,
                &external_name(index),
                &feed,
                store::FEED_MAX,
            );
        }
        if self.state.phase != Phase::Content {
            return Err(invalid("manifest content is already frozen"));
        }
        for skipped in self.state.external_next..index {
            if self.capture.assembly_external(skipped)?.is_some() {
                return Err(invalid("manifest external feed skipped a dependency"));
            }
        }
        self.store
            .immutable(self.capture, &external_name(index), &feed, store::FEED_MAX)?;
        self.state.external_next = index + 1;
        self.state.external_fed += 1;
        self.save()
    }

    pub fn finish_content(&mut self) -> StorageResult<()> {
        if self.state.phase != Phase::Content {
            return Ok(());
        }
        if self.state.native_fed != self.state.selection.native_objects
            || self.state.external_fed != self.state.selection.external_dependencies
        {
            return Err(invalid("manifest content feed is incomplete"));
        }
        self.cache = None;
        self.check_content()?;
        self.build_pages()?;
        self.state.phase = Phase::Pages;
        self.save()
    }

    pub(super) fn native_feed(&self, ordinal: u64) -> StorageResult<NativeFeed> {
        let feed: NativeFeed = self.store.read(&native_name(ordinal), store::FEED_MAX)?;
        feed.input.validate().map_err(protocol)?;
        feed.receipt.validate().map_err(protocol)?;
        if feed.ordinal != ordinal
            || feed.input.job != self.state.selection.begin.job
            || feed.receipt.scope != feed.input.job.stream.scope
            || feed.receipt.object != feed.input.object
            || feed.entries.is_empty()
            || feed.entries.len() > 512
        {
            return Err(invalid("manifest native feed identity"));
        }
        for entry in &feed.entries {
            entry.validate().map_err(protocol)?;
        }
        Ok(feed)
    }
    pub(super) fn external_feed(&self, index: u64) -> StorageResult<ExternalFeed> {
        let feed: ExternalFeed = self.store.read(&external_name(index), store::FEED_MAX)?;
        if feed.index != index {
            return Err(invalid("manifest external feed index"));
        }
        self.validate_external(&feed)?;
        Ok(feed)
    }
    fn validate_external(&self, feed: &ExternalFeed) -> StorageResult<()> {
        feed.dependency.validate().map_err(protocol)?;
        if feed.receipts.len() != feed.dependency.extents.len() {
            return Err(invalid("manifest external receipts"));
        }
        for (extent, receipt) in feed.dependency.extents.iter().zip(&feed.receipts) {
            receipt.validate().map_err(protocol)?;
            if receipt.scope != self.state.selection.begin.job.stream.scope
                || receipt.object.kind != cloud_backup_protocol::ObjectKind::BlobPart
                || receipt.receipt_id != extent.receipt_id
                || extent
                    .offset
                    .get()
                    .checked_add(extent.length.get())
                    .is_none_or(|end| end > receipt.object.bytes.get())
            {
                return Err(invalid("manifest external extent receipt"));
            }
        }
        Ok(())
    }

    fn check_content(&self) -> StorageResult<()> {
        let selection = &self.state.selection;
        if selection.external_dependencies
            > (lastdb_storage_policy::backup_decode::FILE_COVERAGE_MEMORY_BYTES
                / lastdb_storage_policy::backup_decode::FILE_COVERAGE_IDENTITY_BYTES)
                as u64
        {
            return Err(invalid(
                "manifest external inventory exceeds current restore memory admission",
            ));
        }
        let mut paths = Vec::<[u8; 32]>::with_capacity(selection.native_files as usize);
        let mut bytes = 0u64;
        for ordinal in 0..self.state.native_fed {
            for entry in self.native_feed(ordinal)?.entries {
                if paths.len() as u64 >= selection.native_files {
                    return Err(invalid("manifest native count exceeds selection"));
                }
                paths.push(raw_digest(&cloud_backup_protocol::bytes_sha256(
                    entry.path.as_str().as_bytes(),
                ))?);
                bytes = bytes
                    .checked_add(entry.native_bytes.get())
                    .ok_or_else(|| invalid("manifest native byte overflow"))?;
            }
        }
        if paths.len() as u64 != selection.native_files || bytes != selection.native_bytes {
            return Err(invalid("manifest exact native totals"));
        }
        paths.sort_unstable();
        if paths.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("manifest duplicate native path"));
        }
        drop(paths);
        let mut pointers = Vec::<[u8; 33]>::with_capacity(selection.external_dependencies as usize);
        for index in 0..selection.dependencies {
            if !self.store.exists(&external_name(index))? {
                continue;
            }
            let feed = self.external_feed(index)?;
            if pointers.len() as u64 >= selection.external_dependencies {
                return Err(invalid("manifest external count exceeds selection"));
            }
            let mut id = [0; 33];
            id[..32].copy_from_slice(&raw_digest(&feed.dependency.logical_pointer_sha256)?);
            id[32] = match feed.dependency.kind {
                cloud_backup_protocol::FileDependencyKind::Main => 0,
                cloud_backup_protocol::FileDependencyKind::Thumbnail => 1,
                cloud_backup_protocol::FileDependencyKind::Poster => 2,
            };
            pointers.push(id);
        }
        if pointers.len() as u64 != selection.external_dependencies {
            return Err(invalid("manifest external count"));
        }
        pointers.sort_unstable();
        if pointers.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("manifest duplicate pointer and kind"));
        }
        Ok(())
    }
}

pub(super) fn raw_digest(value: &Digest) -> StorageResult<[u8; 32]> {
    crate::hex::hex_decode_array(value.as_str()).ok_or_else(|| invalid("manifest digest"))
}
