use super::super::{inventory::Inventory, RestoreError, RestoreSession, Result};
use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::Digest;
use fold_db::storage::disk_pack::byte_backup::{
    run_backup_destination, ClientNativeFile, ClientRoot,
};
use std::collections::{BTreeMap, BTreeSet};

pub(in crate::byte_cloud_restore) struct Coverage {
    metadata: BTreeSet<[u8; 32]>,
    data: BTreeMap<[u8; 32], Object>,
}
struct Object {
    bytes: u64,
    ranges: BTreeMap<u64, u64>,
}

impl Coverage {
    pub(in crate::byte_cloud_restore) fn new(
        root: &ClientRoot,
        session: &RestoreSession,
        inventory: &mut Inventory,
    ) -> Result<Self> {
        Self::new_pages(&root.pages, session, inventory)
    }

    pub(in crate::byte_cloud_restore) fn new_pages(
        pages: &[fold_db::storage::disk_pack::byte_backup::ClientPageReference],
        session: &RestoreSession,
        inventory: &mut Inventory,
    ) -> Result<Self> {
        let mut value = Self {
            metadata: BTreeSet::new(),
            data: BTreeMap::new(),
        };
        value.metadata(&session.accepted().root_receipt_id, inventory)?;
        for page in pages {
            let receipt = inventory.receipt(&page.receipt_id)?;
            if receipt.object.sha256 != page.sha256 || receipt.object.bytes != page.bytes {
                return Err(RestoreError::Invalid("run page receipt"));
            }
            value.metadata(&page.receipt_id, inventory)?;
        }
        Ok(value)
    }

    fn metadata(&mut self, id: &Digest, inventory: &mut Inventory) -> Result<()> {
        if inventory.receipt(id)?.object.destination != Some(Destination::R2) {
            return Err(RestoreError::Invalid("run metadata destination"));
        }
        inventory.charge(lastdb_storage_policy::backup_decode::IDENTITY_MEMORY_BYTES)?;
        if !self.metadata.insert(digest(id)?) {
            return Err(RestoreError::Invalid("duplicate run metadata receipt"));
        }
        Ok(())
    }

    pub(in crate::byte_cloud_restore) fn page(
        &mut self,
        files: &[ClientNativeFile],
        inventory: &mut Inventory,
    ) -> Result<()> {
        self.page_with(files, inventory, |file| {
            run_backup_destination(file.path.as_str())
                .map_err(|_| RestoreError::Invalid("run native path"))
        })
    }

    pub(in crate::byte_cloud_restore) fn page_with(
        &mut self,
        files: &[ClientNativeFile],
        inventory: &mut Inventory,
        destination: impl Fn(&ClientNativeFile) -> Result<Destination>,
    ) -> Result<()> {
        for file in files {
            let destination = destination(file)?;
            for extent in &file.extents {
                let receipt = inventory.receipt(&extent.receipt_id)?;
                let id = digest(&extent.receipt_id)?;
                if self.metadata.contains(&id) || receipt.object.destination != Some(destination) {
                    return Err(RestoreError::Invalid("run native receipt destination"));
                }
                let bytes = receipt.object.bytes.get();
                let object = match self.data.entry(id) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        inventory
                            .charge(lastdb_storage_policy::backup_decode::IDENTITY_MEMORY_BYTES)?;
                        entry.insert(Object {
                            bytes,
                            ranges: BTreeMap::new(),
                        })
                    }
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                };
                let start = extent.offset.get();
                let length = extent.length.get();
                // A deduplicated immutable object can feed identical extents
                // in several files. Charge and cover that exact range once.
                if object
                    .ranges
                    .get(&start)
                    .is_some_and(|end| Some(*end) == start.checked_add(length))
                {
                    continue;
                }
                inventory.charge(lastdb_storage_policy::backup_decode::IDENTITY_MEMORY_BYTES)?;
                object.add(start, length)?;
            }
        }
        Ok(())
    }

    pub(in crate::byte_cloud_restore) fn finish(&self) -> Result<()> {
        for object in self.data.values() {
            let mut cursor = 0;
            for (&start, &end) in &object.ranges {
                if start != cursor {
                    return Err(RestoreError::Invalid("run receipt coverage gap"));
                }
                cursor = end;
            }
            if cursor != object.bytes {
                return Err(RestoreError::Invalid("run complete receipt coverage"));
            }
        }
        Ok(())
    }
}

impl Object {
    fn add(&mut self, start: u64, length: u64) -> Result<()> {
        let end = start
            .checked_add(length)
            .ok_or(RestoreError::Invalid("run extent overflow"))?;
        if length == 0
            || end > self.bytes
            || self
                .ranges
                .range(..=start)
                .next_back()
                .is_some_and(|(_, &last)| last > start)
            || self
                .ranges
                .range(start..)
                .next()
                .is_some_and(|(&next, _)| next < end)
        {
            return Err(RestoreError::Invalid(
                "run receipt extent overlap or bounds",
            ));
        }
        self.ranges.insert(start, end);
        Ok(())
    }
}

fn digest(value: &Digest) -> Result<[u8; 32]> {
    hex::decode(value.as_str())
        .map_err(|_| RestoreError::Invalid("run receipt digest"))?
        .try_into()
        .map_err(|_| RestoreError::Invalid("run receipt digest"))
}
