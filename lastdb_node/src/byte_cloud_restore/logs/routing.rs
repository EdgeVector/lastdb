//! Small local evidence for the route checks on one exact accepted log link.
use super::{local, plan, MutationLogRoot, RestoreError, RestoreSession, Result};
use crate::byte_cloud_restore::inventory::Inventory;
use cloud_backup_protocol::v2::{inventory::InventoryRoot, AcceptedBackup};
use cloud_backup_protocol::Digest;
use fold_db::storage::disk_pack::{byte_backup::ClientNativeFile, mutation_log::LogPosition};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Routing {
    version: u16,
    link_index: u32,
    commit_sha256: Digest,
    root_receipt_id: Digest,
    inventory_root: InventoryRoot,
    from: LogPosition,
    through: LogPosition,
    entry_sha256s: Vec<Digest>,
    native_files: u64,
    native_bytes: u64,
    objects: u64,
    metadata_objects: u64,
    main_files: u64,
    cas_add_files: u64,
    cas_delete_files: u64,
}

impl Routing {
    pub(super) fn new(
        session: &RestoreSession,
        root: &MutationLogRoot,
        inventory: &Inventory,
        link_index: u32,
    ) -> Result<Self> {
        let accepted = session.accepted();
        let objects = u64::try_from(inventory.receipts.len())
            .map_err(|_| RestoreError::Limit("mutation route object count"))?;
        let metadata_objects = u64::try_from(root.pages.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or(RestoreError::Limit("mutation route metadata count"))?;
        Ok(Self {
            version: 1,
            link_index,
            commit_sha256: accepted.commit_sha256.clone(),
            root_receipt_id: accepted.root_receipt_id.clone(),
            inventory_root: accepted.inventory_root.clone(),
            from: root.from.clone(),
            through: root.through.clone(),
            entry_sha256s: entry_hashes(root)?,
            native_files: 0,
            native_bytes: 0,
            objects,
            metadata_objects,
            main_files: 0,
            cas_add_files: 0,
            cas_delete_files: 0,
        })
    }

    /// Call only after page context, file paths and receipt destinations pass.
    pub(super) fn page(
        &mut self,
        files: &[ClientNativeFile],
        root: &MutationLogRoot,
    ) -> Result<()> {
        for file in files {
            add(&mut self.native_files, 1)?;
            add(&mut self.native_bytes, file.native_bytes.get())?;
            let count = match file.path.as_str().split('/').nth(3) {
                Some("main") => &mut self.main_files,
                Some("cas-add") => &mut self.cas_add_files,
                Some("cas-delete") => &mut self.cas_delete_files,
                _ => return Err(RestoreError::Invalid("mutation route role")),
            };
            add(count, 1)?;
        }
        if self.native_files > root.native_file_count || self.native_bytes > root.native_bytes {
            return Err(RestoreError::Invalid("mutation file totals"));
        }
        Ok(())
    }

    /// The caller completes both coverage and inventory checks before this write.
    pub(super) fn save(&self, session: &RestoreSession, root: &MutationLogRoot) -> Result<()> {
        let roles = self
            .main_files
            .checked_add(self.cas_add_files)
            .and_then(|count| count.checked_add(self.cas_delete_files));
        if roles != Some(self.native_files)
            || self.native_files != root.native_file_count
            || self.native_bytes != root.native_bytes
            || self.objects != self.inventory_root.object_count.get()
            || self.metadata_objects > self.objects
        {
            return Err(RestoreError::Invalid("complete mutation route totals"));
        }
        let accepted: AcceptedBackup =
            local::read_json(&plan::link_path(session, self.link_index), 256 * 1024)?;
        if accepted != *session.accepted() {
            return Err(RestoreError::Invalid("mutation route accepted link"));
        }
        let path = session
            .root
            .join(format!("chain/routes-{:08}.json", self.link_index));
        if path.try_exists()? {
            let saved: Self = local::read_json(&path, 256 * 1024)?;
            if saved != *self {
                return Err(RestoreError::Invalid("saved mutation route evidence"));
            }
            return Ok(());
        }
        plan::save(session, &path, self)
    }
}

fn add(total: &mut u64, count: u64) -> Result<()> {
    *total = total
        .checked_add(count)
        .ok_or(RestoreError::Limit("mutation route total"))?;
    Ok(())
}

fn entry_hashes(root: &MutationLogRoot) -> Result<Vec<Digest>> {
    if root.entries.is_empty() || root.entries.len() > crate::byte_cloud_log::MAX_BATCH_ENTRIES {
        return Err(RestoreError::Invalid("mutation route entry bound"));
    }
    let mut position = root.from.clone();
    let mut hashes = Vec::with_capacity(root.entries.len());
    for entry in &root.entries {
        position = entry
            .position_after(&position)
            .map_err(|_| RestoreError::Invalid("mutation route entry identity"))?;
        hashes.push(position.sha256.clone());
    }
    if position != root.through {
        return Err(RestoreError::Invalid("mutation route complete position"));
    }
    Ok(hashes)
}
