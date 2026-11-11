//! Mutation log route checks, checkpoint staging/release, and transient cache cleanup.
use super::super::{
    bounded_bytes, download, inventory::Inventory, native, runs::coverage::Coverage,
};
use super::*;
use crate::byte_cloud_log::MutationLogPage;
use cloud_backup_protocol::v2::{access::ReadSelector, inventory::InventoryRoot, AcceptedBackup};
use cloud_backup_protocol::Digest;
use fold_db::cloud_backup_plan::ClientNativeFile;
use fold_db::storage::disk_pack::{
    byte_backup::CODEC_ENVELOPE_MAX_BYTES, mutation_log::LogPosition,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File};

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
            version: 2,
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
        })
    }

    pub(super) fn page(
        &mut self,
        files: &[ClientNativeFile],
        root: &MutationLogRoot,
    ) -> Result<()> {
        for file in files {
            add(&mut self.native_files, 1)?;
            add(&mut self.native_bytes, file.native_bytes.get())?;
            if file.path.as_str().split('/').nth(3) != Some("main") {
                return Err(RestoreError::Invalid("mutation route role"));
            }
            add(&mut self.main_files, 1)?;
        }
        if self.native_files > root.native_file_count || self.native_bytes > root.native_bytes {
            return Err(RestoreError::Invalid("mutation file totals"));
        }
        Ok(())
    }

    pub(super) fn save(&self, session: &RestoreSession, root: &MutationLogRoot) -> Result<()> {
        if self.main_files != self.native_files
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

pub(super) fn clear_cache(session: &RestoreSession) -> Result<()> {
    let root = session.root.join("cache");
    local::directory(&root)?;
    let mut files = Vec::new();
    for entry in fs::read_dir(&root)? {
        if files.len() > 2 * cloud_backup_protocol::v2::limits::MAX_PAGES + 8 {
            return Err(RestoreError::Limit("mutation metadata cache files"));
        }
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or(RestoreError::Invalid("mutation cache name"))?;
        let digest = name.strip_suffix(".object").is_some_and(|id| {
            id.len() == 64
                && id
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        });
        if !digest && name != "payload.object" {
            return Err(RestoreError::Invalid("mutation cache entry"));
        }
        local::owner_file(&entry.path(), false)?;
        files.push(entry.path());
    }
    for path in files {
        session.remove_spool_file(&path)?;
    }
    File::open(root)?.sync_all()?;
    Ok(())
}

pub(super) async fn download_checkpoint(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    root: &MutationLogRoot,
    mut inventory: Inventory,
    link_index: u32,
) -> Result<()> {
    let mut coverage = Coverage::new_pages(&root.pages, session, &mut inventory)?;
    let mut routes = Routing::new(session, root, &inventory, link_index)?;
    for batch in root
        .pages
        .chunks(cloud_backup_protocol::v2::limits::MAX_BATCH_ITEMS)
    {
        let selectors = batch
            .iter()
            .map(|page| ReadSelector::Receipt {
                receipt_id: page.receipt_id.clone(),
            })
            .collect();
        let grants = download::grants(session, client, selectors).await?;
        for (expected, grant) in batch.iter().zip(grants) {
            grant.validate_for_receipt(inventory.receipt(&expected.receipt_id)?)?;
            inventory.use_receipt(&expected.receipt_id)?;
            let path = download::fetch(session, client, &grant, true).await?;
            let page = MutationLogPage::decode(
                &bounded_bytes(&path, CODEC_ENVELOPE_MAX_BYTES)?,
                decryption.content_key(),
                &session.accepted().begin,
            )
            .map_err(|_| RestoreError::Invalid("mutation file page"))?;
            root.validate_page(&page)
                .map_err(|_| RestoreError::Invalid("mutation file page context"))?;
            if page.page_index != expected.page_index {
                return Err(RestoreError::Invalid("mutation page order"));
            }
            coverage.page_with(&page.files, &mut inventory, |file| {
                root.validate_file(file)
                    .map_err(|_| RestoreError::Invalid("mutation native file identity"))
            })?;
            routes.page(&page.files, root)?;
            native::restore_at(
                session,
                client,
                &mut inventory,
                &page.files,
                &session.root.join("replay-files"),
            )
            .await?;
        }
    }
    coverage.finish()?;
    inventory.finish()?;
    routes.save(session, root)
}

pub(super) fn release_checkpoint(session: &RestoreSession, root: &MutationLogRoot) -> Result<()> {
    let mut parents = BTreeSet::new();
    for entry in &root.entries {
        for part in &entry.parts {
            let name = fold_db::storage::disk_pack::mutation_log::log_path(
                &root.anchor.epoch,
                entry.sequence,
                part.role,
                &part.run_id,
            );
            let path = session.root.join("replay-files").join(&name);
            match fs::symlink_metadata(&path) {
                Ok(meta) if meta.is_file() && meta.len() == part.bytes => {
                    fs::remove_file(&path)?;
                }
                Ok(_) => return Err(RestoreError::Invalid("mutation source release identity")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            parents.insert(
                path.parent()
                    .ok_or(RestoreError::Invalid("mutation source parent"))?
                    .to_path_buf(),
            );
            session
                .remove_spool_file(&session.root.join("entries").join(native::path_key(&name)))?;
        }
    }
    for parent in parents {
        File::open(parent)?.sync_all()?;
    }
    File::open(session.root.join("entries"))?.sync_all()?;
    File::open(session.root.join("replay-files"))?.sync_all()?;
    clear_cache(session)
}
