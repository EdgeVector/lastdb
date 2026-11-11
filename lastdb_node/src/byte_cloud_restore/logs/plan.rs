//! Mutation restore chain planning, route proofs, and checkpoint staging/release.
use super::super::{inventory::Inventory, load_closure, runs::stage_pages};
use super::*;
use crate::byte_cloud_log::MutationLogPage;
use cloud_backup_protocol::v2::{inventory::InventoryRoot, AcceptedBackup};
use cloud_backup_protocol::Digest;
use fold_db::cloud_backup_plan::ClientNativeFile;
use fold_db::storage::disk_pack::mutation_log::LogPosition;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::PathBuf;

const CHAIN_RESERVE: u64 = 2 * 256 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plan {
    pub version: u16,
    pub head: Digest,
    pub base: AcceptedBackup,
    pub links: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached: Option<CachedStart>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CachedStart {
    pub accepted: AcceptedBackup,
    pub position: LogPosition,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Routing {
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

pub(super) async fn prepare(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    latest: &MutationLogRoot,
    cache: Option<&GuestReplayCache<'_>>,
) -> Result<Plan> {
    let path = session.root.join("chain/plan.json");
    if path.try_exists()? {
        let plan: Plan = local::read_json(&path, 256 * 1024)?;
        if plan.version != 1
            || plan.head != session.accepted().commit_sha256
            || plan.base != latest.base
            || plan.links == 0
            || plan.links > MAX_CHAIN
            || plan.cached.as_ref().is_some_and(|start| {
                !matches_cache(cache, &start.accepted, &start.position, &latest.base)
            })
        {
            return Err(RestoreError::Invalid("saved mutation restore plan"));
        }
        return Ok(plan);
    }
    let mut accepted = session.accepted().clone();
    let mut root = latest.clone();
    let mut count = 0;
    let cached = loop {
        if count >= MAX_CHAIN {
            return Err(RestoreError::Limit("mutation restore chain"));
        }
        root.validate_for(&accepted.begin)
            .map_err(|_| RestoreError::Invalid("mutation checkpoint context"))?;
        if root.base != latest.base
            || root.anchor != latest.anchor
            || root.recovery != latest.recovery
            || root.previous.ordinal.get() >= accepted.ordinal.get()
        {
            return Err(RestoreError::Invalid("mutation predecessor chain"));
        }
        let link = link_path(session, count);
        if link.try_exists()? {
            let prior: AcceptedBackup = local::read_json(&link, 256 * 1024)?;
            if prior != accepted {
                return Err(RestoreError::Invalid("saved mutation ancestor"));
            }
        } else {
            session.write_spool_json(&link, CHAIN_RESERVE, &accepted)?;
        }
        count += 1;
        clear_cache(session)?;
        if let Some(cached) = cached_predecessor(cache, &root)? {
            break Some(cached);
        }
        if root.previous == latest.base {
            break None;
        }
        let ancestor = session.ancestor(root.previous.clone())?;
        let (_, bytes) = load_closure(&ancestor, client).await?;
        let previous =
            MutationLogRoot::decode(&bytes, decryption.content_key(), &ancestor.accepted().begin)
                .map_err(|_| RestoreError::Invalid("mutation ancestor root"))?;
        root.validate_previous(&previous)
            .map_err(|_| RestoreError::Invalid("mutation ancestor prefix"))?;
        accepted = root.previous;
        root = previous;
    };
    let plan = Plan {
        version: 1,
        head: session.accepted().commit_sha256.clone(),
        base: latest.base.clone(),
        links: count,
        cached,
    };
    session.write_spool_json(&path, CHAIN_RESERVE, &plan)?;
    Ok(plan)
}

fn matches_cache(
    cache: Option<&GuestReplayCache<'_>>,
    accepted: &AcceptedBackup,
    position: &LogPosition,
    base: &AcceptedBackup,
) -> bool {
    cache.is_some_and(|cache| {
        accepted.head() == *cache.head
            && cache
                .through
                .map_or(accepted == base, |saved| *saved == *position)
    })
}

fn cached_predecessor(
    cache: Option<&GuestReplayCache<'_>>,
    root: &MutationLogRoot,
) -> Result<Option<CachedStart>> {
    if cache.is_none_or(|cache| root.previous.head() != *cache.head) {
        return Ok(None);
    }
    if !matches_cache(cache, &root.previous, &root.from, &root.base) {
        return Err(RestoreError::Invalid(
            "cached mutation checkpoint predecessor",
        ));
    }
    Ok(Some(CachedStart {
        accepted: root.previous.clone(),
        position: root.from.clone(),
    }))
}

pub(super) fn link_path(session: &RestoreSession, index: u32) -> PathBuf {
    session.root.join(format!("chain/link-{index:08}.json"))
}

impl Routing {
    fn new(
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
        if root.entries.is_empty() || root.entries.len() > crate::byte_cloud_log::MAX_BATCH_ENTRIES
        {
            return Err(RestoreError::Invalid("mutation route entry bound"));
        }
        let mut position = root.from.clone();
        let mut entry_sha256s = Vec::with_capacity(root.entries.len());
        for entry in &root.entries {
            position = entry
                .position_after(&position)
                .map_err(|_| RestoreError::Invalid("mutation route entry identity"))?;
            entry_sha256s.push(position.sha256.clone());
        }
        if position != root.through {
            return Err(RestoreError::Invalid("mutation route complete position"));
        }
        Ok(Self {
            version: 2,
            link_index,
            commit_sha256: accepted.commit_sha256.clone(),
            root_receipt_id: accepted.root_receipt_id.clone(),
            inventory_root: accepted.inventory_root.clone(),
            from: root.from.clone(),
            through: root.through.clone(),
            entry_sha256s,
            native_files: 0,
            native_bytes: 0,
            objects,
            metadata_objects,
            main_files: 0,
        })
    }

    fn page(&mut self, files: &[ClientNativeFile], root: &MutationLogRoot) -> Result<()> {
        for file in files {
            if file.path.as_str().split('/').nth(3) != Some("main") {
                return Err(RestoreError::Invalid("mutation route role"));
            }
            self.native_files = self
                .native_files
                .checked_add(1)
                .ok_or(RestoreError::Limit("mutation route total"))?;
            self.main_files = self.native_files;
            self.native_bytes = self
                .native_bytes
                .checked_add(file.native_bytes.get())
                .ok_or(RestoreError::Limit("mutation route total"))?;
        }
        if self.native_files > root.native_file_count || self.native_bytes > root.native_bytes {
            return Err(RestoreError::Invalid("mutation file totals"));
        }
        Ok(())
    }

    fn save(&self, session: &RestoreSession, root: &MutationLogRoot) -> Result<()> {
        if self.main_files != self.native_files
            || self.native_files != root.native_file_count
            || self.native_bytes != root.native_bytes
            || self.objects != self.inventory_root.object_count.get()
            || self.metadata_objects > self.objects
        {
            return Err(RestoreError::Invalid("complete mutation route totals"));
        }
        let accepted: AcceptedBackup =
            local::read_json(&link_path(session, self.link_index), 256 * 1024)?;
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
        session.write_spool_json(&path, CHAIN_RESERVE, self)
    }
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
        let digest = name
            .strip_suffix(".object")
            .is_some_and(|id| Digest::try_from(id.to_owned()).is_ok());
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
    inventory: Inventory,
    link_index: u32,
) -> Result<()> {
    let mut routes = Routing::new(session, root, &inventory, link_index)?;
    let target = session.root.join("replay-files");
    stage_pages(
        session,
        client,
        &root.pages,
        inventory,
        &target,
        |index, raw| {
            let page =
                MutationLogPage::decode(raw, decryption.content_key(), &session.accepted().begin)
                    .map_err(|_| RestoreError::Invalid("mutation file page"))?;
            root.validate_page(&page)
                .map_err(|_| RestoreError::Invalid("mutation file page context"))?;
            if page.page_index != index {
                return Err(RestoreError::Invalid("mutation page order"));
            }
            routes.page(&page.files, root)?;
            page.files
                .into_iter()
                .map(|file| {
                    let dest = root
                        .validate_file(&file)
                        .map_err(|_| RestoreError::Invalid("mutation native file identity"))?;
                    Ok((file, dest))
                })
                .collect()
        },
    )
    .await?;
    routes.save(session, root)
}
