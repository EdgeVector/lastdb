//! Mutation restore chain planning, route proofs, and checkpoint staging/release.
use super::super::{load_closure, runs::stage_pages};
use super::*;
use crate::byte_cloud_log::{MutationLogPage, MAX_BATCH_ENTRIES};
use cloud_backup_protocol::v2::{inventory::InventoryRoot, limits::MAX_PAGES, AcceptedBackup};
use cloud_backup_protocol::Digest;
use fold_db::cloud_backup_plan::ClientNativeFile;
use fold_db::storage::disk_pack::mutation_log::LogPosition;
use serde::{Deserialize, Serialize};
use std::fs;
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
        let p: Plan = local::read_json(&path, 256 * 1024)?;
        if p.version != 1
            || p.head != session.accepted().commit_sha256
            || p.base != latest.base
            || !(1..=MAX_CHAIN).contains(&p.links)
            || p.cached
                .as_ref()
                .is_some_and(|s| !matches_cache(cache, &s.accepted, &s.position, &latest.base))
        {
            return Err(RestoreError::Invalid("saved mutation restore plan"));
        }
        return Ok(p);
    }
    let (mut accepted, mut root, mut count) = (session.accepted().clone(), latest.clone(), 0);
    let cached = loop {
        if count >= MAX_CHAIN {
            return Err(RestoreError::Limit("mutation restore chain"));
        }
        root.validate_for(&accepted.begin)
            .map_err(|_| RestoreError::Invalid("mutation checkpoint context"))?;
        if (
            &root.base,
            &root.anchor,
            &root.recovery,
            root.previous.ordinal.get() >= accepted.ordinal.get(),
        ) != (&latest.base, &latest.anchor, &latest.recovery, false)
        {
            return Err(RestoreError::Invalid("mutation predecessor chain"));
        }
        session.save_or_verify_spool_json(
            &link_path(session, count),
            CHAIN_RESERVE,
            &accepted,
            "saved mutation ancestor",
        )?;
        count += 1;
        clear_cache(session)?;
        if cache.is_some_and(|c| root.previous.head() == *c.head) {
            if !matches_cache(cache, &root.previous, &root.from, &root.base) {
                return Err(RestoreError::Invalid(
                    "cached mutation checkpoint predecessor",
                ));
            }
            break Some(CachedStart {
                accepted: root.previous.clone(),
                position: root.from.clone(),
            });
        }
        if root.previous == latest.base {
            break None;
        }
        let ancestor = session.ancestor(root.previous.clone())?;
        let (_, bytes) = load_closure(&ancestor, client).await?;
        let prev = MutationLogRoot::decode(&bytes, &decryption.content, &ancestor.accepted().begin)
            .map_err(|_| RestoreError::Invalid("mutation ancestor root"))?;
        root.validate_previous(&prev)
            .map_err(|_| RestoreError::Invalid("mutation ancestor prefix"))?;
        (accepted, root) = (root.previous, prev);
    };
    let plan = Plan {
        version: 1,
        head: session.accepted().commit_sha256.clone(),
        base: latest.base.clone(),
        links: count,
        cached,
    };
    session
        .write_spool_json(&path, CHAIN_RESERVE, &plan)
        .map(|()| plan)
}

fn matches_cache(
    cache: Option<&GuestReplayCache<'_>>,
    accepted: &AcceptedBackup,
    position: &LogPosition,
    base: &AcceptedBackup,
) -> bool {
    cache.is_some_and(|c| {
        accepted.head() == *c.head && c.through.map_or(accepted == base, |s| *s == *position)
    })
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
            .and_then(|c| c.checked_add(1))
            .ok_or(RestoreError::Limit("mutation route metadata count"))?;
        if !(1..=MAX_BATCH_ENTRIES).contains(&root.entries.len()) {
            return Err(RestoreError::Invalid("mutation route entry bound"));
        }
        let mut pos = root.from.clone();
        let mut entry_sha256s = Vec::with_capacity(root.entries.len());
        for entry in &root.entries {
            pos = entry
                .position_after(&pos)
                .map_err(|_| RestoreError::Invalid("mutation route entry identity"))?;
            entry_sha256s.push(pos.sha256.clone());
        }
        if pos != root.through {
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
        if local::read_json::<AcceptedBackup>(&link_path(session, self.link_index), 256 * 1024)?
            != *session.accepted()
        {
            return Err(RestoreError::Invalid("mutation route accepted link"));
        }
        session.save_or_verify_spool_json(
            &session
                .root
                .join(format!("chain/routes-{:08}.json", self.link_index)),
            CHAIN_RESERVE,
            self,
            "saved mutation route evidence",
        )
    }
}

pub(super) fn clear_cache(session: &RestoreSession) -> Result<()> {
    let root = session.root.join("cache");
    local::directory(&root)?;
    let mut files = Vec::new();
    for entry in fs::read_dir(&root)? {
        if files.len() > 2 * MAX_PAGES + 8 {
            return Err(RestoreError::Limit("mutation metadata cache files"));
        }
        let entry = entry?;
        let fname = entry.file_name();
        let name = fname
            .to_str()
            .ok_or(RestoreError::Invalid("mutation cache name"))?;
        let is_obj = name
            .strip_suffix(".object")
            .is_some_and(|id| Digest::try_from(id.to_owned()).is_ok());
        if !is_obj && name != "payload.object" {
            return Err(RestoreError::Invalid("mutation cache entry"));
        }
        local::owner_file(&entry.path(), false)?;
        files.push(entry.path());
    }
    files
        .iter()
        .try_for_each(|p| session.remove_spool_file(p))?;
    local::sync_dir(&root)
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
        |index, bytes| {
            let page =
                MutationLogPage::decode(bytes, &decryption.content, &session.accepted().begin)
                    .map_err(|_| RestoreError::Invalid("mutation file page"))?;
            root.validate_page(&page)
                .map_err(|_| RestoreError::Invalid("mutation file page context"))?;
            if page.page_index != index {
                return Err(RestoreError::Invalid("mutation page order"));
            }
            routes.page(&page.files, root)?;
            page.files
                .into_iter()
                .map(|f| {
                    let dest = root
                        .validate_file(&f)
                        .map_err(|_| RestoreError::Invalid("mutation native file identity"))?;
                    Ok((f, dest))
                })
                .collect()
        },
    )
    .await?;
    routes.save(session, root)
}
