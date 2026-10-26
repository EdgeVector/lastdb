use super::dependency::Dependencies;
use super::generation;
use super::live_parts::{self, Changes, LiveParts, ReferenceCache};
use super::locator_forest::LocatorChanges;
use super::pack_write::WrittenData;
use super::snapshot::PreparedMutation;
use super::tree_edit::TreeEdit;
use super::write_lock::WriteLock;
use super::{artifact, corrupt, Current, DirectoryEntry};
use crate::{durability, Result};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use uuid::Uuid;

/// One durable artifact publication. It releases no memory durability token.
pub struct PublishedGeneration {
    /// Exact CURRENT after all file and directory barriers.
    pub current: Current,
    /// Immutable receipt identity that retains the complete CURRENT bytes.
    pub receipt: Uuid,
    /// Complete parts with positive live references in this snapshot.
    pub dependency_parts: u64,
    /// Operations included in this complete snapshot commit.
    pub mutations: u64,
    /// New tree pages, including superseded private intermediate roots.
    pub metadata_pages_written: u64,
    /// Fresh metadata parts; no published part receives an append.
    pub metadata_parts_written: u64,
}

struct Publisher<'a> {
    root: &'a Path,
    proposed: Current,
    directory: TreeEdit,
    locators: LocatorChanges,
    pages: u64,
    parts: u64,
    mutations: u64,
    dependencies: Dependencies,
}

pub(in crate::disk_pack) struct PublicationBase {
    pub(in crate::disk_pack) current: Current,
    pub(in crate::disk_pack) live: LiveParts,
    pub(in crate::disk_pack) cached: Option<generation::Generation>,
}

/// Publish prepared direct payload locations with one complete root change.
pub(super) fn publish_prepared(
    lock: &WriteLock,
    previous: &Current,
    proposed: Current,
    mutations: Vec<PreparedMutation>,
    payload: Option<WrittenData>,
    legacy_live: Option<LiveParts>,
) -> Result<PublishedGeneration> {
    publish_with_base(
        lock,
        previous,
        proposed,
        mutations,
        payload,
        legacy_live,
        None,
    )
}

pub(super) fn publish_prepared_base(
    lock: &WriteLock,
    previous: &Current,
    proposed: Current,
    mutations: Vec<PreparedMutation>,
    payload: Option<WrittenData>,
    base: PublicationBase,
) -> Result<PublishedGeneration> {
    publish_with_base(
        lock,
        previous,
        proposed,
        mutations,
        payload,
        None,
        Some(base),
    )
}

fn publish_with_base(
    lock: &WriteLock,
    previous: &Current,
    mut proposed: Current,
    mutations: Vec<PreparedMutation>,
    payload: Option<WrittenData>,
    legacy_live: Option<LiveParts>,
    base: Option<PublicationBase>,
) -> Result<PublishedGeneration> {
    let root = lock.root();
    let old_receipt =
        crate::commit_timing::during("prior_generation_us", || ensure_generation(lock, previous))?;
    let lookup_timing = crate::commit_timing::stage("lookup_files_us");
    let rewritten = base.is_some();
    let (base_current, base_live, cached) = match base {
        Some(base) => (base.current, Some(base.live), base.cached),
        None => (previous.clone(), None, None),
    };
    let mut live = if let Some(live) = base_live {
        live
    } else if previous.required_features & super::FEATURE_LIVE_PART_COUNTS != 0 {
        LiveParts::from_receipt(&old_receipt)?
    } else {
        legacy_live
            .ok_or_else(|| corrupt("uncounted writer lacks its admitted live-part bootstrap"))?
    };
    proposed.required_features |= super::FEATURE_LIVE_PART_COUNTS;
    let mut references = ReferenceCache::new(root, &proposed);
    let mut publisher = Publisher::new(root, proposed, &base_current)?;
    if let Some(payload) = payload {
        publisher.dependencies.add_payload(
            payload.object,
            payload.data_parts,
            payload.map_pages,
        )?;
    }
    crate::commit_timing::during("locator_edits_us", || {
        publisher.batch(mutations, &mut references)
    })?;
    let mut changes = Changes::default();
    let outputs =
        crate::commit_timing::during("locator_finish_us", || publisher.locators.finish())?;
    for output in outputs {
        changes.merge(output.changes)?;
        publisher.pages += output.pages;
        publisher.parts += output.parts.len() as u64;
        publisher
            .dependencies
            .add_tree(&output.tree, output.parts)?;
    }
    let (directory, pages, parts, retired) =
        crate::commit_timing::during("directory_finish_us", || {
            publisher.directory.finish_with_changes()
        })?;
    changes.merge(retired)?;
    publisher.proposed.directory = directory;
    publisher.pages += pages;
    publisher.parts += parts.len() as u64;
    publisher
        .dependencies
        .add_tree(&publisher.proposed.directory, parts)?;
    let dependencies = publisher.dependencies.finish();
    crate::commit_timing::during("live_parts_us", || {
        finish_live(
            root,
            &base_current,
            &publisher.proposed,
            &mut live,
            &dependencies,
            changes,
            references,
        )
    })?;
    drop(lookup_timing);
    let generation_timing = crate::commit_timing::stage("new_generation_us");
    let receipt = generation::write_live_cached(
        root,
        &publisher.proposed,
        &live,
        Some(&old_receipt),
        cached.as_ref(),
    )?;
    drop(generation_timing);
    install_publication(
        lock,
        previous,
        rewritten,
        PublishedGeneration {
            current: publisher.proposed,
            receipt: receipt.object,
            dependency_parts: receipt.dependencies,
            mutations: publisher.mutations,
            metadata_pages_written: publisher.pages,
            metadata_parts_written: publisher.parts,
        },
    )
}

fn install_publication(
    lock: &WriteLock,
    previous: &Current,
    rewritten: bool,
    publication: PublishedGeneration,
) -> Result<PublishedGeneration> {
    crate::commit_timing::count("metadata_pages", publication.metadata_pages_written);
    crate::commit_timing::count("metadata_parts", publication.metadata_parts_written);
    crate::commit_timing::count("dependency_parts", publication.dependency_parts);
    crate::commit_timing::count("storage_mutations", publication.mutations);
    let _timing = crate::commit_timing::stage("root_publication_us");
    // A changed CURRENT under the lock is an unsupported writer, never a merge guess.
    if artifact::current(lock.root())? != *previous {
        return Err(corrupt("CURRENT changed during publication"));
    }
    if rewritten {
        // A compacted base can retain older coordinates than the latest root.
        // Retire captured deletion plans before those coordinates become live.
        lock.files.invalidate()?;
    }
    publish_current(lock.root(), &publication.current)?;
    lock.remember_checked(&publication.current)?;
    Ok(publication)
}

fn finish_live(
    root: &Path,
    previous: &Current,
    proposed: &Current,
    live: &mut LiveParts,
    dependencies: &[super::dependency::Coordinate],
    mut changes: Changes,
    mut references: ReferenceCache,
) -> Result<()> {
    live_parts::observe_new_parts(root, proposed, dependencies, &mut changes, &mut references)?;
    drop(references);
    live_parts::add_directory_empty_root(root, previous, -1, &mut changes)?;
    live_parts::add_directory_empty_root(root, proposed, 1, &mut changes)?;
    live.apply(root, proposed, changes)
}

impl<'a> Publisher<'a> {
    fn new(root: &'a Path, proposed: Current, base: &Current) -> Result<Self> {
        Ok(Self {
            root,
            locators: LocatorChanges::new(root, proposed.generation),
            directory: TreeEdit::new(root, base.directory.clone(), proposed.generation)?,
            proposed,
            pages: 0,
            parts: 0,
            mutations: 0,
            dependencies: Dependencies::new(),
        })
    }

    fn batch(
        &mut self,
        mut mutations: Vec<PreparedMutation>,
        references: &mut ReferenceCache,
    ) -> Result<()> {
        crate::commit_timing::count("publisher_batch_calls", 1);
        self.mutations += mutations.len() as u64;
        // Physical sort preserves the original operation version for repeated keys.
        mutations.sort_by(|a, b| {
            a.collection
                .as_bytes()
                .cmp(b.collection.as_bytes())
                .then(a.group.cmp(&b.group))
                .then(a.key.as_bytes().cmp(b.key.as_bytes()))
                .then(a.location.version.cmp(&b.location.version))
        });
        let mut items = mutations.into_iter().peekable();
        let mut directory_changes = BTreeMap::new();
        let mut directory_call = artifact::metadata_call(self.root, self.proposed.clone());
        // Share only bounded descriptors, never a group's pages, checks, or budget.
        let mut locator_files = super::call_io::CallFiles::default();
        while let Some(first) = items.next() {
            crate::commit_timing::count("snapshot_touched_groups", 1);
            let collection = first.collection.clone();
            let group = first.group;
            let mut changes = BTreeMap::new();
            changes.insert(
                first.key.into_bytes(),
                first
                    .location
                    .encode_for_features(self.proposed.required_features)?,
            );
            while items
                .peek()
                .is_some_and(|item| item.collection == collection && item.group == group)
            {
                let item = items.next().expect("peeked operation");
                changes.insert(
                    item.key.into_bytes(),
                    item.location
                        .encode_for_features(self.proposed.required_features)?,
                );
            }
            let key = super::ReadCall::directory_key(&collection, group)?;
            let mut call = artifact::metadata_call(self.root, self.proposed.clone());
            call.files = std::mem::take(&mut locator_files);
            let entry = crate::commit_timing::during("directory_lookup_us", || {
                self.directory_entry(&mut directory_call, &key)
            })?
            .map(|value| DirectoryEntry::decode(&value))
            .transpose()?;
            if entry.as_ref().is_some_and(|entry| entry.group != group) {
                return Err(corrupt("publication directory coordinate mismatch"));
            }
            let changes: Vec<_> = changes.into_iter().collect();
            let locator_timing = crate::commit_timing::stage("locator_tree_edit_us");
            let tree = if let Some(entry) = &entry {
                self.locators
                    .edit(&mut call, references, entry.locator.clone(), &changes)?
            } else {
                if changes.len() > self.proposed.batch_operation_limit() {
                    return Err(corrupt("invalid new locator change count"));
                }
                self.locators
                    .create(&mut call, references, self.directory.root(), &changes)?
            };
            drop(locator_timing);
            let value = DirectoryEntry {
                group,
                locator: tree,
                converted_frontier: entry.map_or(0, |entry| entry.converted_frontier),
            }
            .encode()?;
            directory_changes.insert(key, value);
            locator_files = std::mem::take(&mut call.files);
        }
        // Release locator descriptors before the separate directory edit phase.
        drop(locator_files);
        let mut call = artifact::metadata_call(self.root, self.proposed.clone());
        let _directory_timing = crate::commit_timing::stage("directory_edit_us");
        self.directory.put_many(
            &mut call,
            references,
            &directory_changes.into_iter().collect::<Vec<_>>(),
        )
    }
    fn directory_entry(&self, call: &mut super::ReadCall, key: &[u8]) -> Result<Option<[u8; 128]>> {
        // Reuse only immutable committed directory pages. Locator pages, checks,
        // and budgets remain group-local; their descriptors live for this batch.
        match call.find(self.directory.root(), key) {
            Err(crate::Error::ReadBudgetExceeded) => {
                *call = artifact::metadata_call(self.root, self.proposed.clone());
                call.find(self.directory.root(), key)
            }
            result => result,
        }
    }
}

pub(super) fn ensure_generation(
    lock: &WriteLock,
    current: &Current,
) -> Result<generation::Generation> {
    let root = lock.root();
    let checked = lock.checked(current)?;
    let generation = if checked {
        generation::read_receipt(root, current)?
    } else {
        generation::verify(root, current)?
    };
    lock.remember_checked(current)?;
    Ok(generation)
}

pub(super) fn fresh_generation(root: &Path, previous: u64) -> Result<u64> {
    for attempt in 0..8 {
        let gap = if attempt == 0 {
            1
        } else {
            1 + u64::from(u32::from_le_bytes(
                Uuid::new_v4().as_bytes()[..4]
                    .try_into()
                    .expect("UUID prefix"),
            ))
        };
        let generation = previous
            .checked_add(gap)
            .ok_or_else(|| corrupt("generation exhaustion"))?;
        if !root
            .join("generations")
            .join(generation.to_string())
            .try_exists()?
        {
            return Ok(generation);
        }
    }
    Err(corrupt("generation allocation exhausted bounded attempts"))
}

pub(super) fn publish_current(root: &Path, current: &Current) -> Result<()> {
    let path = root.join(format!("CURRENT.pending-{}", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    file.write_all(&current.encode()?)?;
    durability::sync_publication_file(&file)?;
    fs::rename(path, root.join("CURRENT"))?;
    durability::sync_directory(&File::open(root)?)
}
