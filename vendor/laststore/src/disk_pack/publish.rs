use super::dependency::Dependencies;
use super::generation;
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
    /// Complete retained dependency parts, including conservative prior references.
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

/// Publish prepared direct payload locations with one complete root change.
pub(super) fn publish_prepared(
    lock: &WriteLock,
    previous: &Current,
    proposed: Current,
    mutations: Vec<PreparedMutation>,
    payload: Option<WrittenData>,
) -> Result<PublishedGeneration> {
    let root = lock.root();
    let old_receipt = ensure_generation(lock, previous)?;
    let directory = TreeEdit::new(root, previous.directory.clone(), proposed.generation)?;
    let mut publisher = Publisher {
        root,
        locators: LocatorChanges::new(root, proposed.generation),
        proposed,
        directory,
        pages: 0,
        parts: 0,
        mutations: 0,
        dependencies: Dependencies::from_coordinates(old_receipt.coordinates.clone())?,
    };
    if let Some(payload) = payload {
        publisher.dependencies.add_payload(
            payload.object,
            0,
            payload.data_parts,
            payload.map_pages,
        )?;
    }
    publisher.batch(mutations)?;
    for output in publisher.locators.finish()? {
        publisher.pages += output.pages;
        publisher.parts += output.parts.len() as u64;
        publisher
            .dependencies
            .add_tree(&output.tree, output.parts)?;
    }
    let (directory, pages, parts) = publisher.directory.finish()?;
    publisher.proposed.directory = directory;
    publisher.pages += pages;
    publisher.parts += parts.len() as u64;
    publisher
        .dependencies
        .add_tree(&publisher.proposed.directory, parts)?;
    let dependencies = publisher.dependencies.finish();
    let receipt =
        generation::write_carried(root, &publisher.proposed, &dependencies, &old_receipt)?;
    // A changed CURRENT under the lock is an unsupported writer, never a merge guess.
    if artifact::current(root)? != *previous {
        return Err(corrupt("CURRENT changed during publication"));
    }
    publish_current(root, &publisher.proposed)?;
    lock.remember_checked(&publisher.proposed)?;
    Ok(PublishedGeneration {
        current: publisher.proposed,
        receipt: receipt.object,
        dependency_parts: receipt.dependencies,
        mutations: publisher.mutations,
        metadata_pages_written: publisher.pages,
        metadata_parts_written: publisher.parts,
    })
}

impl Publisher<'_> {
    fn batch(&mut self, mut mutations: Vec<PreparedMutation>) -> Result<()> {
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
        while let Some(first) = items.next() {
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
            let entry = call
                .find(self.directory.root(), &key)?
                .map(|value| DirectoryEntry::decode(&value))
                .transpose()?;
            if entry.as_ref().is_some_and(|entry| entry.group != group) {
                return Err(corrupt("publication directory coordinate mismatch"));
            }
            let changes: Vec<_> = changes.into_iter().collect();
            let tree = if let Some(entry) = &entry {
                self.locators
                    .edit(&mut call, entry.locator.clone(), &changes)?
            } else {
                if changes.len() > self.proposed.batch_operation_limit() {
                    return Err(corrupt("invalid new locator change count"));
                }
                self.locators.create(&changes)?
            };
            let value = DirectoryEntry {
                group,
                locator: tree,
                converted_frontier: entry.map_or(0, |entry| entry.converted_frontier),
            }
            .encode()?;
            directory_changes.insert(key, value);
        }
        let mut call = artifact::metadata_call(self.root, self.proposed.clone());
        self.directory.put_many(
            &mut call,
            &directory_changes.into_iter().collect::<Vec<_>>(),
        )
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
