use super::dependency_walk;
use super::generation;
use super::journal::{JournalRecovery, RecoveredMutation};
use super::tree_edit::TreeEdit;
use super::tree_write::TreeWriter;
use super::{artifact, corrupt, Current, DirectoryEntry, TreeKind};
use crate::{durability, Result};
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
    /// Complete live dependency parts named by the receipt.
    pub dependency_parts: u64,
    /// Operations included from complete captured journal batches.
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
    pages: u64,
    parts: u64,
    mutations: u64,
}

/// Recover complete batches into immutable path copies, then atomically publish
/// a separate artifact. This is not the LastStore home writer or ack path.
/// The exclusive journal lock excludes an artifact writer for the whole command.
pub fn publish_artifact_journal(root: &Path) -> Result<PublishedGeneration> {
    let _lock = publication_lock(root)?;
    let previous = artifact::current(root)?;
    if previous.required_features & super::FEATURE_OBJECT_METADATA == 0 {
        return Err(corrupt("publication requires complete object bounds"));
    }
    let mut recovery = JournalRecovery::open(root)?;
    let old_receipt = ensure_generation(root, &previous)?;
    if recovery.tail().last_batch == previous.frontier {
        return Ok(PublishedGeneration {
            current: previous,
            receipt: old_receipt.object,
            dependency_parts: old_receipt.dependencies,
            mutations: 0,
            metadata_pages_written: 0,
            metadata_parts_written: 0,
        });
    }
    let mut proposed = previous.clone();
    proposed.generation = fresh_generation(root, previous.generation)?;
    proposed.previous_generation = previous.generation;
    proposed.frontier = recovery.tail().last_batch;
    proposed.journal_frontier = recovery.tail().last_batch;
    proposed.published_ms = artifact::unix_ms();
    let directory = TreeEdit::new(root, previous.directory.clone(), proposed.generation)?;
    let mut publisher = Publisher {
        root,
        proposed,
        directory,
        pages: 0,
        parts: 0,
        mutations: 0,
    };
    while let Some(batch) = recovery.next_batch()? {
        publisher.batch(batch.mutations)?;
    }
    if !recovery.is_complete() {
        return Err(corrupt("publication did not reach captured TAIL"));
    }
    let (directory, pages, parts) = publisher.directory.finish()?;
    publisher.proposed.directory = directory;
    publisher.pages += pages;
    publisher.parts += parts;
    let dependencies = dependency_walk::collect(root, &publisher.proposed)?;
    let receipt = generation::write(root, &publisher.proposed, &dependencies)?;
    // A changed CURRENT under the lock is an unsupported writer, never a merge guess.
    if artifact::current(root)? != previous {
        return Err(corrupt("CURRENT changed during publication"));
    }
    publish_current(root, &publisher.proposed)?;
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
    fn batch(&mut self, mut mutations: Vec<RecoveredMutation>) -> Result<()> {
        self.mutations += mutations.len() as u64;
        // Sorting changes only physical work order, after complete COMMIT verification.
        mutations.sort_by(|a, b| {
            a.mutation
                .collection
                .as_bytes()
                .cmp(b.mutation.collection.as_bytes())
                .then(a.mutation.group.cmp(&b.mutation.group))
                .then(a.mutation.key.as_bytes().cmp(b.mutation.key.as_bytes()))
                .then(a.location.version.cmp(&b.location.version))
        });
        let mut items = mutations.into_iter().peekable();
        while let Some(first) = items.next() {
            let collection = first.mutation.collection.clone();
            let group = first.mutation.group;
            let key = super::ReadCall::directory_key(&collection, group)?;
            let mut call = artifact::metadata_call(self.root, self.proposed.clone());
            let entry = call
                .find(self.directory.root(), &key)?
                .map(|value| DirectoryEntry::decode(&value))
                .transpose()?;
            if entry.as_ref().is_some_and(|entry| entry.group != group) {
                return Err(corrupt("publication directory coordinate mismatch"));
            }
            let tree = if let Some(entry) = &entry {
                entry.locator.clone()
            } else {
                let tree = TreeWriter::new(self.root, TreeKind::Locator, self.proposed.generation)?
                    .finish()?;
                self.pages += 1;
                self.parts += 1;
                tree
            };
            let mut locator = TreeEdit::new(self.root, tree, self.proposed.generation)?;
            self.mutation(&mut locator, first)?;
            while items.peek().is_some_and(|item| {
                item.mutation.collection == collection && item.mutation.group == group
            }) {
                self.mutation(&mut locator, items.next().expect("peeked operation"))?;
            }
            let (tree, pages, parts) = locator.finish()?;
            self.pages += pages;
            self.parts += parts;
            let value = DirectoryEntry {
                group,
                locator: tree,
                converted_frontier: entry.map_or(0, |entry| entry.converted_frontier),
            }
            .encode()?;
            let mut call = artifact::metadata_call(self.root, self.proposed.clone());
            self.directory.put(&mut call, &key, value)?;
        }
        Ok(())
    }

    fn mutation(&self, locator: &mut TreeEdit, item: RecoveredMutation) -> Result<()> {
        let mut call = artifact::metadata_call(self.root, self.proposed.clone());
        locator.put(
            &mut call,
            item.mutation.key.as_bytes(),
            item.location.encode()?,
        )
    }
}

fn publication_lock(root: &Path) -> Result<File> {
    artifact::current(root)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join("journal").join("LOCK"))?;
    rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
        .map_err(std::io::Error::from)?;
    Ok(file)
}

fn ensure_generation(root: &Path, current: &Current) -> Result<generation::Generation> {
    if root
        .join("generations")
        .join(current.generation.to_string())
        .try_exists()?
    {
        generation::verify(root, current)
    } else {
        generation::write(root, current, &dependency_walk::collect(root, current)?)
    }
}

fn fresh_generation(root: &Path, previous: u64) -> Result<u64> {
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

fn publish_current(root: &Path, current: &Current) -> Result<()> {
    let path = root.join(format!("CURRENT.pending-{}", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    file.write_all(&current.encode()?)?;
    durability::sync_dirty_file(&file)?;
    fs::rename(path, root.join("CURRENT"))?;
    durability::sync_directory(&File::open(root)?)
}
