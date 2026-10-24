//! Graft newer immutable locators; never copy a foreground payload during catch-up.
use super::concurrent::PreparedCompaction;
use crate::disk_pack::dependency::Dependencies;
use crate::disk_pack::live_parts::{Changes, LocatorImports, ReferenceCache};
use crate::disk_pack::locator_forest::LocatorChanges;
use crate::disk_pack::tree_cursor::TreeCursor;
use crate::disk_pack::tree_edit::TreeEdit;
use crate::disk_pack::{artifact, corrupt, generation, Current, DirectoryEntry, Location};
use crate::Result;

pub(super) fn run(job: &mut PreparedCompaction, cut: &Current, limit: u64) -> Result<u64> {
    let previous = job.caught.current();
    if cut == previous {
        return Ok(0);
    }
    super::concurrent::ensure_newer_compatible(previous, cut)?;
    let root = job.lock.root();
    let mut proposed = cut.clone();
    proposed.generation = cut
        .generation
        .checked_add(1)
        .ok_or_else(|| corrupt("generation exhaustion"))?;
    proposed.required_features |= crate::disk_pack::FEATURE_LIVE_PART_COUNTS;
    proposed.directory = job.output.current.directory.clone();
    if cut.frontier == previous.frontier {
        // A physical rewrite changes addresses, not values. The private output
        // already has these values; replay would carry a second payload copy.
        job.output.current = proposed;
        return Ok(0);
    }
    let mut directory = TreeEdit::protected(
        root,
        proposed.directory.clone(),
        proposed.generation,
        job.protection.clone(),
    )?;
    let mut locators = LocatorChanges::protected(root, proposed.generation, job.protection.clone());
    let mut call = artifact::metadata_call(root, cut.clone());
    let mut groups = TreeCursor::recent(&mut call, cut.directory.clone(), previous.generation)?;
    let mut budget = DeltaBudget { changed: 0, limit };
    let graft = Graft {
        root,
        cut,
        previous,
        proposed: &proposed,
    };
    let mut prior = PriorGroups::new(root, previous);
    let mut references = ReferenceCache::new(root, &proposed);
    let receipt = generation::read_receipt(root, cut)?;
    let mut imported = LocatorImports::new(root, cut, &receipt.coordinates)?;
    let mut directory_changes = Vec::new();
    while let Some((key, value)) = groups.next(&mut call)? {
        if prior.unchanged(&key, &value)? {
            // Keep the cursor's I/O budget bounded even across long runs of
            // unchanged neighbors. PriorGroups owns the shared lookup cache.
            call = artifact::metadata_call(root, cut.clone());
            continue;
        }
        let live = DirectoryEntry::decode(&value)?;
        let mut output_call = artifact::metadata_call(root, proposed.clone());
        let existing = output_call
            .find(directory.root(), &key)?
            .map(|value| DirectoryEntry::decode(&value))
            .transpose()?;
        let tree = if let Some(existing) = existing {
            graft.tree(
                live.locator.clone(),
                existing.locator,
                &mut locators,
                &mut output_call,
                &mut budget,
                &mut references,
            )?
        } else {
            budget.note()?;
            imported.add(&live.locator)?;
            live.locator.clone()
        };
        let replacement = DirectoryEntry {
            locator: tree,
            ..live
        };
        directory_changes.push((key, replacement.encode()?));
        if directory_changes.len() == cut.batch_operation_limit().min(256) {
            directory.put_many(&mut output_call, &mut references, &directory_changes)?;
            directory_changes.clear();
        }
        call = artifact::metadata_call(root, cut.clone());
    }
    if !directory_changes.is_empty() {
        let mut call = artifact::metadata_call(root, proposed.clone());
        directory.put_many(&mut call, &mut references, &directory_changes)?;
    }
    let imported = imported.finish()?;
    finish_round(job, proposed, directory, locators, imported)?;
    Ok(budget.changed)
}

fn finish_round(
    job: &mut PreparedCompaction,
    mut proposed: Current,
    directory: TreeEdit,
    locators: LocatorChanges,
    mut changes: Changes,
) -> Result<()> {
    let mut fresh = Dependencies::new();
    for output in locators.finish()? {
        changes.merge(output.changes)?;
        fresh.add_tree(&output.tree, output.parts)?;
    }
    let (tree, _, parts, directory_changes) = directory.finish_with_changes()?;
    changes.merge(directory_changes)?;
    fresh.add_tree(&tree, parts)?;
    proposed.directory = tree;
    finish_references(job, &proposed, &fresh.finish(), changes)?;
    job.output.current = proposed;
    // A newly grafted locator may retain parts older than the original source.
    // Protect the complete exact closure, including payload and block-map parts.
    job.output.dependencies = job.live.coordinates();
    Ok(())
}

fn finish_references(
    job: &mut PreparedCompaction,
    proposed: &Current,
    fresh: &[crate::disk_pack::dependency::Coordinate],
    mut changes: Changes,
) -> Result<()> {
    use crate::disk_pack::live_parts;
    let root = job.lock.root();
    let mut references = ReferenceCache::new(root, proposed);
    live_parts::observe_new_parts(root, proposed, fresh, &mut changes, &mut references)?;
    drop(references);
    live_parts::add_directory_empty_root(root, &job.output.current, -1, &mut changes)?;
    live_parts::add_directory_empty_root(root, proposed, 1, &mut changes)?;
    job.live.apply(root, proposed, changes)?;
    crate::commit_timing::count("compaction_reference_deltas", fresh.len() as u64);
    Ok(())
}

/// A recent directory leaf also yields unchanged neighbors. Exact immutable
/// entry equality proves that the prior round already includes that group.
/// One bounded call shares directory pages across 128 comparisons.
struct PriorGroups<'a> {
    root: &'a std::path::Path,
    previous: &'a Current,
    call: crate::disk_pack::ReadCall,
    checked: usize,
}

impl<'a> PriorGroups<'a> {
    fn new(root: &'a std::path::Path, previous: &'a Current) -> Self {
        Self {
            root,
            previous,
            call: artifact::metadata_call(root, previous.clone()),
            checked: 0,
        }
    }

    fn unchanged(&mut self, key: &[u8], value: &[u8; 128]) -> Result<bool> {
        if self.checked == 128 {
            self.call = artifact::metadata_call(self.root, self.previous.clone());
            self.checked = 0;
        }
        self.checked += 1;
        let same = self.call.find(&self.previous.directory, key)?.as_ref() == Some(value);
        if same {
            crate::commit_timing::count("compaction_unchanged_groups", 1);
        }
        Ok(same)
    }
}

fn behind() -> crate::Error {
    crate::Error::WriteCapacity(
        "compaction_behind: final catch-up exceeds its bounded key budget; retry after maintenance"
            .into(),
    )
}

struct DeltaBudget {
    changed: u64,
    limit: u64,
}
impl DeltaBudget {
    fn note(&mut self) -> Result<()> {
        self.changed += 1;
        if self.changed > self.limit {
            return Err(behind());
        }
        Ok(())
    }
}

struct Graft<'a> {
    root: &'a std::path::Path,
    cut: &'a Current,
    previous: &'a Current,
    proposed: &'a Current,
}

impl Graft<'_> {
    fn tree(
        &self,
        live: crate::disk_pack::TreeRoot,
        mut tree: crate::disk_pack::TreeRoot,
        locators: &mut LocatorChanges,
        output_call: &mut crate::disk_pack::ReadCall,
        budget: &mut DeltaBudget,
        references: &mut ReferenceCache,
    ) -> Result<crate::disk_pack::TreeRoot> {
        let mut source = artifact::metadata_call(self.root, self.cut.clone());
        let mut records = TreeCursor::recent(&mut source, live, self.previous.generation)?;
        let mut changes = Vec::new();
        let mut examined = 0u64;
        while let Some((key, value)) = records.next(&mut source)? {
            examined += 1;
            if examined % 256 == 0 {
                source = artifact::metadata_call(self.root, self.cut.clone());
            }
            let location = Location::decode_for_current(&value, self.cut)?;
            if location.version.batch <= self.previous.frontier {
                continue;
            }
            budget.note()?;
            changes.push((key, value));
            if changes.len() == self.cut.batch_operation_limit().min(256) {
                tree = locators.edit(output_call, references, tree, &changes)?;
                changes.clear();
                source = artifact::metadata_call(self.root, self.cut.clone());
                *output_call = artifact::metadata_call(self.root, self.proposed.clone());
            }
        }
        if !changes.is_empty() {
            tree = locators.edit(output_call, references, tree, &changes)?;
        }
        Ok(tree)
    }
}
