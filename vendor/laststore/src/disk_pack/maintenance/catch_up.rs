//! Graft newer immutable locators; never copy a foreground payload during catch-up.
use super::concurrent::PreparedCompaction;
use crate::disk_pack::dependency::Dependencies;
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
    if cut.frontier < previous.frontier
        || cut.generation <= previous.generation
        || cut.required_features != previous.required_features
    {
        return Err(corrupt(
            "compaction catch-up root is not a newer compatible snapshot",
        ));
    }
    let root = job.lock.root();
    let mut proposed = cut.clone();
    proposed.generation = cut
        .generation
        .checked_add(1)
        .ok_or_else(|| corrupt("generation exhaustion"))?;
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
    let mut directory_changes = Vec::new();
    let mut templates = std::collections::BTreeSet::new();
    while let Some((key, value)) = groups.next(&mut call)? {
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
            )?
        } else {
            budget.note()?;
            live.locator.clone()
        };
        templates.insert(tree.object);
        let replacement = DirectoryEntry {
            locator: tree,
            ..live
        };
        directory_changes.push((key, replacement.encode()?));
        if directory_changes.len() == cut.batch_operation_limit().min(256) {
            directory.put_many(&mut output_call, &directory_changes)?;
            directory_changes.clear();
        }
        call = artifact::metadata_call(root, cut.clone());
    }
    if !directory_changes.is_empty() {
        let mut call = artifact::metadata_call(root, proposed.clone());
        directory.put_many(&mut call, &directory_changes)?;
    }
    let mut dependencies = carried_dependencies(
        root,
        cut,
        job.source.current().generation,
        std::mem::take(&mut job.output.dependencies),
        templates,
    )?;
    for output in locators.finish()? {
        dependencies.add_tree(&output.tree, output.parts)?;
    }
    let (tree, _, parts) = directory.finish()?;
    dependencies.add_tree(&tree, parts)?;
    proposed.directory = tree;
    job.output.current = proposed;
    job.output.dependencies = dependencies.finish();
    Ok(budget.changed)
}

fn behind() -> crate::Error {
    crate::Error::WriteCapacity(
        "compaction_behind: final catch-up exceeds its bounded key budget; retry after maintenance"
            .into(),
    )
}

fn carried_dependencies(
    root: &std::path::Path,
    cut: &Current,
    source_generation: u64,
    output: Vec<crate::disk_pack::dependency::Coordinate>,
    templates: std::collections::BTreeSet<uuid::Uuid>,
) -> Result<Dependencies> {
    let mut dependencies = Dependencies::from_coordinates(output)?;
    // New groups can retain a live locator object from before the source cut.
    // The shared allocator also needs its empty page zero for later groups,
    // even when none of the currently selected trees points to that page.
    for object in templates {
        dependencies.add(crate::disk_pack::dependency::Coordinate {
            kind: 2,
            object,
            batch: 0,
            part: 0,
        })?;
    }
    let receipt = generation::read_receipt(root, cut)?;
    for (coordinate, record) in receipt.coordinates.into_iter().zip(receipt.records) {
        if crate::disk_pack::bytes::u64_at(&record, 72) > source_generation {
            dependencies.add(coordinate)?;
        }
    }
    Ok(dependencies)
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
                tree = locators.edit(output_call, tree, &changes)?;
                changes.clear();
                source = artifact::metadata_call(self.root, self.cut.clone());
                *output_call = artifact::metadata_call(self.root, self.proposed.clone());
            }
        }
        if !changes.is_empty() {
            tree = locators.edit(output_call, tree, &changes)?;
        }
        Ok(tree)
    }
}
