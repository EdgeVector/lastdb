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
    let mut directory = TreeEdit::new(root, proposed.directory.clone(), proposed.generation)?;
    let mut locators = LocatorChanges::new(root, proposed.generation);
    let mut call = artifact::metadata_call(root, cut.clone());
    let mut groups = TreeCursor::recent(&mut call, cut.directory.clone(), previous.generation)?;
    let mut copied = 0;
    while let Some((key, value)) = groups.next(&mut call)? {
        let live = DirectoryEntry::decode(&value)?;
        let mut output_call = artifact::metadata_call(root, proposed.clone());
        let existing = output_call
            .find(directory.root(), &key)?
            .map(|value| DirectoryEntry::decode(&value))
            .transpose()?;
        let tree = if let Some(existing) = existing {
            let mut tree = existing.locator;
            let mut source = artifact::metadata_call(root, cut.clone());
            let mut records =
                TreeCursor::recent(&mut source, live.locator.clone(), previous.generation)?;
            let mut changes = Vec::new();
            let mut examined = 0u64;
            while let Some((key, value)) = records.next(&mut source)? {
                examined += 1;
                if examined % 256 == 0 {
                    source = artifact::metadata_call(root, cut.clone());
                }
                let location = Location::decode_for_current(&value, cut)?;
                if location.version.batch <= previous.frontier {
                    continue;
                }
                copied += 1;
                if copied > limit {
                    return Err(behind());
                }
                changes.push((key, value));
                if changes.len() == cut.batch_operation_limit().min(256) {
                    tree = locators.edit(&mut output_call, tree, &changes)?;
                    changes.clear();
                    source = artifact::metadata_call(root, cut.clone());
                    output_call = artifact::metadata_call(root, proposed.clone());
                }
            }
            if !changes.is_empty() {
                tree = locators.edit(&mut output_call, tree, &changes)?;
            }
            tree
        } else {
            // Every part of a newly created group is newer than the original cut.
            // Its whole immutable tree can be retained directly.
            copied += 1;
            if copied > limit {
                return Err(behind());
            }
            live.locator.clone()
        };
        let replacement = DirectoryEntry {
            locator: tree,
            ..live
        };
        directory.put_many(&mut output_call, &[(key, replacement.encode()?)])?;
        call = artifact::metadata_call(root, cut.clone());
    }
    let mut dependencies =
        Dependencies::from_coordinates(std::mem::take(&mut job.output.dependencies))?;
    let receipt = generation::read_receipt(root, cut)?;
    for (coordinate, record) in receipt.coordinates.into_iter().zip(receipt.records) {
        if crate::disk_pack::bytes::u64_at(&record, 72) > job.source.current().generation {
            dependencies.add(coordinate)?;
        }
    }
    for output in locators.finish()? {
        dependencies.add_tree(&output.tree, output.parts)?;
    }
    let (tree, _, parts) = directory.finish()?;
    dependencies.add_tree(&tree, parts)?;
    proposed.directory = tree;
    job.output.current = proposed;
    job.output.dependencies = dependencies.finish();
    Ok(copied)
}

fn behind() -> crate::Error {
    crate::Error::WriteCapacity(
        "compaction_behind: final catch-up exceeds its bounded key budget; retry after maintenance"
            .into(),
    )
}
