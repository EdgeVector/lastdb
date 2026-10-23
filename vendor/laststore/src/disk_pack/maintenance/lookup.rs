//! Repack lookup pages while retaining the original opaque payload objects.

use super::space::reserve_replacement;
use super::{MaintenanceReceipt, SpacePolicy};
use crate::disk_pack::dependency::{Dependencies, MAX_DEPENDENCIES};
use crate::disk_pack::locator_forest::LocatorForest;
use crate::disk_pack::tree_cursor::TreeCursor;
use crate::disk_pack::tree_write::TreeWriter;
use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{
    artifact, corrupt, generation, publish, DirectoryEntry, Location, Source, TreeKind,
};
use crate::Result;
use std::collections::BTreeSet;

pub(in crate::disk_pack) fn repack_locked(
    lock: &WriteLock,
    policy: &SpacePolicy,
) -> Result<(crate::disk_pack::Current, MaintenanceReceipt)> {
    let root = lock.root();
    let previous = artifact::current(root)?;
    let tail = crate::disk_pack::journal::JournalTail::read(root)?;
    tail.validate_for_current(root, &previous)?;
    if tail.stream != previous.journal_stream || tail.last_batch != previous.frontier {
        return Err(corrupt("lookup repack requires a complete checkpoint"));
    }
    let old = publish::ensure_generation(lock, &previous)?;
    let reserve = super::quote::lookup_replacement_bytes(root, &previous)?;
    let _space = reserve_replacement(root, policy, reserve)?;
    let mut current = previous.clone();
    current.generation = publish::fresh_generation(root, previous.generation)?;
    current.previous_generation = previous.generation;
    current.published_ms = artifact::unix_ms();
    let mut directory = TreeWriter::new(root, TreeKind::Directory, current.generation)?;
    let mut locators = LocatorForest::new(root, current.generation)?;
    let mut dependencies = Dependencies::new();
    let mut payloads = BTreeSet::new();
    let mut receipt = MaintenanceReceipt {
        parts_before: old.dependencies,
        ..Default::default()
    };
    let mut call = artifact::metadata_call(root, previous.clone());
    let mut groups = TreeCursor::seek(&mut call, previous.directory.clone(), b"")?;
    while let Some((key, value)) = groups.next(&mut call)? {
        let entry = DirectoryEntry::decode(&value)?;
        super::compact::collection(&key, entry.group)?;
        let mut output = locators.tree()?;
        let mut cursor = TreeCursor::seek(&mut call, entry.locator, b"")?;
        while let Some((key, value)) = cursor.next(&mut call)? {
            let location = Location::decode_for_current(&value, &previous)?;
            if location.version.batch > previous.frontier {
                return Err(corrupt("lookup repack record beyond source frontier"));
            }
            if location.source == Source::Tombstone {
                receipt.tombstones_copied += 1;
            } else {
                let batch = if location.source == Source::Journal {
                    location.version.batch
                } else {
                    0
                };
                if payloads.insert((location.object, batch)) {
                    if payloads.len() > MAX_DEPENDENCIES {
                        return Err(crate::disk_pack::resource());
                    }
                    let object = call
                        .object_metadata(&location)?
                        .ok_or_else(|| corrupt("lookup repack object bounds"))?;
                    dependencies.add_payload(
                        location.object,
                        batch,
                        object.data_parts,
                        object.blocks.div_ceil(crate::disk_pack::MAP_ENTRIES),
                    )?;
                }
                receipt.records_copied += 1;
            }
            output.add(&key, value)?;
            if (receipt.records_copied + receipt.tombstones_copied) % 256 == 0 {
                call = artifact::metadata_call(root, previous.clone());
            }
        }
        let locator = locators.complete_tree(output)?;
        directory.add(
            &key,
            DirectoryEntry {
                group: entry.group,
                locator,
                converted_frontier: entry.converted_frontier,
            }
            .encode()?,
        )?;
        call = artifact::metadata_call(root, previous.clone());
    }
    let output = locators.finish()?;
    dependencies.add_tree(&output.tree, output.parts)?;
    let (tree, parts) = directory.finish_with_parts()?;
    dependencies.add_contiguous_tree(&tree, parts)?;
    current.directory = tree;
    let selected = dependencies.finish();
    let generation = generation::write(root, &current, &selected)?;
    lock.note_present(&selected)?;
    if artifact::current(root)? != previous {
        return Err(corrupt("CURRENT changed during lookup repack"));
    }
    publish::publish_current(root, &current)?;
    lock.remember_checked(&current)?;
    receipt.generation = current.generation;
    receipt.parts_after = generation.dependencies;
    Ok((current, receipt))
}
