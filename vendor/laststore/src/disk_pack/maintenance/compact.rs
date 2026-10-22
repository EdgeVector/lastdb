use super::space::reserve_replacement;
use super::stream::RecordBody;
use super::{MaintenanceReceipt, SpacePolicy};
use crate::disk_pack::dependency::Dependencies;
use crate::disk_pack::locator_forest::LocatorForest;
use crate::disk_pack::pack_write::PackWriter;
use crate::disk_pack::tree_cursor::TreeCursor;
use crate::disk_pack::tree_write::TreeWriter;
use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{
    artifact, corrupt, generation, publish, Current, DirectoryEntry, Location, Source, TreeKind,
};
use crate::Result;

struct Compactor<'a> {
    lock: &'a WriteLock,
    current: Current,
    dependencies: Dependencies,
    receipt: MaintenanceReceipt,
}

pub(in crate::disk_pack) fn compact_locked(
    lock: &WriteLock,
    policy: &SpacePolicy,
) -> Result<(Current, MaintenanceReceipt)> {
    let root = lock.root();
    let previous = artifact::current(root)?;
    let old = publish::ensure_generation(lock, &previous)?;
    let reserve = super::quote::replacement_bytes(root, &previous)?;
    let _space = reserve_replacement(root, policy, reserve)?;
    let mut current = previous.clone();
    current.generation = publish::fresh_generation(root, previous.generation)?;
    current.previous_generation = previous.generation;
    current.published_ms = artifact::unix_ms();
    let mut directory = TreeWriter::new(root, TreeKind::Directory, current.generation)?;
    let mut pack = PackWriter::new(root, current.generation)?;
    let mut locators = LocatorForest::new(root, current.generation)?;
    let mut compactor = Compactor {
        lock,
        current,
        dependencies: Dependencies::new(),
        receipt: MaintenanceReceipt {
            parts_before: old.dependencies,
            ..Default::default()
        },
    };
    let mut call = artifact::metadata_call(root, previous.clone());
    let mut cursor = TreeCursor::seek(&mut call, previous.directory.clone(), b"")?;
    while let Some((key, value)) = cursor.next(&mut call)? {
        let entry = DirectoryEntry::decode(&value)?;
        let collection = collection(&key, entry.group)?;
        let replacement =
            compactor.group(&previous, &collection, &entry, &mut pack, &mut locators)?;
        directory.add(&key, replacement.encode()?)?;
        call = artifact::metadata_call(root, previous.clone());
    }
    let data = pack.finish()?;
    compactor
        .dependencies
        .add_payload(data.object, data.data_parts, data.map_pages)?;
    let output = locators.finish()?;
    compactor
        .dependencies
        .add_tree(&output.tree, output.parts)?;
    let (tree, parts) = directory.finish_with_parts()?;
    compactor.dependencies.add_contiguous_tree(&tree, parts)?;
    compactor.current.directory = tree;
    let dependencies = compactor.dependencies.finish();
    let receipt = generation::write(root, &compactor.current, &dependencies)?;
    if artifact::current(root)? != previous {
        return Err(corrupt("CURRENT changed during compaction"));
    }
    publish::publish_current(root, &compactor.current)?;
    lock.remember_checked(&compactor.current)?;
    compactor.receipt.generation = compactor.current.generation;
    compactor.receipt.parts_after = receipt.dependencies;
    Ok((compactor.current, compactor.receipt))
}

impl Compactor<'_> {
    fn group(
        &mut self,
        previous: &Current,
        collection: &str,
        entry: &DirectoryEntry,
        pack: &mut PackWriter,
        locators: &mut LocatorForest,
    ) -> Result<DirectoryEntry> {
        let root = self.lock.root();
        let mut locator = locators.tree()?;
        let mut call = artifact::metadata_call(root, previous.clone());
        let mut cursor = TreeCursor::seek(&mut call, entry.locator.clone(), b"")?;
        let mut records = 0u64;
        while let Some((key, value)) = cursor.next(&mut call)? {
            records += 1;
            if records > 1_000_000 {
                return Err(crate::disk_pack::resource());
            }
            let location = Location::decode_for_current(&value, previous)?;
            if location.version.batch > previous.frontier {
                return Err(corrupt("compaction record beyond source frontier"));
            }
            let key_text = std::str::from_utf8(&key)
                .map_err(|_| corrupt("native storage key is not UTF-8"))?;
            let location = if location.source == Source::Tombstone {
                self.receipt.tombstones_copied += 1;
                location
            } else {
                let version = location.version;
                let mut body = RecordBody::open(&mut call, collection, key_text, location)?;
                let body_bytes = body.body_bytes();
                let location = pack.put(collection, key_text, version, body_bytes, &mut body)?;
                self.receipt.records_copied += 1;
                location
            };
            locator.add(
                &key,
                location.encode_for_features(self.current.required_features)?,
            )?;
            call = artifact::metadata_call(root, previous.clone());
        }
        let locator = locators.complete_tree(locator)?;
        Ok(DirectoryEntry {
            group: entry.group,
            locator,
            converted_frontier: entry.converted_frontier,
        })
    }
}

pub(super) fn collection(key: &[u8], group: u16) -> Result<String> {
    if key.len() < 4
        || crate::disk_pack::bytes::u16_at(key, 0) as usize + 4 != key.len()
        || crate::disk_pack::bytes::u16_at(key, key.len() - 2) != group
    {
        return Err(corrupt("compaction directory coordinate differs"));
    }
    std::str::from_utf8(&key[2..key.len() - 2])
        .map(str::to_string)
        .map_err(|_| corrupt("compaction collection is not UTF-8"))
}
