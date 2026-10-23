use super::cache::PackCaches;
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
    root: &'a std::path::Path,
    current: Current,
    dependencies: Dependencies,
    receipt: MaintenanceReceipt,
}

pub(super) struct PackOutput {
    pub(super) current: Current,
    pub(super) dependencies: Vec<crate::disk_pack::dependency::Coordinate>,
    pub(super) receipt: MaintenanceReceipt,
}

pub(in crate::disk_pack) fn compact_locked(
    lock: &WriteLock,
    policy: &SpacePolicy,
) -> Result<(Current, MaintenanceReceipt)> {
    let previous = artifact::current(lock.root())?;
    let reserve = super::quote::replacement_bytes(lock.root(), &previous)?;
    let _space = reserve_replacement(lock.root(), policy, reserve)?;
    publish::ensure_generation(lock, &previous)?;
    let output = copy(lock, &previous)?;
    super::concurrent::publish_output(lock, &previous, output)
}

pub(super) fn copy(lock: &WriteLock, previous: &Current) -> Result<PackOutput> {
    let root = lock.root();
    let old = generation::read_receipt(root, previous)?;
    let mut current = previous.clone();
    // Private UUID objects need an age, not a receipt identity. Writers can
    // publish source+1 during this copy; reserving a receipt name here could
    // jump the output age beyond the next catch-up cut. Allocate only at swap.
    current.generation = previous
        .generation
        .checked_add(1)
        .ok_or_else(|| corrupt("generation exhaustion"))?;
    current.previous_generation = previous.generation;
    current.published_ms = artifact::unix_ms();
    let mut directory = TreeWriter::new(root, TreeKind::Directory, current.generation)?;
    let mut pack = PackWriter::new(root, current.generation)?;
    let mut locators = LocatorForest::new(root, current.generation)?;
    let mut compactor = Compactor {
        root,
        current,
        dependencies: Dependencies::new(),
        receipt: MaintenanceReceipt {
            parts_before: old.dependencies,
            ..Default::default()
        },
    };
    let mut caches = PackCaches::new();
    let mut call = caches.call(root, previous);
    let mut cursor = TreeCursor::seek(&mut call, previous.directory.clone(), b"")?;
    while let Some((key, value)) = cursor.next(&mut call)? {
        let entry = DirectoryEntry::decode(&value)?;
        let collection = collection(&key, entry.group)?;
        // The group takes the caches while it runs and gives them back at its end.
        caches.keep(call);
        let replacement = compactor.group(
            previous,
            &collection,
            &entry,
            (&mut pack, &mut locators, &mut caches),
        )?;
        directory.add(&key, replacement.encode()?)?;
        call = caches.call(root, previous);
    }
    let data = pack.finish()?;
    compactor
        .dependencies
        .add_payload(data.object, 0, data.data_parts, data.map_pages)?;
    let output = locators.finish()?;
    compactor
        .dependencies
        .add_tree(&output.tree, output.parts)?;
    let (tree, parts) = directory.finish_with_parts()?;
    compactor.dependencies.add_contiguous_tree(&tree, parts)?;
    compactor.current.directory = tree;
    let dependencies = compactor.dependencies.finish();
    Ok(PackOutput {
        current: compactor.current,
        dependencies,
        receipt: compactor.receipt,
    })
}

impl Compactor<'_> {
    fn group(
        &mut self,
        previous: &Current,
        collection: &str,
        entry: &DirectoryEntry,
        (pack, locators, caches): (&mut PackWriter, &mut LocatorForest, &mut PackCaches),
    ) -> Result<DirectoryEntry> {
        let root = self.root;
        let mut locator = locators.tree()?;
        let mut call = caches.call(root, previous);
        let mut cursor = TreeCursor::seek(&mut call, entry.locator.clone(), b"")?;
        while let Some((key, value)) = cursor.next(&mut call)? {
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
            caches.keep(call);
            call = caches.call(root, previous);
        }
        caches.keep(call);
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
