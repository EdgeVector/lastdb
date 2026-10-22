//! Rewrite recent tree pages and payloads; retain older immutable subtrees by address.
use super::cache::PackCaches;
use super::compact::PackOutput;
use super::MaintenanceReceipt;
use crate::disk_pack::dependency::{Coordinate, Dependencies};
use crate::disk_pack::pack_write::PackWriter;
use crate::disk_pack::sparse_pages::SparsePages;
use crate::disk_pack::tree_output::{self, Child, LeafEntry, TreePages};
use crate::disk_pack::{
    corrupt, generation, key_layout, Current, DirectoryEntry, Location, Source, TreeKind, TreeRoot,
};
use crate::Result;
use std::collections::BTreeMap;
use std::path::Path;
use uuid::Uuid;

pub(super) struct TierCopy<'a> {
    root: &'a Path,
    source: Current,
    boundary: u64,
    stamp: u64,
    ages: BTreeMap<Coordinate, u64>,
    payload_ages: BTreeMap<Uuid, u64>,
    pages: BTreeMap<(u8, Uuid), SparsePages>,
    caches: PackCaches,
    pack: PackWriter,
    dependencies: Dependencies,
    receipt: MaintenanceReceipt,
    collection: String,
}

struct Branch {
    number: u64,
    level: u8,
    parent_age: u64,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    root: bool,
}

impl<'a> TierCopy<'a> {
    pub(super) fn copy(root: &'a Path, source: &Current, boundary: u64) -> Result<PackOutput> {
        let receipt = generation::read_receipt(root, source)?;
        let mut ages = BTreeMap::new();
        let mut payload_ages = BTreeMap::new();
        let mut dependencies = Dependencies::new();
        for (coordinate, bytes) in receipt.coordinates.iter().zip(&receipt.records) {
            let age = crate::disk_pack::bytes::u64_at(bytes, 72);
            ages.insert(*coordinate, age);
            if coordinate.kind == 5 {
                if payload_ages
                    .insert(coordinate.object, age)
                    .is_some_and(|old| old != age)
                {
                    return Err(corrupt("payload parts have different creation generations"));
                }
            }
            if age <= boundary {
                dependencies.add(*coordinate)?;
            }
        }
        let stamp = source
            .generation
            .checked_add(1)
            .ok_or_else(|| corrupt("generation exhaustion"))?;
        let mut copy = Self {
            root,
            source: source.clone(),
            boundary,
            stamp,
            ages,
            payload_ages,
            collection: String::new(),
            pages: BTreeMap::new(),
            caches: PackCaches::new(),
            pack: PackWriter::new(root, stamp)?,
            dependencies,
            receipt: MaintenanceReceipt {
                parts_before: receipt.dependencies,
                ..Default::default()
            },
        };
        let mut current = source.clone();
        current.directory = copy.tree(&source.directory)?;
        current.generation = stamp;
        let payload = copy.pack.finish()?;
        copy.dependencies
            .add_payload(payload.object, 0, payload.data_parts, payload.map_pages)?;
        for ((kind, object), pages) in copy.pages {
            let tree = TreeRoot {
                object,
                page: 0,
                height: 1,
                kind: if kind == 1 {
                    TreeKind::Directory
                } else {
                    TreeKind::Locator
                },
            };
            copy.dependencies.add_tree(&tree, pages.finish()?)?;
        }
        Ok(PackOutput {
            current,
            dependencies: copy.dependencies.finish(),
            receipt: copy.receipt,
        })
    }

    fn tree(&mut self, tree: &TreeRoot) -> Result<TreeRoot> {
        let mut output = tree.clone();
        output.page = self.page(
            tree,
            Branch {
                number: tree.page,
                level: tree.height - 1,
                parent_age: self.source.generation,
                lower: None,
                upper: None,
                root: true,
            },
        )?;
        Ok(output)
    }

    fn page(&mut self, tree: &TreeRoot, path: Branch) -> Result<u64> {
        let kind = if tree.kind == TreeKind::Directory {
            1
        } else {
            2
        };
        let coordinate = Coordinate {
            kind,
            object: tree.object,
            batch: 0,
            part: (path.number / 512) as u32,
        };
        let age = *self
            .ages
            .get(&coordinate)
            .ok_or_else(|| corrupt("tree page is absent from its source receipt"))?;
        if age > path.parent_age {
            return Err(corrupt("tree child is newer than its parent"));
        }
        if age <= self.boundary {
            return Ok(path.number);
        }
        let mut call = self.caches.call(self.root, &self.source);
        let page = call.tree_page(tree, path.number)?;
        if page.generation() != age
            || page.level() != path.level
            || (!path.root
                && ((path.level > 0 && page.count() < 7) || (path.level == 0 && page.count() == 0)))
        {
            return Err(corrupt("tier page age, height, or occupancy differs"));
        }
        let mut entries = Vec::new();
        let mut previous: Option<Vec<u8>> = None;
        for index in 0..page.count() {
            let key = call.full_key(tree, page.entry(index))?;
            if previous.as_ref().is_some_and(|old| old >= &key)
                || path.lower.as_ref().is_some_and(|lower| lower > &key)
                || path.upper.as_ref().is_some_and(|upper| upper <= &key)
            {
                return Err(corrupt("tier key order or parent interval differs"));
            }
            previous = Some(key.clone());
            entries.push((key, page.entry(index).to_vec()));
        }
        self.caches.keep(call);
        if path.level == 0 {
            return self.leaf(tree, entries);
        }
        let mut children = Vec::new();
        let mut number = page.left();
        for index in 0..=entries.len() {
            let lower = if index == 0 {
                path.lower.clone()
            } else {
                Some(entries[index - 1].0.clone())
            };
            let upper = entries
                .get(index)
                .map(|(key, _)| key.clone())
                .or_else(|| path.upper.clone());
            let replacement = self.page(
                tree,
                Branch {
                    number,
                    level: path.level - 1,
                    parent_age: age,
                    lower,
                    upper,
                    root: false,
                },
            )?;
            let minimum = if index == 0 {
                None
            } else {
                Some(tree_output::key_descriptor(
                    self.allocator(tree)?,
                    tree.kind,
                    &entries[index - 1].0,
                )?)
            };
            children.push(Child {
                minimum,
                number: replacement,
            });
            if let Some((_, entry)) = entries.get(index) {
                number = key_layout::child(entry);
            }
        }
        Ok(tree_output::parent(
            self.allocator(tree)?,
            tree.kind,
            path.level as usize,
            &children,
        )?
        .number)
    }

    fn leaf(&mut self, tree: &TreeRoot, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<u64> {
        let mut output: Vec<LeafEntry> = Vec::new();
        for (key, entry) in entries {
            let value: [u8; 128] = key_layout::value(&entry).try_into().expect("leaf value");
            let value = if tree.kind == TreeKind::Directory {
                let mut directory = DirectoryEntry::decode(&value)?;
                self.collection = super::compact::collection(&key, directory.group)?;
                directory.locator = self.tree(&directory.locator)?;
                directory.encode()?
            } else {
                self.record(&key, value)?
            };
            let descriptor = tree_output::key_descriptor(self.allocator(tree)?, tree.kind, &key)?;
            output.push((descriptor, value));
        }
        if output.is_empty() {
            return self
                .allocator(tree)?
                .append(tree_output::leaf_fields(tree.kind, 0), &[]);
        }
        Ok(tree_output::leaf(self.allocator(tree)?, tree.kind, &output)?.number)
    }

    fn allocator(&mut self, tree: &TreeRoot) -> Result<&mut SparsePages> {
        let key = (
            if tree.kind == TreeKind::Directory {
                1
            } else {
                2
            },
            tree.object,
        );
        if !self.pages.contains_key(&key) {
            self.pages
                .insert(key, SparsePages::new(self.root, tree, self.stamp)?);
        }
        Ok(self.pages.get_mut(&key).expect("created tree allocator"))
    }

    fn record(&mut self, key: &[u8], value: [u8; 128]) -> Result<[u8; 128]> {
        let location = Location::decode_for_current(&value, &self.source)?;
        if location.version.batch > self.source.frontier {
            return Err(corrupt("tier record is beyond the source frontier"));
        }
        if location.source == Source::Tombstone {
            self.receipt.tombstones_copied += 1;
            return Ok(value);
        }
        let age = self
            .payload_ages
            .get(&location.object)
            .ok_or_else(|| corrupt("tier payload is absent from its receipt"))?;
        if *age <= self.boundary {
            return Ok(value);
        }
        // RecordBody also validates the collection stored in the frame. The directory
        // walk supplies it through the current locator tree's exact group context.
        let collection = self.collection.clone();
        let key =
            std::str::from_utf8(key).map_err(|_| corrupt("native storage key is not UTF-8"))?;
        let mut call = self.caches.call(self.root, &self.source);
        let mut body =
            super::stream::RecordBody::open(&mut call, &collection, key, location.clone())?;
        let bytes = body.body_bytes();
        let replacement = self
            .pack
            .put(&collection, key, location.version, bytes, &mut body)?;
        self.receipt.records_copied += 1;
        drop(body);
        self.caches.keep(call);
        Ok(replacement.encode_for_features(self.source.required_features)?)
    }
}
