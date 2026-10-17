use super::bytes::u64_at;
use super::dependency::{Coordinate, Dependencies};
use super::{artifact, corrupt, Current, DirectoryEntry, Location, Source, TreeKind, TreeRoot};
use crate::Result;
use std::path::Path;

struct Walk<'a> {
    root: &'a Path,
    current: &'a Current,
    dependencies: Dependencies,
    pages: u64,
    entries: u64,
}

struct Bounds {
    first: Option<Vec<u8>>,
    last: Option<Vec<u8>>,
}

pub(super) fn collect(root: &Path, current: &Current) -> Result<Vec<Coordinate>> {
    let mut walk = Walk {
        root,
        current,
        dependencies: Dependencies::new(),
        pages: 0,
        entries: 0,
    };
    walk.tree(
        &current.directory,
        current.directory.page,
        current.directory.height - 1,
        true,
    )?;
    Ok(walk.dependencies.finish())
}

impl Walk<'_> {
    fn tree(&mut self, tree: &TreeRoot, number: u64, level: u8, is_root: bool) -> Result<Bounds> {
        self.pages += 1;
        if self.pages > 262_144 {
            return Err(crate::Error::Config(
                "artifact publication metadata-page budget".into(),
            ));
        }
        let mut call = artifact::metadata_call(self.root, self.current.clone());
        let page = call.tree_page(tree, number)?;
        if page.level() != level
            || (!is_root && level > 0 && page.count() < 7)
            || (!is_root && level == 0 && page.count() == 0)
        {
            return Err(corrupt("dependency tree level or occupancy"));
        }
        let mut previous: Option<Vec<u8>> = None;
        let mut first = None;
        for position in 0..page.count() {
            let key = call.full_key(tree, page.entry(position))?;
            if previous.as_deref().is_some_and(|old| old >= key.as_slice()) {
                return Err(corrupt("dependency tree key order"));
            }
            if position == 0 {
                first = Some(key.clone());
            }
            previous = Some(key);
            self.tree_parts(tree, &call)?;
            call.pages.clear();
        }
        self.tree_parts(tree, &call)?;
        call.pages.clear();
        if level > 0 {
            drop(first);
            drop(previous);
            return self.children(&mut call, tree, &page);
        }
        self.leaf(&mut call, tree, &page)?;
        Ok(Bounds {
            first,
            last: previous,
        })
    }

    fn children(
        &mut self,
        call: &mut super::ReadCall,
        tree: &TreeRoot,
        page: &super::page::Page,
    ) -> Result<Bounds> {
        let mut bounds = self.tree(tree, page.left(), page.level() - 1, false)?;
        for position in 0..page.count() {
            let separator = call.full_key(tree, page.entry(position))?;
            call.pages.clear();
            let next = self.tree(
                tree,
                u64_at(page.entry(position), super::KEY_DESCRIPTOR),
                page.level() - 1,
                false,
            )?;
            if next.first.as_deref() != Some(separator.as_slice())
                || bounds
                    .last
                    .as_deref()
                    .map_or(true, |last| last >= separator.as_slice())
            {
                return Err(corrupt("dependency child separator or interval"));
            }
            bounds.last = next.last;
        }
        Ok(bounds)
    }

    fn leaf(
        &mut self,
        call: &mut super::ReadCall,
        tree: &TreeRoot,
        page: &super::page::Page,
    ) -> Result<()> {
        for position in 0..page.count() {
            self.entries += 1;
            if self.entries > 1_000_000 {
                return Err(crate::Error::Config(
                    "artifact publication metadata-entry budget".into(),
                ));
            }
            let entry: &[u8; 128] = page.entry(position)[super::KEY_DESCRIPTOR..]
                .try_into()
                .expect("leaf value");
            if tree.kind == TreeKind::Directory {
                let entry = DirectoryEntry::decode(entry)?;
                if entry.converted_frontier > self.current.frontier {
                    return Err(corrupt("dependency directory frontier"));
                }
                let key = call.full_key(tree, page.entry(position))?;
                if key.len() < 4
                    || usize::from(super::bytes::u16_at(&key, 0)) + 4 != key.len()
                    || super::bytes::u16_at(&key, key.len() - 2) != entry.group
                    || std::str::from_utf8(&key[2..key.len() - 2]).is_err()
                {
                    return Err(corrupt("dependency directory coordinate"));
                }
                call.pages.clear();
                self.tree(
                    &entry.locator,
                    entry.locator.page,
                    entry.locator.height - 1,
                    true,
                )?;
            } else {
                let location = Location::decode_for_current(entry, self.current)?;
                if location.version.batch > self.current.frontier {
                    return Err(corrupt("dependency location frontier"));
                }
                if location.source != Source::Tombstone {
                    self.object(call, &location)?;
                }
            }
        }
        Ok(())
    }

    fn tree_parts(&mut self, tree: &TreeRoot, call: &super::ReadCall) -> Result<()> {
        for (address, loaded) in &call.pages {
            if loaded.kind() != 5 {
                self.dependencies.add(Coordinate {
                    kind: if tree.kind == TreeKind::Directory {
                        1
                    } else {
                        2
                    },
                    object: tree.object,
                    batch: 0,
                    part: (address.number / 512) as u32,
                })?;
            }
        }
        Ok(())
    }

    fn object(&mut self, call: &mut super::ReadCall, location: &Location) -> Result<()> {
        let object = call
            .object_metadata(location)?
            .ok_or_else(|| corrupt("publication requires complete object bounds"))?;
        let end = location
            .first_block
            .checked_mul(4096)
            .and_then(|n| n.checked_add(u64::from(location.first_position)))
            .and_then(|n| n.checked_add(location.record_bytes))
            .ok_or_else(|| corrupt("dependency extent overflow"))?;
        if end > object.logical_bytes {
            return Err(corrupt("dependency extent outside object"));
        }
        let batch = if location.source == Source::Journal {
            location.version.batch
        } else {
            0
        };
        let map_parts = object.blocks.div_ceil(super::MAP_ENTRIES).div_ceil(512);
        for part in 0..map_parts {
            self.dependencies.add(Coordinate {
                kind: 3,
                object: location.object,
                batch,
                part: part as u32,
            })?;
        }
        for part in 0..object.data_parts {
            self.dependencies.add(Coordinate {
                kind: if location.source == Source::Journal {
                    4
                } else {
                    5
                },
                object: location.object,
                batch,
                part: part as u32,
            })?;
        }
        Ok(())
    }
}
