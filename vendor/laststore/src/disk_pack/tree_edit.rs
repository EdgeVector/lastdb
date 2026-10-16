use super::bytes::u64_at;
use super::page::Page;
use super::sparse_pages::SparsePages;
use super::tree_output::{self, Child, Descriptor, LeafEntry};
use super::{corrupt, ReadCall, TreeRoot, KEY_DESCRIPTOR};
use crate::Result;
use std::collections::HashSet;
use std::path::Path;

pub(super) type MapChange = (Vec<u8>, [u8; 128]);

pub(super) struct TreeEdit {
    root: TreeRoot,
    pages: SparsePages,
    failed: bool,
}

struct PathStep {
    number: u64,
    level: u8,
    minimum: Option<Descriptor>,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    is_root: bool,
}

impl TreeEdit {
    pub(super) fn new(root: &Path, tree: TreeRoot, generation: u64) -> Result<Self> {
        let pages = SparsePages::new(root, &tree, generation)?;
        Ok(Self {
            root: tree,
            pages,
            failed: false,
        })
    }

    pub(super) fn root(&self) -> &TreeRoot {
        &self.root
    }

    /// Install sorted final locators, copying each touched old page only once.
    /// Journal operations and their original physical versions remain unchanged.
    pub(super) fn put_many(&mut self, call: &mut ReadCall, changes: &[MapChange]) -> Result<()> {
        if self.failed
            || changes.len() > 4096
            || changes.iter().any(|(key, _)| key.len() > super::KEY_BYTES)
            || changes.windows(2).any(|pair| pair[0].0 >= pair[1].0)
        {
            return Err(corrupt("failed tree edit or invalid sorted changes"));
        }
        if changes.is_empty() {
            return Ok(());
        }
        self.failed = true;
        let path = PathStep {
            number: self.root.page,
            level: self.root.height - 1,
            minimum: None,
            lower: None,
            upper: None,
            is_root: true,
        };
        let mut children = self.edit(call, path, changes)?;
        let mut height = self.root.height;
        while children.len() > 1 {
            children = self.parents(height, &children)?;
            height += 1;
        }
        self.root.page = children[0].number;
        self.root.height = height;
        self.failed = false;
        Ok(())
    }

    fn edit(
        &mut self,
        call: &mut ReadCall,
        path: PathStep,
        changes: &[MapChange],
    ) -> Result<Vec<Child>> {
        let page = call.tree_page(&self.root, path.number)?;
        validate_page(call, &self.root, &page, &path)?;
        if path.level == 0 {
            return self.edit_leaf(call, &page, changes);
        }
        let mut children = vec![Child {
            minimum: path.minimum,
            number: page.left(),
        }];
        for entry in page.payload().chunks_exact(super::INTERNAL_WIDTH) {
            children.push(Child {
                minimum: Some(entry[..KEY_DESCRIPTOR].try_into().expect("key descriptor")),
                number: u64_at(entry, KEY_DESCRIPTOR),
            });
        }
        let mut boundaries = Vec::with_capacity(children.len() + 1);
        boundaries.push(path.lower);
        for child in &children[1..] {
            boundaries.push(Some(
                call.full_key(&self.root, child.minimum.as_ref().expect("separator"))?,
            ));
        }
        boundaries.push(path.upper);
        let mut out = Vec::new();
        let mut first = 0;
        for (position, child) in children.into_iter().enumerate() {
            let end = boundaries[position + 1]
                .as_deref()
                .map_or(changes.len(), |upper| {
                    first + changes[first..].partition_point(|(key, _)| key.as_slice() < upper)
                });
            if first == end {
                out.push(child);
                continue;
            }
            out.extend(self.edit(
                call,
                PathStep {
                    number: child.number,
                    level: path.level - 1,
                    minimum: child.minimum,
                    lower: boundaries[position].clone(),
                    upper: boundaries[position + 1].clone(),
                    is_root: false,
                },
                &changes[first..end],
            )?);
            first = end;
        }
        if first != changes.len() {
            return Err(corrupt("tree changes outside parent interval"));
        }
        self.parents(path.level, &out)
    }

    fn parents(&mut self, level: u8, children: &[Child]) -> Result<Vec<Child>> {
        let chunk = children.len().div_ceil(children.len().div_ceil(89));
        children
            .chunks(chunk)
            .map(|items| tree_output::parent(&mut self.pages, level as usize, items))
            .collect()
    }

    fn edit_leaf(
        &mut self,
        call: &mut ReadCall,
        page: &Page,
        changes: &[MapChange],
    ) -> Result<Vec<Child>> {
        let mut entries = Vec::<LeafEntry>::with_capacity(page.count() + changes.len());
        let mut updates = changes.iter().peekable();
        for entry in page.payload().chunks_exact(super::LEAF_WIDTH) {
            let key = call.full_key(&self.root, entry)?;
            while updates.peek().is_some_and(|(next, _)| *next < key) {
                let (key, value) = updates.next().expect("peeked change");
                entries.push((tree_output::key_descriptor(&mut self.pages, key)?, *value));
            }
            let descriptor = entry[..KEY_DESCRIPTOR].try_into().expect("key descriptor");
            let value = if updates.peek().is_some_and(|(next, _)| *next == key) {
                updates.next().expect("peeked replacement").1
            } else {
                entry[KEY_DESCRIPTOR..].try_into().expect("leaf value")
            };
            entries.push((descriptor, value));
        }
        for (key, value) in updates {
            entries.push((tree_output::key_descriptor(&mut self.pages, key)?, *value));
        }
        let chunk = entries
            .len()
            .div_ceil(entries.len().div_ceil(super::LEAF_ENTRIES));
        entries
            .chunks(chunk)
            .map(|items| tree_output::leaf(&mut self.pages, self.root.kind, items))
            .collect()
    }

    pub(super) fn finish(self) -> Result<(TreeRoot, u64, Vec<u32>)> {
        if self.failed {
            return Err(corrupt("refusing failed tree edit"));
        }
        let pages = self.pages.written_pages;
        let parts = self.pages.finish()?;
        Ok((self.root, pages, parts))
    }
}

fn validate_page(call: &mut ReadCall, tree: &TreeRoot, page: &Page, path: &PathStep) -> Result<()> {
    if page.level() != path.level
        || (!path.is_root && path.level > 0 && page.count() < 7)
        || (!path.is_root && path.level == 0 && page.count() == 0)
    {
        return Err(corrupt("tree edit level or occupancy"));
    }
    let mut previous: Option<Vec<u8>> = None;
    let mut children = HashSet::new();
    if page.level() > 0 {
        children.insert(page.left());
    }
    for number in 0..page.count() {
        let entry = page.entry(number);
        let key = call.full_key(tree, entry)?;
        if previous.as_deref().is_some_and(|p| p >= key.as_slice())
            || path.lower.as_deref().is_some_and(|p| p > key.as_slice())
            || path.upper.as_deref().is_some_and(|p| p <= key.as_slice())
        {
            return Err(corrupt("tree edit key order or parent interval"));
        }
        if page.level() > 0 {
            let child = u64_at(entry, KEY_DESCRIPTOR);
            if child == path.number || !children.insert(child) {
                return Err(corrupt("tree edit duplicate child"));
            }
        }
        previous = Some(key);
    }
    Ok(())
}
