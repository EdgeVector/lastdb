use super::live_parts::{Changes, ReferenceCache};
use super::page::Page;
use super::sparse_pages::SparsePages;
use super::tree_output::{self, Child, Descriptor, LeafEntry, TreePages};
use super::{corrupt, key_layout, ReadCall, TreeRoot};
use crate::Result;
use std::borrow::Cow;
use std::collections::HashSet;
use std::path::Path;

pub(super) type MapChange = (Vec<u8>, [u8; 128]);

pub(super) struct TreeEdit<P = SparsePages> {
    root: TreeRoot,
    pages: P,
    failed: bool,
    track_changes: bool,
    changes: Changes,
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
        Self::protected(root, tree, generation, Default::default())
    }

    pub(super) fn protected(
        root: &Path,
        tree: TreeRoot,
        generation: u64,
        protection: super::private_files::OutputFiles,
    ) -> Result<Self> {
        let pages = SparsePages::protected(root, &tree, generation, protection)?;
        Self::from_pages(tree, pages)
    }

    pub(super) fn from_pages(tree: TreeRoot, pages: SparsePages) -> Result<Self> {
        pages.check_tree(&tree)?;
        Self::with_pages(tree, pages)
    }

    pub(super) fn finish_with_changes(self) -> Result<(TreeRoot, u64, Vec<u32>, Changes)> {
        let (tree, pages, changes) = self.finish_tree()?;
        let written = pages.written_pages;
        let parts = pages.finish()?;
        Ok((tree, written, parts, changes))
    }
}

impl<P: TreePages> TreeEdit<P> {
    pub(super) fn with_pages(tree: TreeRoot, pages: P) -> Result<Self> {
        tree.validate()?;
        Ok(Self {
            root: tree,
            pages,
            failed: false,
            track_changes: true,
            changes: Changes::default(),
        })
    }

    pub(super) fn root(&self) -> &TreeRoot {
        &self.root
    }

    /// A complete off-gate seed accounts catch-up output after all grafts finish.
    pub(super) fn put_many_untracked(
        &mut self,
        call: &mut ReadCall,
        changes: &[MapChange],
    ) -> Result<()> {
        let mut references = ReferenceCache::new(&call.root, &call.current);
        let tracked = std::mem::replace(&mut self.track_changes, false);
        let result = self.put_many(call, &mut references, changes);
        self.track_changes = tracked;
        result
    }

    /// Install sorted final locators, copying each touched old page only once.
    /// Stored operations and their original physical versions remain unchanged.
    pub(super) fn put_many(
        &mut self,
        call: &mut ReadCall,
        references: &mut ReferenceCache,
        changes: &[MapChange],
    ) -> Result<()> {
        if self.failed
            || changes.len() > call.current.batch_operation_limit()
            || changes
                .iter()
                .any(|(key, _)| key.len() > call.current.storage_key_byte_limit())
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
        let mut children = self.edit(call, references, path, changes)?;
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
        references: &mut ReferenceCache,
        path: PathStep,
        changes: &[MapChange],
    ) -> Result<Vec<Child>> {
        let page = call.tree_page(&self.root, path.number)?;
        validate_page(call, &self.root, &page, &path)?;
        if self.track_changes {
            self.changes.observe_page(
                self.root.kind,
                self.root.object,
                path.number,
                &page,
                &call.current,
                -1,
            )?;
            references.resolve(&mut self.changes)?;
        }
        if path.level == 0 {
            return self.edit_leaf(call, references, &page, changes);
        }
        let mut children = vec![Child {
            minimum: path.minimum,
            number: page.left(),
        }];
        for index in 0..page.count() {
            let entry = page.entry(index);
            children.push(Child {
                minimum: Some(entry[..key_layout::descriptor_bytes(entry)].to_vec()),
                number: key_layout::child(entry),
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
                references,
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
        super::tree_partition::parents(self.root.kind, children)?
            .into_iter()
            .map(|range| {
                tree_output::parent(
                    &mut self.pages,
                    self.root.kind,
                    level as usize,
                    &children[range],
                )
            })
            .collect()
    }

    fn edit_leaf(
        &mut self,
        call: &mut ReadCall,
        references: &mut ReferenceCache,
        page: &Page,
        changes: &[MapChange],
    ) -> Result<Vec<Child>> {
        let mut entries = Vec::<LeafEntry>::with_capacity(page.count() + changes.len());
        let mut updates = changes.iter().peekable();
        for index in 0..page.count() {
            let entry = page.entry(index);
            let key = call.key_bytes(&self.root, entry)?;
            while updates
                .peek()
                .is_some_and(|(next, _)| next.as_slice() < key.as_ref())
            {
                let (key, value) = updates.next().expect("peeked change");
                self.change_value(call, references, None, value)?;
                entries.push((
                    tree_output::key_descriptor(
                        &mut self.pages,
                        self.root.kind,
                        key,
                        call.current.storage_key_byte_limit(),
                    )?,
                    *value,
                ));
            }
            let descriptor = entry[..key_layout::descriptor_bytes(entry)].to_vec();
            let value = if updates
                .peek()
                .is_some_and(|(next, _)| next.as_slice() == key.as_ref())
            {
                let (_, value) = updates.next().expect("peeked replacement");
                self.change_value(call, references, Some(key_layout::value(entry)), value)?;
                *value
            } else {
                key_layout::value(entry).try_into().expect("leaf value")
            };
            entries.push((descriptor, value));
        }
        for (key, value) in updates {
            self.change_value(call, references, None, value)?;
            entries.push((
                tree_output::key_descriptor(
                    &mut self.pages,
                    self.root.kind,
                    key,
                    call.current.storage_key_byte_limit(),
                )?,
                *value,
            ));
        }
        super::tree_partition::leaves(self.root.kind, &entries)?
            .into_iter()
            .map(|range| tree_output::leaf(&mut self.pages, self.root.kind, &entries[range]))
            .collect()
    }

    fn change_value(
        &mut self,
        call: &ReadCall,
        references: &mut ReferenceCache,
        old: Option<&[u8]>,
        new: &[u8; 128],
    ) -> Result<()> {
        if self.track_changes && self.root.kind == super::TreeKind::Locator {
            if let Some(old) = old {
                self.changes.observe_location(old, &call.current, -1)?;
            }
            self.changes.observe_location(new, &call.current, 1)?;
            references.resolve(&mut self.changes)?;
        }
        Ok(())
    }

    /// Keep private pages available to other trees in the same object.
    pub(super) fn finish_tree(self) -> Result<(TreeRoot, P, Changes)> {
        if self.failed {
            return Err(corrupt("refusing failed tree edit"));
        }
        Ok((self.root, self.pages, self.changes))
    }
}

fn validate_page(call: &mut ReadCall, tree: &TreeRoot, page: &Page, path: &PathStep) -> Result<()> {
    if page.level() != path.level
        || (!path.is_root && path.level > 0 && page.count() < 7)
        || (!path.is_root && path.level == 0 && page.count() == 0)
    {
        return Err(corrupt("tree edit level or occupancy"));
    }
    let mut previous: Option<Cow<'_, [u8]>> = None;
    let mut children = HashSet::new();
    if page.level() > 0 {
        children.insert(page.left());
    }
    for number in 0..page.count() {
        let entry = page.entry(number);
        let key = call.key_bytes(tree, entry)?;
        if previous.as_deref().is_some_and(|p| p >= key.as_ref())
            || path.lower.as_deref().is_some_and(|p| p > key.as_ref())
            || path.upper.as_deref().is_some_and(|p| p <= key.as_ref())
        {
            return Err(corrupt("tree edit key order or parent interval"));
        }
        if page.level() > 0 {
            let child = key_layout::child(entry);
            if child == path.number || !children.insert(child) {
                return Err(corrupt("tree edit duplicate child"));
            }
        }
        previous = Some(key);
    }
    Ok(())
}
