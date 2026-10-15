use super::bytes::u64_at;
use super::page::Page;
use super::sparse_pages::SparsePages;
use super::tree_output::{self, Child, Descriptor, LeafEntry};
use super::{corrupt, ReadCall, TreeRoot, KEY_DESCRIPTOR};
use crate::Result;
use std::collections::HashSet;
use std::path::Path;

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

    /// Replace one exact key by path copy. The enclosing publisher owns admission.
    pub(super) fn put(&mut self, call: &mut ReadCall, key: &[u8], value: [u8; 128]) -> Result<()> {
        if self.failed || key.len() > super::KEY_BYTES {
            return Err(corrupt("failed tree edit or oversized key"));
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
        let children = self.edit(call, path, key, value)?;
        let (number, height) = if children.len() == 1 {
            (children[0].number, self.root.height)
        } else {
            let parent =
                tree_output::parent(&mut self.pages, self.root.height as usize, &children)?;
            (parent.number, self.root.height + 1)
        };
        self.root.page = number;
        self.root.height = height;
        self.failed = false;
        Ok(())
    }

    fn edit(
        &mut self,
        call: &mut ReadCall,
        path: PathStep,
        key: &[u8],
        value: [u8; 128],
    ) -> Result<Vec<Child>> {
        let page = call.tree_page(&self.root, path.number)?;
        validate_page(call, &self.root, &page, &path)?;
        let (position, equal) = call.search_page(&self.root, &page, key)?;
        if path.level == 0 {
            return self.edit_leaf(&page, position, equal, key, value);
        }
        let mut children = Vec::with_capacity(page.count() + 2);
        children.push(Child {
            minimum: path.minimum,
            number: page.left(),
        });
        for entry in page.payload().chunks_exact(super::INTERNAL_WIDTH) {
            children.push(Child {
                minimum: Some(entry[..KEY_DESCRIPTOR].try_into().expect("key descriptor")),
                number: u64_at(entry, KEY_DESCRIPTOR),
            });
        }
        let selected = position + usize::from(equal);
        let lower = if selected == 0 {
            path.lower
        } else {
            Some(call.full_key(
                &self.root,
                children[selected].minimum.as_ref().expect("separator"),
            )?)
        };
        let upper = if selected + 1 == children.len() {
            path.upper
        } else {
            Some(call.full_key(
                &self.root,
                children[selected + 1].minimum.as_ref().expect("separator"),
            )?)
        };
        let replacement = self.edit(
            call,
            PathStep {
                number: children[selected].number,
                level: path.level - 1,
                minimum: children[selected].minimum,
                lower,
                upper,
                is_root: false,
            },
            key,
            value,
        )?;
        children.splice(selected..selected + 1, replacement);
        let chunk = if children.len() <= 89 {
            children.len()
        } else {
            children.len().div_ceil(2)
        };
        children
            .chunks(chunk)
            .map(|children| tree_output::parent(&mut self.pages, path.level as usize, children))
            .collect()
    }

    fn edit_leaf(
        &mut self,
        page: &Page,
        position: usize,
        equal: bool,
        key: &[u8],
        value: [u8; 128],
    ) -> Result<Vec<Child>> {
        let mut entries = page
            .payload()
            .chunks_exact(super::LEAF_WIDTH)
            .map(|entry| {
                (
                    entry[..KEY_DESCRIPTOR].try_into().expect("key descriptor"),
                    entry[KEY_DESCRIPTOR..].try_into().expect("leaf value"),
                )
            })
            .collect::<Vec<LeafEntry>>();
        if equal {
            entries[position].1 = value;
        } else {
            entries.insert(
                position,
                (tree_output::key_descriptor(&mut self.pages, key)?, value),
            );
        }
        let chunk = if entries.len() <= super::LEAF_ENTRIES {
            entries.len()
        } else {
            entries.len().div_ceil(2)
        };
        entries
            .chunks(chunk)
            .map(|entries| tree_output::leaf(&mut self.pages, self.root.kind, entries))
            .collect()
    }

    pub(super) fn finish(self) -> Result<(TreeRoot, u64, u64)> {
        if self.failed {
            return Err(corrupt("refusing failed tree edit"));
        }
        let counts = (self.pages.written_pages, self.pages.written_parts);
        self.pages.finish()?;
        Ok((self.root, counts.0, counts.1))
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
