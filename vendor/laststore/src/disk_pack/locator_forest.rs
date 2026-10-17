//! Independent locator roots share final object-wide page addresses and barriers.

use super::dependency::MAX_DEPENDENCIES;
use super::page_write::PageWriter;
use super::sparse_pages::SparsePages;
use super::tree_edit::{MapChange, TreeEdit};
use super::tree_output;
use super::tree_write::TreeWriter;
use super::{corrupt, ReadCall, TreeKind, TreeRoot};
use crate::Result;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub(super) struct LocatorOutput {
    pub(super) tree: TreeRoot,
    pub(super) pages: u64,
    pub(super) parts: Vec<u32>,
}

/// Page zero is one shared empty leaf. Nonempty roots, children and overflow
/// chains receive final nonzero addresses when their pages enter the allocator.
pub(super) struct LocatorForest {
    object: Uuid,
    pages: Option<PageWriter>,
}

impl LocatorForest {
    pub(super) fn new(root: &Path, generation: u64) -> Result<Self> {
        let object = Uuid::new_v4();
        let mut pages = PageWriter::new(
            root.join("locator").join(object.to_string()),
            object,
            generation,
        )?;
        pages.append(tree_output::leaf_fields(TreeKind::Locator, 0), &[])?;
        Ok(Self {
            object,
            pages: Some(pages),
        })
    }

    pub(super) fn tree(&mut self) -> Result<TreeWriter> {
        TreeWriter::from_pages(self.take_pages()?, TreeKind::Locator)
    }

    pub(super) fn complete_tree(&mut self, writer: TreeWriter) -> Result<TreeRoot> {
        let (tree, pages) = writer.finish_tree()?;
        self.restore_pages(&tree, pages)?;
        Ok(tree)
    }

    fn edit(
        &mut self,
        call: &mut ReadCall,
        tree: TreeRoot,
        changes: &[MapChange],
    ) -> Result<TreeRoot> {
        if tree.object != self.object || tree.kind != TreeKind::Locator {
            return Err(corrupt("locator forest edit identity differs"));
        }
        let mut edit = TreeEdit::with_pages(tree, self.take_pages()?)?;
        edit.put_many(call, changes)?;
        let (tree, pages) = edit.finish_tree()?;
        self.restore_pages(&tree, pages)?;
        Ok(tree)
    }

    fn take_pages(&mut self) -> Result<PageWriter> {
        self.pages
            .take()
            .ok_or_else(|| corrupt("locator forest has an incomplete tree"))
    }

    fn restore_pages(&mut self, tree: &TreeRoot, pages: PageWriter) -> Result<()> {
        if self.pages.is_some()
            || tree.object != self.object
            || tree.kind != TreeKind::Locator
            || pages.object() != self.object
        {
            return Err(corrupt("locator forest allocator identity differs"));
        }
        self.pages = Some(pages);
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<LocatorOutput> {
        let pages = self.take_pages()?;
        let written = pages.next_page();
        let parts = written.div_ceil(512);
        if parts > MAX_DEPENDENCIES as u64 {
            return Err(super::resource());
        }
        pages.finish()?;
        Ok(LocatorOutput {
            tree: TreeRoot {
                object: self.object,
                page: 0,
                height: 1,
                kind: TreeKind::Locator,
            },
            pages: written,
            parts: (0..parts).map(|part| part as u32).collect(),
        })
    }
}

/// One allocator per existing object and one shared object for new groups.
/// Parked sparse allocators hold no descriptors between individual tree edits.
pub(super) struct LocatorChanges {
    root: PathBuf,
    generation: u64,
    new: Option<LocatorForest>,
    edited: BTreeMap<Uuid, (TreeRoot, SparsePages)>,
}

impl LocatorChanges {
    pub(super) fn new(root: &Path, generation: u64) -> Self {
        Self {
            root: root.to_path_buf(),
            generation,
            new: None,
            edited: BTreeMap::new(),
        }
    }

    pub(super) fn create(&mut self, changes: &[MapChange]) -> Result<TreeRoot> {
        if self.new.is_none() {
            self.new = Some(LocatorForest::new(&self.root, self.generation)?);
        }
        let forest = self.new.as_mut().expect("created forest");
        let mut writer = forest.tree()?;
        for (key, value) in changes {
            writer.add(key, *value)?;
        }
        forest.complete_tree(writer)
    }

    pub(super) fn edit(
        &mut self,
        call: &mut ReadCall,
        tree: TreeRoot,
        changes: &[MapChange],
    ) -> Result<TreeRoot> {
        if let Some(forest) = self
            .new
            .as_mut()
            .filter(|forest| forest.object == tree.object)
        {
            return forest.edit(call, tree, changes);
        }
        let pages = match self.edited.remove(&tree.object) {
            Some((_, pages)) => pages,
            None => {
                if self.edited.len() >= MAX_DEPENDENCIES {
                    return Err(super::resource());
                }
                SparsePages::new(&self.root, &tree, self.generation)?
            }
        };
        let mut edit = TreeEdit::from_pages(tree, pages)?;
        edit.put_many(call, changes)?;
        let (tree, mut pages) = edit.finish_tree()?;
        pages.park();
        self.edited.insert(tree.object, (tree.clone(), pages));
        Ok(tree)
    }

    pub(super) fn finish(self) -> Result<Vec<LocatorOutput>> {
        let mut outputs = Vec::with_capacity(self.edited.len() + usize::from(self.new.is_some()));
        if let Some(forest) = self.new {
            outputs.push(forest.finish()?);
        }
        for (_, (tree, pages)) in self.edited {
            let written = pages.written_pages;
            outputs.push(LocatorOutput {
                tree,
                pages: written,
                parts: pages.finish()?,
            });
        }
        Ok(outputs)
    }
}
