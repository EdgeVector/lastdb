//! Independent locator roots share final object-wide page addresses and barriers.

use super::dependency::MAX_DEPENDENCIES;
use super::page_write::PageWriter;
use super::sparse_pages::SparsePages;
use super::tree_cursor::TreeCursor;
use super::tree_edit::{MapChange, TreeEdit};
use super::tree_output;
use super::tree_write::TreeWriter;
use super::{corrupt, DirectoryEntry, ReadCall, TreeKind, TreeRoot};
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
        Self::protected(root, generation, Default::default())
    }

    pub(super) fn protected(
        root: &Path,
        generation: u64,
        protection: super::private_files::OutputFiles,
    ) -> Result<Self> {
        let object = Uuid::new_v4();
        let mut pages = PageWriter::protected(
            root.join("locator").join(object.to_string()),
            object,
            generation,
            protection,
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

/// One allocator per existing object. New groups join an existing object, so one
/// publication adds one locator part, not one part for each object it touches.
/// A fresh object exists only for the first groups of an empty directory.
/// Parked sparse allocators hold no descriptors between individual tree edits.
pub(super) struct LocatorChanges {
    protection: super::private_files::OutputFiles,
    root: PathBuf,
    generation: u64,
    new: Option<LocatorForest>,
    /// The existing object that receives new groups, chosen once per publication.
    shared: Option<Uuid>,
    edited: BTreeMap<Uuid, (TreeRoot, SparsePages)>,
}

impl LocatorChanges {
    pub(super) fn new(root: &Path, generation: u64) -> Self {
        Self::protected(root, generation, Default::default())
    }

    pub(super) fn protected(
        root: &Path,
        generation: u64,
        protection: super::private_files::OutputFiles,
    ) -> Self {
        Self {
            protection,
            root: root.to_path_buf(),
            generation,
            new: None,
            shared: None,
            edited: BTreeMap::new(),
        }
    }

    /// Build the tree of a group that has no directory entry yet.
    pub(super) fn create(
        &mut self,
        call: &mut ReadCall,
        directory: &TreeRoot,
        changes: &[MapChange],
    ) -> Result<TreeRoot> {
        if self.new.is_none() {
            if let Some(empty) = self.shared_empty_tree(call, directory)? {
                // The empty tree is page zero of the object. Editing it takes a fresh
                // part in the same object, as the edit of any other group does.
                return self.edit(call, empty, changes);
            }
            self.new = Some(LocatorForest::protected(
                &self.root,
                self.generation,
                self.protection.clone(),
            )?);
        }
        let forest = self.new.as_mut().expect("created forest");
        let mut writer = forest.tree()?;
        for (key, value) in changes {
            writer.add(key, *value)?;
        }
        forest.complete_tree(writer)
    }

    /// The empty tree of the object that receives new groups, or None when the
    /// directory has no group yet or page zero of the object is not an empty leaf.
    fn shared_empty_tree(
        &mut self,
        call: &mut ReadCall,
        directory: &TreeRoot,
    ) -> Result<Option<TreeRoot>> {
        if self.shared.is_none() {
            // Prefer an object this publication already edits: its part is open.
            self.shared = match self.edited.keys().next() {
                Some(object) => Some(*object),
                None => {
                    let mut cursor = TreeCursor::seek(call, directory.clone(), b"")?;
                    match cursor.next(call)? {
                        Some((_, value)) => Some(DirectoryEntry::decode(&value)?.locator.object),
                        None => None,
                    }
                }
            };
        }
        let Some(object) = self.shared else {
            return Ok(None);
        };
        let empty = TreeRoot {
            object,
            page: 0,
            height: 1,
            kind: TreeKind::Locator,
        };
        let page = call.tree_page(&empty, 0)?;
        Ok((page.level() == 0 && page.count() == 0).then_some(empty))
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
                SparsePages::protected(&self.root, &tree, self.generation, self.protection.clone())?
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
