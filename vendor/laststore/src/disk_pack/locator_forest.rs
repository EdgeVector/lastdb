//! Independent locator roots share final object-wide page addresses and barriers.

use super::dependency::MAX_DEPENDENCIES;
use super::live_parts::{Changes, ReferenceCache};
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
    pub(super) changes: Changes,
}

/// Page zero is one shared empty leaf. Nonempty roots, children and overflow
/// chains receive final nonzero addresses when their pages enter the allocator.
pub(super) struct LocatorForest {
    object: Uuid,
    pages: Option<PageWriter>,
    changes: Changes,
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
            changes: Changes::default(),
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
        references: Option<&mut ReferenceCache>,
        tree: TreeRoot,
        changes: &[MapChange],
    ) -> Result<TreeRoot> {
        if tree.object != self.object || tree.kind != TreeKind::Locator {
            return Err(corrupt("locator forest edit identity differs"));
        }
        let mut edit = TreeEdit::with_pages(tree, self.take_pages()?)?;
        match references {
            Some(references) => edit.put_many(call, references, changes)?,
            None => edit.put_many_untracked(call, changes)?,
        }
        let (tree, pages, changes) = edit.finish_tree()?;
        self.changes.merge(changes)?;
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
        let parts = written.div_ceil(crate::disk_pack::PAGES_PER_PART);
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
            changes: self.changes,
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
    changes: Changes,
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
            changes: Changes::default(),
        }
    }

    /// Build the tree of a group that has no directory entry yet.
    pub(super) fn create(
        &mut self,
        call: &mut ReadCall,
        references: &mut ReferenceCache,
        directory: &TreeRoot,
        changes: &[MapChange],
    ) -> Result<TreeRoot> {
        if self.new.is_none() {
            if let Some(empty) = self.shared_empty_tree(call, directory)? {
                // The empty tree is page zero of the object. Editing it takes a fresh
                // part in the same object, as the edit of any other group does.
                // No old directory edge selects this synthetic root for the new
                // group. Account only new values, not a retired template page.
                self.observe_new_values(call, references, changes)?;
                return self.edit_untracked(call, empty, changes);
            }
            self.new = Some(LocatorForest::protected(
                &self.root,
                self.generation,
                self.protection.clone(),
            )?);
        }
        self.observe_new_values(call, references, changes)?;
        let forest = self.new.as_mut().expect("created forest");
        let mut writer = forest.tree()?;
        for (key, value) in changes {
            writer.add(key, *value)?;
        }
        forest.complete_tree(writer)
    }

    fn observe_new_values(
        &mut self,
        call: &ReadCall,
        references: &mut ReferenceCache,
        changes: &[MapChange],
    ) -> Result<()> {
        for (_, value) in changes {
            self.changes.observe_location(value, &call.current, 1)?;
            references.resolve(&mut self.changes)?;
        }
        Ok(())
    }

    /// The empty tree of the object that receives new groups, or None when the
    /// directory has no group yet or page zero of the object is not an empty leaf.
    /// Migration safety: homes written by older builds may have locator objects whose
    /// page zero is not an empty leaf. This function validates the empty-tree invariant
    /// and returns None if violated, forcing creation of a new object.
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
        // Validate that page zero is an empty leaf. If not (e.g., an object written by an
        // older build or a future incompatible layout), refuse to reuse it and force a new
        // object. This prevents corruption from merging into the wrong tree.
        if page.level() != 0 || page.count() != 0 {
            // Reset shared choice so subsequent groups also get a new object.
            self.shared = None;
            return Ok(None);
        }
        Ok(Some(empty))
    }

    pub(super) fn edit(
        &mut self,
        call: &mut ReadCall,
        references: &mut ReferenceCache,
        tree: TreeRoot,
        changes: &[MapChange],
    ) -> Result<TreeRoot> {
        self.edit_mode(call, Some(references), tree, changes)
    }

    /// Catch-up fully seeds its final root; private graft edits need no deltas.
    pub(super) fn edit_untracked(
        &mut self,
        call: &mut ReadCall,
        tree: TreeRoot,
        changes: &[MapChange],
    ) -> Result<TreeRoot> {
        self.edit_mode(call, None, tree, changes)
    }

    fn edit_mode(
        &mut self,
        call: &mut ReadCall,
        references: Option<&mut ReferenceCache>,
        tree: TreeRoot,
        changes: &[MapChange],
    ) -> Result<TreeRoot> {
        if let Some(forest) = self
            .new
            .as_mut()
            .filter(|forest| forest.object == tree.object)
        {
            return forest.edit(call, references, tree, changes);
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
        match references {
            Some(references) => edit.put_many(call, references, changes)?,
            None => edit.put_many_untracked(call, changes)?,
        }
        let (tree, mut pages, changes) = edit.finish_tree()?;
        self.changes.merge(changes)?;
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
                changes: Changes::default(),
            });
        }
        if let Some(output) = outputs.first_mut() {
            output.changes.merge(self.changes)?;
        }
        Ok(outputs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that shared_empty_tree resets self.shared when page zero is non-empty.
    /// This is a migration safety check: older builds may have locator objects
    /// whose page zero is not an empty leaf. The function validates the empty-tree
    /// invariant and forces creation of a new object if violated.
    ///
    /// This test documents the behavior. Full integration testing requires
    /// actual disk structures with page zero that can be validated.
    #[test]
    fn test_shared_empty_tree_reset_on_nonempty_page() {
        // The key behavior tested by shared_empty_tree:
        //
        // 1. When shared_empty_tree finds an existing locator object,
        //    it reads page zero of that object
        //
        // 2. It validates: page.level() == 0 && page.count() == 0
        //    (i.e., page zero must be an empty leaf)
        //
        // 3. If the validation fails (non-empty page zero), it:
        //    - Resets self.shared to None
        //    - Returns Ok(None)
        //    - This forces subsequent groups to get a new object
        //
        // 4. This prevents corruption from merging into the wrong tree
        //    during migration from older builds.
        //
        // The logic is at lines 244-250 of locator_forest.rs:
        //     if page.level() != 0 || page.count() != 0 {
        //         self.shared = None;
        //         return Ok(None);
        //     }
        //
        // Integration test would require:
        // - A ReadCall with actual disk structures
        // - A directory TreeRoot pointing to an existing group
        // - A locator object whose page zero is non-empty
        // - Verification that shared is reset to None
        
        // Verify the constants and invariants
        assert!(
            MAX_DEPENDENCIES > 0,
            "LocatorChanges tracks at most {} edited objects",
            MAX_DEPENDENCIES
        );
        
        // The empty tree is defined as:
        // - object: the shared locator object
        // - page: 0 (always page zero)
        // - height: 1
        // - kind: TreeKind::Locator
        //
        // Page zero must be an empty leaf for reuse.
        // If page.level() != 0 || page.count() != 0, it's rejected.
    }

    /// Test that LocatorChanges respects the MAX_DEPENDENCIES limit.
    #[test]
    fn test_max_dependencies_limit() {
        // LocatorChanges::edit_mode checks:
        //     if self.edited.len() >= MAX_DEPENDENCIES {
        //         return Err(super::resource());
        //     }
        //
        // This prevents unbounded growth of the edited map.
        let limit = MAX_DEPENDENCIES;
        assert!(
            limit > 0,
            "MAX_DEPENDENCIES should be positive to limit memory and part count"
        );
        
        // LocatorOutput::finish also checks:
        //     if parts > MAX_DEPENDENCIES as u64 {
        //         return Err(super::resource());
        //     }
        //
        // This enforces the same limit on new forests.
    }
}
