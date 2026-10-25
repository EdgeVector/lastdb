use super::page::Page;
use super::{corrupt, key_layout, ReadCall, TreeRoot};
use crate::Result;
use std::sync::Arc;

type Bound = Option<Arc<Vec<u8>>>;

struct Path {
    number: u64,
    level: u8,
    parent_generation: u64,
    lower: Bound,
    upper: Bound,
}

struct Branch {
    page: Arc<Page>,
    child: usize,
    lower: Bound,
    upper: Bound,
}

/// An ancestor stack replaces sibling links, which are unsafe across path copies.
pub(super) struct TreeCursor {
    tree: TreeRoot,
    branches: Vec<Branch>,
    leaf: Option<Arc<Page>>,
    position: usize,
    lower: Bound,
    upper: Bound,
    previous: Option<Vec<u8>>,
    after_generation: u64,
}

impl TreeCursor {
    /// Account the unique keys and pages that survive a per-record call reset.
    pub(super) fn retained_bytes(&self) -> Result<u64> {
        let mut bytes = self
            .previous
            .as_ref()
            .map_or(0, |key| key.capacity() as u64);
        let mut keys = Vec::new();
        for bound in [&self.lower, &self.upper].into_iter().chain(
            self.branches
                .iter()
                .flat_map(|branch| [&branch.lower, &branch.upper]),
        ) {
            if let Some(key) = bound {
                let identity = Arc::as_ptr(key);
                if !keys.contains(&identity) {
                    keys.push(identity);
                    bytes = bytes
                        .checked_add(key.capacity() as u64)
                        .ok_or_else(super::resource)?;
                }
            }
        }
        let mut pages = Vec::new();
        for page in self
            .leaf
            .iter()
            .chain(self.branches.iter().map(|branch| &branch.page))
        {
            let identity = Arc::as_ptr(page);
            if !pages.contains(&identity) {
                pages.push(identity);
                bytes = bytes
                    .checked_add(super::PAGE_BYTES as u64)
                    .ok_or_else(super::resource)?;
            }
        }
        Ok(bytes)
    }

    pub(super) fn seek(call: &mut ReadCall, tree: TreeRoot, key: &[u8]) -> Result<Self> {
        Self::seek_inner(call, tree, key, 0)
    }

    pub(super) fn recent(call: &mut ReadCall, tree: TreeRoot, after: u64) -> Result<Self> {
        Self::seek_inner(call, tree, b"", after)
    }

    fn seek_inner(call: &mut ReadCall, tree: TreeRoot, key: &[u8], after: u64) -> Result<Self> {
        tree.validate()?;
        let path = Path {
            number: tree.page,
            parent_generation: call.current.generation,
            level: tree.height - 1,
            lower: None,
            upper: None,
        };
        let mut cursor = Self {
            tree,
            branches: Vec::new(),
            leaf: None,
            position: 0,
            lower: None,
            upper: None,
            previous: None,
            after_generation: after,
        };
        cursor.descend(call, path, Some(key))?;
        if cursor.leaf.is_none() {
            cursor.advance_leaf(call)?;
        }
        Ok(cursor)
    }

    fn descend(&mut self, call: &mut ReadCall, mut path: Path, seek: Option<&[u8]>) -> Result<()> {
        loop {
            let page = call.tree_page(&self.tree, path.number)?;
            if page.generation() > path.parent_generation {
                return Err(corrupt("tree child is newer than its parent"));
            }
            if page.generation() <= self.after_generation {
                return Ok(());
            }
            let is_root = self.branches.is_empty();
            if page.level() != path.level
                || (!is_root && page.level() > 0 && page.count() < 7)
                || (!is_root && page.level() == 0 && page.count() == 0)
                || self
                    .branches
                    .iter()
                    .any(|parent| Arc::ptr_eq(&parent.page, &page))
            {
                return Err(corrupt("cursor tree level, occupancy, or ancestor cycle"));
            }
            let (position, equal) = if let Some(key) = seek {
                call.search_page(&self.tree, &page, key)?
            } else {
                (0, false)
            };
            call.check_boundaries(
                &self.tree,
                &page,
                position,
                path.lower.as_ref().map(|key| key.as_slice()),
                path.upper.as_ref().map(|key| key.as_slice()),
            )?;
            if path.level == 0 {
                self.leaf = Some(page);
                self.position = position;
                self.lower = path.lower;
                self.upper = path.upper;
                return Ok(());
            }
            let child = position + usize::from(equal);
            let next =
                self.child_path(call, &page, child, path.lower.clone(), path.upper.clone())?;
            self.branches.push(Branch {
                page,
                child,
                lower: path.lower,
                upper: path.upper,
            });
            if self.branches.len() >= 8 {
                return Err(corrupt("cursor height limit"));
            }
            path = next;
        }
    }

    fn child_path(
        &self,
        call: &mut ReadCall,
        page: &Page,
        child: usize,
        lower: Bound,
        upper: Bound,
    ) -> Result<Path> {
        if child > page.count() || page.level() == 0 {
            return Err(corrupt("cursor child position"));
        }
        Ok(Path {
            parent_generation: page.generation(),
            number: if child == 0 {
                page.left()
            } else {
                key_layout::child(page.entry(child - 1))
            },
            level: page.level() - 1,
            lower: if child == 0 {
                lower
            } else {
                Some(Arc::new(call.full_key(&self.tree, page.entry(child - 1))?))
            },
            upper: if child == page.count() {
                upper
            } else {
                Some(Arc::new(call.full_key(&self.tree, page.entry(child))?))
            },
        })
    }

    fn advance_leaf(&mut self, call: &mut ReadCall) -> Result<bool> {
        self.leaf = None;
        while let Some(mut branch) = self.branches.pop() {
            if branch.child < branch.page.count() {
                branch.child += 1;
                let path = self.child_path(
                    call,
                    &branch.page,
                    branch.child,
                    branch.lower.clone(),
                    branch.upper.clone(),
                )?;
                self.branches.push(branch);
                self.descend(call, path, None)?;
                if self.leaf.is_some() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub(super) fn next(&mut self, call: &mut ReadCall) -> Result<Option<(Vec<u8>, [u8; 128])>> {
        loop {
            let Some(page) = self.leaf.as_ref().map(Arc::clone) else {
                return Ok(None);
            };
            if self.position == page.count() {
                if !self.advance_leaf(call)? {
                    return Ok(None);
                }
                continue;
            }
            call.examine()?;
            let entry = page.entry(self.position);
            let key = call.full_key(&self.tree, entry)?;
            if self
                .previous
                .as_deref()
                .is_some_and(|old| old >= key.as_slice())
                || self
                    .lower
                    .as_ref()
                    .is_some_and(|lower| lower.as_slice() > key.as_slice())
                || self
                    .upper
                    .as_ref()
                    .is_some_and(|upper| upper.as_slice() <= key.as_slice())
            {
                return Err(corrupt("cursor key order or parent interval"));
            }
            call.charge(key.len() as u64)?;
            self.previous = Some(key.clone());
            self.position += 1;
            return Ok(Some((
                key,
                key_layout::value(entry).try_into().expect("leaf value"),
            )));
        }
    }
}
