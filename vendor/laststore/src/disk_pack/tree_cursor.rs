use super::bytes::u64_at;
use super::page::Page;
use super::{corrupt, ReadCall, TreeRoot, KEY_DESCRIPTOR};
use crate::Result;
use std::sync::Arc;

type Bound = Option<Arc<Vec<u8>>>;

struct Path {
    number: u64,
    level: u8,
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
}

impl TreeCursor {
    pub(super) fn seek(call: &mut ReadCall, tree: TreeRoot, key: &[u8]) -> Result<Self> {
        tree.validate()?;
        let path = Path {
            number: tree.page,
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
        };
        cursor.descend(call, path, Some(key))?;
        Ok(cursor)
    }

    fn descend(&mut self, call: &mut ReadCall, mut path: Path, seek: Option<&[u8]>) -> Result<()> {
        loop {
            let page = call.tree_page(&self.tree, path.number)?;
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
            number: if child == 0 {
                page.left()
            } else {
                u64_at(page.entry(child - 1), KEY_DESCRIPTOR)
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
                return Ok(true);
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
                entry[KEY_DESCRIPTOR..].try_into().expect("leaf value"),
            )));
        }
    }
}
