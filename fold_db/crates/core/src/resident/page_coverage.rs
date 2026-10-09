//! Exact-window proofs. Boundaries and identity only; tips remain in the graph.
use super::*;
use crate::schema::types::field::KeyWindow;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const MAX_PAGES: usize = 128;
const MAX_PARTITION_PAGES: usize = 32;
const MAX_PAGE_KEYS: usize = 1024;
const MAX_KEY_BYTES: usize = 1024;

#[derive(Debug, Clone)]
struct PageBounds {
    first: Option<String>,
    last: Option<String>,
    count: usize,
}

#[derive(Debug)]
pub(crate) struct PageRead {
    id: u64,
    molecule: String,
    hash: String,
    window: KeyWindow,
    include_tombstones: bool,
    valid: AtomicBool,
    last_used: AtomicU64,
    bounds: Mutex<Option<PageBounds>>,
}

pub(crate) struct PageReadLease<'a> {
    graph: &'a ResidentGraph,
    read: Arc<PageRead>,
}

impl std::ops::Deref for PageReadLease<'_> {
    type Target = PageRead;
    fn deref(&self) -> &Self::Target {
        &self.read
    }
}

impl Drop for PageReadLease<'_> {
    fn drop(&mut self) {
        let pending = self.read.bounds.lock().expect("page bounds lock").is_none();
        if pending && Arc::strong_count(&self.read) <= 2 {
            self.graph.evict_page_read(self.read.id);
        }
    }
}

pub(super) type PageRegistry = BTreeMap<(String, String), Vec<Arc<PageRead>>>;

impl ResidentGraph {
    pub(crate) fn begin_page_read(
        &self,
        molecule: &str,
        hash: &str,
        window: &KeyWindow,
        include_tombstones: bool,
    ) -> Option<PageReadLease<'_>> {
        let (limit, cursor_bytes) = match window {
            KeyWindow::Offset { limit, .. } => (*limit, 0),
            KeyWindow::After { after, limit } if after.hash.as_deref() == Some(hash) => (
                *limit,
                after.range.as_ref().map_or(0, String::len) + hash.len(),
            ),
            KeyWindow::After { .. } => return None,
        };
        let identity_bytes = molecule.len() + hash.len() + cursor_bytes;
        if limit == 0 || limit > MAX_PAGE_KEYS || identity_bytes > MAX_KEY_BYTES {
            return None;
        }
        let read = {
            let mut pages = self.page_coverage.lock().expect("page coverage lock");
            let key = (molecule.to_owned(), hash.to_owned());
            if let Some(read) = pages.get(&key).and_then(|entries| {
                entries.iter().find(|read| {
                    read.window == *window
                        && read.include_tombstones == include_tombstones
                        && read.valid.load(Ordering::Acquire)
                })
            }) {
                return Some(PageReadLease {
                    graph: self,
                    read: Arc::clone(read),
                });
            }
            if pages.values().map(Vec::len).sum::<usize>() >= MAX_PAGES {
                let oldest = pages
                    .values()
                    .flatten()
                    .min_by_key(|read| read.last_used.load(Ordering::Relaxed))
                    .map(|read| read.id);
                pages.retain(|_, entries| {
                    entries.retain(|read| {
                        if Some(read.id) != oldest {
                            return true;
                        }
                        read.valid.store(false, Ordering::Release);
                        self.discharge(&DirtyKey::QueryPage(read.id));
                        false
                    });
                    !entries.is_empty()
                });
            }
            let entries = pages.entry(key).or_default();
            if entries.len() >= MAX_PARTITION_PAGES {
                let oldest = entries
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, read)| read.last_used.load(Ordering::Relaxed))
                    .map(|(index, _)| index)
                    .expect("full page partition");
                let old = entries.remove(oldest);
                old.valid.store(false, Ordering::Release);
                self.discharge(&DirtyKey::QueryPage(old.id));
            }
            let id = self
                .next_page_id
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .ok()?;
            let read = Arc::new(PageRead {
                id,
                molecule: molecule.to_owned(),
                hash: hash.to_owned(),
                window: window.clone(),
                include_tombstones,
                valid: AtomicBool::new(true),
                last_used: AtomicU64::new(id),
                bounds: Mutex::new(None),
            });
            entries.push(Arc::clone(&read));
            // Reserve both completed boundary strings before the cold read.
            // Charge under the registry lock so invalidation cannot discharge
            // the entry before its charge appears. Enforce after lock release.
            self.ledger.lock().expect("resident ledger lock").charge(
                DirtyKey::QueryPage(id),
                (identity_bytes + 2 * MAX_KEY_BYTES + 256) as u64,
            );
            read
        };
        self.enforce_budget();
        Some(PageReadLease { graph: self, read })
    }

    pub(crate) fn resolve_exact_page(
        &self,
        molecule: &str,
        hash: &str,
        window: &KeyWindow,
        include_tombstones: bool,
    ) -> Option<Vec<ResidentTip>> {
        let read = {
            let pages = self.page_coverage.lock().expect("page coverage lock");
            Arc::clone(
                pages
                    .get(&(molecule.to_owned(), hash.to_owned()))?
                    .iter()
                    .find(|read| {
                        read.window == *window && read.include_tombstones == include_tombstones
                    })?,
            )
        };
        if !read.valid.load(Ordering::Acquire) {
            return None;
        }
        let bounds = read.bounds.lock().expect("page bounds lock").clone()?;
        let tips = self.page_tips(&read, &bounds)?;
        if !read.valid.load(Ordering::Acquire) {
            return None;
        }
        self.touch_page(&read);
        self.metrics.record_key_set_hit();
        Some(tips)
    }

    pub(crate) fn finish_page_read(&self, read: &PageRead, mut tips: Vec<ResidentTip>) -> bool {
        let limit = match read.window {
            KeyWindow::Offset { limit, .. } | KeyWindow::After { limit, .. } => limit,
        };
        if !read.valid.load(Ordering::Acquire)
            || tips.len() > limit
            || tips.iter().any(|tip| {
                tip.molecule_uuid != read.molecule
                    || tip.hash != read.hash
                    || tip.range.len() > MAX_KEY_BYTES
                    || (!read.include_tombstones
                        && tip
                            .key_metadata
                            .as_ref()
                            .is_some_and(|meta| meta.tombstoned))
            })
        {
            return false;
        }
        tips.sort_by(|a, b| a.range.cmp(&b.range));
        if tips.windows(2).any(|pair| pair[0].range == pair[1].range) {
            return false;
        }
        for tip in &tips {
            let observed = self.observe_slot(&read.molecule, &read.hash, &tip.range);
            if !read.valid.load(Ordering::Acquire)
                || self
                    .rehydrate_tip_at(tip.clone(), Some(observed.revisions()))
                    .is_none_or(|hit| hit.value != *tip)
            {
                return false;
            }
        }
        let bounds = PageBounds {
            first: tips.first().map(|tip| tip.range.clone()),
            last: tips.last().map(|tip| tip.range.clone()),
            count: tips.len(),
        };
        if self.page_tips(read, &bounds).as_ref() != Some(&tips)
            || !read.valid.load(Ordering::Acquire)
        {
            return false;
        }
        *read.bounds.lock().expect("page bounds lock") = Some(bounds);
        self.touch_page(read);
        read.valid.load(Ordering::Acquire)
    }

    fn touch_page(&self, read: &PageRead) {
        self.note_touch(&DirtyKey::QueryPage(read.id));
        if let Ok(tick) =
            self.next_page_id
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        {
            read.last_used.store(tick, Ordering::Relaxed);
        }
    }

    fn page_tips(&self, read: &PageRead, bounds: &PageBounds) -> Option<Vec<ResidentTip>> {
        if bounds.count == 0 {
            return Some(Vec::new());
        }
        let start = ResidentMoleculeKey::new(&read.hash, bounds.first.as_ref()?);
        let end = ResidentMoleculeKey::new(&read.hash, format!("{}\0", bounds.last.as_ref()?));
        let keys: Vec<_> = self
            .key_index
            .read()
            .expect("key index lock")
            .get(&read.molecule)?
            .range(start..end)
            .take(MAX_PAGE_KEYS + 1)
            .cloned()
            .collect();
        if keys.len() > MAX_PAGE_KEYS {
            return None;
        }
        let mut tips = Vec::with_capacity(bounds.count);
        for key in keys {
            let tip = self
                .resolve_tip(&read.molecule, &read.hash, &key.range)?
                .value;
            if read.include_tombstones
                || !tip
                    .key_metadata
                    .as_ref()
                    .is_some_and(|meta| meta.tombstoned)
            {
                tips.push(tip);
            }
        }
        (tips.len() == bounds.count).then_some(tips)
    }

    pub(super) fn invalidate_page_partition(&self, molecule: &str, hash: &str) {
        let mut pages = self.page_coverage.lock().expect("page coverage lock");
        if let Some(entries) = pages.remove(&(molecule.to_owned(), hash.to_owned())) {
            for read in entries {
                read.valid.store(false, Ordering::Release);
                self.discharge(&DirtyKey::QueryPage(read.id));
            }
        }
    }

    pub(super) fn invalidate_page_molecule(&self, molecule: &str) {
        let mut pages = self.page_coverage.lock().expect("page coverage lock");
        let start = (molecule.to_owned(), String::new());
        let end = (format!("{molecule}\0"), String::new());
        let keys: Vec<_> = pages
            .range(start..end)
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            for read in pages.remove(&key).into_iter().flatten() {
                read.valid.store(false, Ordering::Release);
                self.discharge(&DirtyKey::QueryPage(read.id));
            }
        }
    }

    pub(super) fn evict_page_read(&self, id: u64) -> bool {
        let mut pages = self.page_coverage.lock().expect("page coverage lock");
        let mut removed = false;
        pages.retain(|_, entries| {
            entries.retain(|read| {
                if read.id != id {
                    return true;
                }
                read.valid.store(false, Ordering::Release);
                removed = true;
                false
            });
            !entries.is_empty()
        });
        self.discharge(&DirtyKey::QueryPage(id));
        removed
    }
}
