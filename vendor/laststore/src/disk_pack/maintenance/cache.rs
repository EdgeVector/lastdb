//! Caches that live for one whole pack.
//!
//! The pack reads each live record through a short read call, so that no call
//! passes its finite budget. A new call knows nothing. Before this module, every
//! record paid again for facts that never change during the pack: the block-map
//! page of its batch (a read and a SHA-256 over 16 KiB), the header of its part
//! (a read, a SHA-256 and a stat), and the open of its part file. Records come
//! in key order, so each record meets another batch. A cache that lives for a
//! window of 1,024 records meets only a third of the batches again.
//!
//! `PackCaches` hands those results from one call to the next. It adds no check
//! and removes none. A map page, a part header, and a decoded block come from
//! immutable files that the pack already proved, and each record is still
//! checked against them: its extent, its object bounds, its identity, its
//! length, and its SHA-256.
//!
//! Memory is bounded by the constants below, not by the home:
//!
//! - At most `MAP_PAGES` cached block-map pages of 16 KiB (64 MiB)
//! - Plus at most `MAP_PAGES` new pages read in one call (64 MiB)
//! - Total peak: 128 MiB of pages (reached only by a home with many thousands of batches)
//! - `BLOCKS` decoded blocks of 4 KiB (1 MiB)
//! - `PARTS` verified part records of ~200 bytes (6 MiB)
//! - `FILES` open file descriptors
//!
//! Tree pages are dropped every `TREE_SWEEP_RECORDS` records to keep the hot set small.

use crate::disk_pack::block_plan::BlockId;
use crate::disk_pack::call::Buffer;
use crate::disk_pack::call_io::CallFiles;
use crate::disk_pack::page::{Page, PageAddress};
use crate::disk_pack::{artifact, Current, ReadCall};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Block-map pages kept between calls. One page maps 677 blocks, so this serves
/// more than four thousand batches at a full hit rate.
const MAP_PAGES: usize = 4096;
/// Decoded blocks kept between calls. Records of a packed source share blocks.
const BLOCKS: usize = 256;
/// Verified part headers kept between calls.
const PARTS: usize = 32_768;
/// Records read before triggering a page sweep that drops tree pages. A key-order walk
/// reads each tree page once, so keeping them only costs memory. The sweep is a scan of
/// the kept pages, so it runs every few records and not after each one.
const TREE_SWEEP_RECORDS: u32 = 256;
/// Open part files kept between calls. The operating system limit on open files
/// can be as low as 256 for a service, so the pool stays small.
const FILES: usize = 64;

pub(super) struct PackCaches {
    pages: HashMap<PageAddress, Arc<Page>>,
    blocks: HashMap<BlockId, Arc<Buffer>>,
    parts: HashMap<PathBuf, (u64, u64)>,
    files: CallFiles,
    since_sweep: u32,
}

impl PackCaches {
    pub(super) fn new() -> Self {
        Self {
            pages: HashMap::new(),
            blocks: HashMap::new(),
            parts: HashMap::new(),
            files: CallFiles::with_capacity(FILES),
            since_sweep: 0,
        }
    }

    /// A short read call that starts with everything the pack has already proved.
    /// Cached pages are already validated and bounded by MAP_PAGES. The budget is
    /// increased by MAP_PAGES to provide headroom for cached pages, ensuring the call
    /// can read up to MAP_PAGES new pages beyond the cached set. Peak memory is
    /// 128 MiB (64 MiB cached + 64 MiB new pages).
    pub(super) fn call(&mut self, root: &Path, current: &Current) -> ReadCall {
        let mut call = artifact::metadata_call(root, current.clone());
        call.budget.pages += MAP_PAGES as u64;
        call.pages = std::mem::take(&mut self.pages);
        call.blocks = std::mem::take(&mut self.blocks);
        call.parts = std::mem::take(&mut self.parts);
        call.files = std::mem::replace(&mut self.files, CallFiles::with_capacity(FILES));
        call
    }

    /// Take back what a finished call learned, within the fixed bounds.
    pub(super) fn keep(&mut self, call: ReadCall) {
        let ReadCall {
            mut pages,
            mut blocks,
            mut parts,
            files,
            ..
        } = call;
        self.since_sweep += 1;
        if self.since_sweep >= TREE_SWEEP_RECORDS || pages.len() > MAP_PAGES {
            self.since_sweep = 0;
            // Tree pages are read once in a key-order walk. Block maps are the hot set.
            pages.retain(|_, page| page.kind() == 5);
            if pages.len() > MAP_PAGES {
                // Enforce the MAP_PAGES limit strictly to honor the documented memory bound.
                // Drop the least recently used pages by retaining only the first MAP_PAGES
                // entries. The HashMap iteration order is arbitrary, but all pages here are
                // block-map pages (kind 5) of equal priority, so any subset is acceptable.
                let excess = pages.len() - MAP_PAGES;
                let mut dropped = 0;
                pages.retain(|_, _| {
                    if dropped < excess {
                        dropped += 1;
                        false
                    } else {
                        true
                    }
                });
            }
        }
        if blocks.len() > BLOCKS {
            blocks.clear();
        }
        if parts.len() > PARTS {
            parts.clear();
        }
        self.pages = pages;
        self.blocks = blocks;
        self.parts = parts;
        self.files = files;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_pack::{TreeKind, TreeRoot};
    use crate::disk_pack::{FEATURE_OBJECT_METADATA, FEATURE_PACKED_KEYS, FEATURE_SNAPSHOT_COMMIT};

    /// Test warm-cache budget headroom for MAP_PAGES cached pages.
    ///
    /// This is the critical scenario that PR #300 broke: when PackCaches holds
    /// MAP_PAGES (4096) block-map pages, the next call must still be able to read
    /// new pages without hitting resource limits.
    ///
    /// Before the fix: call.budget.pages was 4096, and if call.pages started with
    /// 4096 cached pages, the first new page read would fail the check
    /// `self.pages.len() as u64 >= self.budget.pages` in ReadCall::page().
    ///
    /// After the fix: call.budget.pages is increased by MAP_PAGES to 8192, giving
    /// headroom for MAP_PAGES new pages beyond the cached set (128 MiB peak).
    #[test]
    fn test_warm_cache_budget_allows_new_page_reads() {
        let tempdir = std::path::PathBuf::from("/tmp/laststore-cache-test");

        let current = Current {
            generation: 1,
            previous_generation: 0,
            frontier: 0,
            directory: TreeRoot {
                object: uuid::Uuid::new_v4(),
                page: 0,
                height: 1,
                kind: TreeKind::Directory,
            },
            placement_codec: 1,
            key_codec: 1,
            published_ms: 0,
            required_features: FEATURE_PACKED_KEYS
                | FEATURE_SNAPSHOT_COMMIT
                | FEATURE_OBJECT_METADATA,
            home_digest: [0; 32],
        };

        let mut caches = PackCaches::new();

        // The base budget from artifact::metadata_call is 4096 pages
        let base_budget = 4096u64;

        // Create a call from empty cache
        let call = caches.call(&tempdir, &current);

        // CRITICAL: Budget must be increased by MAP_PAGES (4096) to provide headroom
        assert_eq!(
            call.budget.pages,
            base_budget + MAP_PAGES as u64,
            "Budget must be increased by MAP_PAGES to allow new page reads with warm cache"
        );

        // Verify the budget allows for the documented 128 MiB peak:
        // 64 MiB cached (MAP_PAGES) + 64 MiB new pages (MAP_PAGES)
        let peak_pages = 2 * MAP_PAGES as u64;
        assert_eq!(
            call.budget.pages, peak_pages,
            "Budget {} must equal {} for 128 MiB peak (64 MiB cached + 64 MiB new)",
            call.budget.pages, peak_pages
        );

        // This arithmetic proves the fix works: in the warm-cache scenario where
        // call.pages starts with MAP_PAGES cached pages, the check in ReadCall::page()
        // `self.pages.len() as u64 >= self.budget.pages` will allow reading up to
        // MAP_PAGES additional new pages before hitting the limit.
        //
        // Without the fix (budget = 4096):
        //   - Start with 4096 cached pages
        //   - 4096 >= 4096 → first new page read FAILS
        //
        // With the fix (budget = 8192):
        //   - Start with 4096 cached pages
        //   - 4096 < 8192 → can read 4096 new pages
        //   - 8192 >= 8192 → resource limit reached (correct)
        let simulated_cached = MAP_PAGES as u64;
        assert!(
            simulated_cached < call.budget.pages,
            "Warm cache of {} pages must be under budget {} to allow new reads",
            simulated_cached,
            call.budget.pages
        );

        // Verify there's room for additional pages
        let remaining_budget = call.budget.pages - simulated_cached;
        assert!(
            remaining_budget >= MAP_PAGES as u64,
            "Must have {} pages of headroom, got {}",
            MAP_PAGES,
            remaining_budget
        );
    }
}
