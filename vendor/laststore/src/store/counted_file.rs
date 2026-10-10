use super::*;

/// An open append handle that counts itself while it exists.
///
/// The handle cap is a **file-descriptor** budget, so it has to be spent by
/// things that hold a descriptor. Counting resident groups instead over-counts:
/// a group holds a descriptor only once it has taken a write (`open_file` is
/// populated in [`LastStore::spill_open`] and nowhere else), so a group that is
/// resident because something *read* it is charged for a file it does not have.
/// Measured on the primary node 2026-07-30 in steady state, half an hour into an
/// uncapped run: **13,818 resident groups against 5,048 open `.seg`
/// descriptors** — the numerator was 2.7x the resource it claimed to bound, so
/// the cap bound long before descriptors were scarce.
///
/// The throttled state read more extreme still — 4,915 resident groups pinned at
/// a 4,915 cap against **2** open descriptors — but that number is a consequence
/// of the churn rather than independent evidence, and it is worth being precise
/// about why: eviction closes the file of the group it evicts, so a cap
/// reclaiming continuously holds almost nothing open. That is the tell. The
/// budget was evicting groups to reclaim descriptors it had already reclaimed,
/// and it paid 4.7M cold shard loads in 43 minutes to do it.
///
/// A gauge on the store would be easy to desynchronize — `open_file` is cleared
/// on eleven paths (segment roll, seal, compaction, both flush reclaim paths,
/// two encrypted rewrites). Tying the count to the `File`'s own lifetime means
/// every one of those decrements without knowing it has to.
pub(super) struct CountedFile {
    pub(super) file: File,
    pub(super) gauge: Arc<AtomicUsize>,
}

impl CountedFile {
    pub(super) fn new(file: File, gauge: Arc<AtomicUsize>) -> Self {
        gauge.fetch_add(1, Ordering::Relaxed);
        Self { file, gauge }
    }
}

impl Drop for CountedFile {
    fn drop(&mut self) {
        // `fetch_update` rather than `fetch_sub`: a saturating floor keeps a
        // miscount from wrapping to `usize::MAX` and locking the store into
        // permanent descriptor reclaim.
        let _ = self
            .gauge
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
    }
}

impl std::ops::Deref for CountedFile {
    type Target = File;

    fn deref(&self) -> &File {
        &self.file
    }
}

impl std::ops::DerefMut for CountedFile {
    fn deref_mut(&mut self) -> &mut File {
        &mut self.file
    }
}
