//! [`LastStore`] — multi-collection sharded segment engine.

use crate::durability;
use crate::frame::{self, FrameHeader};
use crate::keysidecar;
use crate::options::{
    CollectionPolicy, HashAlgo, HashGroupKey, LastStoreOptions, LayoutMode, PackagingMode,
};
use crate::segfmt::{self, encode_del, encode_put};
use crate::sorted;
use crate::sorted_file;
use crate::sorted_merge::{BodyLocation, Merge};
use crate::{Error, Result};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const SEAL_RECORD_MAGIC: &[u8; 8] = b"LSSEAL1\0";
const FOOTER_RECORD_MAGIC: &[u8; 8] = b"LSFOOT1\0";
const FOOTER_TRAILER_MAGIC: &[u8; 8] = b"LSFTRL1\0";
const FOOTER_TRAILER_LEN: usize = 24;
const FRAME_CACHE_LIMIT: usize = 64;
/// Sorted encrypted append frames never expand beyond the bounded tail.
const SORTED_ENCRYPTED_TAIL_BYTES: u64 = 1024 * 1024;
/// Fixed gate count for conflicting point writes and multi-key rollback.
const TRANSACTION_GATE_COUNT: usize = 1024;
/// Bounds the lock table that permits one cold load per physical group.
const COLD_LOAD_GATE_COUNT: usize = 1024;
/// Wide transaction scopes can sync independent group files in parallel.
/// Keep the worker count small so a flush cannot exhaust append descriptors.
const PARALLEL_SCOPE_MIN_GROUPS: usize = 64;
const PARALLEL_SCOPE_WORKERS: usize = 8;
/// Permit one parallel scope at a time across all store instances. Other
/// callers use the serial barrier, so worker creation stays bounded.
static PARALLEL_SCOPE_FLUSH: Mutex<()> = Mutex::new(());
/// Wall-clock age after which a molecule-tip index may leave under pressure.
const INTERACTIVE_PROTECT_MS: u64 = 600_000;
/// `0` reads the env, `1` forces on, `2` forces off. Tests must not cache the env.
static PRESSURE_SHED_INTERACTIVE: AtomicU8 = AtomicU8::new(0);

thread_local! {
    static WARM_ADMIT_CODE: Cell<u8> = const { Cell::new(0) };
}

/// Run `work` with an admission code, then restore the previous code.
///
/// `0` is unspecified (evictable, the raw laststore default). `1` is
/// interactive. `2` is background (`lastgit`).
pub fn with_admit_code<T>(code: u8, work: impl FnOnce() -> T) -> T {
    struct Restore(u8);
    impl Drop for Restore {
        fn drop(&mut self) {
            WARM_ADMIT_CODE.with(|cell| cell.set(self.0));
        }
    }
    let previous = WARM_ADMIT_CODE.with(|cell| cell.replace(code));
    let _restore = Restore(previous);
    work()
}

/// Classify one schema owner for the warm set.
///
/// `lastgit` is background. A null owner and every other owner stay
/// interactive, including the factory ids `fbrain`, `fkanban`, `loom`,
/// `routines`, and `fsituations`. This does not read `X-LastDB-Client`.
pub fn with_schema_warm_owner<T>(owner: Option<&str>, work: impl FnOnce() -> T) -> T {
    let code = if owner == Some("lastgit") { 2 } else { 1 };
    with_admit_code(code, work)
}

/// Override `LASTDB_PRESSURE_SHED_INTERACTIVE` for this process.
///
/// `None` reads the env again. Unset defaults on. Falsy values are
/// `0`, `false`, `no`, and `off`.
pub fn set_pressure_shed_interactive_override(enabled: Option<bool>) {
    let value = match enabled {
        None => 0,
        Some(true) => 1,
        Some(false) => 2,
    };
    PRESSURE_SHED_INTERACTIVE.store(value, Ordering::Relaxed);
}

fn pressure_shed_interactive() -> bool {
    match PRESSURE_SHED_INTERACTIVE.load(Ordering::Relaxed) {
        1 => true,
        2 => false,
        _ => match std::env::var("LASTDB_PRESSURE_SHED_INTERACTIVE") {
            Ok(value) => {
                let value = value.trim().to_ascii_lowercase();
                !matches!(value.as_str(), "0" | "false" | "no" | "off")
            }
            Err(_) => true,
        },
    }
}

fn current_admit_class() -> AdmitClass {
    match WARM_ADMIT_CODE.with(Cell::get) {
        2 => AdmitClass::Background,
        1 => AdmitClass::Interactive,
        _ => AdmitClass::Unspecified,
    }
}

fn wall_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Shared atom plane: `atom\0…` and an org-prefixed `*:atom\0…`.
///
/// `atom:uuid` (no NUL) is not this plane. A cold load must rebuild the
/// index; body bytes are not what this check protects.
fn is_atom_plane_id(id: &str) -> bool {
    let partition = partition_of(id);
    let bare = partition.strip_suffix('\0').unwrap_or(partition);
    bare == "atom" || bare.ends_with(":atom")
}

/// Plaintext a compaction rewrite buffers before it encodes and writes a frame.
///
/// Compaction used to build the whole shard's live set as one `Vec<u8>`, encode
/// a second full copy of it, and then keep two more (the resident tail and a
/// frame-cache entry). On the primary's 5.2 GiB tips plane that was a ~2.9 GiB
/// transient spike over a ~14.4 GiB baseline; the external memory guard
/// SIGKILLed the node mid-rewrite at 02:39Z, 05:03Z and 06:06Z on 2026-08-31,
/// and an interrupted rewrite reclaims nothing, so the next probe repeated it.
/// Bounding the buffer makes the rewrite's peak a function of this constant
/// instead of the plane.
///
/// It bounds the read side too. A point read decodes a whole frame, so writing
/// one frame per shard made every later read of a compacted chunk decode the
/// shard's entire live set — and kept it in [`FRAME_CACHE_LIMIT`].
const COMPACT_FRAME_TARGET_BYTES: usize = 1024 * 1024;

/// Transient heap a plane rewrite may hold at once, whatever the plane's size.
///
/// This is the rewrite's cost contract, not a plane measurement. With
/// [`COMPACT_FRAME_TARGET_BYTES`] bounding the buffer, one shard's rewrite
/// holds a small fixed number of frame-sized buffers (the plaintext being
/// filled, its encoded form, the source frame being drained, the resident
/// tail) plus two more copies of that shard's own index — and a hash-group
/// shard's index is one group's keys, not the plane's. Nothing in that list is
/// a function of the plane, so this stays a constant on purpose: it must hold
/// for the 8 MiB plane `compact_memory_bound` writes by default and for the
/// multi-GiB one an operator points it at.
///
/// Callers that must decide *before* a rewrite whether it fits in their memory
/// budget should reserve this rather than modelling the rewrite a second time;
/// `compact_memory_bound` is what proves the number.
pub const COMPACT_REWRITE_PEAK_BUDGET_BYTES: u64 = 64 * 1024 * 1024;
const LAYOUT_FILE: &str = "laststore-layout-v1";

/// Byte separating a key's partition prefix from its in-partition suffix.
///
/// Callers that want [`HashGroupKey::PartitionPrefix`] locality encode keys as
/// `{partition}\0{suffix}`. LastDB's molecule codec already does: its per-key
/// record is `mk:{molecule}:{esc(hash)}\0{range}`, and it byte-stuffs the hash
/// so `esc(hash)` can never contain a NUL — which is what makes "the first NUL"
/// an unambiguous partition boundary rather than a guess.
const PARTITION_SEP: char = '\0';

/// Whether `LASTDB_TRACE_SWEEP` asked for unprunable-walk reporting.
///
/// Read once and cached: the check sits on every prefix and range walk, so a
/// per-call `env::var` (which locks and allocates) would itself distort the
/// cost being measured.
fn full_sweep_trace_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("LASTDB_TRACE_SWEEP")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(false)
    })
}

/// The partition prefix of `id`: everything up to and **including** the first
/// [`PARTITION_SEP`], or all of `id` when it contains none.
///
/// Including the separator is what keeps partitions from colliding: without it,
/// `a\0x` and `ab\0x` would both reduce to a prefix that the other's scan
/// prefix also matches.
fn partition_of(id: &str) -> &str {
    match id.find(PARTITION_SEP) {
        Some(index) => &id[..index + PARTITION_SEP.len_utf8()],
        None => id,
    }
}

/// The exclusive upper bound of the ids beginning with `partition`: the ids in
/// that partition are exactly the half-open interval `[partition, end)`.
///
/// `partition` here always ends with [`PARTITION_SEP`] (`0x00`), so the bound is
/// that trailing byte incremented to `0x01`. Any id `partition + suffix` sorts
/// below it (they agree up to the separator, where `0x00 < 0x01`), and the bound
/// itself is not prefixed by `partition` — which is what makes the interval
/// exact rather than merely safe.
fn partition_end(partition: &str) -> String {
    debug_assert!(partition.ends_with(PARTITION_SEP));
    let head = &partition[..partition.len() - PARTITION_SEP.len_utf8()];
    let mut end = String::with_capacity(partition.len());
    end.push_str(head);
    end.push('\u{1}');
    end
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Loc {
    Sorted {
        segment: usize,
        offset: u64,
        body_len: usize,
        len: u64,
    },
    Legacy {
        seg: u64,
        offset: u64,
        len: u64,
    },
    Chunk {
        chunk_uuid: Uuid,
        frame_idx: u64,
        offset_in_frame: u64,
        len: u64,
    },
}

impl Loc {
    /// Encoded record bytes this location addresses (plaintext record line,
    /// including its id and framing — the same unit `raw_len` reports on a
    /// parse). This is what residue accounting sums.
    fn record_len(&self) -> u64 {
        match self {
            Loc::Legacy { len, .. } | Loc::Chunk { len, .. } | Loc::Sorted { len, .. } => *len,
        }
    }
}

/// One group's address: collection, shard, and hash-group index.
///
/// `group` is `None` on a segment-log home, which has no hash groups.
/// Embedders name this type because a durable batch flushes the keys it
/// wrote. The alias stays a tuple so existing match sites keep their shape.
pub type ShardKey = (String, u16, Option<u32>);
type ShardHandle = Arc<Mutex<Shard>>;
type FrameKey = (Uuid, u64);

#[derive(Clone)]
struct FrameDiskLoc {
    path: PathBuf,
    disk_offset: u64,
    disk_len: u64,
}

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
struct CountedFile {
    file: File,
    gauge: Arc<AtomicUsize>,
}

impl CountedFile {
    fn new(file: File, gauge: Arc<AtomicUsize>) -> Self {
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

#[derive(Default)]
struct Shard {
    dir: PathBuf,
    collection: String,
    shard: u16,
    data_key: Option<[u8; 32]>,
    policy: CollectionPolicy,
    /// Legacy homes keep their compatibility index here. Sorted homes keep
    /// only open-tail entries; None hides a key in every older sealed segment.
    index: BTreeMap<String, Option<Loc>>,
    sorted_segments: Vec<sorted::Segment>,
    sorted_mode: bool,
    /// Legacy encrypted source files still require conversion before a new
    /// sorted tail can use its generation-scoped directory.
    sorted_legacy_encrypted: bool,
    sorted_rollback_buffer: bool,
    sorted_tail_directory_dirty: bool,
    /// An immutable append is visible; its staging link and directory barrier
    /// retire only after the transaction records the inverse operation.
    pending_sorted_publish: Option<PathBuf>,
    max_sorted_tail_bytes: u64,
    plain_high_seq: u64,
    /// Sum of the heap capacities owned by `index` keys.
    ///
    /// Residency refresh runs on point operations. Keeping this total beside
    /// the map makes full key accounting O(1) instead of walking the complete
    /// group index after each read or write.
    index_key_bytes: u64,
    values: HashMap<String, Vec<u8>>,
    seg_bytes: HashMap<u64, Vec<u8>>,
    frame_cache: HashMap<FrameKey, Vec<u8>>,
    frame_cache_order: VecDeque<FrameKey>,
    frame_locs: HashMap<FrameKey, FrameDiskLoc>,
    /// Sum of the heap capacities owned by `frame_locs` paths.
    ///
    /// Residency refresh runs on point operations, so walking every frame
    /// location here would make a point read scale with the complete group.
    frame_loc_path_bytes: u64,
    open_buf: Vec<u8>,
    /// File offset of `open_buf[0]` within the open segment.
    ///
    /// The open tail used to be held whole, from offset 0, because `read_at`
    /// served the last segment straight out of it. That charged every group
    /// that had ever taken a write for up to `max_segment_bytes` of bytes the
    /// file already had — the single largest thing left in a resident group's
    /// warm-set charge once #942 gave up `seg_bytes` and `values`.
    ///
    /// With a base, `open_buf` may hold only the suffix past `base`, and the
    /// prefix is read back off disk exactly like a sealed segment. The
    /// invariant that makes that safe is `open_buf_base <= file_len`: the base
    /// only ever advances to a point already written, so anything below it is
    /// on disk. Sorted frame AEAD uses logical plaintext offsets here and
    /// frame locations to read flushed records; legacy frame AEAD keeps base 0.
    open_buf_base: u64,
    segments: Vec<u64>,
    open_len: u64,
    /// Plaintext bytes of the open segment already written to `open_file`.
    file_len: u64,
    open_file: Option<CountedFile>,
    /// The store-wide open-descriptor gauge this group's append handle spends.
    ///
    /// Wired in [`LastStore::shard_handle_at`] as the group is admitted. A
    /// `Default` shard gets its own unshared counter, which is what throwaway
    /// verify shards want: they open files no warm set is accounting for.
    fd_gauge: Arc<AtomicUsize>,
    open_chunk_uuid: Option<Uuid>,
    next_frame_counter: u64,
    open_frame_start_csn: Option<u64>,
    max_csn: u64,
    dirty_ops: u32,
    dirty_bytes: u64,
    /// Record-byte residue of this group. Maintained by every index mutation
    /// (`insert_index` / `remove_index` / `replace_index` / `clear_index`)
    /// and by every delete-marker append, so it is exact for a resident group
    /// without walking the index. Rebuilt from the segment scan on load, so a
    /// cold group is exact the moment it is resident; persisted into the id
    /// sidecar so a cold group can answer without a load.
    residue: GroupResidue,
    /// Segment stamps this group's on-disk id sidecar is known to describe, or
    /// `None` if this run has no such proof.
    ///
    /// Seeded at load from the sidecar's own header and updated on every write,
    /// so it tracks the file rather than just this handle's writes. Both
    /// sidecar writers compare against it and skip a group whose sidecar is
    /// already valid: [`LastStore::persist_key_sidecars`] so repeat calls cost
    /// a readdir per group instead of rewriting every id list, and eviction so
    /// a group cycling through the warm set does not rewrite identical bytes —
    /// with an fsync — on what is otherwise a pure read path.
    sidecar_stamps: Option<Vec<keysidecar::SegmentStamp>>,
    frame_compression_stats: Arc<Mutex<BTreeMap<String, FrameCompressionStats>>>,
}

impl Shard {
    fn uses_sorted_index(&self) -> bool {
        self.sorted_mode || !self.sorted_segments.is_empty()
    }

    fn cache_legacy_bodies(&self) -> bool {
        self.data_key.is_none() && !self.uses_sorted_index()
    }

    /// True when `stamps` (from disk) is this handle's view of the files.
    ///
    /// A scan lease uses this to detect a later writer that flushed a
    /// different Arc. Sealed seqs are immutable, so a new seq or a longer
    /// final segment means this image is stale.
    ///
    /// Sidecar validity is a different question (`sidecar_stamps`). A corrupt
    /// sidecar is `None` and must still be repaired when the handle matches
    /// disk (`eviction_repairs_a_corrupt_sidecar`).
    fn matches_segment_stamps(&self, stamps: &[keysidecar::SegmentStamp]) -> bool {
        if stamps.len() != self.segments.len() {
            return false;
        }
        if stamps
            .iter()
            .zip(self.segments.iter())
            .any(|(stamp, seq)| stamp.seq != *seq)
        {
            return false;
        }
        match stamps.last() {
            None => self.file_len == 0,
            Some(last) => last.len == self.file_len,
        }
    }

    fn lookup(&self, id: &str) -> Result<Option<Loc>> {
        if let Some(location) = self.index.get(id) {
            return Ok(*location);
        }
        for (segment, sealed) in self.sorted_segments.iter().enumerate().rev() {
            if let Some(record) = sealed.find(self.data_key.as_ref(), id)? {
                return Ok(record.body.map(|(offset, body_len)| Loc::Sorted {
                    segment,
                    offset,
                    body_len,
                    len: 7 + record.key.len() as u64 + body_len as u64,
                }));
            }
        }
        Ok(None)
    }

    /// Visit current live keys in order without building a complete key map.
    fn visit_keys(
        &self,
        start: &str,
        end: Option<&str>,
        mut visit: impl FnMut(&str, Loc) -> bool,
    ) -> Result<u64> {
        let mut visited = 0u64;
        if self.sorted_segments.is_empty() {
            for (id, location) in self.index.range(start.to_string()..) {
                if end.is_some_and(|end| id.as_str() >= end) {
                    break;
                }
                if let Some(location) = location {
                    visited += 1;
                    if !visit(id, *location) {
                        break;
                    }
                }
            }
            return Ok(visited);
        }
        let mut merged = Merge::new(
            &self.sorted_segments,
            self.data_key.as_ref(),
            &self.index,
            start,
        )?;
        while let Some(record) = merged.next_key()? {
            if end.is_some_and(|end| record.key.as_str() >= end) {
                break;
            }
            let Some(location) = record.body else {
                continue;
            };
            let location = match location {
                BodyLocation::Tail(location) => location,
                BodyLocation::Sealed {
                    segment,
                    offset,
                    length,
                } => Loc::Sorted {
                    segment,
                    offset,
                    body_len: length,
                    len: 7 + record.key.len() as u64 + length as u64,
                },
            };
            visited += 1;
            if !visit(&record.key, location) {
                break;
            }
        }
        Ok(visited)
    }

    fn live_locations(&self) -> Result<Vec<(String, Loc)>> {
        let mut entries = Vec::new();
        self.visit_keys("", None, |key, location| {
            entries.push((key.to_string(), location));
            true
        })?;
        Ok(entries)
    }

    fn live_keys(&self) -> Result<BTreeSet<String>> {
        let mut keys = BTreeSet::new();
        self.visit_keys("", None, |key, _| {
            keys.insert(key.to_string());
            true
        })?;
        Ok(keys)
    }

    fn insert_index(&mut self, id: String, loc: Loc) -> Result<()> {
        let previous_len = self.lookup(&id)?.map(|location| location.record_len());
        self.insert_index_known(id, loc, previous_len);
        Ok(())
    }

    // Resolve the previous record before append. A seal can replace physical
    // locations, but it preserves the old live record's encoded length.
    // Publication after append must not perform another fallible disk read.
    fn insert_index_known(&mut self, id: String, loc: Loc, previous_len: Option<u64>) {
        let new_len = loc.record_len();
        if let Some(old_len) = previous_len {
            self.residue.live_bytes = self.residue.live_bytes.saturating_sub(old_len);
            self.residue.dead_bytes = self.residue.dead_bytes.saturating_add(old_len);
        }
        if matches!(loc, Loc::Sorted { .. }) {
            if let Some((key, _)) = self.index.remove_entry(&id) {
                self.index_key_bytes = self.index_key_bytes.saturating_sub(key.capacity() as u64);
            }
            self.residue.live_bytes = self.residue.live_bytes.saturating_add(new_len);
            return;
        }
        match self.index.entry(id) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.insert(Some(loc));
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                self.index_key_bytes = self
                    .index_key_bytes
                    .saturating_add(entry.key().capacity() as u64);
                entry.insert(Some(loc));
            }
        }
        self.residue.live_bytes = self.residue.live_bytes.saturating_add(new_len);
    }

    fn remove_index(&mut self, id: &str) -> Result<()> {
        let previous_len = self.lookup(id)?.map(|location| location.record_len());
        self.remove_index_known(id, previous_len);
        Ok(())
    }

    fn remove_index_known(&mut self, id: &str, previous_len: Option<u64>) {
        self.remove_index_known_at(id, previous_len, None);
    }

    fn remove_index_known_at(
        &mut self,
        id: &str,
        previous_len: Option<u64>,
        location: Option<Loc>,
    ) {
        if let Some(old_len) = previous_len {
            self.residue.live_bytes = self.residue.live_bytes.saturating_sub(old_len);
            self.residue.dead_bytes = self.residue.dead_bytes.saturating_add(old_len);
        }
        if !self.uses_sorted_index() || matches!(location, Some(Loc::Sorted { .. })) {
            if let Some((id, _)) = self.index.remove_entry(id) {
                self.index_key_bytes = self.index_key_bytes.saturating_sub(id.capacity() as u64);
            }
        } else {
            match self.index.entry(id.to_string()) {
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    entry.insert(None);
                }
                std::collections::btree_map::Entry::Vacant(entry) => {
                    self.index_key_bytes = self
                        .index_key_bytes
                        .saturating_add(entry.key().capacity() as u64);
                    entry.insert(None);
                }
            }
        }
    }

    /// Account one delete marker appended to this group. The marker makes an
    /// id unreachable and is itself dead the moment it lands: only a rewrite
    /// removes it.
    fn note_delete_marker(&mut self, marker_len: u64) {
        self.residue.dead_bytes = self.residue.dead_bytes.saturating_add(marker_len);
    }

    /// Adopt a freshly rewritten index. Every record it addresses was just
    /// written by the rewrite and nothing else is in the new segment, so the
    /// group has no dead bytes.
    fn replace_index(&mut self, index: BTreeMap<String, Loc>) {
        self.index_key_bytes = index.keys().map(|id| id.capacity() as u64).sum::<u64>();
        self.residue = GroupResidue {
            live_bytes: index.values().map(Loc::record_len).sum(),
            dead_bytes: 0,
        };
        self.index = index
            .into_iter()
            .map(|(key, location)| (key, Some(location)))
            .collect();
    }

    fn clear_index(&mut self) {
        self.index.clear();
        self.index_key_bytes = 0;
        self.residue = GroupResidue::default();
    }

    fn insert_frame_loc(&mut self, key: FrameKey, loc: FrameDiskLoc) {
        let path_bytes = loc.path.capacity() as u64;
        if let Some(previous) = self.frame_locs.insert(key, loc) {
            self.frame_loc_path_bytes = self
                .frame_loc_path_bytes
                .saturating_sub(previous.path.capacity() as u64);
        }
        self.frame_loc_path_bytes = self.frame_loc_path_bytes.saturating_add(path_bytes);
    }

    fn replace_frame_locs(&mut self, frame_locs: HashMap<FrameKey, FrameDiskLoc>) {
        self.frame_loc_path_bytes = frame_locs
            .values()
            .map(|loc| loc.path.capacity() as u64)
            .sum::<u64>();
        self.frame_locs = frame_locs;
    }

    fn clear_frame_locs(&mut self) {
        self.frame_locs.clear();
        self.frame_loc_path_bytes = 0;
    }

    fn rename_frame_loc_paths(&mut self, src: &Path, dst: &Path) {
        let mut removed = 0u64;
        let mut added = 0u64;
        for loc in self.frame_locs.values_mut() {
            if loc.path == src {
                removed = removed.saturating_add(loc.path.capacity() as u64);
                loc.path = dst.to_path_buf();
                added = added.saturating_add(loc.path.capacity() as u64);
            }
        }
        self.frame_loc_path_bytes = self
            .frame_loc_path_bytes
            .saturating_sub(removed)
            .saturating_add(added);
    }
}

#[derive(Default)]
struct StoreMeta {
    next_csn: u64,
    capture_suspended: bool,
    capture_log: Vec<CaptureEvent>,
    sealed_chunks: Vec<SealedChunkMeta>,
}

#[derive(Default)]
struct ShardWarmSet {
    handles: HashMap<ShardKey, ShardHandle>,
    /// Groups admitted by a scan or batch read.
    ///
    /// These groups use the part of the full warm byte limit that point-read
    /// groups do not need. When the shared limit is full, a new scan candidate
    /// leaves before an established point-read group.
    scan_keys: HashSet<ShardKey>,
    /// Recency order for scan entries only. This avoids a linear search of
    /// the complete point-read set on each scan rejection.
    scan_order: BTreeMap<u64, ShardKey>,
    /// Recency order, least recently used first.
    ///
    /// A `VecDeque` here made every touch an O(n) linear scan for the key's
    /// current position. That is invisible while the set holds a few hundred
    /// groups and quadratic once it holds thousands — which is exactly the
    /// state cache trimming restores it to. Keyed by a monotonic tick instead,
    /// so a touch is two map operations and the LRU end is `pop_first`.
    order: BTreeMap<u64, ShardKey>,
    tick_by_key: HashMap<ShardKey, u64>,
    next_tick: u64,
    resident_by_key: HashMap<ShardKey, u64>,
    resident_bytes: u64,
    /// Highest `resident_bytes` ever published since open.
    ///
    /// A point-in-time stats read cannot see a charge that was published and
    /// evicted again between two samples; this can. It is what proves that no
    /// admission or re-charge published a charge past the budget.
    peak_resident_bytes: u64,
    /// Of each group's charge, how much is reproducible read cache.
    ///
    /// Only groups that have some are listed. Without this the trim pass would
    /// have to lock every resident handle to discover there is nothing left to
    /// give up — which is precisely the steady state once indexes alone fill
    /// the budget, so it would run on every single operation.
    trimmable_by_key: HashMap<ShardKey, u64>,
    trimmable_bytes: u64,
    /// Scan leases whose group did not fit the byte budget.
    ///
    /// A scan rejects its own newest group rather than evict for it, so that
    /// group's handle is never published or charged. It is still the only
    /// handle for the group while a lease holds it: a later load for the same
    /// key adopts it from here instead of reading a second copy from disk, so
    /// the group never has two authorities. Weak, so an entry dies with the
    /// last lease.
    leased: HashMap<ShardKey, Weak<Mutex<Shard>>>,
    /// Wall-clock ms of the last interactive point admit. Not `next_tick`.
    interactive_touch_unix_ms: HashMap<ShardKey, u64>,
    /// Owner class recorded by a point admit. Absent means unspecified.
    owner_class: HashMap<ShardKey, AdmitClass>,
    /// Groups whose keys are the shared `atom\0` plane. The flag sticks.
    atom_plane: HashSet<ShardKey>,
}

impl ShardWarmSet {
    /// Would charging `key` keep the index, the body, and the total inside budget?
    ///
    /// Index and total both use `index_budget` (`effective_warm_bytes`). Body
    /// uses `body_budget`, which is `0` while host pressure is high and equal
    /// to the index budget otherwise. A non-growing charge always fits, so a
    /// trim can be recorded while other groups still hold body bytes.
    /// `index_budget == 0` is no budget. Two independent ceilings would let
    /// resident reach about twice the pin.
    fn fits(
        &self,
        key: &ShardKey,
        residency: Residency,
        index_budget: u64,
        body_budget: u64,
    ) -> bool {
        if index_budget == 0 {
            return true;
        }
        let prev_total = self.resident_by_key.get(key).copied().unwrap_or(0);
        let prev_body = self.trimmable_by_key.get(key).copied().unwrap_or(0);
        let prev_index = prev_total.saturating_sub(prev_body);
        let index = residency.total.saturating_sub(residency.trimmable);
        let body = residency.trimmable;
        if index.saturating_sub(prev_index) == 0
            && body.saturating_sub(prev_body) == 0
            && residency.total.saturating_sub(prev_total) == 0
        {
            return true;
        }
        let new_index = self
            .resident_bytes
            .saturating_sub(self.trimmable_bytes)
            .saturating_sub(prev_index)
            .saturating_add(index);
        let new_body = self
            .trimmable_bytes
            .saturating_sub(prev_body)
            .saturating_add(body);
        let new_total = self
            .resident_bytes
            .saturating_sub(prev_total)
            .saturating_add(residency.total);
        new_index <= index_budget && new_body <= body_budget && new_total <= index_budget
    }

    fn growth(&self, key: &ShardKey, total: u64) -> u64 {
        let previous = self.resident_by_key.get(key).copied().unwrap_or(0);
        total.saturating_sub(previous)
    }

    /// Re-charge `key` only when `handle` is still its resident authority and
    /// the new charge fits `budget`.
    ///
    /// For paths that re-measure a group they did not grow (cache trims). A
    /// charge that no longer fits means a concurrent write grew the group; that
    /// writer re-charges it through the budget gate itself.
    fn recharge_if_fits(
        &mut self,
        key: &ShardKey,
        handle: &ShardHandle,
        residency: Residency,
        index_budget: u64,
        body_budget: u64,
    ) {
        let current = self
            .handles
            .get(key)
            .is_some_and(|resident| Arc::ptr_eq(resident, handle));
        if current && self.fits(key, residency, index_budget, body_budget) {
            self.recharge(key, residency);
        }
    }

    /// The live unpublished scan handle for `key`, if a lease still holds one.
    fn leased_handle(&mut self, key: &ShardKey) -> Option<ShardHandle> {
        let handle = self.leased.get(key)?.upgrade();
        if handle.is_none() {
            self.leased.remove(key);
        }
        handle
    }

    /// Does any handle own `key` right now, published or leased?
    fn holds(&self, key: &ShardKey) -> bool {
        self.handles.contains_key(key)
            || self
                .leased
                .get(key)
                .is_some_and(|handle| handle.strong_count() > 0)
    }

    /// Record `key` as the most recently used group.
    fn touch(&mut self, key: &ShardKey) {
        if let Some(previous) = self.tick_by_key.get(key) {
            self.order.remove(previous);
            if self.scan_keys.contains(key) {
                self.scan_order.remove(previous);
            }
        }
        let tick = self.next_tick;
        self.next_tick = self.next_tick.saturating_add(1);
        self.order.insert(tick, key.clone());
        if self.scan_keys.contains(key) {
            self.scan_order.insert(tick, key.clone());
        }
        self.tick_by_key.insert(key.clone(), tick);
    }

    /// Remove the least recently used key from the recency order and return it.
    ///
    /// The handle itself is left in place: the caller decides whether the group
    /// can be evicted, and re-admits it with [`Self::touch`] when it cannot.
    fn pop_lru(&mut self) -> Option<ShardKey> {
        let (_, key) = self.order.pop_first()?;
        self.tick_by_key.remove(&key);
        Some(key)
    }

    /// Remove the least-recently-used scan-admitted key from the order.
    fn pop_scan_lru(&mut self) -> Option<ShardKey> {
        let (tick, key) = self.scan_order.pop_first()?;
        self.order.remove(&tick);
        self.tick_by_key.remove(&key);
        Some(key)
    }

    fn take_from_order(&mut self, key: &ShardKey) -> Option<ShardKey> {
        let tick = self.tick_by_key.remove(key)?;
        self.scan_order.remove(&tick);
        self.order.remove(&tick)
    }

    /// Mark a resident key as scan-admitted without changing its charge.
    fn mark_scan(&mut self, key: &ShardKey) {
        if self.scan_keys.insert(key.clone()) {
            if let Some(tick) = self.tick_by_key.get(key).copied() {
                self.scan_order.insert(tick, key.clone());
            }
        }
    }

    /// A point operation promotes a scan entry into the protected segment.
    fn promote_point(&mut self, key: &ShardKey) {
        if self.scan_keys.remove(key) {
            if let Some(tick) = self.tick_by_key.get(key) {
                self.scan_order.remove(tick);
            }
        }
    }

    /// Keys that still hold reproducible read cache, least recently used first.
    fn trimmable_lru_order(&self) -> Vec<ShardKey> {
        self.order
            .values()
            .filter(|key| self.trimmable_by_key.contains_key(*key))
            .cloned()
            .collect()
    }

    /// Drop every trace of `key` from the warm set, returning the bytes it was
    /// charged.
    fn remove(&mut self, key: &ShardKey) -> Option<u64> {
        self.handles.remove(key)?;
        if let Some(tick) = self.tick_by_key.remove(key) {
            self.order.remove(&tick);
            self.scan_order.remove(&tick);
        }
        if let Some(trimmable) = self.trimmable_by_key.remove(key) {
            self.trimmable_bytes = self.trimmable_bytes.saturating_sub(trimmable);
        }
        let charged = self.resident_by_key.remove(key).unwrap_or_default();
        self.resident_bytes = self.resident_bytes.saturating_sub(charged);
        self.scan_keys.remove(key);
        self.interactive_touch_unix_ms.remove(key);
        self.owner_class.remove(key);
        self.atom_plane.remove(key);
        Some(charged)
    }

    /// Record the wall-clock time of an interactive point touch.
    ///
    /// A background touch must not call this. The value is unix milliseconds,
    /// not `next_tick`.
    fn note_interactive_touch(&mut self, key: &ShardKey, unix_ms: u64) {
        self.interactive_touch_unix_ms.insert(key.clone(), unix_ms);
    }

    /// Apply an admit's class and atom-plane flag.
    ///
    /// Interactive sticks. Background cannot demote it or clear its stamp.
    /// The atom-plane flag sticks. Scan admission sets the atom flag only:
    /// it does not stamp and it does not record an owner class. Unspecified
    /// leaves the class unchanged so a raw laststore group stays evictable.
    fn note_admission(&mut self, key: &ShardKey, admission: WarmAdmission, touch: WarmTouch) {
        if touch.atom {
            self.atom_plane.insert(key.clone());
        }
        if admission != WarmAdmission::Point {
            return;
        }
        match touch.class {
            AdmitClass::Interactive => {
                self.owner_class
                    .insert(key.clone(), AdmitClass::Interactive);
                self.note_interactive_touch(key, wall_unix_ms());
            }
            AdmitClass::Background => {
                if self.atom_plane.contains(key)
                    || self.owner_class.get(key).copied() == Some(AdmitClass::Interactive)
                {
                    return;
                }
                self.owner_class.insert(key.clone(), AdmitClass::Background);
            }
            AdmitClass::Unspecified => {}
        }
    }

    /// Atom-plane groups never leave. A fresh interactive molecule-tip index
    /// leaves only while pressure is high and the shed flag is on.
    fn eviction_protected(
        &self,
        key: &ShardKey,
        pressure_high: bool,
        shed_interactive: bool,
        now_ms: u64,
    ) -> bool {
        if self.atom_plane.contains(key) {
            return true;
        }
        if self.owner_class.get(key).copied() != Some(AdmitClass::Interactive) {
            return false;
        }
        if !pressure_high || !shed_interactive {
            return true;
        }
        !matches!(
            self.interactive_touch_unix_ms.get(key).copied(),
            Some(stamp) if now_ms.saturating_sub(stamp) > INTERACTIVE_PROTECT_MS
        )
    }

    /// Re-charge `key` with a freshly measured residency estimate.
    fn recharge(&mut self, key: &ShardKey, residency: Residency) {
        if !self.handles.contains_key(key) {
            return;
        }
        let previous = self
            .resident_by_key
            .insert(key.clone(), residency.total)
            .unwrap_or_default();
        self.resident_bytes = self
            .resident_bytes
            .saturating_sub(previous)
            .saturating_add(residency.total);
        self.peak_resident_bytes = self.peak_resident_bytes.max(self.resident_bytes);

        let previous_trimmable = if residency.trimmable == 0 {
            self.trimmable_by_key.remove(key)
        } else {
            self.trimmable_by_key
                .insert(key.clone(), residency.trimmable)
        }
        .unwrap_or_default();
        self.trimmable_bytes = self
            .trimmable_bytes
            .saturating_sub(previous_trimmable)
            .saturating_add(residency.trimmable);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WarmAdmission {
    Point,
    Scan,
}

/// Who admitted a molecule-tip group. Not stored on [`ShardKey`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmitClass {
    /// No schema owner. Stays on the historical LRU path.
    Unspecified,
    /// Factory, null, or any owner other than `lastgit`.
    Interactive,
    /// `lastgit`, plus maintenance that opts in. Unpublished when it does not fit.
    Background,
}

/// Class and atom-plane bit for one admit. Scan uses `atom` and ignores `class`.
#[derive(Clone, Copy)]
struct WarmTouch {
    class: AdmitClass,
    atom: bool,
}

impl WarmTouch {
    fn unspecified() -> Self {
        Self {
            class: AdmitClass::Unspecified,
            atom: false,
        }
    }
}

/// What one resident group costs the warm budget, split by what it would take
/// to get the bytes back.
#[derive(Debug, Clone, Copy, Default)]
struct Residency {
    /// Everything the group is charged.
    total: u64,
    /// The part of `total` that is a copy of bytes already on disk, and so can
    /// be released without costing a cold load.
    trimmable: u64,
}

/// Loader pin table: one handle per group plus the bytes that pin currently
/// charges. `loader_pin_bytes` is the sum of `charged_bytes`.
#[derive(Default)]
struct PinTable {
    handles: HashMap<ShardKey, ShardHandle>,
    charged_bytes: HashMap<ShardKey, u64>,
}

fn apply_loader_pin_byte_delta(total: &AtomicU64, old: u64, new: u64) {
    if new >= old {
        total.fetch_add(new - old, Ordering::Relaxed);
    } else {
        total.fetch_sub(old - new, Ordering::Relaxed);
    }
}

/// Progress of one attempt to make warm-set room for a charge.
#[derive(Default)]
struct WarmRoom {
    trimmed_own: bool,
    passes: u32,
    exhausted: bool,
}

impl WarmRoom {
    /// Eviction passes before a point charge is published regardless. Each
    /// pass either reaches its limit or finds nothing left to evict; more
    /// passes are only needed when concurrent admissions keep taking the room.
    const MAX_PASSES: u32 = 8;

    /// Give up the charged group's own reproducible bytes, once, before
    /// anything else is trimmed or evicted for it. Returns the new residency
    /// when there was something to trim.
    fn trim_own_caches(&mut self, handle: &ShardHandle, residency: Residency) -> Option<Residency> {
        if self.trimmed_own || residency.trimmable == 0 {
            return None;
        }
        self.trimmed_own = true;
        let mut shard = handle.lock().expect("poison");
        trim_shard_read_caches(&mut shard);
        Some(estimate_shard_residency_locked(&shard))
    }

    fn record(&mut self, reached: bool) {
        self.passes += 1;
        if !reached || self.passes >= Self::MAX_PASSES {
            self.exhausted = true;
        }
    }
}

/// What the warm-set lock holder decided about one admission.
enum Admit {
    /// The caller gets this handle; it is published, or leased when a scan
    /// group did not fit.
    Done(ShardHandle),
    /// A scan lease already holds this group's handle; admit that one instead.
    Adopt(ShardHandle),
    /// Other groups must get down to this many resident bytes first; `None`
    /// when the charge alone is larger than the whole budget.
    MakeRoom(Option<u64>),
}

/// **Last Store** — multi-collection local document store.
///
/// Layout under the home path:
/// ```text
/// <home>/data/<collection>/<shard>/*.seg                 # keyless legacy mode
/// <home>/data/<collection>/<shard>/tail/<uuid>.seg       # encrypted open tail
/// <home>/data/<collection>/<shard>/chunks/<uuid>.seg     # encrypted sealed chunks
/// ```
///
/// Durability: appends are memory-first and group-committed; call [`Self::flush`]
/// (or [`Self::transaction`], which flushes at the end) for a durability barrier.
/// Drop also flushes.
pub struct LastStore {
    root: PathBuf,
    opts: LastStoreOptions,
    shards: Mutex<ShardWarmSet>,
    /// Ids of groups evicted from `shards`, so a repeat keys-only pass does not
    /// re-read their segments. Never holds a key that `shards` holds.
    ///
    /// Lock order: never acquire this while holding `shards`.
    key_index: Mutex<KeyIndexCache>,
    meta: Mutex<StoreMeta>,
    /// Count of cold shard loads (a full segment parse + index rebuild) served
    /// by [`Self::shard_handle_at`]. A warm-set hit does not count. Exposed via
    /// [`Self::shard_loads`] so operators — and the walk regression tests — can
    /// tell an accelerated read from one that is re-parsing groups per row.
    shard_loads: AtomicU64,
    /// Count of ids that a walk listed but could not hydrate, because the row
    /// was deleted between the keys pass and the bodies pass.
    ///
    /// A walk is not a snapshot, so this is an ordinary outcome under a
    /// concurrent writer and the walk skips the id. It is counted rather than
    /// ignored because the same observation would also be produced by a genuine
    /// index-entry-without-body corruption, and that must not be silent.
    /// Exposed via [`Self::walk_vanished_ids`].
    walk_vanished_ids: AtomicU64,
    /// Count of [`Self::flush`] durability barriers since open. Exposed via
    /// [`Self::flush_barriers`] so callers — and the deferred-transaction
    /// guard tests — can tell a write path that pays the sync-every-open-shard
    /// barrier from one that defers to the group-commit buffers.
    flush_barriers: AtomicU64,
    /// Groups actually `sync_data`'d by those barriers — the *width* of the
    /// durability work, where [`Self::flush_barriers`] is only its count.
    ///
    /// The two answer different questions, and only this one distinguishes a
    /// barrier that synced the two groups its transaction wrote from one that
    /// also synced 256 groups belonging to concurrent writers. Exposed via
    /// [`Self::groups_synced`].
    groups_synced: AtomicU64,
    /// Width of the last [`Self::flush_scope`] call. Not [`Self::groups_synced`].
    ///
    /// The lifetime counter adds one per dirty resident group a barrier
    /// actually synced, and concurrent flushes share it. This gauge is the
    /// deduped key count of one scoped call, including groups that were
    /// already clean.
    groups_synced_last_flush: AtomicU64,
    /// Scoped keys outside the embedder's written set on the last scoped flush.
    ///
    /// The written set lives in the embedder. [`Self::set_flush_foreign_groups`]
    /// publishes the count. A whole-store [`Self::flush`] does not clear it.
    flush_foreign_groups: AtomicU64,
    /// Open append descriptors across every resident group, maintained by
    /// [`CountedFile`]'s own lifetime rather than by the paths that clear
    /// `open_file`.
    ///
    /// This is what the handle cap spends. Exposed via
    /// [`Self::open_append_handles`] and reported next to the cap in
    /// [`HashGroupWarmStats`], because the gap between resident groups and open
    /// descriptors is exactly what made the 2026-07-30 throttle invisible:
    /// resident groups read 4,915 of a 4,915 cap while the process held 2, and in
    /// steady state on the same home the two sit at 13,818 and 5,048.
    open_append_handles: Arc<AtomicUsize>,
    /// Count of ids a keys-only pass stepped over, whether or not they made the
    /// page. Exposed via [`Self::walk_ids_visited`].
    ///
    /// `shard_loads` cannot see this cost: with the key-index cache warm, a
    /// paged walk that re-merges the whole band on every page does *zero* shard
    /// loads and still burns `O(band)` per page. That is how the quadratic paged
    /// walk hid — the counter that would have shown it did not exist.
    walk_ids_visited: AtomicU64,
    rejected_partition_reads: AtomicU64,
    all_group_walks: AtomicU64,
    frame_compression_stats: Arc<Mutex<BTreeMap<String, FrameCompressionStats>>>,
    /// Effective warm-set byte budget. Starts as
    /// [`LastStoreOptions::hash_group_warm_bytes`] and may shrink under
    /// measured footprint pressure, then grow back. `0` still means eviction
    /// is disabled for the ordinary admit path.
    effective_warm_bytes: AtomicU64,
    /// While set, a point admit that still does not fit stays leased and
    /// uncharged. The governor sets this for the whole drain. `fits` treats
    /// budget 0 as unlimited, so the drain stores at least 1 and uses this
    /// flag to close the over-budget point arm.
    warm_drain_hold: AtomicBool,
    /// Host swap/compressor pressure. While set, the body budget is 0.
    /// Independent of [`Self::warm_drain_hold`]: the body budget stays 0
    /// after the drain has stopped under the 4 GiB line.
    host_pressure_high: AtomicBool,
    /// Point publishes whose index alone exceeded the index budget.
    ///
    /// The exception does not raise `effective_warm_bytes`. Background admits
    /// do not increment this; they stay on the leased map.
    index_over_budget_publishes: AtomicU64,
    /// Cold loads currently holding decrypt/frame buffers that are not yet a
    /// published warm-set handle.
    in_flight_cold_loads: AtomicU64,
    /// Estimated bytes of those in-flight loads.
    in_flight_cold_bytes: AtomicU64,
    /// Groups removed from the warm set since open.
    eviction_events: AtomicU64,
    /// Keys-only resolutions that found the group already warm.
    id_tier_resident: AtomicU64,
    /// Keys-only resolutions served by the in-memory key-index cache.
    id_tier_key_cache_hits: AtomicU64,
    /// Keys-only resolutions served by the on-disk id sidecar.
    id_tier_sidecar_hits: AtomicU64,
    /// Keys-only resolutions that fell through both id tiers to a live scan.
    id_tier_live_scans: AtomicU64,
    /// [`Self::transaction`] / [`Self::transaction_deferred`] calls where an
    /// op failed after apply and the prior value was restored.
    torn_transaction_rollbacks: AtomicU64,
    /// Transactions whose restore or rollback flush failed.
    /// Nonzero means a failed transaction may still have left a mixed row.
    torn_transaction_rollback_failures: AtomicU64,
    /// Committed transactions whose post-commit cache refresh failed.
    transaction_residency_refresh_failures: AtomicU64,
    /// Striped point-key gates. A transaction holds every target stripe until
    /// its apply or durable rollback is complete.
    transaction_gates: [Mutex<()>; TRANSACTION_GATE_COUNT],
    /// A cold load holds its group stripe through publication. Warm hits never
    /// take this gate. Collisions serialize loads but do not change authority.
    cold_load_gates: [Mutex<()>; COLD_LOAD_GATE_COUNT],
    /// Loader-private write pins. One `Shard` per group. Not a warm-set member.
    /// [`Self::load_point`] and [`Self::load_hash`] read a pin when it exists
    /// and do not call `load_shard` for that group.
    pins: Mutex<PinTable>,
    /// Sum of `PinTable::charged_bytes`. Updated when a pin opens, a write
    /// refreshes that pin's estimate, or a reap drops it. O(1) to read.
    loader_pin_bytes: AtomicU64,
    /// Next [`DurabilityToken`] assigned by [`Self::append_for_resident`].
    /// The live value is also [`Self::assigned_through`].
    next_durability_token: AtomicU64,
    /// Highest seal for which every token at or below it reached disk.
    durable_through: AtomicU64,
}

/// RAII charge for one in-flight cold load. Drop releases the admission
/// reservation even when `load_shard` fails.
struct InFlightAdmission<'a> {
    store: &'a LastStore,
    bytes: u64,
}

impl<'a> InFlightAdmission<'a> {
    fn enter(store: &'a LastStore, bytes: u64) -> Self {
        store.in_flight_cold_loads.fetch_add(1, Ordering::Relaxed);
        store
            .in_flight_cold_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        Self { store, bytes }
    }
}

impl Drop for InFlightAdmission<'_> {
    fn drop(&mut self) {
        let _ = self.store.in_flight_cold_loads.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| Some(current.saturating_sub(1)),
        );
        let bytes = self.bytes;
        let _ = self.store.in_flight_cold_bytes.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| Some(current.saturating_sub(bytes)),
        );
    }
}

mod append;
mod key_cache;
mod reports;
mod retire_receipt;
mod sorted_rewrite;
use key_cache::*;
pub use reports::*;
pub use retire_receipt::*;
use sorted_rewrite::*;
mod append_tail;
mod chunk_install;
mod chunk_verify;
mod compact;
mod evict;
mod flush;
mod handles;
mod hash_group_admin;
mod knobs;
mod list_bodies;
mod list_prefix;
mod list_range;
mod open;
mod pins;
mod placement;
mod point_ops;
mod seal_sorted;
mod shard_handle;
mod sidecars;
mod transaction;
mod unpublished;
mod warm_room;
mod warm_trim;

impl Drop for LastStore {
    fn drop(&mut self) {
        let _ = self.flush();
        // After the flush, so the stamps describe segments already on disk.
        // This is the only point where a group that stayed resident for the
        // whole process gets a sidecar at all.
        self.persist_key_sidecars();
    }
}

pub(crate) fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

mod chunk_lookup;
mod encrypted_compact;
mod encrypted_load;
mod footer_frames;
mod records;
mod residency;
mod shard_load;
use chunk_lookup::*;
use encrypted_compact::*;
use encrypted_load::*;
use footer_frames::*;
use records::*;
use residency::*;
use shard_load::*;

mod group_compact;
mod layout;
mod layout_restore;
mod maintenance;
pub use layout::{describe_home, home_has_frame_aead_segments, LayoutDescriptor};
use layout::{resolve_layout_options, write_layout_descriptor};
pub use layout_restore::restore_chunk_cache_paths;
pub use maintenance::{
    MaintenanceReport, VersionRetentionReport, SUPERSEDED_VERSION_RETENTION_NANOS,
};
