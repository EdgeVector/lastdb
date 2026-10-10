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

/// Record-byte residue of one hash group: how many record bytes the live
/// index still addresses versus how many are dead — superseded put records,
/// the put records of deleted ids, and the delete markers themselves.
///
/// Both totals are **plaintext record lengths**, the unit every put and delete
/// appends in, not on-disk block allocation. A store whose groups are sealed
/// or compressed at rest still reports the same ratio, because both sides are
/// measured in the same unit; the ratio is what a compaction trigger needs.
///
/// A LastStore delete is an append: it makes the id unreachable and adds a
/// marker, and no byte leaves the segment until the group is rewritten. Without
/// this counter the only observable on a deleted plane is filesystem block
/// slack (`st_blocks` past `st_size`), which a delete does not change — so a
/// plane could hold gigabytes of deleted rows and read as 0% reclaimable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupResidue {
    /// Record bytes the live index addresses.
    pub live_bytes: u64,
    /// Record bytes no live id addresses (superseded puts, deleted puts, and
    /// delete markers).
    pub dead_bytes: u64,
}

/// Residue of one collection, summed over every hash group on disk.
///
/// Produced by [`LastStore::collection_residue`] without loading a single cold
/// group: a resident group answers from its in-memory counters, a cold group
/// answers from its newest sorted seal or the residue its id sidecar recorded,
/// and a group with neither is counted under `unknown_bytes` (its on-disk
/// size) so a trigger can see how much of the plane it could not measure.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CollectionResidue {
    /// Groups enumerated on disk (plus resident groups the directory listing
    /// did not show yet).
    pub groups: u64,
    /// Groups answered from a resident handle (exact, current).
    pub groups_resident: u64,
    /// Groups answered from an id sidecar (exact as of the sidecar's stamps;
    /// any records appended since count as live under `unknown_bytes`).
    pub groups_sidecar: u64,
    /// Groups answered from sorted seal metadata. A newer append tail counts
    /// as unknown bytes; the sealed counters describe the last seal.
    pub groups_sealed: u64,
    /// Groups with no resident handle and no usable persisted residue.
    pub groups_unknown: u64,
    /// Record bytes live ids address, over the groups that could answer.
    pub live_bytes: u64,
    /// Dead record bytes, over the groups that could answer.
    pub dead_bytes: u64,
    /// On-disk bytes of groups (or appended suffixes) whose residue is not
    /// known. Never counted as dead.
    pub unknown_bytes: u64,
}

impl CollectionResidue {
    /// Dead record bytes as a fraction of measured record bytes, in basis
    /// points. Zero when nothing was measured.
    #[must_use]
    pub fn dead_bps(&self) -> u64 {
        let measured = self.live_bytes.saturating_add(self.dead_bytes);
        if measured == 0 {
            return 0;
        }
        ((u128::from(self.dead_bytes) * 10_000) / u128::from(measured)) as u64
    }
}

/// File name of a retire receipt, directly under the store root.
///
/// A missing file is a no-op. The file is not a collection and is not a scan
/// of `tips`.
pub const RETIRED_GROUPS_RECEIPT_FILE: &str = "retired-groups.receipt";

/// One hash group named by a retire receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredGroupId {
    /// Collection directory under `data/`.
    pub collection: String,
    /// Shard number. Not the on-disk hex name.
    pub shard: u16,
    /// Hash-group number. Not the on-disk hex name.
    pub group: u32,
}

/// Whether a retired-group compact may start.
///
/// `footprint_stop_bytes` is the caller's pressure stop. Fold passes
/// `pressure_footprint_target_bytes` (4 GiB, then the RAM minimum), the same
/// stop `footprint_evict_should_stop` uses while host pressure is high.
/// This store does not read `phys_footprint` itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetiredCompactGate {
    /// Host pressure is high. Either the store flag or a fresh sample.
    pub pressure_high: bool,
    /// Current `phys_footprint`. `None` is not a reading, not zero.
    pub phys_footprint_bytes: Option<u64>,
    /// Skip when footprint is strictly over this stop.
    pub footprint_stop_bytes: u64,
}

impl RetiredCompactGate {
    /// `true` when the whole pass must not open a group.
    #[must_use]
    pub fn blocks(self) -> bool {
        self.pressure_high
            || self
                .phys_footprint_bytes
                .is_some_and(|bytes| bytes > self.footprint_stop_bytes)
    }
}

/// Groups named in `path`.
///
/// `Ok(None)` when the file is absent. That is a no-op, not an error.
/// Each line is `collection shard group` in decimal. `#` comments and blank
/// lines are ignored.
pub fn read_retire_receipt(path: &Path) -> Result<Option<Vec<RetiredGroupId>>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut groups = Vec::new();
    for (index, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split_whitespace();
        let collection = fields.next().unwrap_or("");
        let shard_text = fields.next().unwrap_or("");
        let group_text = fields.next().unwrap_or("");
        if collection.is_empty()
            || shard_text.is_empty()
            || group_text.is_empty()
            || fields.next().is_some()
        {
            return Err(Error::Config(format!(
                "retire receipt line {} must be collection shard group",
                index + 1
            )));
        }
        let shard = shard_text.parse::<u16>().map_err(|_| {
            Error::Config(format!("retire receipt line {} has a bad shard", index + 1))
        })?;
        let group = group_text.parse::<u32>().map_err(|_| {
            Error::Config(format!("retire receipt line {} has a bad group", index + 1))
        })?;
        groups.push(RetiredGroupId {
            collection: collection.to_string(),
            shard,
            group,
        });
    }
    Ok(Some(groups))
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

/// Mutation kind captured in the local CDC log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureOp {
    /// A document was inserted or replaced.
    Put,
    /// A document was deleted.
    Delete,
}

/// One CSN-ordered capture event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureEvent {
    /// Monotonic commit sequence number.
    pub csn: u64,
    /// Collection affected by the mutation.
    pub collection: String,
    /// Document id affected by the mutation.
    pub id: String,
    /// Mutation kind.
    pub op: CaptureOp,
}

/// Metadata for one immutable encrypted chunk sealed by [`LastStore::snapshot`]
/// or segment rollover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedChunkMeta {
    /// Collection the chunk belongs to.
    pub collection: String,
    /// Shard number within the collection.
    pub shard: u16,
    /// Hash group within the shard for hash-group layout chunks.
    ///
    /// `None` identifies the legacy segment-log shard directory.
    pub group_id: Option<u32>,
    /// Stable chunk UUID used in the local filename and AEAD subkey derivation.
    pub chunk_uuid: Uuid,
    /// Local sealed chunk path.
    pub path: PathBuf,
    /// Highest CSN covered by this chunk's seal record.
    pub end_csn: u64,
}

/// Result of force-sealing a store snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Chunks sealed by this snapshot call.
    pub sealed_chunks: Vec<SealedChunkMeta>,
    /// Highest CSN assigned when the snapshot was cut.
    pub max_csn: u64,
    /// Groups with unsealed tails that the cold-load cap prevented us from sealing.
    pub skipped_capped_groups: u64,
}

/// Deterministic placement for one id in hash-group layout mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashGroupPlacement {
    /// Collection name.
    pub collection: String,
    /// Shard number selected by `shard_bits`.
    pub shard: u16,
    /// Hash group selected by `hash_group_bits`.
    pub group_id: u32,
    /// Relative directory under the store root.
    pub relative_dir: PathBuf,
}

/// Permitted call sites for explicit physical traversal. Product query paths
/// must name a partition instead of acquiring this maintenance access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllGroupsPurpose {
    /// Initial catalog/index recovery before ordinary requests are served.
    Startup,
    /// An explicit operator-maintenance operation.
    Admin,
}

/// Durable position for a range walk that advances by physical shard/group.
///
/// A logical key cursor still asks every hash group for its next key. This
/// cursor names one physical handle and an optional last id inside that handle,
/// so a maintenance pass can bound group resolution as well as row count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalRangeCursor {
    /// Shard that the next page starts in.
    pub shard: u16,
    /// Hash group that the next page starts in. `None` is a segment-log shard.
    pub group_id: Option<u32>,
    /// Last id consumed in this handle. The next page reads ids after it.
    pub after_id: Option<String>,
}

/// One physically bounded range page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalRangePage {
    /// Rows returned from the visited physical handles.
    pub rows: Vec<(String, Vec<u8>)>,
    /// Resume position, or `None` when every current handle is exhausted.
    pub next_cursor: Option<PhysicalRangeCursor>,
    /// Physical handle that supplied `rows`, when the page returned rows.
    pub row_handle: Option<(u16, Option<u32>)>,
    /// Physical handles resolved by this call.
    pub handles_visited: u64,
    /// Cold shard loads observed during this call.
    pub cold_shard_loads: u64,
}

/// Verified counts from an offline layout migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayoutMigrationReport {
    /// Number of live documents copied and verified, grouped by collection.
    pub collections: BTreeMap<String, u64>,
    /// Total number of live documents copied and verified.
    pub total_documents: u64,
}

/// Estimated in-process residency of hash-group handles.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HashGroupWarmStats {
    /// Number of hash-group shard handles currently retained in memory.
    pub resident_groups: usize,
    /// Estimated full owned heap across retained hash-group handles.
    pub resident_bytes: u64,
    /// Configured resident byte budget. `0` means eviction is disabled.
    pub budget_bytes: u64,
    /// Configured append-descriptor cap. `0` means the handle cap is off.
    ///
    /// Read this against `open_append_handles`, **not** against
    /// `resident_groups`. Reading it against the group count is what hid the
    /// 2026-07-30 throttle for 48 minutes: the pair looked like 4,915 of 4,915 —
    /// a warm set at its descriptor ceiling — while the process held 2 open
    /// descriptors and was paying millions of cold loads for the difference.
    pub budget_handles: usize,
    /// Open append descriptors across all resident groups, store-wide.
    ///
    /// Store-wide even on the per-collection stats: a descriptor budget is a
    /// process resource and there is no such thing as one collection's share of
    /// it.
    pub open_append_handles: usize,
    /// Cold loads currently parsing a group that is not yet published.
    ///
    /// Store-wide even on the per-collection stats: in-flight buffers are a
    /// process charge, and LRU pressure must cover them before the handle is
    /// reachable.
    pub in_flight_cold_load_count: u64,
    /// Estimated on-disk bytes of those in-flight loads, charged to the warm
    /// budget at admission.
    pub in_flight_cold_load_bytes: u64,
    /// Groups removed from the warm set since open.
    pub eviction_events: u64,
    /// Ids of evicted groups retained in the in-memory key-index cache.
    pub key_cache_groups: usize,
    /// Estimated bytes those retained id lists occupy.
    pub key_cache_bytes: u64,
    /// Configured key-index cache budget. `0` means the cache is disabled.
    pub key_cache_budget_bytes: u64,
}

/// How the id tiers answered keys-only group resolutions since open.
///
/// [`LastStore::group_key_source`] resolves each keys-only pass through one of
/// exactly four outcomes, cheapest first, and every call increments exactly one
/// of these counters. The sum is therefore the number of resolutions, which is
/// the property `id_tier_counters_account_for_every_resolution` asserts.
///
/// Why they exist: the warm body set has reported `cold_shard_loads` and
/// `eviction_events` since fold #906, and the two id tiers under it reported
/// nothing. Every read-thrash diagnosis therefore reached for
/// `LASTDB_HASH_GROUP_WARM_BYTES` — the only tier with a number and the most
/// expensive one to raise, because it is charged 1:1 into the process memory
/// budget and multiplied into the projection against the guard ceiling. These
/// counters say whether the two cheap tiers are working before that knob is
/// the answer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IdTierStats {
    /// The group's handle was warm when the tiers were chosen between, so no
    /// id tier was consulted.
    ///
    /// This records the decision the resolution made, not the state it found
    /// afterwards: the warm-set lock is released before the scan, so a
    /// concurrent eviction can still turn a resolution counted here into a
    /// cold load. That race is the pre-existing one in the residency check
    /// itself, and it is why these are a ranking signal across many
    /// resolutions rather than a claim about any single one.
    pub resident: u64,
    /// Served from the in-memory key-index cache. No segment read.
    pub key_cache_hits: u64,
    /// Served from the on-disk id sidecar. No segment read.
    pub sidecar_hits: u64,
    /// Both id tiers missed and the group was resolved by a live scan.
    pub live_scans: u64,
}

/// Result of one LRU eviction pass against an explicit resident-byte target.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WarmEvictionReport {
    /// Groups actually dropped from the warm set.
    pub groups_evicted: u64,
    /// Resident bytes before the pass.
    pub bytes_before: u64,
    /// Resident bytes after the pass.
    pub bytes_after: u64,
}

/// Result of [`LastStore::drop_hash_group_dir`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DroppedGroupReport {
    /// Collection that owned the group.
    pub collection: String,
    /// Shard the group lives in.
    pub shard: u16,
    /// Hash group index.
    pub group: u32,
    /// The group directory (as it was before the drop).
    pub dir: PathBuf,
    /// `*.seg` files in the group.
    pub segments: u64,
    /// Bytes on disk under the group directory, sidecar included.
    pub on_disk_bytes: u64,
    /// The one id the group's sidecar vouched for, or empty when the group
    /// recorded no live id at all (every put was later deleted).
    pub ids: Vec<String>,
    /// Whether the directory was actually removed (`execute`), or only
    /// measured and verified (dry run).
    pub dropped: bool,
    /// True when the group directory did not exist at `dir`: an earlier
    /// drop already returned its bytes (or the group was never written).
    /// The reclaim is idempotent, so this is a success with zero bytes, not
    /// a refusal. `dropped` stays false because this call removed nothing.
    pub already_absent: bool,
}

/// Process-lifetime frame compression totals for one collection (storage plane).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FrameCompressionStats {
    /// Plaintext group-commit bytes presented to frame encoding.
    pub input_bytes: u64,
    /// Bytes encrypted after optional compression, excluding header and tag.
    pub stored_bytes: u64,
    /// Frames whose compressed representation was smaller and therefore stored.
    pub compressed_frames: u64,
    /// Frames stored verbatim because compression would not help.
    pub uncompressed_frames: u64,
}

impl FrameCompressionStats {
    /// Bytes avoided by compression, saturating at zero.
    pub fn saved_bytes(self) -> u64 {
        self.input_bytes.saturating_sub(self.stored_bytes)
    }

    /// Percent saved in basis points (`10_000` = 100%).
    pub fn saved_basis_points(self) -> u64 {
        if self.input_bytes == 0 {
            return 0;
        }
        self.saved_bytes()
            .saturating_mul(10_000)
            .checked_div(self.input_bytes)
            .unwrap_or(0)
    }
}

/// One op in a multi-document transaction (apply all, then flush).
#[derive(Debug, Clone)]
pub enum TxnOp {
    /// Insert or replace a document.
    Put {
        /// Collection name.
        collection: String,
        /// Document id.
        id: String,
        /// Opaque body bytes.
        body: Vec<u8>,
    },
    /// Remove a document if present.
    Delete {
        /// Collection name.
        collection: String,
        /// Document id.
        id: String,
    },
}

impl TxnOp {
    /// Build a put op.
    pub fn put(collection: &str, id: &str, body: impl Into<Vec<u8>>) -> Self {
        Self::Put {
            collection: collection.to_string(),
            id: id.to_string(),
            body: body.into(),
        }
    }

    /// Build a delete op.
    pub fn delete(collection: &str, id: &str) -> Self {
        Self::Delete {
            collection: collection.to_string(),
            id: id.to_string(),
        }
    }

    /// The collection this op writes.
    pub fn collection(&self) -> &str {
        match self {
            Self::Put { collection, .. } | Self::Delete { collection, .. } => collection,
        }
    }

    /// The document id this op writes.
    pub fn id(&self) -> &str {
        match self {
            Self::Put { id, .. } | Self::Delete { id, .. } => id,
        }
    }
}

/// One applied transaction write, with the body that lived at that key before it.
struct TxnUndo {
    shard_index: usize,
    id: String,
    previous_loc: Option<Loc>,
    previous: Option<Vec<u8>>,
    // Reverse-order undo knows the current value's record length without
    // dereferencing a physical location that a merge may have replaced.
    applied_len: Option<u64>,
}

/// One storage record copied from an unpublished group open.
///
/// The record uses the storage id Last Store already stores. It has no
/// group id, no molecule id, and no resident key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedPoint {
    /// Last Store document id.
    pub storage_key: String,
    /// Current body bytes, if the id is live.
    pub body: Option<Vec<u8>>,
}

/// One live storage record from a hash-prefix fill.
///
/// Disk and pin bytes only. Tombstones are not applied here. The record uses
/// the storage id Last Store already stores. It has no group id, no molecule
/// id, and no resident key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedTip {
    /// Last Store document id.
    pub storage_key: String,
    /// Current body bytes.
    pub body: Option<Vec<u8>>,
}

/// One u64 assigned in Last Store. Not a resident key. Not a group id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DurabilityToken(u64);

impl DurabilityToken {
    /// Wrap a raw token value.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Raw token value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Result of one loader pin append.
///
/// `token` is the record this call appended. `covered_by_file_len` is true
/// only when this call's own spill wrote that record. `previous` is the body
/// this append replaced, copied under the same shard lock as the write, so a
/// batch undo does not open the group a second time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteAck {
    /// Token assigned to this append.
    pub token: DurabilityToken,
    /// True when this call spilled the record into `file_len`.
    pub covered_by_file_len: bool,
    /// Body this append replaced, if the id had one.
    pub previous: Option<Vec<u8>>,
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
mod append_tail;
mod chunk_install;
mod chunk_verify;
mod compact;
mod evict;
mod flush;
mod handles;
mod hash_group_admin;
mod key_index;
mod knobs;
mod list_bodies;
mod list_prefix;
mod list_range;
mod open;
mod pins;
mod placement;
mod point_ops;
mod seal_sorted;
mod sorted_rewrite;
mod warm_set;
use key_index::*;
use sorted_rewrite::SortedRewrite;
use warm_set::*;
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
