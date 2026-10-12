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

fn wall_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
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

#[derive(Default)]
struct StoreMeta {
    next_csn: u64,
    capture_suspended: bool,
    capture_log: Vec<CaptureEvent>,
    sealed_chunks: Vec<SealedChunkMeta>,
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

mod admit_scope;
mod append;
mod append_tail;
mod chunk_install;
mod chunk_install_file;
mod chunk_verify;
mod cold_metadata;
mod compact;
mod counted_file;
mod evict;
mod flush;
mod handles;
mod hash_group_admin;
mod key_index;
mod knobs;
mod list_bodies;
mod list_prefix;
mod list_range;
mod metadata_walk;
mod open;
mod partition;
mod pins;
mod placement;
mod point_metadata;
mod point_ops;
mod point_prefix;
pub use point_metadata::PointValueSize;
mod report_types;
mod residue;
mod seal_sorted;
mod shard;
mod sorted_rewrite;
mod warm_admission;
mod warm_set;
use admit_scope::*;
pub use admit_scope::{
    set_pressure_shed_interactive_override, with_admit_code, with_schema_warm_owner,
};
use counted_file::*;
use key_index::*;
use partition::*;
pub use report_types::*;
pub use residue::*;
pub use shard::ShardKey;
use shard::*;
use sorted_rewrite::SortedRewrite;
use warm_admission::*;
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
mod compact_parts;
mod encrypted_compact;
mod encrypted_load;
mod footer_frames;
mod records;
mod residency;
mod shard_load;
use chunk_lookup::*;
use compact_parts::*;
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
mod version_chain;
mod version_retention;
pub use layout::{describe_home, home_has_frame_aead_segments, LayoutDescriptor};
use layout::{resolve_layout_options, write_layout_descriptor};
pub use layout_restore::restore_chunk_cache_paths;
pub use maintenance::MaintenanceReport;
pub use version_chain::{VersionRetentionReport, SUPERSEDED_VERSION_RETENTION_NANOS};
