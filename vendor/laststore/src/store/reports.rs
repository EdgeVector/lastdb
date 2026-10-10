use super::*;

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
pub(super) struct TxnUndo {
    pub(super) shard_index: usize,
    pub(super) id: String,
    pub(super) previous_loc: Option<Loc>,
    pub(super) previous: Option<Vec<u8>>,
    // Reverse-order undo knows the current value's record length without
    // dereferencing a physical location that a merge may have replaced.
    pub(super) applied_len: Option<u64>,
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
