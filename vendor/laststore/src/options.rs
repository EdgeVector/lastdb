//! Tunables for [`crate::LastStore`].

use crate::store::{CaptureEvent, SealedChunkMeta};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

const ATOMS_COLLECTION: &str = "atoms";
const DEFAULT_HASH_GROUP_WARM_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_HASH_GROUP_KEY_CACHE_BYTES: u64 = 64 * 1024 * 1024;
/// Conservative resident-handle cap for embedders that do not set one.
///
/// A resident group costs one open file descriptor, and this crate cannot see
/// the host process's `RLIMIT_NOFILE` or how many stores share it. The default
/// must therefore leave room for concurrent stores and unrelated descriptors.
/// Closing an append descriptor keeps the group index resident, so this cap
/// does not reduce the warm read set. Hosts can opt into a larger cap.
const DEFAULT_HASH_GROUP_WARM_MAX_HANDLES: usize = 64;
/// Default HARD cold-group load cap (see
/// [`LastStoreOptions::max_cold_group_load_bytes`]): a cold group larger than
/// this is refused. 2 GiB leaves room for two concurrent cold loads of one
/// group (loads are not single-flight) inside the primary's 16 GiB memory
/// guard at its recorded 12.4 GB peak, and still refuses the 39 GB group of
/// 2026-09-21. It was 1 GiB until 2026-09-25, when a 1.08 GB keep_small group
/// made every new build refuse to boot the primary home.
const DEFAULT_HARD_COLD_GROUP_LOAD_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Default SOFT cold-group load cap (see
/// [`LastStoreOptions::soft_cold_group_load_bytes`]): a cold group larger than
/// this still loads; the store logs it and the product compactors reclaim it. No legitimate group comes near it (the primary's largest
/// outside an incident was 79 MB); a group past it is superseded copies.
const DEFAULT_SOFT_COLD_GROUP_LOAD_BYTES: u64 = 1024 * 1024 * 1024;

/// Per-collection storage policy.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct CollectionPolicy {
    /// Never rewrite this collection during compaction.
    pub never_compact: bool,
    /// Keep this collection out of cloud-backup chunk enumeration.
    pub backup_excluded: bool,
}

/// On-disk document placement strategy.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum LayoutMode {
    /// Place each id in a deterministic UUID/string hash group under
    /// `data/<collection>/<shard>/g/<group>/`.
    ///
    /// Default for new homes: point `get` opens that group instead of consulting
    /// a global id → location map.
    #[default]
    HashGroup,
    /// Legacy per-shard append segments with an in-memory id → location map.
    ///
    /// Existing homes keep this layout from their durable descriptor. New homes
    /// must opt in with [`LastStoreOptions::segment_log`].
    SegmentLog,
}

/// Hash algorithm used for shard and hash-group placement.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum HashAlgo {
    /// 64-bit FNV-1a over the document id bytes.
    #[default]
    Fnv1a64,
}

/// Which part of a document id decides its shard and hash group.
///
/// This is a **physical placement** choice, recorded in the layout descriptor:
/// changing it on a populated home requires a migration, not a reopen.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum HashGroupKey {
    /// Hash the entire id. Groups stay uniformly sized, but rows sharing a
    /// partition prefix scatter across every group, so a prefix walk cannot
    /// prune and must visit all of them.
    #[default]
    FullKey,
    /// Hash only the partition prefix — everything up to and including the
    /// first NUL byte (the whole id when it has none).
    ///
    /// Rows of one partition land in a bounded set of groups
    /// ([`LastStoreOptions::hash_group_partition_fanout`]), so a prefix walk
    /// whose prefix already contains the separator resolves those groups
    /// directly instead of sweeping the collection.
    ///
    /// Ids with no NUL (plain point keys) hash exactly as under
    /// [`HashGroupKey::FullKey`].
    PartitionPrefix,
}

/// How logical values are protected inside on-disk group/segment files.
///
/// Plain packaging keeps group files structurally readable (ids, indexes, atom
/// metadata) and leaves body secrecy to upper layers (e.g. atom `content`
/// field seal). Frame AEAD wraps every spilled batch in AES-GCM frames under
/// [`LastStoreOptions::data_key`].
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum PackagingMode {
    /// Raw segment records / open tails (no frame AEAD). Default for new
    /// hash-group homes — restart-safe without sealing open encrypted tails.
    #[default]
    Plain,
    /// AES-256-GCM frame cabinet (`LSF1` frames). Requires `data_key`.
    FrameAead,
}

/// Hook invoked after a commit is assigned a CSN and before it is exposed in the
/// capture log.
pub type CaptureHook =
    Arc<dyn Fn(&CaptureEvent) -> std::result::Result<(), String> + Send + Sync + 'static>;

/// Hook invoked after an encrypted tail is sealed into an immutable chunk.
pub type SealHook =
    Arc<dyn Fn(&SealedChunkMeta) -> std::result::Result<(), String> + Send + Sync + 'static>;

/// Configuration for opening a Last Store home directory.
///
/// A new home defaults to UUID hash-group addressing (S=0, G=1024) with a
/// 256 MiB warm-set budget and group-commit every 16 384 ops or 16 MiB.
/// Existing homes keep the layout recorded on disk.
#[derive(Clone)]
pub struct LastStoreOptions {
    /// Point-document layout mode.
    pub layout_mode: LayoutMode,
    /// Number of high bits of a 16-bit hash used for sharding (0..=12).
    /// `0` = one shard per collection (best sequential / batch-flush throughput).
    /// Higher values spread writers and shrink per-shard locks.
    pub shard_bits: u8,
    /// Number of low hash bits used for hash-group placement (1..=16).
    ///
    /// Default is `10`, so hash-group mode creates up to 1024 groups per
    /// shard. Ignored by [`LayoutMode::SegmentLog`].
    pub hash_group_bits: u8,
    /// Hash algorithm for shard and group placement.
    pub hash_algo: HashAlgo,
    /// Which part of an id decides its shard and hash group.
    ///
    /// Layout-affecting: recorded in the descriptor and compared on reopen.
    pub hash_group_key: HashGroupKey,
    /// How many groups one partition may spread across under
    /// [`HashGroupKey::PartitionPrefix`]. Power of two, `1..=1 << hash_group_bits`.
    ///
    /// `1` gives maximum locality: a partition read visits exactly one group.
    /// Larger values trade visits for a smaller worst-case group, which matters
    /// because a group is loaded whole — one oversized partition would otherwise
    /// make every point read into its group parse the entire partition.
    ///
    /// Offsets `0..fanout` are a superset of those any smaller fanout could
    /// produce, so raising it stays readable against rows already written.
    ///
    /// Layout-affecting: recorded in the descriptor and compared on reopen.
    pub hash_group_partition_fanout: u32,
    /// Reject product prefix/range reads that do not name one partition.
    /// Runtime-only and default off during the caller migration. Enabling it
    /// requires partition-prefix placement with at most 16 groups per partition.
    pub reads_require_partition: bool,
    /// Layout epoch recorded in options for callers coordinating migrations.
    pub layout_epoch: u32,
    /// On-disk packaging / encryption of segment and tail files.
    pub packaging: PackagingMode,
    /// Roll the open segment file after this many bytes.
    pub max_segment_bytes: u64,
    /// Opt in to sorted sealed segments. Existing log segments remain readable.
    ///
    /// Default is false. Write-path conversion is a later card. Isolated-copy
    /// proof and a protected retirement must pass before live activation.
    pub sorted_segments: bool,
    /// Maximum plaintext bytes in a sorted group's append tail before seal.
    ///
    /// Default is 1 MiB. Encrypted sorted tails cap this value at 1 MiB.
    pub max_open_tail_bytes: u64,
    /// Spill + fsync after this many unflushed ops (group-commit).
    pub max_dirty_ops: u32,
    /// Spill + fsync after this many unflushed bytes (group-commit).
    pub max_dirty_bytes: u64,
    /// Optional 256-bit content data key for frame encryption.
    ///
    /// `None` keeps the keyless segment format. `Some` writes group-commit
    /// batches as AES-256-GCM frames and authenticates them on reopen.
    pub data_key: Option<[u8; 32]>,
    /// Per-collection policy overrides.
    ///
    /// Defaults include `atoms.never_compact = true`: ordinary compaction must
    /// never rewrite atom chunks. Product code may use the explicit
    /// receipt-provenance path after durably recording every retired digest.
    pub collection_policies: BTreeMap<String, CollectionPolicy>,
    /// Minimum already-restored CSN. New commits start after this floor.
    pub csn_floor: u64,
    /// Optional best-effort capture hook. Failure suspends capture, never the
    /// local write path.
    pub capture_hook: Option<CaptureHook>,
    /// Optional best-effort seal hook for daemon upload notification.
    pub on_seal: Option<SealHook>,
    /// Approximate resident warm-set budget for hash-group shard handles.
    ///
    /// `0` disables hash-group handle eviction. Segment-log layout ignores this
    /// field because the single shard-level index is the legacy correctness map.
    pub hash_group_warm_bytes: u64,
    /// Approximate budget for the hash-group **key-index cache**: the ids of
    /// groups that have been evicted from the warm set.
    ///
    /// A keys-only pass (`list_prefix_keys_paged`, `list_range_keys_paged`)
    /// visits every group in the collection, and rebuilding one group's index
    /// means reading, decrypting, and parsing its entire segment — even though
    /// the ids themselves are a small fraction of that. Retaining just the ids
    /// of evicted groups lets repeat walks skip the segment read entirely,
    /// while the bulky bodies stay evicted under
    /// [`Self::hash_group_warm_bytes`].
    ///
    /// `0` disables the cache. Segment-log layout ignores this field.
    pub hash_group_key_cache_bytes: u64,
    /// Maximum number of hash-group shard handles retained at once.
    ///
    /// **This is a file-descriptor budget, not a memory one.** A resident group
    /// holds its append handle open ([`crate::store`]'s `Shard::open_file`) and
    /// only eviction closes it, so resident groups and open fds are the same
    /// number. [`Self::hash_group_warm_bytes`] cannot stand in for this: with
    /// many small groups the fd ceiling arrives long before the byte ceiling,
    /// and a home with more `collections x groups` than the process has
    /// descriptors will exhaust them while the byte budget still reads as
    /// half-used. That is not hypothetical — on 2026-07-30 the primary node held
    /// 8,167 group handles against an 8,192-fd limit with 2.75 GiB resident
    /// against a 4 GiB budget, and its whole data plane stopped accepting
    /// connections (`EMFILE` at `accept`) while every memory metric read
    /// healthy.
    ///
    /// `0` disables the handle cap (bytes-only eviction, the pre-2026-07-30
    /// behaviour). Segment-log layout ignores this field.
    pub hash_group_warm_max_handles: usize,
    /// HARD cap: largest cold hash group `shard_handle_at` will load whole, in
    /// on-disk bytes. `0` means the default (2 GiB), or no cap when
    /// [`Self::hash_group_warm_bytes`] is `0`. Between
    /// [`Self::soft_cold_group_load_bytes`] and this cap a group loads and is
    /// reclaimed by the product compactors (a node must stay bootable and
    /// upgradeable — Tom, 2026-09-25); over this cap the load is refused.
    ///
    /// A cold load reads, decrypts and parses every segment in the group
    /// before it serves one row, and nothing bounded a group's size: on
    /// 2026-09-21 the primary's `metadata/0/g/025` held 39 GB of superseded
    /// copies of one key (a per-write whole-map snapshot flush), the first
    /// write after boot loaded it, and the daemon blew its 16 GiB memory
    /// guard every 7-17 minutes. Over the cap the load is refused with
    /// [`crate::Error::ColdGroupTooLarge`] — a request error that names the
    /// group — instead of taking the process down.
    ///
    /// Per group, never per collection, so a large sharded plane stays
    /// loadable: the same primary's `tips` is 7.5 GB across 1024 groups of
    /// ~7 MB, and its next-largest group anywhere (`cas_blobs`) is 79 MB. The
    /// default stays far above every legitimate group and below the one that
    /// looped.
    pub max_cold_group_load_bytes: u64,
    /// SOFT cap, in on-disk bytes. A cold hash group larger than this (and not
    /// larger than the hard cap) still loads and logs
    /// `LASTSTORE_COLD_GROUP_OVER_SOFT_CAP`. The store does not rewrite it:
    /// sealed-file rewrites belong to the product compactors, which hold the
    /// backup publish-target lock. `0` means the default (1 GiB), except that
    /// a warm budget of `0` derives no soft cap.
    pub soft_cold_group_load_bytes: u64,
    /// Persist each hash group's ids to a sidecar file beside its segments.
    ///
    /// [`Self::hash_group_key_cache_bytes`] only spans the life of one process
    /// and one bounded budget, so the *first* keys pass over a group — after a
    /// restart, or once the cache drops the entry — still pays a full segment
    /// read just to recover ids. The sidecar is the on-disk tier of the same
    /// idea: ids written when a group is evicted, read back instead of loading
    /// the shard.
    ///
    /// Strictly an advisory cache. It is validated against the group's segment
    /// files on every read and any mismatch falls back to a full load, so a
    /// stale, truncated, or corrupt sidecar costs time, never correctness.
    ///
    /// Ignored unless [`LayoutMode::HashGroup`] **and**
    /// [`PackagingMode::Plain`]: plain packaging already stores ids readably in
    /// the segment, so a sidecar adds no exposure, whereas writing a plaintext
    /// id list beside a frame-AEAD cabinet would.
    pub hash_group_key_sidecar: bool,
}

impl Default for LastStoreOptions {
    fn default() -> Self {
        let mut collection_policies = BTreeMap::new();
        collection_policies.insert(
            ATOMS_COLLECTION.to_string(),
            CollectionPolicy {
                never_compact: true,
                backup_excluded: false,
            },
        );
        Self {
            layout_mode: LayoutMode::HashGroup,
            shard_bits: 0,
            hash_group_bits: 10,
            hash_algo: HashAlgo::Fnv1a64,
            hash_group_key: HashGroupKey::FullKey,
            hash_group_partition_fanout: 1,
            reads_require_partition: false,
            layout_epoch: 0,
            packaging: PackagingMode::Plain,
            max_segment_bytes: 8 * 1024 * 1024,
            sorted_segments: false,
            max_open_tail_bytes: 1024 * 1024,
            max_dirty_ops: 16_384,
            max_dirty_bytes: 16 * 1024 * 1024,
            data_key: None,
            collection_policies,
            csn_floor: 0,
            capture_hook: None,
            on_seal: None,
            hash_group_warm_bytes: DEFAULT_HASH_GROUP_WARM_BYTES,
            hash_group_key_cache_bytes: 0,
            hash_group_warm_max_handles: 0,
            max_cold_group_load_bytes: 0,
            soft_cold_group_load_bytes: 0,
            hash_group_key_sidecar: false,
        }
    }
}

impl fmt::Debug for LastStoreOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LastStoreOptions")
            .field("layout_mode", &self.layout_mode)
            .field("shard_bits", &self.shard_bits)
            .field("hash_group_bits", &self.hash_group_bits)
            .field("hash_algo", &self.hash_algo)
            .field("hash_group_key", &self.hash_group_key)
            .field(
                "hash_group_partition_fanout",
                &self.hash_group_partition_fanout,
            )
            .field("layout_epoch", &self.layout_epoch)
            .field("reads_require_partition", &self.reads_require_partition)
            .field("packaging", &self.packaging)
            .field("max_segment_bytes", &self.max_segment_bytes)
            .field("sorted_segments", &self.sorted_segments)
            .field("max_open_tail_bytes", &self.max_open_tail_bytes)
            .field("max_dirty_ops", &self.max_dirty_ops)
            .field("max_dirty_bytes", &self.max_dirty_bytes)
            .field("data_key", &self.data_key.as_ref().map(|_| "<redacted>"))
            .field("collection_policies", &self.collection_policies)
            .field("csn_floor", &self.csn_floor)
            .field("capture_hook", &self.capture_hook.is_some())
            .field("on_seal", &self.on_seal.is_some())
            .field("hash_group_warm_bytes", &self.hash_group_warm_bytes)
            .field(
                "hash_group_key_cache_bytes",
                &self.hash_group_key_cache_bytes,
            )
            .field(
                "hash_group_warm_max_handles",
                &self.hash_group_warm_max_handles,
            )
            .field("max_cold_group_load_bytes", &self.max_cold_group_load_bytes)
            .field(
                "soft_cold_group_load_bytes",
                &self.soft_cold_group_load_bytes,
            )
            .field("hash_group_key_sidecar", &self.hash_group_key_sidecar)
            .finish()
    }
}

impl LastStoreOptions {
    /// Enforce the keyed-read contract without changing durable placement.
    pub fn with_reads_require_partition(mut self, required: bool) -> Self {
        self.reads_require_partition = required;
        self
    }

    /// Defaults tuned for sequential / batch-flush workloads (H2H vs sled tip).
    ///
    /// New homes use hash-group addressing; batched writes still group-commit.
    pub fn sequential() -> Self {
        Self::default()
    }

    /// More shards for concurrent writers (trades some single-thread flush cost).
    pub fn concurrent(shard_bits: u8) -> Self {
        Self {
            shard_bits,
            ..Self::default()
        }
    }

    /// Use deterministic hash-group point placement with default S=0, G=1024.
    ///
    /// This is also [`LastStoreOptions::default`], plus the Fold product
    /// key-cache / handle-cap / sidecar knobs. Default packaging is
    /// [`PackagingMode::Plain`] (no frame AEAD). Callers that need the
    /// encrypted frame cabinet must set `packaging = FrameAead` and
    /// `data_key = Some(...)`.
    pub fn hash_group() -> Self {
        Self {
            hash_group_key_cache_bytes: DEFAULT_HASH_GROUP_KEY_CACHE_BYTES,
            hash_group_warm_max_handles: DEFAULT_HASH_GROUP_WARM_MAX_HANDLES,
            hash_group_key_sidecar: true,
            ..Self::default()
        }
    }

    /// Opt in to the legacy journal layout (per-shard segments + id → Loc map).
    ///
    /// New homes should use [`Self::default`] / [`Self::hash_group`]. This
    /// constructor is for tests and for callers that must still write the
    /// pre-hash-group format.
    pub fn segment_log() -> Self {
        Self {
            layout_mode: LayoutMode::SegmentLog,
            hash_group_warm_bytes: 0,
            hash_group_key_cache_bytes: 0,
            hash_group_warm_max_handles: 0,
            hash_group_key_sidecar: false,
            ..Self::default()
        }
    }

    /// Hash-group layout with frame-AEAD packaging (legacy cabinet).
    pub fn hash_group_frame_aead(data_key: [u8; 32]) -> Self {
        Self {
            packaging: PackagingMode::FrameAead,
            data_key: Some(data_key),
            // A plaintext id list beside an encrypted cabinet would give away
            // exactly what the cabinet is hiding. The read path enforces this
            // too; setting it here keeps the options honest on inspection.
            hash_group_key_sidecar: false,
            ..Self::hash_group()
        }
    }

    /// Set the approximate resident warm-set budget for hash-group handles.
    pub fn with_hash_group_warm_bytes(mut self, bytes: u64) -> Self {
        self.hash_group_warm_bytes = bytes;
        self
    }

    /// Set the approximate budget for the hash-group key-index cache.
    pub fn with_hash_group_key_cache_bytes(mut self, bytes: u64) -> Self {
        self.hash_group_key_cache_bytes = bytes;
        self
    }

    /// Cap the on-disk size of a cold hash group the store will load whole.
    /// See [`Self::max_cold_group_load_bytes`]; `0` means the 2 GiB default.
    pub fn with_max_cold_group_load_bytes(mut self, bytes: u64) -> Self {
        self.max_cold_group_load_bytes = bytes;
        self
    }

    /// The HARD cold-group load cap in force: the explicit option, or
    /// [`DEFAULT_HARD_COLD_GROUP_LOAD_BYTES`] (2 GiB) when unset. A warm budget
    /// of `0` (eviction off) derives no cap.
    ///
    /// The cap does not scale with the warm budget on purpose. The first draft
    /// derived 4 × warm, which on the primary (4 GiB warm) is 16 GiB: exactly
    /// the memory-guard limit, so a group could still grow to the size that
    /// killed the process on 2026-09-21 before this cap said no.
    pub fn effective_max_cold_group_load_bytes(&self) -> u64 {
        if self.max_cold_group_load_bytes > 0 {
            self.max_cold_group_load_bytes
        } else if self.hash_group_warm_bytes == 0 {
            0
        } else {
            DEFAULT_HARD_COLD_GROUP_LOAD_BYTES
        }
    }

    /// Set the SOFT cold-group load cap. See [`Self::soft_cold_group_load_bytes`];
    /// `0` means the 1 GiB default (none when the warm budget is `0`).
    pub fn with_soft_cold_group_load_bytes(mut self, bytes: u64) -> Self {
        self.soft_cold_group_load_bytes = bytes;
        self
    }

    /// The SOFT cold-group load cap in force: the explicit option, or
    /// [`DEFAULT_SOFT_COLD_GROUP_LOAD_BYTES`] (1 GiB). `0` (no soft cap) only
    /// when the warm budget is `0`, like the hard cap.
    pub fn effective_soft_cold_group_load_bytes(&self) -> u64 {
        if self.soft_cold_group_load_bytes > 0 {
            self.soft_cold_group_load_bytes
        } else if self.hash_group_warm_bytes == 0 {
            0
        } else {
            DEFAULT_SOFT_COLD_GROUP_LOAD_BYTES
        }
    }

    /// Set the maximum number of hash-group handles retained at once.
    ///
    /// One resident group is one open file descriptor, so this is the budget
    /// that keeps a store from exhausting the process's descriptors. Hosts
    /// should derive it from their own `RLIMIT_NOFILE` with headroom rather than
    /// take the crate default, which has to assume a small limit.
    pub fn with_hash_group_warm_max_handles(mut self, handles: usize) -> Self {
        self.hash_group_warm_max_handles = handles;
        self
    }

    /// Enable or disable the on-disk hash-group key sidecar.
    pub fn with_hash_group_key_sidecar(mut self, enabled: bool) -> Self {
        self.hash_group_key_sidecar = enabled;
        self
    }

    /// Mark a collection as non-compacting.
    pub fn with_never_compact_collection(mut self, collection: impl Into<String>) -> Self {
        self.collection_policies
            .entry(collection.into())
            .or_default()
            .never_compact = true;
        self
    }

    /// Select which part of an id decides placement (layout-affecting).
    pub fn with_hash_group_key(mut self, key: HashGroupKey) -> Self {
        self.hash_group_key = key;
        self
    }

    /// Bound how many groups one partition may occupy (layout-affecting).
    pub fn with_hash_group_partition_fanout(mut self, fanout: u32) -> Self {
        self.hash_group_partition_fanout = fanout;
        self
    }

    /// Mark a collection as excluded from backup chunk enumeration.
    pub fn with_backup_excluded_collection(mut self, collection: impl Into<String>) -> Self {
        self.collection_policies
            .entry(collection.into())
            .or_default()
            .backup_excluded = true;
        self
    }

    /// Return the effective policy for `collection`.
    pub fn collection_policy(&self, collection: &str) -> CollectionPolicy {
        self.collection_policies
            .get(collection)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.reads_require_partition
            && (self.layout_mode != LayoutMode::HashGroup
                || self.hash_group_key != HashGroupKey::PartitionPrefix
                || self.hash_group_partition_fanout > 16)
        {
            return Err("reads_require_partition requires hash-group partition-prefix placement with fanout <= 16".into());
        }
        if self.shard_bits > 12 {
            return Err(format!(
                "shard_bits must be 0..=12, got {}",
                self.shard_bits
            ));
        }
        if self.hash_group_bits == 0 || self.hash_group_bits > 16 {
            return Err(format!(
                "hash_group_bits must be 1..=16, got {}",
                self.hash_group_bits
            ));
        }
        let fanout = self.hash_group_partition_fanout;
        if fanout == 0 || !fanout.is_power_of_two() {
            return Err(format!(
                "hash_group_partition_fanout must be a power of two >= 1, got {fanout}"
            ));
        }
        if u64::from(fanout) > 1u64 << self.hash_group_bits {
            return Err(format!(
                "hash_group_partition_fanout {fanout} exceeds the {} groups implied by \
                 hash_group_bits={}",
                1u64 << self.hash_group_bits,
                self.hash_group_bits
            ));
        }
        if self.hash_group_key == HashGroupKey::FullKey && fanout != 1 {
            return Err(format!(
                "hash_group_partition_fanout must be 1 under HashGroupKey::FullKey \
                 (partitions have no locality to spread), got {fanout}"
            ));
        }
        if self.max_open_tail_bytes < 64 * 1024 {
            return Err("max_open_tail_bytes must be at least 64 KiB".into());
        }
        if self.max_segment_bytes < 64 * 1024 {
            return Err("max_segment_bytes must be >= 64 KiB".into());
        }
        if self.max_dirty_ops == 0 {
            return Err("max_dirty_ops must be >= 1".into());
        }
        if self.max_dirty_bytes == 0 {
            return Err("max_dirty_bytes must be >= 1".into());
        }
        if let Some(collection) = self.collection_policies.keys().find(|k| k.is_empty()) {
            return Err(format!(
                "collection policy name must not be empty: {collection:?}"
            ));
        }
        match self.packaging {
            PackagingMode::Plain if self.data_key.is_some() => {
                return Err(
                    "packaging=plain forbids data_key (frame AEAD); use PackagingMode::FrameAead"
                        .into(),
                );
            }
            PackagingMode::FrameAead if self.data_key.is_none() => {
                return Err("packaging=frame_aead requires data_key".into());
            }
            _ => {}
        }
        Ok(())
    }
}
