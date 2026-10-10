use super::*;

/// Target-scoped pin-mode / mutation-log state, extracted from `SyncEngine`.
///
/// Owns the durable pin-log runtime map. `store`, `config` and `targets` are
/// shared handles from the engine — `config` is `Clone` and never mutated after
/// construction, the other two are already `Arc` — so this observes the same
/// state the engine does rather than a divergent copy.
///
/// A few operations still need engine collaborators (cloud-plane gating, entry
/// replay, target config). Those take `engine: &SyncEngine` explicitly rather
/// than reaching for it, which keeps the remaining coupling visible instead of
/// hiding it behind a back-reference.
pub(crate) struct PinLog {
    pub(super) state: Arc<Mutex<std::collections::HashMap<String, PinLogRuntime>>>,
    /// Volatile hint for targets whose bounded durable read found more rows.
    /// A restart clears it; a rotating target pass discovers old work again.
    pub(super) known_pending_prefixes: Arc<Mutex<std::collections::HashSet<String>>>,
    /// Serializes append-floor max-merge with its pin-log record batch.
    pub(super) append_lock: Arc<Mutex<()>>,
    /// Serializes read/max-merge/write of whole published-F maps.
    pub(super) published_f_lock: Arc<Mutex<()>>,
    pub(super) store: Arc<dyn NamespacedStore>,
    pub(super) config: SyncConfig,
    pub(super) targets: Arc<Mutex<Vec<SyncTarget>>>,
    /// Confirmed pin-log rows deleted since the plane was last compacted.
    ///
    /// A `LastStore` delete is an append, so truncation alone never returns a
    /// byte — see [`Self::maybe_compact_pin_log_plane`]. This counter is the
    /// trigger for the rewrite that does.
    pub(super) truncated_since_compact: Arc<std::sync::atomic::AtomicU64>,
    /// Truncated rows that must accumulate before the plane is compacted.
    ///
    /// Read from the environment once at construction rather than per cycle:
    /// the value cannot change under a running daemon, and a per-cycle
    /// `env::var` on the publish path buys nothing.
    pub(super) compact_after_rows: u64,
    /// On-disk plane bytes above which the plane is compacted regardless of how
    /// many rows *this process* happened to truncate. `0` disables.
    ///
    /// See [`Self::maybe_compact_pin_log_plane`] for why the row counter alone
    /// is not a retention policy.
    pub(super) compact_max_plane_bytes: u64,
    /// Unix seconds of the last bloat probe, so the stat walk is rate-limited.
    ///
    /// `0` means "never probed", which is deliberately due immediately: the
    /// first publish cycle after a start is exactly when an inherited bloated
    /// plane needs to be noticed.
    pub(super) last_bloat_probe_unix_s: Arc<std::sync::atomic::AtomicU64>,
    /// Minimum seconds between bloat probes.
    pub(super) compact_bloat_probe_interval_s: u64,
    /// Size the plane must exceed for the *next* size-triggered compaction,
    /// raised after each one so a legitimately large live set cannot turn the
    /// cap into a rewrite treadmill. See
    /// [`Self::raise_size_trigger_floor`]. `0` means "use the cap".
    pub(super) size_trigger_floor_bytes: Arc<std::sync::atomic::AtomicU64>,
    /// Trigger for the most recent successful physical rewrite.
    pub(super) last_compact_trigger: Arc<Mutex<Option<String>>>,
    /// Photograph packing lock, shared with [`SyncEngine`]. Held across the
    /// physical rewrite so a backup cut cannot start (or continue) while this
    /// plane's sealed files are being rewritten.
    pub(super) backup_publish_target:
        Arc<Mutex<Option<super::backup_uploader::BackupPublishTarget>>>,
}

impl PinLog {
    /// Same as [`Self::new`], sharing the engine's photograph packing lock.
    pub(crate) fn new_with_packing_lock(
        store: Arc<dyn NamespacedStore>,
        config: SyncConfig,
        targets: Arc<Mutex<Vec<SyncTarget>>>,
        backup_publish_target: Arc<Mutex<Option<super::backup_uploader::BackupPublishTarget>>>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(std::collections::HashMap::new())),
            known_pending_prefixes: Arc::new(Mutex::new(std::collections::HashSet::new())),
            append_lock: Arc::new(Mutex::new(())),
            published_f_lock: Arc::new(Mutex::new(())),
            store,
            config,
            targets,
            truncated_since_compact: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            compact_after_rows: pin_log_compact_after_rows(),
            compact_max_plane_bytes: pin_log_compact_max_plane_bytes(),
            last_bloat_probe_unix_s: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            compact_bloat_probe_interval_s: pin_log_compact_bloat_probe_interval_s(),
            size_trigger_floor_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            last_compact_trigger: Arc::new(Mutex::new(None)),
            backup_publish_target,
        }
    }
}
