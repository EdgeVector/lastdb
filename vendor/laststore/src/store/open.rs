use super::*;

impl LastStore {
    /// Open or create a store at `path` with default options.
    ///
    /// A newly created home uses UUID hash-group addressing. An existing home
    /// keeps the layout recorded in `laststore-layout-v1` (or detected from
    /// files when the descriptor is absent).
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        let opts = resolve_layout_options(&root, LastStoreOptions::default(), false)?;
        Self::open_resolved(root, opts)
    }

    /// Open or create a store with explicit options.
    pub fn open_with(path: impl AsRef<Path>, opts: LastStoreOptions) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        let opts = resolve_layout_options(&root, opts, true)?;
        Self::open_resolved(root, opts)
    }

    /// Open an existing store using its durable layout while retaining
    /// runtime-only options such as data keys, hooks, and CSN floors.
    /// For a fresh home, `opts` also selects the initial layout.
    pub fn open_existing_or_with(path: impl AsRef<Path>, opts: LastStoreOptions) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        let opts = resolve_layout_options(&root, opts, false)?;
        Self::open_resolved(root, opts)
    }

    pub(super) fn open_resolved(root: PathBuf, mut opts: LastStoreOptions) -> Result<Self> {
        // Backward compatible: a present data_key implies frame-AEAD packaging
        // even when callers only set the key (pre-packaging-mode tests/APIs).
        if opts.data_key.is_some() {
            opts.packaging = PackagingMode::FrameAead;
        }
        opts.validate().map_err(Error::Config)?;
        fs::create_dir_all(root.join("data"))?;
        write_layout_descriptor(&root, &opts)?;
        let next_csn = opts.csn_floor.saturating_add(1);
        let configured_warm_bytes = opts.hash_group_warm_bytes;
        Ok(Self {
            root,
            opts,
            shards: Mutex::new(ShardWarmSet::default()),
            key_index: Mutex::new(KeyIndexCache::default()),
            meta: Mutex::new(StoreMeta {
                next_csn,
                ..StoreMeta::default()
            }),
            shard_loads: AtomicU64::new(0),
            walk_vanished_ids: AtomicU64::new(0),
            flush_barriers: AtomicU64::new(0),
            groups_synced: AtomicU64::new(0),
            groups_synced_last_flush: AtomicU64::new(0),
            flush_foreign_groups: AtomicU64::new(0),
            open_append_handles: Arc::new(AtomicUsize::new(0)),
            walk_ids_visited: AtomicU64::new(0),
            rejected_partition_reads: AtomicU64::new(0),
            all_group_walks: AtomicU64::new(0),
            frame_compression_stats: Arc::new(Mutex::new(BTreeMap::new())),
            effective_warm_bytes: AtomicU64::new(configured_warm_bytes),
            warm_drain_hold: AtomicBool::new(false),
            host_pressure_high: AtomicBool::new(false),
            index_over_budget_publishes: AtomicU64::new(0),
            in_flight_cold_loads: AtomicU64::new(0),
            in_flight_cold_bytes: AtomicU64::new(0),
            eviction_events: AtomicU64::new(0),
            id_tier_resident: AtomicU64::new(0),
            id_tier_key_cache_hits: AtomicU64::new(0),
            id_tier_sidecar_hits: AtomicU64::new(0),
            id_tier_live_scans: AtomicU64::new(0),
            torn_transaction_rollbacks: AtomicU64::new(0),
            torn_transaction_rollback_failures: AtomicU64::new(0),
            transaction_residency_refresh_failures: AtomicU64::new(0),
            transaction_gates: std::array::from_fn(|_| Mutex::new(())),
            cold_load_gates: std::array::from_fn(|_| Mutex::new(())),
            pins: Mutex::new(PinTable::default()),
            loader_pin_bytes: AtomicU64::new(0),
            next_durability_token: AtomicU64::new(0),
            durable_through: AtomicU64::new(0),
        })
    }

    /// Open append descriptors across every resident group.
    ///
    /// This is the number the handle cap bounds, and it is not the resident
    /// group count: a group opens its append handle on its first spill and can
    /// stay resident for the rest of the process without one.
    pub fn open_append_handles(&self) -> usize {
        self.open_append_handles.load(Ordering::Relaxed)
    }
}
