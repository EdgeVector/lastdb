//! Maintenance, drain, and compaction operations on [`LastStoreNamespacedStore`].

use super::*;

impl LastStoreNamespacedStore {
    /// Product options for a new hash-group home.
    ///
    /// PartitionPrefix so a range under one molecule hash passes the loader
    /// range gate. An existing home keeps the layout recorded on disk via
    /// [`LastStore::open_existing_or_with`]. FullKey remains available through
    /// [`Self::open_with_options`] (the copy-proof FullKey case).
    pub fn product_hash_group_options() -> LastStoreOptions {
        LastStoreOptions::hash_group().with_hash_group_key(HashGroupKey::PartitionPrefix)
    }

    /// Open or create a LastStore home.
    ///
    /// A new home uses hash-group addressing with
    /// [`HashGroupKey::PartitionPrefix`]. An existing home keeps the layout
    /// recorded on disk, including FullKey and legacy journal homes.
    pub fn open(path: &Path) -> StorageResult<Self> {
        Self::open_with_options(path, Self::product_hash_group_options())
    }

    pub fn open_with_options(path: &Path, opts: LastStoreOptions) -> StorageResult<Self> {
        let store =
            LastStore::open_existing_or_with(path, opts).map_err(LastStoreKvStore::map_error)?;
        Ok(Self::assemble(Arc::new(store), None))
    }

    pub fn options(&self) -> &LastStoreOptions {
        self.store.options()
    }

    /// Approximate warm-set residency (hash-group handles) for Mini metrics/tests.
    pub fn hash_group_warm_stats(&self) -> laststore::HashGroupWarmStats {
        self.store.hash_group_warm_stats()
    }

    /// Per-collection warm-set residency.
    pub fn hash_group_warm_stats_for(&self, collection: &str) -> laststore::HashGroupWarmStats {
        self.store.hash_group_warm_stats_for(collection)
    }

    pub fn open_with_options_and_high_water(
        path: &Path,
        mut opts: LastStoreOptions,
        high_water_path: impl Into<PathBuf>,
    ) -> StorageResult<Self> {
        let high_water = Arc::new(LastStoreHighWaterFile::new(high_water_path));
        opts.data_key = None;
        opts.csn_floor = high_water.csn_floor()?;
        let store =
            LastStore::open_existing_or_with(path, opts).map_err(LastStoreKvStore::map_error)?;
        high_water.record_csn_high_water(store.csn_high_water())?;
        Ok(Self::assemble(Arc::new(store), Some(high_water)))
    }

    pub fn open_with_data_key_and_high_water(
        path: &Path,
        data_key: [u8; 32],
        high_water_path: impl Into<PathBuf>,
    ) -> StorageResult<Self> {
        Self::open_with_options_and_high_water_data_key(
            path,
            LastStoreOptions::hash_group_frame_aead(data_key)
                .with_hash_group_key(HashGroupKey::PartitionPrefix),
            high_water_path,
        )
    }

    pub fn open_with_options_and_high_water_data_key(
        path: &Path,
        mut opts: LastStoreOptions,
        high_water_path: impl Into<PathBuf>,
    ) -> StorageResult<Self> {
        let high_water = Arc::new(LastStoreHighWaterFile::new(high_water_path));
        opts.csn_floor = high_water.csn_floor()?;
        let store =
            LastStore::open_existing_or_with(path, opts).map_err(LastStoreKvStore::map_error)?;
        high_water.record_csn_high_water(store.csn_high_water())?;
        Ok(Self::assemble(Arc::new(store), Some(high_water)))
    }

    pub fn new(store: LastStore) -> Self {
        Self::assemble(Arc::new(store), None)
    }

    /// Single construction point: derives the chunk-sha memo (and its
    /// persistence location, when a durable high-water sidecar dir exists).
    pub(super) fn assemble(
        store: Arc<LastStore>,
        high_water: Option<Arc<LastStoreHighWaterFile>>,
    ) -> Self {
        let memo_path = high_water
            .as_ref()
            .and_then(|hw| hw.sidecar_dir())
            .map(|dir| dir.join(backup_manifest::CHUNK_SHA_MEMO_FILE));
        Self {
            store,
            high_water,
            chunk_sha_memo: Arc::new(backup_manifest::ChunkShaMemo::new(memo_path)),
            atom_retirement_lock: Arc::new(std::sync::Mutex::new(())),
        }
    }
}

mod backup;
mod backup_file;
mod compact_admin;
mod index_residue;
mod residue_drain;

pub(super) fn collection_dir_bytes(store_root: &Path, collection: &str) -> u64 {
    let dir = store_root.join("data").join(collection);
    dir_tree_bytes(&dir)
}

fn dir_tree_bytes(path: &Path) -> u64 {
    let Ok(meta) = std::fs::metadata(path) else {
        return 0;
    };
    if meta.is_file() {
        return meta.len();
    }
    let Ok(rd) = std::fs::read_dir(path) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in rd.flatten() {
        total = total.saturating_add(dir_tree_bytes(&entry.path()));
    }
    total
}
