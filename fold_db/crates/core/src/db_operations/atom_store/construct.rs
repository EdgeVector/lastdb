//! `AtomStore` constructors, builder hooks and the atom-ref v2 compatibility
//! shims.

use super::*;

impl AtomStore {
    /// Open only the atom operation surface over an existing store stack.
    ///
    /// Admin proofs and audit workers use this constructor when they must not
    /// hydrate the complete [`crate::FoldDB`] schema and resident graph.
    pub async fn from_namespaced_store(
        store: Arc<dyn NamespacedStore>,
    ) -> Result<Self, crate::schema::SchemaError> {
        let main_store = Arc::new(TypedKvStore::new(
            store.open_namespace("main").await.map_err(|error| {
                crate::schema::SchemaError::InvalidData(format!(
                    "open atom main namespace: {error}"
                ))
            })?,
        ));
        let schema_index_store = Arc::new(TypedKvStore::new(
            store
                .open_namespace("schema_index")
                .await
                .map_err(|error| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "open atom schema-index namespace: {error}"
                    ))
                })?,
        ));
        Ok(Self::new(main_store, schema_index_store).with_namespaced_store(store))
    }

    pub(crate) fn new(
        main_store: Arc<TypedKvStore<dyn KvStore>>,
        schema_index_store: Arc<TypedKvStore<dyn KvStore>>,
    ) -> Self {
        Self::new_with_content_key(main_store, schema_index_store, None)
    }

    pub(crate) fn new_with_content_key(
        main_store: Arc<TypedKvStore<dyn KvStore>>,
        schema_index_store: Arc<TypedKvStore<dyn KvStore>>,
        content_key: Option<[u8; 32]>,
    ) -> Self {
        Self::new_with_content_and_hash_key_codec(
            main_store,
            schema_index_store,
            content_key,
            crate::atom::MoleculeKeyCodec::plain(),
        )
    }

    pub(crate) fn new_with_content_and_hash_key_codec(
        main_store: Arc<TypedKvStore<dyn KvStore>>,
        schema_index_store: Arc<TypedKvStore<dyn KvStore>>,
        content_key: Option<[u8; 32]>,
        key_codec: crate::atom::MoleculeKeyCodec,
    ) -> Self {
        Self {
            main_store,
            schema_index_store,
            namespaced_store: None,
            resident_graph: None,
            content_key,
            atom_content_binary: crate::atom::atom_content_binary_enabled(),
            key_codec,
            molecule_keys: None,
            atom_keys_partition_prefixed: Arc::new(AtomicBool::new(
                crate::atom::AtomKeyEncoding::from_env_or_default().writes_partition_prefix(),
            )),
            atom_ref_v2_read_ready: Arc::default(),
            molecule_commit_locks: Arc::default(),
            tip_commit_locks: Arc::default(),
            tip_publication_locks: Arc::default(),
            pending_delete_barriers: Arc::default(),
            keep_small: Arc::new(KeepSmallMeters::default()),
            keep_small_persist: None,
            keep_small_last_persist: Arc::default(),
            keep_small_dirty: Arc::default(),
            keep_small_clean_stop_written: Arc::default(),
            keep_small_persist_lock: Arc::default(),
            keep_small_hard_erase_totals: Arc::default(),
            keep_small_hard_erase_next_seq: Arc::default(),
            keep_small_hard_erase_applied_seq: Arc::default(),
            keep_small_hard_erase_fenced: Arc::default(),
            keep_small_hard_erase_replay_skipped: Arc::default(),
            keep_small_hard_erase_mutation_epoch: Arc::default(),
            keep_small_hard_erase_pending: Arc::default(),
            keep_small_hard_erase_orphaned: Arc::default(),
            automatic_gc_atoms_generation: Arc::default(),
            automatic_gc_atom_locks: Arc::new(
                (0..256)
                    .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                    .collect(),
            ),
            atom_ref_count_locks: Arc::new(
                (0..256)
                    .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                    .collect(),
            ),
            catalog_atom_ref_transition_lock: Arc::default(),
            molecule_liveness_locks: Arc::new(
                (0..256)
                    .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                    .collect(),
            ),
            blob_liveness_locks: Arc::new(
                (0..256)
                    .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                    .collect(),
            ),
            store_id: crate::atom::legacy_history_memo::StoreId::next(),
        }
    }

    #[must_use]
    pub(crate) fn with_molecule_keys(
        mut self,
        molecule_keys: crate::db_operations::MoleculeKeyStore,
    ) -> Self {
        self.molecule_keys = Some(molecule_keys);
        self
    }

    pub(crate) fn with_resident_graph(
        mut self,
        resident: Arc<crate::resident::ResidentGraph>,
    ) -> Self {
        self.resident_graph = Some(resident);
        self
    }

    pub(super) fn invalidate_coverage_during<'a>(
        &self,
        molecules: impl IntoIterator<Item = &'a str>,
        storage_prefix: Option<&str>,
    ) -> Option<CoverageMutation> {
        if storage_prefix.is_some() {
            return None;
        }
        let graph = self.resident_graph.as_ref()?;
        let molecules: std::collections::BTreeSet<String> =
            molecules.into_iter().map(str::to_string).collect();
        for molecule in &molecules {
            graph.invalidate_molecule_coverage(molecule);
        }
        Some(CoverageMutation {
            graph: Arc::clone(graph),
            molecules,
        })
    }

    #[must_use]
    pub(crate) fn with_namespaced_store(mut self, store: Arc<dyn NamespacedStore>) -> Self {
        self.namespaced_store = Some(store);
        self
    }
}
