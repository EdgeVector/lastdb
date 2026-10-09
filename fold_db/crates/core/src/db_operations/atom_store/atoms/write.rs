use super::*;

impl AtomStore {
    /// Creates an atom in memory without storing it.
    /// Used for batch operations where atoms are collected first then stored together.
    ///
    /// Enforces [`crate::atom::max_atom_content_bytes`] on `value` so oversized
    /// field payloads fail at the create boundary (not only on store).
    /// Default 64 KiB; see `LASTDB_MAX_ATOM_CONTENT_BYTES` and
    /// `fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md`.
    pub fn create_atom(
        schema_name: &str,
        value: Value,
        source_file_name: Option<String>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<Atom, SchemaError> {
        // Enforce and record in one call: a rejection names its schema in the
        // node log, and an accepted write that is running out of headroom says
        // so before the growth that would wedge it.
        crate::atom::enforce_atom_content_limit(schema_name, &value)?;
        let mut atom = Atom::new(schema_name.to_string(), value);
        if let Some(filename) = source_file_name {
            atom = atom.with_source_file_name(filename);
        }
        if let Some(meta) = metadata {
            atom = atom.with_metadata(meta);
        }
        Ok(atom)
    }

    /// Batch store multiple atoms efficiently.
    /// Deduplicates by key since atoms with identical content have the same UUID.
    ///
    /// When `storage_prefix` is `Some`, all keys are prefixed with `{storage_prefix}:`.
    ///
    /// Rejects any atom whose content exceeds
    /// [`crate::atom::max_atom_content_bytes`] (defense in depth for paths that
    /// bypass [`Self::create_atom`]).
    pub async fn batch_store_atoms(
        &self,
        atoms: Vec<Atom>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.batch_store_atoms_located(
            atoms.into_iter().map(|atom| (atom, None)).collect(),
            storage_prefix,
        )
        .await
    }

    /// Batch store atoms, each with the partition of the slot that owns it.
    ///
    /// This is [`Self::batch_store_atoms`] plus locality. Under
    /// [`crate::atom::AtomKeyEncoding::PartitionPrefix`] an atom whose
    /// partition is `Some` is written at the prefixed key — co-located with the
    /// tips that point at it — and gets an [`atom_locator_codec`] row so the
    /// uuid-only read surface can still find it. An atom whose partition is
    /// `None` (an orphan, a caller with no slot in scope) is written flat, with
    /// no locator: the defined fallback, not an error.
    ///
    /// The locator rows go in the **same** `batch_put_items` call as the
    /// bodies. LastStore applies one batch as a single transaction across
    /// collections, so a body and its locator are durable together or not at
    /// all — there is no window where a prefixed body exists that the uuid-only
    /// path cannot find.
    ///
    /// Under [`crate::atom::AtomKeyEncoding::Flat`] the partitions are ignored
    /// and no locator rows are written, so this is byte-for-byte what
    /// [`Self::batch_store_atoms`] has always done.
    pub async fn batch_store_atoms_located(
        &self,
        atoms: Vec<(Atom, Option<crate::atom::AtomPartition>)>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.batch_store_atoms_located_borrowed(&atoms, storage_prefix)
            .await
    }

    /// Borrowed form of [`Self::batch_store_atoms_located`] for callers that
    /// must retain ownership of a retryable batch.
    pub(crate) async fn batch_store_atoms_located_borrowed(
        &self,
        atoms: &[(Atom, Option<crate::atom::AtomPartition>)],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        // lint:fn-size-ok verbatim move from atoms.rs; splitting this function is separate work
        if atoms.is_empty() {
            return Ok(());
        }

        let guarded_uuids = atoms
            .iter()
            .map(|(atom, _)| atom.uuid().to_string())
            .collect::<Vec<_>>();
        let _automatic_gc_guards = self.lock_automatic_gc_atoms(&guarded_uuids).await;
        let _ref_count_guards = self.lock_atom_ref_counts(&guarded_uuids).await;
        let guarded_blob_refs = atoms
            .iter()
            .flat_map(|(atom, _)| {
                crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
            })
            .collect::<Vec<_>>();
        let _blob_liveness_guards = self.lock_liveness_blobs(&guarded_blob_refs).await;
        let automatic_gc_generation = self.automatic_gc_atoms_generation();

        let encoding = self.atom_key_encoding();

        // Deduplicate by key - atoms with same content have same UUID.
        // Canonical atoms stay in `main`; the schema-keyed marker is derived
        // and lives in the separate local-only `schema_index` namespace so
        // point reads over `atom:{uuid}` do not pay for index marker churn.
        let mut seen_keys = std::collections::HashSet::new();
        let mut seen_gc_markers = std::collections::HashSet::new();
        let mut seen_blob_edges = std::collections::HashSet::new();
        let mut blob_edge_items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut atom_items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut index_items: Vec<(String, Value)> = Vec::new();
        let mut keep_small_puts: Vec<(String, String, u64, u64, bool)> = Vec::new();
        for (atom, partition) in atoms {
            crate::atom::enforce_atom_content_limit(atom.source_schema_name(), atom.content())?;
            let base_key = atom_key_codec::storage_key(encoding, partition.as_ref(), atom.uuid());
            let key = build_storage_key(storage_prefix, &base_key);
            if !seen_keys.insert(key.clone()) {
                continue; // Skip duplicate keys
            }
            let index_key = build_storage_key(
                storage_prefix,
                &schema_index_codec::record_key(atom.source_schema_name(), atom.uuid()),
            );
            let bundle_molecule = storage_prefix
                .and(partition.as_ref())
                .and_then(crate::atom::AtomPartition::molecule_uuid);
            let atom_value = self.encode_atom_bytes(atom, bundle_molecule).await?;
            let logical = serde_json::to_vec(atom).map_or(0, |v| v.len() as u64);
            let blob_bytes = crate::atom::file_pointer::blob_logical_bytes_of_atom(
                atom.content(),
                atom.metadata(),
            );
            keep_small_puts.push((
                atom.source_schema_name().to_string(),
                atom.uuid().to_string(),
                logical,
                blob_bytes.unwrap_or_default(),
                blob_bytes.is_some(),
            ));
            for blob_ref in
                crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
            {
                let edge = BlobRefEdge::atom(atom.uuid(), &blob_ref);
                let edge_key = edge.storage_key(storage_prefix);
                if seen_blob_edges.insert(edge_key.clone()) {
                    let edge_value = serde_json::to_vec(&edge).map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "serialize blob reference edge for {}: {error}",
                            atom.uuid()
                        ))
                    })?;
                    blob_edge_items.push((edge_key.into_bytes(), edge_value));
                }
            }
            atom_items.push((key.into_bytes(), atom_value));
            index_items.push((index_key, Value::Bool(true)));

            // Only a body that actually landed at a prefixed key needs a
            // locator; a flat body is already where the uuid-only path looks.
            if let Some(partition) = partition
                .as_ref()
                .filter(|_| encoding.writes_partition_prefix())
            {
                let locator_key = build_storage_key(
                    storage_prefix,
                    &atom_locator_codec::locator_key(atom.uuid()),
                );
                let locator_value = serde_json::to_vec(&atom_locator_codec::encode_value(
                    partition,
                ))
                .map_err(|e| SchemaError::InvalidData(format!("serialize atom locator: {e}")))?;
                atom_items.push((locator_key.into_bytes(), locator_value));
            }
            if automatic_gc_generation > 0 && seen_gc_markers.insert(atom.uuid().to_string()) {
                let marker_key =
                    Self::automatic_gc_atoms_reference_marker_key(storage_prefix, atom.uuid());
                let marker_value = serde_json::to_vec(&serde_json::json!({
                    "generation": automatic_gc_generation,
                }))
                .map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "serialize automatic gc-atoms reference marker: {e}"
                    ))
                })?;
                atom_items.push((marker_key.into_bytes(), marker_value));
            }
        }

        // `index_items` is one per stored atom; `atom_items` also carries the
        // locator rows, so it is not the atom count.
        tracing::info!(
            "Batch storing {} atoms (after dedup), {} locator rows",
            index_items.len(),
            atom_items.len() - index_items.len()
        );

        // Every atom carries an explicit durable count from birth. A deduped
        // body never resets an existing count.
        blob_edge_items.extend(
            self.missing_atom_ref_count_items(&guarded_uuids, storage_prefix)
                .await?,
        );
        // Retain-first order: active blob edges and the zero-count row precede
        // immutable atom bodies in the same durable batch.
        blob_edge_items.extend(atom_items);
        self.main_store
            .inner()
            .batch_put(blob_edge_items)
            .await
            .map_err(|e| batch_write_error("atoms", e))?;
        self.schema_index_store
            .batch_put_items(index_items)
            .await
            .map_err(|e| batch_write_error("atom schema index", e))?;

        for (schema, uuid, logical, blob_bytes, complete) in keep_small_puts {
            self.keep_small
                .record_atom_put_with_blob(&schema, &uuid, logical, blob_bytes, complete);
        }
        let _ = self.persist_keep_small().await;

        tracing::info!("Batch stored atoms successfully");
        Ok(())
    }

    /// Creates and stores an atom for a mutation field with deferred flush.
    /// If an atom with the same content already exists (content-based deduplication),
    /// returns the existing atom instead of creating a duplicate.
    ///
    /// When `storage_prefix` is `Some`, all keys are prefixed with `{storage_prefix}:`.
    pub async fn create_and_store_atom_for_mutation_deferred(
        &self,
        schema_name: &str,
        value: Value,
        source_file_name: Option<String>,
        metadata: Option<HashMap<String, String>>,
        storage_prefix: Option<&str>,
    ) -> Result<Atom, SchemaError> {
        self.create_and_store_atom_for_mutation_deferred_located(
            schema_name,
            value,
            source_file_name,
            metadata,
            storage_prefix,
            None,
        )
        .await
    }

    /// [`Self::create_and_store_atom_for_mutation_deferred`] with the owning
    /// slot's partition.
    ///
    /// Content-based dedup is checked at the key this atom would be written to,
    /// so under `PartitionPrefix` dedup is scoped to the partition: two slots
    /// holding identical content each keep a body, where today they share one.
    /// That is the deliberate trade for locality — see
    /// `design-lastdb-atom-key-partition-locality`, "dedup scopes to the
    /// partition". Within a partition (a row rewritten to the same value, a
    /// repeated value across a partition's rows) dedup is unchanged.
    pub async fn create_and_store_atom_for_mutation_deferred_located(
        &self,
        schema_name: &str,
        value: Value,
        source_file_name: Option<String>,
        metadata: Option<HashMap<String, String>>,
        storage_prefix: Option<&str>,
        partition: Option<&crate::atom::AtomPartition>,
    ) -> Result<Atom, SchemaError> {
        // Size fence before hashing/storing (same rule as create_atom).
        let new_atom = Self::create_atom(schema_name, value, source_file_name, metadata)?;

        let encoding = self.atom_key_encoding();
        let partition = partition.filter(|_| encoding.writes_partition_prefix());

        // Check if atom with this content-based UUID already exists
        let base_key = atom_key_codec::storage_key(encoding, partition, new_atom.uuid());
        let atom_key = build_storage_key(storage_prefix, &base_key);
        let atom_uuid = new_atom.uuid().to_string();
        let _ref_count_guards = self
            .lock_atom_ref_counts(std::slice::from_ref(&atom_uuid))
            .await;
        tracing::debug!("Checking for existing atom: {}", atom_key);
        if let Some(raw) = self
            .main_store
            .inner()
            .get(atom_key.as_bytes())
            .await
            .map_err(|e| {
                tracing::error!("Failed to check existing atom '{}': {}", atom_key, e);
                SchemaError::InvalidData(format!("Failed to check existing atom: {e}"))
            })?
        {
            let existing_atom = self.decode_atom_bytes(&raw).await?;
            tracing::debug!("Atom already exists, returning existing: {}", atom_key);
            self.put_schema_index_marker(storage_prefix, &existing_atom)
                .await?;
            return Ok(existing_atom);
        }

        // Store the canonical atom first; the schema marker is a derived index
        // and can be rebuilt from `atom:` rows if interrupted.
        tracing::info!("Writing atom: key={}, uuid={}", atom_key, new_atom.uuid());
        let bundle_molecule = storage_prefix
            .and(partition)
            .and_then(crate::atom::AtomPartition::molecule_uuid);
        let atom_value = self.encode_atom_bytes(&new_atom, bundle_molecule).await?;
        // Body and locator in one batch — LastStore applies it as a single
        // transaction, so the uuid-only path never sees a prefixed body it
        // cannot locate.
        let mut items = self
            .missing_atom_ref_count_items(std::slice::from_ref(&atom_uuid), storage_prefix)
            .await?;
        items.push((atom_key.clone().into_bytes(), atom_value));
        if let Some(partition) = partition {
            let locator_key = build_storage_key(
                storage_prefix,
                &atom_locator_codec::locator_key(new_atom.uuid()),
            );
            let locator_value = serde_json::to_vec(&atom_locator_codec::encode_value(partition))
                .map_err(|e| SchemaError::InvalidData(format!("serialize atom locator: {e}")))?;
            items.push((locator_key.into_bytes(), locator_value));
        }
        self.main_store
            .inner()
            .batch_put(items)
            .await
            .map_err(|e| {
                tracing::error!("Failed to store atom '{}': {}", atom_key, e);
                SchemaError::InvalidData(format!("Failed to store atom: {e}"))
            })?;
        let logical = serde_json::to_vec(&new_atom).map_or(0, |v| v.len() as u64);
        let blob_bytes = crate::atom::file_pointer::blob_logical_bytes_of_atom(
            new_atom.content(),
            new_atom.metadata(),
        );
        self.keep_small.record_atom_put_with_blob(
            new_atom.source_schema_name(),
            new_atom.uuid(),
            logical,
            blob_bytes.unwrap_or_default(),
            blob_bytes.is_some(),
        );
        let _ = self.persist_keep_small().await;
        self.put_schema_index_marker(storage_prefix, &new_atom)
            .await?;
        tracing::info!("Atom written: {}", atom_key);

        Ok(new_atom)
    }

    pub(super) async fn put_schema_index_marker(
        &self,
        storage_prefix: Option<&str>,
        atom: &Atom,
    ) -> Result<(), SchemaError> {
        let index_key = build_storage_key(
            storage_prefix,
            &schema_index_codec::record_key(atom.source_schema_name(), atom.uuid()),
        );
        self.schema_index_store
            .put_item(&index_key, &Value::Bool(true))
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("Failed to store atom schema index: {e}"))
            })
    }
}
