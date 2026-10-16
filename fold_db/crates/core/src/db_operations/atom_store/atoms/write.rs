use super::*;

mod rows;
use rows::AtomBatchRows;

/// Whether the atom batch writes the birth rows of its new atoms.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AtomBirth {
    /// Write a zero count and a GC candidate pair for each new atom.
    Rows,
    /// A tip write follows in the same job. Skip the birth rows.
    TipFollows,
}

impl AtomStore {
    /// Creates an atom in memory without storing it.
    /// Used for batch operations where atoms are collected first then stored together.
    ///
    /// Enforces [`crate::atom::max_atom_content_bytes`] on `value` so oversized
    /// field payloads fail at the create boundary (not only on store).
    /// Default 256 KiB; see `LASTDB_MAX_ATOM_CONTENT_BYTES` and
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
    /// must retain ownership of a retryable batch. Every new atom gets its
    /// birth rows in the same batch.
    pub(crate) async fn batch_store_atoms_located_borrowed(
        &self,
        atoms: &[(Atom, Option<crate::atom::AtomPartition>)],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.batch_store_atoms_located_with_birth(atoms, storage_prefix, AtomBirth::Rows)
            .await
    }

    /// Store atoms that the same persist job tips right after. The birth rows
    /// (a zero count and a GC candidate pair) are not written, because the tip
    /// phase writes the real count and cancels the candidate in the next
    /// barrier. Call [`Self::settle_atom_births`] after the tip phase so an
    /// atom that no tip took still gets its birth rows.
    pub(crate) async fn batch_store_atoms_located_tip_follows(
        &self,
        atoms: &[(Atom, Option<crate::atom::AtomPartition>)],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.batch_store_atoms_located_with_birth(atoms, storage_prefix, AtomBirth::TipFollows)
            .await
    }

    /// Write the birth rows of every atom that still has no durable count.
    ///
    /// An atom that a tip took already has its count from the tip phase and is
    /// skipped by the same probe that the birth path uses. An atom that no tip
    /// took (a lost last-writer-wins race, a same-content rewrite) gets a zero
    /// count and a GC candidate pair now, so it stays reclaimable. The loser of
    /// a recorded conflict also holds a history edge, written before this
    /// step (`record_tip_conflicts`), and the delete gate keeps such an atom.
    pub(crate) async fn settle_atom_births(
        &self,
        atoms: &[(Atom, Option<crate::atom::AtomPartition>)],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if atoms.is_empty() {
            return Ok(());
        }
        let uuids: Vec<String> = atoms
            .iter()
            .map(|(atom, _)| atom.uuid().to_string())
            .collect();
        let _automatic_gc_guards = self.lock_automatic_gc_atoms(&uuids).await;
        let _ref_count_guards = self.lock_atom_ref_counts(&uuids).await;
        let items = self
            .missing_atom_ref_count_items(&uuids, storage_prefix)
            .await?;
        if items.is_empty() {
            return Ok(());
        }
        tracing::info!(
            rows = items.len(),
            "settled atom birth rows for untipped atoms"
        );
        self.main_store
            .inner()
            .batch_put(items)
            .await
            .map_err(|e| batch_write_error("atom births", e))
    }

    async fn batch_store_atoms_located_with_birth(
        &self,
        atoms: &[(Atom, Option<crate::atom::AtomPartition>)],
        storage_prefix: Option<&str>,
        birth: AtomBirth,
    ) -> Result<(), SchemaError> {
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
        // A body write revives the atom. Under the locks the reaper takes,
        // start a new epoch for each candidate that could reap, so an old
        // audit cannot delete the body before the tip of this job lands.
        let rearm = self
            .rearm_atom_gc_candidate_mutations(&guarded_uuids, storage_prefix)
            .await?;

        let AtomBatchRows {
            mut blob_edge_items,
            atom_items,
            index_items,
            keep_small_puts,
        } = self
            .build_atom_batch_rows(atoms, storage_prefix, automatic_gc_generation)
            .await?;

        // `index_items` is one per stored atom; `atom_items` also carries the
        // locator rows, so it is not the atom count.
        tracing::info!(
            "Batch storing {} atoms (after dedup), {} locator rows",
            index_items.len(),
            atom_items.len() - index_items.len()
        );

        // Every atom carries an explicit durable count from birth. A deduped
        // body never resets an existing count.
        if birth == AtomBirth::Rows {
            blob_edge_items.extend(
                self.missing_atom_ref_count_items(&guarded_uuids, storage_prefix)
                    .await?,
            );
        }
        // Retain-first order: active blob edges and the zero-count row precede
        // immutable atom bodies in the same durable batch.
        // The hash groups of one batch have no durable order. The contract is
        // that this batch and its barrier finish before the tip batch of the
        // same job starts. The re-arm relies on that contract.
        if !rearm.is_empty() {
            let rearmed = (rearm.len() / 2) as u64;
            // A separate batch keeps the body batch and its capture path as
            // they are.
            self.main_store
                .inner()
                .batch_mutate(rearm)
                .await
                .map_err(|e| batch_write_error("atom GC re-arm", e))?;
            self.note_atom_gc_candidates_rearmed(rearmed);
        }
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
