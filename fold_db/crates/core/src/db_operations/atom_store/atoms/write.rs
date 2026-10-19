use super::*;

mod dedup;
mod rows;
use rows::AtomBatchRows;

/// An atom to store, with the molecule whose key bundle seals its body in an
/// org scope. A personal scope seals every body with the home content key.
pub(crate) type SealedAtom = (Atom, Option<String>);

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
        let sealed: Vec<SealedAtom> = atoms.into_iter().map(|atom| (atom, None)).collect();
        self.batch_store_atoms_borrowed(&sealed, storage_prefix).await
    }

    /// Store atoms, each with the molecule whose key bundle seals the body in an
    /// org scope. Callers that must retain ownership of a retryable batch use
    /// this form. Every new atom gets its birth rows in the same batch.
    pub(crate) async fn batch_store_atoms_borrowed(
        &self,
        atoms: &[SealedAtom],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.batch_store_atoms_with_birth(atoms, storage_prefix, AtomBirth::Rows)
            .await
    }

    /// Store atoms that the same persist job tips right after. The birth rows
    /// (a zero count and a GC candidate pair) are not written, because the tip
    /// phase writes the real count and cancels the candidate in the next
    /// barrier. Call [`Self::settle_atom_births`] after the tip phase so an
    /// atom that no tip took still gets its birth rows.
    pub(crate) async fn batch_store_atoms_tip_follows(
        &self,
        atoms: &[SealedAtom],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.batch_store_atoms_with_birth(atoms, storage_prefix, AtomBirth::TipFollows)
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
        atoms: &[SealedAtom],
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

    async fn batch_store_atoms_with_birth(
        &self,
        atoms: &[SealedAtom],
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
        let batch = self.native_atom_batch(atoms, storage_prefix).await?;
        let _blob_liveness_guards = self.lock_liveness_blobs(&batch.blob_refs).await;
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
            .build_atom_batch_rows(
                atoms,
                storage_prefix,
                automatic_gc_generation,
                &batch.existing,
            )
            .await?;

        // Existing immutable bodies need only their marker/reference work.
        tracing::info!(
            "Batch storing {} atom markers and {} body/locator/GC rows",
            index_items.len(),
            atom_items.len()
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

        batch.record_keep_small(&self.keep_small, keep_small_puts);
        let _ = self.persist_keep_small().await;

        tracing::info!("Batch stored atoms successfully");
        Ok(())
    }
}
