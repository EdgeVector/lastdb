//! Admin compaction of an allowlisted collection.

use super::*;

use backup_manifest::PendingPurgedAtomRetirements;

/// Sizes read before a compaction so every report names what the plane held
/// going in.
#[derive(Clone, Copy)]
struct CompactBaseline {
    live_keys: u64,
    bytes_before: u64,
    live_bytes: Option<u64>,
    dead_bytes: Option<u64>,
    residue_unknown_bytes: Option<u64>,
}

impl CompactBaseline {
    fn report(self, collection: String, dry_run: bool) -> CollectionCompactReport {
        CollectionCompactReport {
            collection,
            dry_run,
            live_keys: self.live_keys,
            bytes_before: self.bytes_before,
            live_bytes: self.live_bytes,
            dead_bytes: self.dead_bytes,
            residue_unknown_bytes: self.residue_unknown_bytes,
            ..Default::default()
        }
    }
}

impl LastStoreNamespacedStore {
    /// Compact one allowlisted collection (schemas catalog reclaim).
    ///
    /// Default dry-run: reports live key count and on-disk bytes without
    /// rewriting. Execute rewrites live docs into fresh segments and deletes
    /// superseded segment files so OS space returns.
    pub async fn compact_collection_admin(
        &self,
        options: CollectionCompactOptions,
    ) -> StorageResult<CollectionCompactReport> {
        let collection = options.collection.trim().to_string();
        if collection.is_empty() {
            return Err(StorageError::BackendError(
                "compact: collection name is required".to_string(),
            ));
        }
        if !COMPACT_ALLOWLIST.contains(&collection.as_str()) {
            return Ok(CollectionCompactReport {
                collection: collection.clone(),
                dry_run: options.dry_run,
                skipped_reason: Some(format!(
                    "collection `{collection}` is not on the compact allowlist ({})",
                    COMPACT_ALLOWLIST.join(", ")
                )),
                ..Default::default()
            });
        }

        let namespaced = self.clone();
        LastStoreKvStore::run_blocking(move || {
            namespaced.compact_collection_blocking(
                collection,
                options.dry_run,
                options.seed_committed_history,
            )
        })
        .await
    }

    fn compact_collection_blocking(
        &self,
        collection: String,
        dry_run: bool,
        seed_committed_history: bool,
    ) -> StorageResult<CollectionCompactReport> {
        let store = &self.store;
        let _retirement_guard = (collection == "atoms").then(|| {
            self.atom_retirement_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        let never = store.options().collection_policy(&collection).never_compact;
        // Report metadata needs cardinality, not global key order. A
        // sorted page loop revisits all groups for every 100K keys and
        // stalls publication before the compaction even starts. Count
        // each bounded group index once without collecting key strings.
        let live_keys = store
            .collection_live_key_count(&collection)
            .map_err(LastStoreKvStore::map_error)?;
        let bytes_before = collection_dir_bytes(store.path(), &collection);
        // Record-byte residue: what a delete leaves behind and what this
        // rewrite returns. Read once, before anything is rewritten, so
        // the executed report says what the plane held going in.
        let usage = self.collection_disk_usage(&collection);
        let baseline = CompactBaseline {
            live_keys,
            bytes_before,
            live_bytes: usage.and_then(|u| u.live_bytes),
            dead_bytes: usage.and_then(|u| u.dead_bytes),
            residue_unknown_bytes: usage.map(|u| u.residue_unknown_bytes),
        };

        // `atoms` keeps `never_compact` for ordinary LastStore compact, but
        // this admin path is the signed-receipt admission: record retirement
        // provenance then call `compact_atoms_with_retirement_provenance`.
        // Every other never_compact collection still hard-refuses here.
        if never && collection != "atoms" {
            return Ok(CollectionCompactReport {
                never_compact: true,
                skipped_reason: Some(
                    "collection policy is never_compact; refusing compact".to_string(),
                ),
                ..baseline.report(collection, dry_run)
            });
        }

        if dry_run {
            return Ok(self.compact_dry_run_report(collection, never, usage, baseline));
        }

        let mut atom_retirement_state = (collection == "atoms")
            .then(|| self.begin_atom_retirement())
            .transpose()?;

        if collection == "atoms" {
            store
                .compact_atoms_with_retirement_provenance()
                .map_err(LastStoreKvStore::map_error)?;
        } else {
            compact_dispatch::compact_collection_in_store(store, &collection)?;
        }
        if let Some(state) = atom_retirement_state.as_mut() {
            self.finish_atom_retirement(state, seed_committed_history)?;
        }
        let bytes_after = collection_dir_bytes(store.path(), &collection);
        Ok(CollectionCompactReport {
            bytes_after: Some(bytes_after),
            // The store-level policy, reported the same way the dry run
            // reports it. It was hardcoded `false` here, so one plane
            // answered `true` on the dry run and `false` on the execute
            // that followed it — the same field contradicting itself
            // across two calls about the same collection.
            never_compact: never,
            compactable_here: true,
            executed: true,
            ..baseline.report(collection, false)
        })
    }

    /// Projected reclaim for a dry run: allocated − apparent. Apparent is the
    /// live-set size a rewrite would leave; reporting it as `bytes_after` is
    /// the number dry-run exists to show.
    fn compact_dry_run_report(
        &self,
        collection: String,
        never: bool,
        usage: Option<crate::storage::traits::CollectionDiskUsage>,
        baseline: CompactBaseline,
    ) -> CollectionCompactReport {
        let dir = self.store.path().join("data").join(&collection);
        let dir_usage = crate::mini_cutover::plane_roles::dir_size_bytes(&dir).unwrap_or(
            crate::mini_cutover::plane_roles::DirSize {
                allocated: baseline.bytes_before,
                apparent: baseline.bytes_before,
            },
        );
        // Reaching here means the refusal did not fire, so this verb WILL
        // compact the plane. When the store-level policy still reads
        // never-compact — `atoms`, and only `atoms` — say so in
        // `skipped_reason` rather than leaving a bare `true` beside a `null`.
        // That pairing is what made an operator read the report as a refusal
        // and route 3.37 GiB of reclaimable atoms work elsewhere (2026-08-18).
        let policy_note = never.then(|| {
            format!(
                "store policy for `{collection}` is never_compact, but this admin path \
                 compacts it via signed-receipt retirement; nothing was skipped"
            )
        });
        // Projected `bytes_after` subtracts the expected reclaim:
        // filesystem slack plus the dead-record share. Slack alone
        // projected zero reclaim on a plane full of deleted rows.
        let projected_after = usage.map_or(dir_usage.apparent, |u| {
            u.allocated_bytes
                .saturating_sub(u.reclaimable_estimate_bytes())
        });
        CollectionCompactReport {
            bytes_before: dir_usage.allocated,
            bytes_after: Some(projected_after),
            never_compact: never,
            compactable_here: true,
            skipped_reason: policy_note,
            ..baseline.report(collection, true)
        }
    }

    /// Record the atom chunks a compaction is about to replace, so a crash
    /// mid-compaction can still retire them.
    fn begin_atom_retirement(&self) -> StorageResult<PendingPurgedAtomRetirements> {
        let mut state = backup_manifest::load_pending_purged_atom_retirements(self)?;
        if !state.compaction_in_progress_shas.is_empty() {
            let current = backup_manifest::local_atom_chunk_shas(self)?;
            state.pending_shas.extend(
                state
                    .compaction_in_progress_shas
                    .difference(&current)
                    .cloned(),
            );
            state.compaction_in_progress_shas.clear();
            // A crash does not prove which successor contains each
            // old prefix's live records. Never promote interrupted
            // prefix claims through disk-absence recovery.
            state.compaction_in_progress_prefixes.clear();
            backup_manifest::write_pending_purged_atom_retirements(self, &state)?;
        }
        let before = backup_manifest::local_atom_chunk_refs(self)?;
        state.compaction_in_progress_prefixes =
            backup_manifest::verified_atom_prefix_lineage(self, &state, &before)?;
        state.compaction_in_progress_shas =
            before.into_iter().map(|(chunk, _)| chunk.sha256).collect();
        backup_manifest::write_pending_purged_atom_retirements(self, &state)?;
        Ok(state)
    }

    /// Move every chunk the compaction replaced into the pending-retirement
    /// set and clear the in-progress markers.
    fn finish_atom_retirement(
        &self,
        state: &mut PendingPurgedAtomRetirements,
        seed_committed_history: bool,
    ) -> StorageResult<()> {
        let after_refs = backup_manifest::local_atom_chunk_refs(self)?;
        let after: std::collections::BTreeSet<_> = after_refs
            .iter()
            .map(|(chunk, _)| chunk.sha256.clone())
            .collect();
        for (successor_sha, prefix_shas) in &state.compaction_in_progress_prefixes {
            if !after.contains(successor_sha) {
                state.pending_shas.extend(prefix_shas.iter().cloned());
            }
        }
        state.pending_shas.extend(
            state
                .compaction_in_progress_shas
                .difference(&after)
                .cloned(),
        );
        if seed_committed_history {
            state
                .pending_shas
                .extend(backup_manifest::committed_successor_history_shas(
                    self,
                    state,
                    &after_refs,
                ));
        }
        state.compaction_in_progress_shas.clear();
        state.compaction_in_progress_prefixes.clear();
        backup_manifest::write_pending_purged_atom_retirements(self, state)
    }
}
