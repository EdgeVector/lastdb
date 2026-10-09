//! Admin compaction of an allowlisted collection.

use super::*;

impl LastStoreNamespacedStore {
    /// Compact one allowlisted collection (schemas catalog reclaim).
    ///
    /// Default dry-run: reports live key count and on-disk bytes without
    /// rewriting. Execute rewrites live docs into fresh segments and deletes
    /// superseded segment files so OS space returns.
    // lint:fn-size-ok moved verbatim from its original module; no logic change
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
                live_keys: 0,
                bytes_before: 0,
                bytes_after: None,
                never_compact: false,
                compactable_here: false,
                executed: false,
                skipped_reason: Some(format!(
                    "collection `{collection}` is not on the compact allowlist ({})",
                    COMPACT_ALLOWLIST.join(", ")
                )),
                live_bytes: None,
                dead_bytes: None,
                residue_unknown_bytes: None,
            });
        }

        let store = Arc::clone(&self.store);
        let namespaced = self.clone();
        let dry_run = options.dry_run;
        let seed_committed_history = options.seed_committed_history;
        LastStoreKvStore::run_blocking(move || {
            let _retirement_guard = (collection == "atoms").then(|| {
                namespaced
                    .atom_retirement_lock
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
            let usage = namespaced.collection_disk_usage(&collection);
            let live_bytes = usage.and_then(|u| u.live_bytes);
            let dead_bytes = usage.and_then(|u| u.dead_bytes);
            let residue_unknown_bytes = usage.map(|u| u.residue_unknown_bytes);

            // `atoms` keeps `never_compact` for ordinary LastStore compact, but
            // this admin path is the signed-receipt admission: record retirement
            // provenance then call `compact_atoms_with_retirement_provenance`.
            // Every other never_compact collection still hard-refuses here.
            if never && collection != "atoms" {
                return Ok(CollectionCompactReport {
                    collection,
                    dry_run,
                    live_keys,
                    bytes_before,
                    bytes_after: None,
                    never_compact: true,
                    compactable_here: false,
                    executed: false,
                    skipped_reason: Some(
                        "collection policy is never_compact; refusing compact".to_string(),
                    ),
                    live_bytes,
                    dead_bytes,
                    residue_unknown_bytes,
                });
            }

            if dry_run {
                // Projected reclaim is allocated − apparent. Apparent is the
                // live-set size a rewrite would leave; reporting it as
                // `bytes_after` is the number dry-run exists to show.
                let dir = store.path().join("data").join(&collection);
                let dir_usage = crate::mini_cutover::plane_roles::dir_size_bytes(&dir).unwrap_or(
                    crate::mini_cutover::plane_roles::DirSize {
                        allocated: bytes_before,
                        apparent: bytes_before,
                    },
                );
                // Reaching here means the refusal above did not fire, so this
                // verb WILL compact the plane. When the store-level policy
                // still reads never-compact — `atoms`, and only `atoms` — say
                // so in `skipped_reason` rather than leaving a bare `true`
                // beside a `null`. That pairing is what made an operator read
                // the report as a refusal and route 3.37 GiB of reclaimable
                // atoms work elsewhere (2026-08-18).
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
                return Ok(CollectionCompactReport {
                    collection,
                    dry_run: true,
                    live_keys,
                    bytes_before: dir_usage.allocated,
                    bytes_after: Some(projected_after),
                    never_compact: never,
                    compactable_here: true,
                    executed: false,
                    skipped_reason: policy_note,
                    live_bytes,
                    dead_bytes,
                    residue_unknown_bytes,
                });
            }

            let mut atom_retirement_state = if collection == "atoms" {
                let mut state = backup_manifest::load_pending_purged_atom_retirements(&namespaced)?;
                if !state.compaction_in_progress_shas.is_empty() {
                    let current = backup_manifest::local_atom_chunk_shas(&namespaced)?;
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
                    backup_manifest::write_pending_purged_atom_retirements(&namespaced, &state)?;
                }
                let before = backup_manifest::local_atom_chunk_refs(&namespaced)?;
                state.compaction_in_progress_prefixes =
                    backup_manifest::verified_atom_prefix_lineage(&namespaced, &state, &before)?;
                state.compaction_in_progress_shas =
                    before.into_iter().map(|(chunk, _)| chunk.sha256).collect();
                backup_manifest::write_pending_purged_atom_retirements(&namespaced, &state)?;
                Some(state)
            } else {
                None
            };

            if collection == "atoms" {
                store
                    .compact_atoms_with_retirement_provenance()
                    .map_err(LastStoreKvStore::map_error)?;
            } else {
                compact_dispatch::compact_collection_in_store(&store, &collection)?;
            }
            if let Some(state) = atom_retirement_state.as_mut() {
                let after_refs = backup_manifest::local_atom_chunk_refs(&namespaced)?;
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
                            &namespaced,
                            state,
                            &after_refs,
                        ));
                }
                state.compaction_in_progress_shas.clear();
                state.compaction_in_progress_prefixes.clear();
                backup_manifest::write_pending_purged_atom_retirements(&namespaced, state)?;
            }
            let bytes_after = collection_dir_bytes(store.path(), &collection);
            Ok(CollectionCompactReport {
                collection,
                dry_run: false,
                live_keys,
                bytes_before,
                bytes_after: Some(bytes_after),
                // The store-level policy, reported the same way the dry run
                // reports it. It was hardcoded `false` here, so one plane
                // answered `true` on the dry run and `false` on the execute
                // that followed it — the same field contradicting itself
                // across two calls about the same collection.
                never_compact: never,
                compactable_here: true,
                executed: true,
                skipped_reason: None,
                live_bytes,
                dead_bytes,
                residue_unknown_bytes,
            })
        })
        .await
    }
}
