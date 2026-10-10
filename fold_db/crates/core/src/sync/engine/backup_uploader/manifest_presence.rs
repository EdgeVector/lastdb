use super::*;

impl SyncEngine {
    /// Returns `Ok(())` when every chunk is present, or `Err(missing_shas)`.
    ///
    /// CAS-critical: never treat a positive-only `backup_known_present` entry
    /// as proof a chunk is still in the bucket. Stale positives (external
    /// delete, GC race, wrong seed) would otherwise publish a tip whose
    /// chunks are gone and make whole-home restore fail later. Drain may
    /// still skip re-presign via the cache; tip publish may not.
    ///
    /// The error carries the missing **digests**, not just how many. Pass 2
    /// already HEADs every chunk and therefore already knows which ones missed;
    /// returning only a count made the retirement path re-probe the whole
    /// manifest to recover identities it had just thrown away — a third full
    /// sweep, and (until this signature changed) a sequential one. See
    /// [`Self::try_retire_cas_proven_unbackable_on_held_target`].
    pub(super) async fn try_verify_manifest_chunks_present(
        &self,
        manifest: &BackupManifest,
    ) -> Result<(), ManifestPresenceProbe> {
        // Two-pass verify (both passes re-HEAD every digest):
        // 1) Real presence probe for every manifest chunk; update cache on
        //    hit and invalidate on miss (drop stale positives).
        // 2) If anything is still missing, re-seed from a cloud listing once
        //    then re-HEAD. The local drain can report a cut "complete" over
        //    its frozen *candidates* while the *manifest* still names digests
        //    that are present in the bucket but never entered this process's
        //    cache (or that arrived out-of-band). Without a reseed here, CAS
        //    fails with a fixed shortfall (observed 871 on the 2026-08-01
        //    primary) while chunk-only cycles upload 0 and the counter never
        //    advances. Reseed is add-only for false *negatives*; false
        //    *positives* are already caught by the HEAD on pass 1.
        let missing = self.probe_missing_manifest_chunk_shas(manifest).await;
        if missing.is_empty() {
            return Ok(());
        }
        tracing::info!(
            target: "fold_db::sync::backup",
            missing = missing.len(),
            "CAS verify shortfall; refreshing presence from cloud listing before retry"
        );
        self.seed_backup_presence_from_listing().await;
        let missing = self.probe_missing_manifest_chunk_shas(manifest).await;
        if missing.is_empty() {
            Ok(())
        } else {
            Err(missing)
        }
    }

    /// When CAS HEAD proves carried-forward digests absent and they have no
    /// local candidate, retire them on the held cut (receipted) so the same
    /// generation can publish instead of livelocking.
    ///
    /// `missing_shas` comes from the verify that just failed. It is not
    /// re-derived here, and that is the point: this used to call a private
    /// collector that HEADed **every** manifest chunk a third time, in a
    /// sequential loop, purely to recover identities pass 2 had already seen
    /// and discarded.
    ///
    /// Timed on the 2026-08-17 primary, generation 508, 14,831 chunks. The
    /// reseed finished 11:17:59Z and the retirement logged at 12:17:36Z —
    /// 59m37s covering verify pass 2 plus the collector. Pass 2 is the
    /// concurrent probe, measured twice on the same manifest minutes later at
    /// 6m41s and 6m37s, so the sequential collector accounts for roughly
    /// **53 minutes** against **6.7** for the identical work: ~8x, same
    /// process, same chunks. That is the lesson
    /// [`Self::probe_missing_manifest_chunk_shas`] already records in its own
    /// history, re-learned by a caller written after it. Threading the set
    /// through costs zero round trips and makes the third sweep unexpressible.
    ///
    /// Returns what the retirement did, or `None` when nothing was retired.
    pub(super) async fn try_retire_cas_proven_unbackable_on_held_target(
        &self,
        previous_manifest: Option<&BackupManifest>,
        missing_shas: &BTreeSet<String>,
    ) -> Option<UnbackableRetirement> {
        let (mut manifest, candidate_shas, generation) = {
            let target = self.backup_publish_target.lock().await;
            let target = target.as_ref()?;
            (
                target.manifest.clone(),
                candidate_sha_set(&target.candidates),
                target.generation,
            )
        };
        let ghosts = cas_proven_unbackable_atom_shas(&manifest, &candidate_shas, missing_shas);
        if ghosts.is_empty() {
            return None;
        }
        let previous_counter =
            previous_manifest.map_or_else(|| manifest.counter.saturating_sub(1), |m| m.counter);
        let now = crate::clock::unix_secs().max(1);
        let retired =
            apply_cas_proven_unbackable_retirement(&mut manifest, previous_manifest, &ghosts, now);
        if retired == 0 {
            return None;
        }
        let unbackable_manifest_chunks =
            unbackable_manifest_chunk_count(&manifest, &candidate_shas);
        tracing::info!(
            target: "fold_db::sync::backup",
            generation,
            retired,
            remaining_unbackable = unbackable_manifest_chunks,
            previous_counter,
            "retired unbackable carried-forward atom chunk refs at CAS \
             (HEAD-proven absent after reseed; held cut kept)"
        );
        let mut guard = self.backup_publish_target.lock().await;
        let target = guard.as_mut()?;
        // Only rewrite if we still hold the same generation (no concurrent abandon).
        if target.generation != generation {
            return None;
        }
        target.manifest = manifest;
        target.unbackable_manifest_chunks = unbackable_manifest_chunks;
        target.refresh_reachability_identity();
        self.backup_unbackable_manifest_chunks
            .store(unbackable_manifest_chunks as u64, Ordering::Relaxed);
        Some(UnbackableRetirement {
            retired,
            unbackable_after: unbackable_manifest_chunks,
        })
    }

    /// Record leftover names that exist nowhere as named holes on the held cut.
    ///
    /// Extends unbackable-atom retirement to every leftover digest (mutable
    /// planes included). Names still in the last finished cloud photograph
    /// do not appear in `missing_shas` and are not holed.
    pub(super) async fn try_record_named_holes_on_held_target(
        &self,
        previous_manifest: Option<&BackupManifest>,
        missing_shas: &BTreeSet<String>,
    ) -> Option<usize> {
        let (mut manifest, candidate_shas, generation) = {
            let target = self.backup_publish_target.lock().await;
            let target = target.as_ref()?;
            (
                target.manifest.clone(),
                candidate_sha_set(&target.candidates),
                target.generation,
            )
        };
        let source_missing = {
            let mut missing = self.backup_unresolvable.lock().await;
            missing
                .for_generation(Some(generation))
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        };
        let hole_shas =
            cas_proven_named_hole_shas(&manifest, &candidate_shas, missing_shas, &source_missing);
        if hole_shas.is_empty() {
            return None;
        }
        let previous_counter =
            previous_manifest.map_or_else(|| manifest.counter.saturating_sub(1), |m| m.counter);
        let now = crate::clock::unix_secs().max(1);
        let holed = apply_named_hole_exclusions(&mut manifest, previous_manifest, &hole_shas, now);
        if holed == 0 {
            return None;
        }
        let unbackable_manifest_chunks =
            unbackable_manifest_chunk_count(&manifest, &candidate_shas);
        tracing::info!(
            target: "fold_db::sync::backup",
            generation,
            named_holes = holed,
            remaining_unbackable = unbackable_manifest_chunks,
            previous_counter,
            "recorded named holes on held cut (HEAD-proven absent; no local candidate)"
        );
        let mut guard = self.backup_publish_target.lock().await;
        let target = guard.as_mut()?;
        if target.generation != generation {
            return None;
        }
        target.manifest = manifest;
        let still_referenced = manifest_referenced_chunk_shas(&target.manifest);
        target
            .candidates
            .retain(|c| still_referenced.contains(&c.chunk.sha256));
        target.unbackable_manifest_chunks = unbackable_manifest_chunk_count(
            &target.manifest,
            &candidate_sha_set(&target.candidates),
        );
        target.refresh_reachability_identity();
        self.backup_unbackable_manifest_chunks
            .store(target.unbackable_manifest_chunks as u64, Ordering::Relaxed);
        {
            let mut missing = self.backup_unresolvable.lock().await;
            let set = missing.for_generation(Some(generation));
            set.retain(|sha| !hole_shas.contains(sha));
        }
        Some(holed)
    }

    /// Digests in `manifest` that fail a live presence probe.
    ///
    /// Always probes the cloud (no cache short-circuit). The positive-only
    /// presence cache is a drain optimization only — CAS tip publish must not
    /// succeed solely because a digest was once inserted into
    /// `backup_known_present`. Hits extend the cache; misses remove a stale
    /// positive so progress/drain stop treating the digest as present.
    /// Fresh-HEAD every manifest chunk and collect the ones not in cloud.
    ///
    /// **Every chunk is still probed over the network on every call** — the
    /// presence cache is deliberately NOT consulted to skip a probe here. This
    /// is the safety gate immediately before the `backup/latest` CAS, and a
    /// cached "present" can be stale (orphan GC deletes chunks), so trusting it
    /// would let CAS publish a manifest pointing at objects that no longer
    /// exist. HEAD-after-reseed is the same truth CAS uses.
    ///
    /// What changed (2026-08-08): the probes now run **concurrently** instead of
    /// one sequential round trip per chunk. On the primary this pass sat for
    /// 45+ minutes on a 12,399-chunk manifest at ~3 probes/s — the uploader
    /// parked in the tokio IO driver, moving a few KB/s, while `phase` still
    /// read `draining` and `eta_secs` still read `55`. Identical semantics, same
    /// number of HEADs, same cache writes; only the scheduling differs.
    ///
    /// What changed (2026-08-17): this returns the missing **digests** rather
    /// than only how many, so the retirement path can reuse them instead of
    /// re-probing the whole manifest for identities this pass already saw. Two
    /// consequences worth stating, because the count is operator-facing:
    ///
    /// - The shortfall is now **per distinct digest**, not per manifest ref. A
    ///   manifest can name one sha from both `mutable_chunks` and `atom_chunks`,
    ///   and the counting form scored that twice. Per-digest is the number the
    ///   bucket holds one object for, and it is what
    ///   [`unbackable_manifest_chunk_count`] already reports — so the shortfall
    ///   and the unbackable count are finally the same unit and may be compared.
    /// - Same probes, same cache writes, same order-independence; only the
    ///   accumulator changed.
    pub(super) async fn probe_missing_manifest_chunk_shas(
        &self,
        manifest: &BackupManifest,
    ) -> ManifestPresenceProbe {
        self.ensure_backup_presence_cache_loaded().await;
        // Reuse the drain's adaptive fan-out so a verify cannot be wider than
        // the uploads that fed it (same RSS / interactive-pressure budget).
        let concurrency = self
            .effective_backup_upload_concurrency(self.active_upload_caps().await.concurrency, true)
            .await
            .max(1);

        let probes = manifest
            .mutable_chunks
            .iter()
            .chain(manifest.atom_chunks.iter())
            .map(|chunk| chunk.object_sha256().to_string())
            .map(|sha| async {
                // `None` means the probe could not determine presence this
                // cycle (transport/auth failure) — distinct from a
                // server-confirmed `Some(false)` absence. Collapsing the two
                // let a flaky HEAD punch a false hole in the keep-set.
                let outcome = match self.auth.require_backup_chunk_present(&sha).await {
                    Ok(present) => Some(present),
                    Err(err) => {
                        tracing::warn!(
                            target: "fold_db::sync::backup",
                            sha = %sha,
                            error = %err,
                            "backup chunk presence probe failed (transport/auth); \
                             leaving prior presence classification untouched"
                        );
                        None
                    }
                };
                (sha, outcome)
            });
        let mut probe_stream = stream::iter(probes).buffer_unordered(concurrency);

        let mut confirmed_missing = BTreeSet::new();
        let mut unconfirmed = BTreeSet::new();
        let mut cache_changed = false;
        while let Some((sha, outcome)) = probe_stream.next().await {
            let Some(present) = outcome else {
                // Fail closed: not a confirmed absence, do not evict or
                // insert into the known-present cache. A later probe retries.
                unconfirmed.insert(sha);
                continue;
            };
            let mut known = self.backup_known_present.lock().await;
            if present {
                if known.insert(sha) {
                    cache_changed = true;
                }
            } else {
                if known.remove(&sha) {
                    cache_changed = true;
                }
                confirmed_missing.insert(sha);
            }
        }
        if cache_changed {
            self.persist_backup_presence_cache().await;
        }
        ManifestPresenceProbe {
            confirmed_missing,
            unconfirmed,
        }
    }

    pub(super) fn backup_presence_cache_path(&self) -> Option<std::path::PathBuf> {
        self.laststore_backup_source
            .as_ref()
            .and_then(|store| store.durable_sidecar_dir())
            .map(|dir| dir.join(BACKUP_PRESENCE_CACHE_FILE))
    }

    pub(super) async fn clear_backup_presence_cache_for_fresh_root(&self) -> SyncResult<()> {
        let path = self.backup_presence_cache_path().ok_or_else(|| {
            SyncError::Storage("fresh cloud backup requires a local presence cache path".into())
        })?;
        write_backup_presence_cache(&path, &HashSet::new())
            .and_then(|()| std::fs::File::open(&path)?.sync_all())
            .and_then(|()| std::fs::File::open(path.parent().unwrap())?.sync_all())
            .map_err(|error| SyncError::Storage(format!("clear cloud presence cache: {error}")))?;
        self.backup_known_present.lock().await.clear();
        self.backup_known_present_loaded
            .store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) async fn ensure_backup_presence_cache_loaded(&self) {
        if self
            .backup_known_present_loaded
            .swap(true, Ordering::SeqCst)
        {
            return;
        }
        let Some(path) = self.backup_presence_cache_path() else {
            return;
        };
        let shas = read_backup_presence_cache(&path);
        if shas.is_empty() {
            return;
        }
        let loaded = shas.len();
        self.backup_known_present.lock().await.extend(shas);
        tracing::info!(
            target: "fold_db::sync::backup",
            loaded,
            "loaded persisted backup presence cache"
        );
    }

    pub(crate) async fn persist_backup_presence_cache(&self) {
        let Some(path) = self.backup_presence_cache_path() else {
            return;
        };
        let shas = {
            let known = self.backup_known_present.lock().await;
            known.clone()
        };
        if let Err(e) = write_backup_presence_cache(&path, &shas) {
            tracing::warn!(
                target: "fold_db::sync::backup",
                error = %e,
                "persist backup presence cache failed; future restart may re-probe chunks"
            );
        }
    }
}
// lint:file-size-ok moved verbatim from backup_uploader.rs; cohesive unit, split further in a later pass
