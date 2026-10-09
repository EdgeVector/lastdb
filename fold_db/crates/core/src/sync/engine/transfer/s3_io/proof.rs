//! Prefix decryptability proofs.

use super::super::super::*;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::LogEntry;
use crate::sync::org_sync::SyncTarget;

impl SyncEngine {
    /// Latch a deterministic pre-upload proof failure so the cycle stops
    /// re-staging a doomed upload, and `status()` can name it.
    ///
    /// Idempotent: re-latching the same target keeps the original
    /// `blocked_since` so operators see how long backup has been unable to
    /// start, not how long ago the last cycle looked.
    pub(crate) async fn latch_backup_blocker(
        &self,
        target_label: &str,
        head_seq: Option<u64>,
        reason: &str,
        action: &str,
    ) {
        let now = crate::clock::unix_secs();
        let mut slot = self.backup_blocker.lock().await;
        let blocked_since = slot
            .as_ref()
            .filter(|existing| existing.target == target_label)
            .map_or(now, |existing| existing.blocked_since);
        let blocker = SyncBackupBlocker {
            code: "backup_bootstrap_blocked".to_string(),
            target: target_label.to_string(),
            head_seq,
            reason: redact_sync_error_text(reason),
            action: action.to_string(),
            blocked_since,
        };
        if slot.as_ref() != Some(&blocker) {
            tracing::error!(
                target: "fold_db::sync",
                sync_target = %target_label,
                head_seq = ?head_seq,
                reason = %blocker.reason,
                "backup bootstrap blocked: no cycle can upload until this is resolved; \
                 suppressing further upload staging for this target"
            );
        }
        *slot = Some(blocker);
    }

    /// Clear a latched backup block once the prefix proves (or is empty).
    ///
    /// Scoped to `target_label` so proving one target does not silently clear a
    /// block recorded against a different one.
    pub(crate) async fn clear_backup_blocker(&self, target_label: &str) {
        let mut slot = self.backup_blocker.lock().await;
        if slot
            .as_ref()
            .is_some_and(|existing| existing.target == target_label)
        {
            tracing::info!(
                target: "fold_db::sync",
                sync_target = %target_label,
                "backup bootstrap block cleared: prefix proved decryptable (or is empty)"
            );
            *slot = None;
        }
    }

    /// The currently latched backup block, if any.
    pub async fn backup_blocker(&self) -> Option<SyncBackupBlocker> {
        self.backup_blocker.lock().await.clone()
    }

    /// Positively prove the current sync key for `target` can still decrypt the
    /// existing cloud prefix before we upload anything new to it.
    ///
    /// PR #304 proved decryptability only by *replaying the tail after the
    /// download cursor*. That leaves the original incident vector open: a node
    /// whose local account/sync key drifted but whose cursor is already at head
    /// (empty tail) or whose tail was compacted away replays nothing, proves
    /// nothing, and then appends wrong-key ciphertext the correct node can
    /// never unseal — poisoning the shared log, snapshot, or log index.
    ///
    /// This is the explicit, cursor-independent proof. Prefer **small** proof
    /// objects:
    /// 1. Bounded `keycheck.enc`, before any index or listing request.
    /// 2. Personal `log_index.enc` when the keycheck is absent.
    /// 3. `list_log_objects` and the highest log entry when the index is absent
    ///    or unavailable for a non-crypto reason.
    /// 4. `latest.enc` snapshot only as last resort, **size-capped** so a
    ///    multi-GB snapshot cannot pin process memory (re-enable thrash
    ///    2026-07-14: download=0 → prove path downloaded full snapshot).
    ///
    /// Returns:
    /// - `Ok(())` when decryption is proven, OR the prefix is genuinely empty
    ///   (nothing to poison — a safe first write).
    /// - `Err(SyncError::KeyProofFailed)` on a definitive wrong-key signal.
    /// - `Err(retryable)` if the proof object could not be fetched this cycle
    ///   (fail closed: block the upload rather than write unproven).
    pub(crate) async fn prove_prefix_decryptable(&self, target: &SyncTarget) -> SyncResult<()> {
        if self.prove_prefix_from_keycheck(target).await? {
            return Ok(());
        }

        // Legacy fallback: an encrypted personal index can prove the key, but
        // its size grows with the log. Do not read it before the bounded proof.
        if Self::supports_personal_log_index(target) {
            match self.read_personal_log_index(target).await {
                Ok(Some(_index)) => {
                    tracing::debug!(
                        target = %target.label,
                        "pre-upload decrypt proof: personal log index opened"
                    );
                    self.clear_backup_blocker(&target.label).await;
                    return Ok(());
                }
                Ok(None) => {
                    // Index missing — fall through to list/log-head/snapshot.
                }
                Err(SyncError::Crypto(reason)) => {
                    return Err(SyncError::KeyProofFailed {
                        target: target.label.clone(),
                        reason: format!(
                            "personal log index did not decrypt with the current sync key: {reason}"
                        ),
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        target = %target.label,
                        error = %e,
                        "pre-upload proof: personal log index read failed (non-crypto); falling back"
                    );
                }
            }
        }

        let objects = self.auth.list_log_objects(target).await?;
        self.prove_prefix_decryptable_from_objects(target, &objects)
            .await
    }

    /// `false` means absent, not empty or proven. Every read/validation error
    /// returns to the caller before it can enter a legacy fallback.
    async fn prove_prefix_from_keycheck(&self, target: &SyncTarget) -> SyncResult<bool> {
        // Fastest, smallest, most robust proof: `keycheck.enc` is a fixed
        // sub-kilobyte object genesis writes the moment a prefix is first
        // confirmed empty (see the write below and
        // `transfer::keycheck::write_keycheck`). Try it before touching a log
        // head (which can be arbitrarily large — including, on a large local
        // backlog, larger than `max_download_entry_bytes`) or `latest.enc`
        // (which can be multi-GB). This is what stops an oversized first
        // segment from leaving a freshly-established prefix permanently
        // `Indeterminate` — design-lastdb-cloud-genesis-initial-snapshot §3.2.
        match self.read_keycheck(target).await {
            Ok(Some(())) => {
                tracing::debug!(
                    target = %target.label,
                    "pre-upload decrypt proof: keycheck.enc opened"
                );
                self.clear_backup_blocker(&target.label).await;
                Ok(true)
            }
            Ok(None) => {
                // Not written yet — pre-existing account, or this prefix
                // predates keycheck.enc. The caller must use the legacy ladder.
                Ok(false)
            }
            Err(SyncError::Crypto(reason)) => Err(SyncError::KeyProofFailed {
                target: target.label.clone(),
                reason: format!("keycheck.enc did not decrypt with the current sync key: {reason}"),
            }),
            // Fail closed on transport, size, or payload errors. Falling
            // through could classify a prefix whose keycheck merely failed to
            // read as Empty and overwrite the only proof under a different
            // key.
            Err(e) => Err(e),
        }
    }

    /// Core of [`Self::prove_prefix_decryptable`] that reuses an already-fetched
    /// object listing (avoids a redundant `list_objects` on the download path).
    pub(crate) async fn prove_prefix_decryptable_from_objects(
        &self,
        target: &SyncTarget,
        objects: &[crate::sync::auth::S3ObjectInfo],
    ) -> SyncResult<()> {
        // Recheck after legacy discovery: a keycheck may have appeared while
        // the index/list request was in flight. Preserve this observation point
        // before the legacy ladder can classify the prefix as empty and write.
        if self.prove_prefix_from_keycheck(target).await? {
            return Ok(());
        }

        // Head discovery reads S3 KEY NAMES only (no decryption), so it works
        // even when the local key is wrong.
        let head = objects.iter().filter_map(|obj| {
            parse_mutation_log_object_key(&obj.key).map(|(_, seq)| {
                (
                    seq,
                    relative_mutation_log_key(&obj.key)
                        .unwrap_or(obj.key.as_str())
                        .to_string(),
                )
            })
        });
        let head = head.max_by_key(|(seq, _)| *seq);

        let mut head_proof_failure: Option<(u64, String)> = None;
        let mut unproven_head: Option<(u64, String)> = None;

        if let Some((head_seq, head_key)) = head {
            let urls = self
                .auth
                .presign_download_object_keys(target, &[head_seq], &[head_key])
                .await?;
            let url = urls.into_iter().next().ok_or_else(|| {
                SyncError::Auth(format!(
                    "presign_download '{}': no url for decrypt-proof head seq {head_seq}",
                    target.label
                ))
            })?;
            // Size-limit BEFORE buffering: same memory safety as steady-state
            // fetch (`download_limited`). Post-download length checks still
            // re-open the 2026-07-14 thrash class on multi-GB objects.
            let max_entry = self.config.max_download_entry_bytes;
            let max_opt = (max_entry > 0).then_some(max_entry);
            match self.s3.download_limited(&url, max_opt).await {
                Ok(Some(bytes)) => {
                    match LogEntry::unseal(&bytes, &target.crypto, &target.label, head_seq).await {
                        Ok(_) => {
                            self.clear_backup_blocker(&target.label).await;
                            return Ok(());
                        }
                        // Decrypted and hash-verified, but the plaintext is not a
                        // `LogEntry`. `unseal` only reaches its `serde_json::from_slice`
                        // after BOTH the AEAD open and the SHA-256 check have
                        // passed, so a serialization error here is positive proof
                        // that the current key opens this prefix — which is the
                        // only question this function asks.
                        //
                        // This is not hypothetical. Mutation-log segments seal a
                        // `PinLogRecord` under `log/{writer}/{seq}.enc` (and
                        // legacy flat `log/{seq}.enc`). The prover may still pick
                        // a segment as its head. Treating a verified decrypt that
                        // is not a `LogEntry` as corruption would report a key
                        // problem on a readable object.
                        Err(SyncError::Serialization(e)) => {
                            tracing::debug!(
                                target: "fold_db::sync",
                                sync_target = %target.label,
                                head_seq,
                                error = %e,
                                "pre-upload decrypt proof: head opened but is not a LogEntry \
                                 (mutation-log segment on the legacy key); decryptability proven"
                            );
                            self.clear_backup_blocker(&target.label).await;
                            return Ok(());
                        }
                        // A future envelope version at head proves nothing about the
                        // key — fall through to the snapshot proof.
                        Err(e) if Self::should_quarantine_unsealed_entry(&e) => {
                            unproven_head = Some((head_seq, e.to_string()));
                        }
                        Err(e) => {
                            head_proof_failure = Some((head_seq, e.to_string()));
                        }
                    }
                }
                Ok(None) => {
                    // Listed but missing (racing delete / eventual consistency).
                    // Fail closed for THIS cycle rather than upload unproven.
                    return Err(SyncError::S3(format!(
                        "could not fetch cloud log head seq {head_seq} for '{}' to prove decryptability; blocking upload this cycle",
                        target.label
                    )));
                }
                Err(e) if super::is_oversize_s3_error(&e) => {
                    let byte_len = oversize_reported_bytes(&e, max_entry);
                    tracing::warn!(
                        target = %target.label,
                        head_seq,
                        bytes = byte_len,
                        max_entry,
                        "pre-upload proof: log head exceeds max_download_entry_bytes; trying snapshot"
                    );
                    unproven_head = Some((
                        head_seq,
                        format!(
                            "log head was {byte_len} bytes (cap {max_entry}) and was not opened"
                        ),
                    ));
                }
                Err(e) => return Err(e),
            }
        }

        // No log objects (or head was an unsupported/oversize envelope): fall
        // back to the bootstrap snapshot as the proof object. This closes the
        // empty/compacted-tail hole where all history lives in `latest.enc`.
        match self.try_decrypt_latest_snapshot(target).await? {
            // Decrypted → proof passes unless the log head was definitively
            // corrupt. A corrupt head must fail closed even when another proof
            // object opens successfully.
            SnapshotDecryptProof::Decrypted => {
                if let Some((head_seq, reason)) = head_proof_failure {
                    Err(SyncError::CorruptProofObject {
                        target: target.label.clone(),
                        object: format!("log/{head_seq}.enc"),
                        reason: format!("cloud log head did not decrypt: {reason}"),
                    })
                } else {
                    self.clear_backup_blocker(&target.label).await;
                    Ok(())
                }
            }
            // Absent means "empty" only when there were no log objects at all.
            // If a head object existed but was oversized or forward-versioned,
            // this cycle has no positive proof and must not upload.
            SnapshotDecryptProof::Absent => {
                if let Some((head_seq, reason)) = head_proof_failure {
                    Err(SyncError::CorruptProofObject {
                        target: target.label.clone(),
                        object: format!("log/{head_seq}.enc"),
                        reason: format!("cloud log head did not decrypt: {reason}"),
                    })
                } else if let Some((head_seq, reason)) = unproven_head {
                    // Deterministic, not transient: the same head object fails
                    // identically every cycle and there is no snapshot to fall
                    // back to. Returning a retryable `S3` here is what produced
                    // the 2026-08-09 forever-loop, in which each doomed cycle
                    // still paid full staging cost (RSS 6.4 -> 10.6 GiB in ~10
                    // minutes). Latch it so the cycle stops re-staging, and
                    // surface it as a distinct, operator-actionable blocker.
                    let reason = format!(
                        "cloud log head seq {head_seq} was not usable as a proof ({reason}) and latest.enc is absent"
                    );
                    self.latch_backup_blocker(
                        &target.label,
                        Some(head_seq),
                        &reason,
                        "The prefix holds no proof object this build can open. Re-check after \
                         upgrading (a forward envelope version may be readable by a newer build); \
                         otherwise the existing objects must be adopted or retired deliberately \
                         before a fresh backup lineage can start.",
                    )
                    .await;
                    Err(SyncError::BackupBootstrapBlocked {
                        target: target.label.clone(),
                        reason,
                    })
                } else {
                    // Plant the small always-openable proof object NOW, before
                    // the caller's very next step uploads the first real
                    // object (which, on a large local backlog, is routinely a
                    // raw log segment bigger than `max_download_entry_bytes`).
                    // Without this, that first upload becomes the only
                    // candidate proof object on every later cycle and — if
                    // oversized — leaves the prefix permanently blocked, which
                    // is exactly the bug this proof object exists to close.
                    // Fail closed if the PUT fails: the prefix remains empty,
                    // so a later cycle can retry safely. Returning Ok here
                    // would let the caller publish the oversized first log
                    // segment and recreate the permanent bootstrap block.
                    self.write_keycheck(target).await?;
                    // Clear any stale latch only after genesis has planted its
                    // durable proof and is safe to publish the first payload.
                    self.clear_backup_blocker(&target.label).await;
                    Ok(())
                }
            }
            SnapshotDecryptProof::Undecryptable { reason } => {
                if let Some((head_seq, head_reason)) = head_proof_failure {
                    Err(SyncError::KeyProofFailed {
                        target: target.label.clone(),
                        reason: format!(
                            "cloud log head seq {head_seq} and snapshot 'latest.enc' did not decrypt with the current sync key: head={head_reason}; snapshot={reason}"
                        ),
                    })
                } else {
                    Err(SyncError::CorruptProofObject {
                        target: target.label.clone(),
                        object: "snapshots/latest.enc".to_string(),
                        reason: format!("cloud snapshot did not decrypt: {reason}"),
                    })
                }
            }
            SnapshotDecryptProof::Oversized { bytes, max } => {
                // Prefer a definitive head KeyProofFailed (actionable key
                // mismatch) over a generic oversize block, matching the
                // Undecryptable arm above. Oversized alone still fails closed
                // rather than load multi-GB into RAM for a proof.
                if let Some((head_seq, head_reason)) = head_proof_failure {
                    Err(SyncError::KeyProofFailed {
                        target: target.label.clone(),
                        reason: format!(
                            "cloud log head seq {head_seq} did not decrypt with the current sync key ({head_reason}); latest.enc is also {bytes} bytes (cap {max}) so snapshot proof was not loaded"
                        ),
                    })
                } else {
                    Err(SyncError::S3(format!(
                        "could not prove decryptability for '{}': latest.enc is {bytes} bytes (cap {max}); blocking upload this cycle",
                        target.label
                    )))
                }
            }
        }
    }
    /// Attempt to open the target's `latest.enc` snapshot with the current key,
    /// used purely as a decrypt-proof (the plaintext is discarded).
    ///
    /// Applies [`SyncConfig::max_download_entry_bytes`] via
    /// [`crate::sync::s3::S3Client::download_limited`] so a multi-GB snapshot
    /// cannot pin process memory during pre-upload proof (incident 2026-07-14
    /// re-enable thrash). Content-Length over cap refuses before any body
    /// bytes are read; missing Content-Length aborts mid-stream at the cap.
    pub(crate) async fn try_decrypt_latest_snapshot(
        &self,
        target: &SyncTarget,
    ) -> SyncResult<SnapshotDecryptProof> {
        let url = self
            .auth
            .presign_snapshot_download_for_target(target, "latest.enc")
            .await?;
        let max_entry = self.config.max_download_entry_bytes;
        let max_opt = (max_entry > 0).then_some(max_entry);
        let downloaded = match self.s3.download_limited(&url, max_opt).await {
            Ok(v) => v,
            Err(e) if super::is_oversize_s3_error(&e) => {
                let bytes = oversize_reported_bytes(&e, max_entry);
                tracing::warn!(
                    target: "fold_db::sync::memory",
                    target_label = %target.label,
                    bytes,
                    max_entry,
                    "pre-upload proof refused oversized latest.enc"
                );
                return Ok(SnapshotDecryptProof::Oversized {
                    bytes,
                    max: max_entry,
                });
            }
            Err(e) => return Err(e),
        };
        let Some(bytes) = downloaded else {
            return Ok(SnapshotDecryptProof::Absent);
        };
        match target.crypto.decrypt(&bytes).await {
            Ok(_) => Ok(SnapshotDecryptProof::Decrypted),
            Err(e) => Ok(SnapshotDecryptProof::Undecryptable {
                reason: e.to_string(),
            }),
        }
    }
}

/// Best-effort size reported by an oversize `download_limited` error.
///
/// Prefers the Content-Length named in the preflight message so status/errors
/// show the real object size; falls back to `max + 1` when the body was aborted
/// mid-stream without a declared length.
fn oversize_reported_bytes(err: &SyncError, max: usize) -> usize {
    if let SyncError::S3(msg) = err {
        // "object content-length {n} exceeds max_download_entry_bytes {max}"
        if let Some(rest) = msg.strip_prefix("object content-length ") {
            if let Some(n_str) = rest.split_whitespace().next() {
                if let Ok(n) = n_str.parse::<usize>() {
                    return n;
                }
            }
        }
    }
    max.saturating_add(1)
}
