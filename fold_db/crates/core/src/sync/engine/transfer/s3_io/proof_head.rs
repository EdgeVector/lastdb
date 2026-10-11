//! Log-head decrypt-proof probing for [`SyncEngine::prove_prefix_decryptable_from_objects`].

use super::super::super::*;
use super::proof::oversize_reported_bytes;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::LogEntry;
use crate::sync::org_sync::SyncTarget;

impl SyncEngine {
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

        let mut head_proof_failure: Option<(u64, String)> = None;
        let mut unproven_head: Option<(u64, String)> = None;

        if let Some((head_seq, head_key)) = discover_log_head(objects) {
            match self.probe_log_head(target, head_seq, head_key).await? {
                HeadProbe::Proven => return Ok(()),
                HeadProbe::Failed(failure) => head_proof_failure = Some(failure),
                HeadProbe::Unproven(unproven) => unproven_head = Some(unproven),
            }
        }

        // No log objects (or head was an unsupported/oversize envelope): fall
        // back to the bootstrap snapshot as the proof object. This closes the
        // empty/compacted-tail hole where all history lives in `latest.enc`.
        let snapshot = self.try_decrypt_latest_snapshot(target).await?;
        self.resolve_snapshot_proof(target, snapshot, head_proof_failure, unproven_head)
            .await
    }

    /// Download and open the log head object as a decrypt proof.
    ///
    /// Fail-closed conditions (missing object, transport errors) return `Err`;
    /// inconclusive heads are reported through [`HeadProbe`].
    pub(super) async fn probe_log_head(
        &self,
        target: &SyncTarget,
        head_seq: u64,
        head_key: String,
    ) -> SyncResult<HeadProbe> {
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
            Ok(Some(bytes)) => Ok(self.classify_head_unseal(target, head_seq, &bytes).await),
            Ok(None) => {
                // Listed but missing (racing delete / eventual consistency).
                // Fail closed for THIS cycle rather than upload unproven.
                Err(SyncError::S3(format!(
                    "could not fetch cloud log head seq {head_seq} for '{}' to prove decryptability; blocking upload this cycle",
                    target.label
                )))
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
                Ok(HeadProbe::Unproven((
                    head_seq,
                    format!("log head was {byte_len} bytes (cap {max_entry}) and was not opened"),
                )))
            }
            Err(e) => Err(e),
        }
    }

    /// Unseal downloaded head bytes and classify the result as a proof.
    pub(super) async fn classify_head_unseal(
        &self,
        target: &SyncTarget,
        head_seq: u64,
        bytes: &[u8],
    ) -> HeadProbe {
        match LogEntry::unseal(bytes, &target.crypto, &target.label, head_seq).await {
            Ok(_) => {
                self.clear_backup_blocker(&target.label).await;
                HeadProbe::Proven
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
                HeadProbe::Proven
            }
            // A future envelope version at head proves nothing about the
            // key — fall through to the snapshot proof.
            Err(e) if Self::should_quarantine_unsealed_entry(&e) => {
                HeadProbe::Unproven((head_seq, e.to_string()))
            }
            Err(e) => HeadProbe::Failed((head_seq, e.to_string())),
        }
    }

    /// Combine the snapshot proof with the head probe outcome into the final
    /// decryptability verdict.
    pub(super) async fn resolve_snapshot_proof(
        &self,
        target: &SyncTarget,
        snapshot: SnapshotDecryptProof,
        head_proof_failure: Option<(u64, String)>,
        unproven_head: Option<(u64, String)>,
    ) -> SyncResult<()> {
        match snapshot {
            // Decrypted → proof passes unless the log head was definitively
            // corrupt. A corrupt head must fail closed even when another proof
            // object opens successfully.
            SnapshotDecryptProof::Decrypted => {
                if let Some((head_seq, reason)) = head_proof_failure {
                    Err(corrupt_head_error(target, head_seq, &reason))
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
                    Err(corrupt_head_error(target, head_seq, &reason))
                } else if let Some((head_seq, reason)) = unproven_head {
                    Err(self.block_on_unproven_head(target, head_seq, &reason).await)
                } else {
                    self.plant_keycheck_for_empty_prefix(target).await
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

    /// Latch and return the blocker error for a head that no build can open
    /// as a proof, when no snapshot exists to fall back to.
    pub(super) async fn block_on_unproven_head(
        &self,
        target: &SyncTarget,
        head_seq: u64,
        reason: &str,
    ) -> SyncError {
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
        SyncError::BackupBootstrapBlocked {
            target: target.label.clone(),
            reason,
        }
    }

    /// Genuinely empty prefix: plant the durable keycheck proof object and
    /// clear any stale blocker.
    pub(super) async fn plant_keycheck_for_empty_prefix(
        &self,
        target: &SyncTarget,
    ) -> SyncResult<()> {
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

/// Outcome of probing the cloud log head as a decrypt-proof object.
pub(super) enum HeadProbe {
    /// The current key opens the head: decryptability proven.
    Proven,
    /// Definitive decrypt failure `(head_seq, reason)`.
    Failed((u64, String)),
    /// Head could not be opened for a non-key reason `(head_seq, reason)`
    /// (oversize or unsupported envelope).
    Unproven((u64, String)),
}

/// Pick the highest-seq mutation-log object as `(seq, relative key)`.
///
/// Head discovery reads S3 KEY NAMES only (no decryption), so it works
/// even when the local key is wrong.
pub(super) fn discover_log_head(
    objects: &[crate::sync::auth::S3ObjectInfo],
) -> Option<(u64, String)> {
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
    head.max_by_key(|(seq, _)| *seq)
}

/// Error for a log head that definitively failed to decrypt.
pub(super) fn corrupt_head_error(target: &SyncTarget, head_seq: u64, reason: &str) -> SyncError {
    SyncError::CorruptProofObject {
        target: target.label.clone(),
        object: format!("log/{head_seq}.enc"),
        reason: format!("cloud log head did not decrypt: {reason}"),
    }
}
