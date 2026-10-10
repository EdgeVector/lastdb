//! Entry-point replay_entry and org-scope skip policy.

use super::super::helpers::*;
use super::super::types::*;
use super::super::SyncEngine;
use crate::atom::delete_barrier::{delete_barrier_key, DeleteBarrier, DeleteKind};
use crate::crypto::CryptoProvider;
use crate::storage::traits::{KvMutation, KvStore};
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::{LogEntry, LogOp};
use crate::sync::org_sync::{storage_prefix_for_key, SyncTarget};
use crate::sync::{ReplayCause, ReplayOperation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// The latest physical replay delete for one molecule key. The cloud log keeps
/// every mutation; this small row keeps the ordering boundary after tip removal.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ReplayDeleteMarker {
    pub written_at: u64,
    pub device_id: String,
    pub seq: u64,
    pub displaced_atom_uuid: Option<String>,
}

impl ReplayDeleteMarker {
    fn to_barrier(&self, mk_key: &[u8]) -> SyncResult<DeleteBarrier> {
        Ok(DeleteBarrier {
            mk_key: std::str::from_utf8(mk_key)
                .map_err(|error| SyncError::Serialization(error.to_string()))?
                .to_string(),
            written_at: self.written_at,
            logical_counter: 0,
            device_id: self.device_id.clone(),
            mutation_uuid: String::new(),
            kind: DeleteKind::LegacyPhysical,
            displaced_atom_uuid: self.displaced_atom_uuid.clone(),
            cloud_sequence: Some(self.seq),
        })
    }
}

enum ReplayDeleteDecision {
    Skip,
    Apply {
        marker: Option<(Vec<u8>, Vec<u8>)>,
        mol_uuid: Option<String>,
    },
}

impl SyncEngine {
    fn legacy_physical_molecule_uuid(key: &str) -> Option<String> {
        let base_key =
            crate::sync::org_sync::strip_storage_prefix(key).map_or(key, |(_, base_key)| base_key);
        // A partition-prefixed atom body carries an embedded `mk:` segment.
        // Its Delete removes the atom body, not the molecule tip.
        if crate::atom::atom_key_codec::uuid_of(base_key).is_some() {
            return None;
        }
        molecule_uuid_from_record_key(key)
    }

    fn legacy_delete_barrier(entry: &LogEntry, key: &str) -> DeleteBarrier {
        DeleteBarrier {
            mk_key: key.to_string(),
            written_at: entry.timestamp_ms.saturating_mul(1_000_000),
            logical_counter: 0,
            device_id: entry.device_id.clone(),
            mutation_uuid: String::new(),
            kind: DeleteKind::LegacyPhysical,
            displaced_atom_uuid: None,
            cloud_sequence: Some(entry.seq),
        }
    }

    pub(super) async fn replay_entry_inner(
        &self,
        entry: &LogEntry,
        target: Option<&SyncTarget>,
    ) -> SyncResult<()> {
        // The content-key provider for this entry's prefix. Record values reach
        // the sync log as the at-rest `ENC:` envelope (Syncing sits below the
        // encrypting seam — see `unwrap_at_rest_value`), so the per-key / header
        // / order merge paths decrypt with this provider before decoding. For an
        // org/share target it is the org/share E2E provider; for personal replay
        // (`target == None`) it is the node's own content-key provider, which is
        // exactly the key the personal at-rest seam sealed under.
        let crypto: &dyn CryptoProvider = match target {
            Some(t) => t.crypto.as_ref(),
            None => self.crypto.as_ref(),
        };
        match &entry.op {
            LogOp::Put {
                namespace,
                key,
                value,
            } => {
                // Key-framing / rewrite failures must go through the same poison
                // classifier as value apply: invalid base64 is skip-and-advance.
                let final_key = match Self::rewrite_key_if_needed(
                    namespace,
                    key,
                    target,
                    KeyRewriteOp::Write,
                ) {
                    Ok(k) => k,
                    Err(e) => {
                        Self::handle_replay_apply_error(e, namespace, None)?;
                        return Ok(());
                    }
                };
                if self.skip_org_scoped_replay_key(namespace, &final_key, target, entry.seq) {
                    return Ok(());
                }
                if let Err(e) = self
                    .replay_put(namespace, &LogOp::encode_bytes(&final_key), value, crypto)
                    .await
                {
                    // A poison value (deterministically un-applicable) is
                    // skipped so the cursor can advance; any other error
                    // propagates so the caller aborts without advancing.
                    Self::handle_replay_apply_error(e, namespace, None)?;
                }
            }
            LogOp::Delete { namespace, key } => {
                let final_key =
                    match Self::rewrite_key_if_needed(namespace, key, target, KeyRewriteOp::Delete)
                    {
                        Ok(k) => k,
                        Err(e) => {
                            Self::handle_replay_apply_error(e, namespace, None)?;
                            return Ok(());
                        }
                    };
                if self.skip_org_scoped_replay_key(namespace, &final_key, target, entry.seq) {
                    return Ok(());
                }
                if std::str::from_utf8(&final_key)
                    .ok()
                    .and_then(Self::legacy_physical_molecule_uuid)
                    .is_some()
                {
                    return Err(SyncError::replay_refused(
                        namespace,
                        entry.seq,
                        ReplayOperation::Delete,
                        ReplayCause::LegacyMoleculeDelete,
                    ));
                }
                let kv = self.store.open_namespace(namespace).await?;
                match self
                    .prepare_replay_delete(&kv, namespace, &final_key, entry, crypto)
                    .await?
                {
                    ReplayDeleteDecision::Skip => {
                        self.absorb_delete_or_warn(namespace, &final_key, false, "delete")
                            .await;
                    }
                    ReplayDeleteDecision::Apply { marker, mol_uuid } => {
                        if let Some((marker_key, marker_value)) = marker {
                            let mut mutations = vec![
                                KvMutation::put(marker_key, marker_value),
                                KvMutation::delete(final_key.clone()),
                            ];
                            if let Some(mol_uuid) = mol_uuid.as_ref() {
                                Self::append_hash_range_index_delete(
                                    &mut mutations,
                                    &final_key,
                                    mol_uuid,
                                );
                            }
                            kv.batch_mutate(mutations).await?;
                        } else {
                            kv.delete(&final_key).await?;
                        }
                        self.absorb_delete_or_warn(namespace, &final_key, true, "delete")
                            .await;
                    }
                }
            }
            LogOp::BatchPut { namespace, items } => {
                // Per-item apply. A *transient* failure (store IO, non-poison
                // rewrite) aborts the whole entry so the cursor doesn't
                // advance past a partial replay (alpha BLOCKER 4439b). A *poison*
                // item — invalid base64 framing after decrypt, or a value that
                // deterministically fails to deserialize into its record type —
                // is SKIPPED instead: aborting on it wedged the download cursor
                // forever re-hitting the same seq (the ~6.5k-events/day
                // sync-replay storm). Sibling items in the batch still apply.
                let total = items.len();
                for (key, _) in items {
                    if let Ok(final_key) =
                        Self::rewrite_key_if_needed(namespace, key, target, KeyRewriteOp::Write)
                    {
                        if !Self::should_skip_org_scoped_replay_key(&final_key, target)
                            && super::apply::is_raw_delete_barrier_key(namespace, &final_key)
                        {
                            return Err(SyncError::replay_refused(
                                namespace,
                                entry.seq,
                                ReplayOperation::BatchPut,
                                ReplayCause::RawDeleteBarrierPut,
                            ));
                        }
                    }
                }
                for (idx, (key, value)) in items.iter().enumerate() {
                    let final_key = match Self::rewrite_key_if_needed(
                        namespace,
                        key,
                        target,
                        KeyRewriteOp::Write,
                    ) {
                        Ok(k) => k,
                        Err(e) => {
                            // Poison (e.g. invalid base64 key) → skip item;
                            // transient rewrite failure → abort batch.
                            Self::handle_replay_apply_error(e, namespace, Some((idx + 1, total)))?;
                            continue;
                        }
                    };
                    if self.skip_org_scoped_replay_key(namespace, &final_key, target, entry.seq) {
                        continue;
                    }
                    if let Err(e) = self
                        .replay_put(namespace, &LogOp::encode_bytes(&final_key), value, crypto)
                        .await
                    {
                        Self::handle_replay_apply_error(e, namespace, Some((idx + 1, total)))?;
                    }
                }
            }
            LogOp::BatchDelete { namespace, keys } => {
                let total = keys.len();
                let mut decoded: Vec<Vec<u8>> = Vec::with_capacity(total);
                for (idx, k) in keys.iter().enumerate() {
                    match Self::rewrite_key_if_needed(namespace, k, target, KeyRewriteOp::Delete) {
                        Ok(final_key) => decoded.push(final_key),
                        Err(e) => {
                            // Poison framing → skip this key; other errors abort.
                            Self::handle_replay_apply_error(e, namespace, Some((idx + 1, total)))?;
                        }
                    }
                }
                decoded.retain(|final_key| {
                    !self.skip_org_scoped_replay_key(namespace, final_key, target, entry.seq)
                });
                if decoded.iter().any(|key| {
                    std::str::from_utf8(key)
                        .ok()
                        .and_then(Self::legacy_physical_molecule_uuid)
                        .is_some()
                }) {
                    return Err(SyncError::replay_refused(
                        namespace,
                        entry.seq,
                        ReplayOperation::BatchDelete,
                        ReplayCause::LegacyMoleculeDelete,
                    ));
                }
                let kv = self.store.open_namespace(namespace).await?;
                let mut mutations = Vec::with_capacity(decoded.len() * 2);
                let mut accepted = Vec::with_capacity(decoded.len());
                for key in &decoded {
                    match self
                        .prepare_replay_delete(&kv, namespace, key, entry, crypto)
                        .await?
                    {
                        ReplayDeleteDecision::Skip => {
                            self.absorb_delete_or_warn(namespace, key, false, "batch delete")
                                .await;
                        }
                        ReplayDeleteDecision::Apply { marker, mol_uuid } => {
                            if let Some((marker_key, marker_value)) = marker {
                                mutations.push(KvMutation::put(marker_key, marker_value));
                            }
                            if let Some(mol_uuid) = mol_uuid {
                                Self::append_hash_range_index_delete(
                                    &mut mutations,
                                    key,
                                    &mol_uuid,
                                );
                            }
                            mutations.push(KvMutation::delete(key.clone()));
                            accepted.push(key.clone());
                        }
                    }
                }
                if !mutations.is_empty() {
                    kv.batch_mutate(mutations).await?;
                }
                for key in accepted {
                    self.absorb_delete_or_warn(namespace, &key, true, "batch delete")
                        .await;
                }
            }
            LogOp::LogicalCommit { changes } => {
                for change in changes {
                    let op = if change.value.is_some() {
                        KeyRewriteOp::Write
                    } else {
                        KeyRewriteOp::Delete
                    };
                    if let Ok(final_key) =
                        Self::rewrite_key_if_needed(&change.namespace, &change.key, target, op)
                    {
                        if Self::should_skip_org_scoped_replay_key(&final_key, target) {
                            continue;
                        }
                        let blocked = if change.value.is_some() {
                            super::apply::is_raw_delete_barrier_key(&change.namespace, &final_key)
                        } else {
                            std::str::from_utf8(&final_key)
                                .ok()
                                .and_then(Self::legacy_physical_molecule_uuid)
                                .is_some()
                        };
                        if blocked {
                            return Err(SyncError::replay_refused(
                                &change.namespace,
                                entry.seq,
                                ReplayOperation::LogicalCommit,
                                if change.value.is_some() {
                                    ReplayCause::RawDeleteBarrierPut
                                } else {
                                    ReplayCause::LegacyMoleculeDelete
                                },
                            ));
                        }
                    }
                }
                // Preserve the writer's exact physical apply semantics and
                // ordering while treating the whole top-level mutation as one
                // durable log record. Boxing is the explicit recursion
                // boundary; each child is an existing Put/Delete apply path.
                // Suppress leftover KV capture so a v1 bag cannot refill the
                // pin-log on replay.
                crate::sync::capture::with_capture_suppressed(async {
                    for change in changes {
                        let op = match &change.value {
                            Some(value) => LogOp::Put {
                                namespace: change.namespace.clone(),
                                key: change.key.clone(),
                                value: value.clone(),
                            },
                            None => LogOp::Delete {
                                namespace: change.namespace.clone(),
                                key: change.key.clone(),
                            },
                        };
                        let child = LogEntry {
                            seq: entry.seq,
                            timestamp_ms: entry.timestamp_ms,
                            device_id: entry.device_id.clone(),
                            op,
                        };
                        Box::pin(self.replay_entry_inner(&child, target)).await?;
                    }
                    SyncResult::Ok(())
                })
                .await?;
            }
            LogOp::PhysicalDigest { .. } => {
                // Local write already committed. Digest is an audit row, not
                // a body. Snapshot / serving store is SOT for leftover
                // catalog/drain keys.
            }
            LogOp::Unknown { tag } => {
                return Self::handle_replay_apply_error(
                    SyncError::PoisonEntry {
                        namespace: "unknown".to_string(),
                        reason: format!("unknown LogOp tag {tag}"),
                    },
                    "unknown",
                    None,
                );
            }
            LogOp::MutationIntent { mutations } => {
                let applier = self.mutation_intent_applier.lock().await.clone();
                let Some(applier) = applier else {
                    return Err(SyncError::replay_refused(
                        "mutation_intent",
                        entry.seq,
                        ReplayOperation::MutationIntent,
                        ReplayCause::MissingMutationApplier,
                    ));
                };
                applier(mutations.clone())
                    .await
                    .map_err(|error| SyncError::ReplayApplyFailed {
                        target: "mutation_intent".to_string(),
                        seq: entry.seq,
                        reason: error.reason,
                        diagnosis: error.diagnosis,
                    })?;
            }
        }
        Ok(())
    }

    /// Read both barrier versions and the pending local Delete. A v1 marker
    /// has no mutation UUID or logical counter; its origin time still leads.
    pub(super) async fn read_replay_delete_barrier(
        &self,
        kv: &Arc<dyn KvStore>,
        namespace: &str,
        key: &[u8],
        mol_uuid: &str,
        crypto: &dyn CryptoProvider,
    ) -> SyncResult<Option<DeleteBarrier>> {
        let v2_key = delete_barrier_key(key);
        let v2: Option<DeleteBarrier> = Self::decode_local(
            namespace,
            crypto,
            self.enc_key.as_ref(),
            kv.get(v2_key.as_bytes()).await?,
        )
        .await?;
        if v2.as_ref().is_some_and(|barrier| !barrier.matches_key(key)) {
            return Err(SyncError::replay_refused(
                namespace,
                0,
                ReplayOperation::Unknown,
                ReplayCause::DeleteBarrierIdentityMismatch,
            ));
        }
        let v1_key = Self::replay_delete_marker_key(key, mol_uuid);
        let v1: Option<ReplayDeleteMarker> = Self::decode_local(
            namespace,
            crypto,
            self.enc_key.as_ref(),
            kv.get(&v1_key).await?,
        )
        .await?;
        let pending = self
            .automatic_gc_atom_store
            .lock()
            .await
            .as_ref()
            .and_then(|atoms| {
                std::str::from_utf8(key)
                    .ok()
                    .and_then(|key| atoms.pending_delete_barrier(key))
            });
        let mut winner = v2;
        for candidate in [
            v1.map(|marker| marker.to_barrier(key)).transpose()?,
            pending,
        ]
        .into_iter()
        .flatten()
        {
            if winner
                .as_ref()
                .is_none_or(|current| candidate.is_newer_than(current))
            {
                winner = Some(candidate);
            }
        }
        Ok(winner)
    }

    /// A physical delete must beat both the present tip and the last delete.
    /// Its barrier survives tip removal and blocks a delayed older Put.
    async fn prepare_replay_delete(
        &self,
        kv: &Arc<dyn KvStore>,
        namespace: &str,
        key: &[u8],
        entry: &LogEntry,
        crypto: &dyn CryptoProvider,
    ) -> SyncResult<ReplayDeleteDecision> {
        use crate::db_operations::atom_store::PerKeyRecord;

        let Some(key_str) = std::str::from_utf8(key).ok() else {
            return Ok(ReplayDeleteDecision::Apply {
                marker: None,
                mol_uuid: None,
            });
        };
        let mol_uuid = Self::legacy_physical_molecule_uuid(key_str);
        let Some(mol_uuid) = mol_uuid else {
            return Ok(ReplayDeleteDecision::Apply {
                marker: None,
                mol_uuid: None,
            });
        };
        let mut barrier = Self::legacy_delete_barrier(entry, key_str);
        if self
            .read_replay_delete_barrier(kv, namespace, key, &mol_uuid, crypto)
            .await?
            .is_some_and(|current| !barrier.is_newer_than(&current))
        {
            return Ok(ReplayDeleteDecision::Skip);
        }
        let local: Option<PerKeyRecord> =
            Self::decode_local(namespace, crypto, self.enc_key.as_ref(), kv.get(key).await?)
                .await?;
        barrier.displaced_atom_uuid = local.as_ref().map(|record| record.entry.atom_uuid.clone());
        if local
            .as_ref()
            .is_some_and(|record| !barrier.blocks_tip(&record.entry))
        {
            return Ok(ReplayDeleteDecision::Skip);
        }
        let marker_key = delete_barrier_key(key).into_bytes();
        let marker_plaintext = serde_json::to_vec(&barrier)?;
        let marker_stored = self
            .stored_replay_value(namespace, &marker_key, crypto, &marker_plaintext)
            .await?;
        Ok(ReplayDeleteDecision::Apply {
            marker: Some((marker_key, marker_stored)),
            mol_uuid: Some(mol_uuid),
        })
    }

    pub(in crate::sync::engine) fn replay_delete_marker_key(key: &[u8], mol_uuid: &str) -> Vec<u8> {
        let key_str = std::str::from_utf8(key).expect("molecule key is UTF-8");
        let scope = storage_scope_for_key_marker(key_str, "mk:");
        let digest = Sha256::digest(key);
        let bare = format!("rdel:v1:{mol_uuid}\0{digest:x}");
        crate::schema::types::field::build_storage_key(scope, &bare).into_bytes()
    }

    fn append_hash_range_index_delete(mutations: &mut Vec<KvMutation>, key: &[u8], mol_uuid: &str) {
        if let Ok(key_str) = std::str::from_utf8(key) {
            if let Some(index_key) =
                hash_range_page_index_complete_key_for_record_key(key_str, mol_uuid)
            {
                mutations.push(KvMutation::delete(index_key.into_bytes()));
            }
        }
    }

    /// Whether `key` is an org-scoped storage key that must be dropped on
    /// personal single-provider replay (see [`storage_prefix_for_key`]).
    pub(super) fn is_org_scoped_storage_key_bytes(key: &[u8]) -> bool {
        std::str::from_utf8(key)
            .ok()
            .and_then(storage_prefix_for_key)
            .is_some()
    }

    fn should_skip_org_scoped_replay_key(key: &[u8], target: Option<&SyncTarget>) -> bool {
        target.is_none() && Self::is_org_scoped_storage_key_bytes(key)
    }

    /// Skip + loudly count an org-scoped key during personal replay. Returns
    /// `true` when the caller should not apply the op.
    pub(super) fn skip_org_scoped_replay_key(
        &self,
        namespace: &str,
        key: &[u8],
        target: Option<&SyncTarget>,
        seq: u64,
    ) -> bool {
        // An explicit org/share target supplies the right E2E provider for the
        // entry. The skip guard is only for personal single-provider replay of
        // old mixed fixtures or personal bootstrap rows.
        if !Self::should_skip_org_scoped_replay_key(key, target) {
            return false;
        }
        let n = self.org_scoped_replay_skips.fetch_add(1, Ordering::Relaxed) + 1;
        let key_preview = std::str::from_utf8(key).map_or_else(
            |_| format!("<{} binary bytes>", key.len()),
            |s| {
                if s.len() > 96 {
                    format!("{}…", &s[..96])
                } else {
                    s.to_string()
                }
            },
        );
        tracing::warn!(
            target: "fold_db::sync::replay",
            namespace = %namespace,
            seq = seq,
            skip_count = n,
            key = %key_preview,
            "SKIPPING org-scoped storage key during sync replay (consented drop after org-crypto strip; \
             org-E2E ciphertext is unreadable under the personal provider)"
        );
        true
    }

    /// Total org-scoped keys skipped during replay in this process.
    pub fn org_scoped_replay_skips(&self) -> u64 {
        self.org_scoped_replay_skips.load(Ordering::Relaxed)
    }

    /// Classify an error from applying one replayed op.
    ///
    /// A [`SyncError::PoisonEntry`] is *deterministically* un-applicable after a
    /// successful decrypt (invalid base64 key/value framing, JSON shape / type
    /// mismatch). It is SKIPPED — warn + a `poison`-tagged event — and `Ok(())`
    /// is returned so the caller can advance the cursor. At-rest unwrap /
    /// decrypt / local seal failures are [`SyncError::Crypto`] and abort
    /// WITHOUT advancing — never permanently drop unrecovered peer data under
    /// key drift.
    ///
    /// Every other error is also returned as `Err` (store IO, lock, network, …)
    /// so the caller aborts WITHOUT advancing the cursor — never silently
    /// dropping a recoverable entry (alpha BLOCKER 4439b). `batch_pos` is
    /// `Some((idx, total))` for a BatchPut item (1-based) and `None` for a
    /// single `Put` (whose seq-level abort the download loop already logs).
    pub(crate) fn handle_replay_apply_error(
        err: SyncError,
        namespace: &str,
        batch_pos: Option<(usize, usize)>,
    ) -> SyncResult<()> {
        if let SyncError::PoisonEntry { reason, .. } = &err {
            if let Some((idx, total)) = batch_pos {
                tracing::warn!(
                    target: "fold_db::sync::replay",
                    namespace = %namespace,
                    poison = true,
                    "replay BatchPut: SKIPPING poison item {idx}/{total} ns={namespace} and advancing cursor: {reason}"
                );
            } else {
                tracing::warn!(
                    target: "fold_db::sync::replay",
                    namespace = %namespace,
                    poison = true,
                    "replay Put: SKIPPING poison entry ns={namespace} and advancing cursor: {reason}"
                );
            }
            return Ok(());
        }
        // Transient / retryable. Preserve the per-item BatchPut abort detail;
        // a single Put's abort is logged with its seq by the download loop.
        if let Some((idx, total)) = batch_pos {
            tracing::error!(
                "replay BatchPut abort: item {}/{} ns={} failed to apply: {}",
                idx,
                total,
                namespace,
                err
            );
        }
        Err(err)
    }
}
