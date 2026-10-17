//! Per-operation replay of one sync log entry.

use super::*;
use crate::sync::log::{LogicalChange, MutationEnvelope};

impl SyncEngine {
    pub(super) async fn replay_put_op(
        &self,
        namespace: &str,
        key: &str,
        value: &str,
        entry: &LogEntry,
        target: Option<&SyncTarget>,
        crypto: &dyn CryptoProvider,
    ) -> SyncResult<()> {
        // Key-framing / rewrite failures must go through the same poison
        // classifier as value apply: invalid base64 is skip-and-advance.
        let final_key =
            match Self::rewrite_key_if_needed(namespace, key, target, KeyRewriteOp::Write) {
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
        Ok(())
    }

    pub(super) async fn replay_delete_op(
        &self,
        namespace: &str,
        key: &str,
        entry: &LogEntry,
        target: Option<&SyncTarget>,
        crypto: &dyn CryptoProvider,
    ) -> SyncResult<()> {
        let final_key =
            match Self::rewrite_key_if_needed(namespace, key, target, KeyRewriteOp::Delete) {
                Ok(k) => k,
                Err(e) => {
                    Self::handle_replay_apply_error(e, namespace, None)?;
                    return Ok(());
                }
            };
        if self.skip_org_scoped_replay_key(namespace, &final_key, target, entry.seq) {
            return Ok(());
        }
        if crate::sync::policy::is_node_local_gc_scratch_key(&final_key) {
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
                        Self::append_hash_range_index_delete(&mut mutations, &final_key, mol_uuid);
                    }
                    kv.batch_mutate(mutations).await?;
                } else {
                    kv.delete(&final_key).await?;
                }
                self.absorb_delete_or_warn(namespace, &final_key, true, "delete")
                    .await;
            }
        }
        Ok(())
    }

    pub(super) async fn replay_batch_put_op(
        &self,
        namespace: &str,
        items: &[(String, String)],
        entry: &LogEntry,
        target: Option<&SyncTarget>,
        crypto: &dyn CryptoProvider,
    ) -> SyncResult<()> {
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
                    && super::super::apply::is_raw_delete_barrier_key(namespace, &final_key)
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
            let final_key =
                match Self::rewrite_key_if_needed(namespace, key, target, KeyRewriteOp::Write) {
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
        Ok(())
    }

    pub(super) async fn replay_batch_delete_op(
        &self,
        namespace: &str,
        keys: &[String],
        entry: &LogEntry,
        target: Option<&SyncTarget>,
        crypto: &dyn CryptoProvider,
    ) -> SyncResult<()> {
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
                && !crate::sync::policy::is_node_local_gc_scratch_key(final_key)
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
                        Self::append_hash_range_index_delete(&mut mutations, key, &mol_uuid);
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
        Ok(())
    }

    pub(super) async fn replay_logical_commit_op(
        &self,
        changes: &[LogicalChange],
        entry: &LogEntry,
        target: Option<&SyncTarget>,
    ) -> SyncResult<()> {
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
                    super::super::apply::is_raw_delete_barrier_key(&change.namespace, &final_key)
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
        Ok(())
    }

    pub(super) async fn replay_mutation_intent_op(
        &self,
        mutations: &[MutationEnvelope],
        entry: &LogEntry,
        target: Option<&SyncTarget>,
    ) -> SyncResult<()> {
        let applier = self.mutation_intent_applier.lock().await.clone();
        let Some(applier) = applier else {
            return Err(SyncError::replay_refused(
                "mutation_intent",
                entry.seq,
                ReplayOperation::MutationIntent,
                ReplayCause::MissingMutationApplier,
            ));
        };
        applier(
            super::super::foreign_intent::receiver_mutations(mutations, target)?,
            entry.device_id.clone(),
        )
        .await
        .map_err(|error| SyncError::ReplayApplyFailed {
            target: "mutation_intent".to_string(),
            seq: entry.seq,
            reason: error.reason,
            diagnosis: error.diagnosis,
        })?;
        Ok(())
    }
}
