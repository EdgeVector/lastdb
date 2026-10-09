//! Conflict storage for per-key LWW merge during replay.

use super::super::SyncEngine;
use crate::atom::{MergeConflict, MutationEvent};
use crate::db_operations::{
    home_conflict_event_key, next_home_conflict_event_seq, HomeConflictEvent,
};
use crate::schema::types::field::build_storage_key;
use crate::sync::error::SyncResult;
use crate::SyncConflict;
use chrono::Utc;
use std::sync::Arc;

impl SyncEngine {
    /// Store merge conflicts as MutationEvent entries in the atoms namespace
    /// and as dedicated conflict records for efficient scanning.
    pub(super) async fn store_merge_conflicts(
        kv: &Arc<dyn crate::storage::traits::KvStore>,
        mol_uuid: &str,
        storage_prefix: Option<&str>,
        conflicts: &[MergeConflict],
    ) -> SyncResult<()> {
        let now = Utc::now();
        // Hoist the mcc:{M} read once for the whole batch; accumulate ids and
        // write a single put after the loop (avoids N get/put round-trips).
        let mcc_base = format!("mcc:{mol_uuid}");
        let mcc_key = build_storage_key(storage_prefix, &mcc_base);
        let mut mcc_ids: Vec<String> = match kv.get(mcc_key.as_bytes()).await? {
            Some(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            None => Vec::new(),
        };
        let mut mcc_dirty = false;
        let mut writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(conflicts.len() * 4 + 1);
        let mut event_keys = Vec::with_capacity(conflicts.len());
        for (i, conflict) in conflicts.iter().enumerate() {
            let ts_nanos = now.timestamp_nanos_opt().unwrap_or(0) + i as i64;

            // Read the typed FieldKey directly from the conflict. The prior
            // path reconstructed it from `conflict.key` by `split_once(':')`,
            // which is lossy: it mangled hash and range for HashRange
            // conflicts whose values legitimately contain `:` (URLs, ISO
            // timestamps), and routed every Range conflict to
            // `FieldKey::Hash` because the bare string can't disambiguate
            // hash- from range-keyed molecules. The MutationEvent shape
            // must reflect the actual key, otherwise the history row no
            // longer matches the live molecule entry and
            // `FieldVariant::rewind_to` silently skips it. See
            // `molecule_hash_range.rs::merge_conflict_field_key_preserves_hash_range_with_colons`.
            let field_key = conflict.field_key.clone();

            // Store as mutation event in history
            let event = MutationEvent {
                molecule_uuid: mol_uuid.to_string(),
                timestamp: now,
                field_key,
                old_atom_uuid: Some(conflict.loser_atom.clone()),
                new_atom_uuid: conflict.winner_atom.clone(),
                kind: crate::atom::MutationEventKind::Transition,
                suppressed_by_delete: None,
                source_order: None,
                version: 0,
                is_conflict: true,
                conflict_loser_atom: Some(conflict.loser_atom.clone()),
                writer_pubkey: String::new(),
                signature: String::new(),
                // Conflict-originated events are synthesized during merge
                // resolution and have no originating user mutation in scope;
                // leave provenance `None`.
                provenance: None,
            };
            let event_base_key =
                crate::atom::molecule_key_codec::history_event_key(mol_uuid, ts_nanos);
            let event_key = build_storage_key(storage_prefix, &event_base_key);
            let event_bytes = serde_json::to_vec(&event)?;
            writes.push((event_key.as_bytes().to_vec(), event_bytes));
            for (edge_key, edge) in
                crate::db_operations::atom_store::atom_ref_edges::mutation_history_edge_items(
                    &event_key,
                    &event,
                    storage_prefix,
                )
                .map_err(|error| crate::sync::SyncError::Serialization(error.to_string()))?
            {
                writes.push((edge_key.into_bytes(), serde_json::to_vec(&edge)?));
            }
            event_keys.push(event_key);

            // Also store in dedicated conflict index for efficient scanning
            let conflict_record = SyncConflict {
                id: format!("{mol_uuid}:{ts_nanos:020}"),
                molecule_uuid: mol_uuid.to_string(),
                conflict_key: conflict.key.clone(),
                winner_atom: conflict.winner_atom.clone(),
                loser_atom: conflict.loser_atom.clone(),
                winner_written_at: conflict.winner_written_at,
                loser_written_at: conflict.loser_written_at,
                detected_at: now,
                resolved: false,
            };
            let conflict_base_key =
                crate::kind_partition::anchored("conflict", &format!("{mol_uuid}:{ts_nanos:020}"));
            let conflict_key = build_storage_key(storage_prefix, &conflict_base_key);
            let conflict_bytes = serde_json::to_vec(&conflict_record)?;
            writes.push((conflict_key.into_bytes(), conflict_bytes));

            // Accumulate unresolved-id cache (`mcc:{M}`) for a single put after
            // the batch — same semantics as the former per-conflict get/put.
            if !mcc_ids.iter().any(|id| id == &conflict_record.id) {
                mcc_ids.push(conflict_record.id.clone());
                mcc_dirty = true;
            }
        }
        if mcc_dirty {
            let mcc_bytes = serde_json::to_vec(&mcc_ids)?;
            writes.push((mcc_key.as_bytes().to_vec(), mcc_bytes));
        }
        // Name the molecule and leave `hcu:mols`. A delete used to force the
        // next reader to walk `conflict\0`, and a missing stamp then read as
        // clean. One event per batch is enough: every conflict here shares
        // `mol_uuid`. The key contains NUL so the range stays in one partition.
        if !conflicts.is_empty() {
            let seq = next_home_conflict_event_seq();
            let event_key = build_storage_key(storage_prefix, &home_conflict_event_key(&seq));
            debug_assert!(
                event_key.contains('\0'),
                "a conflict event key without NUL walks every hash group"
            );
            let event_bytes = serde_json::to_vec(&HomeConflictEvent::named(mol_uuid))?;
            writes.push((event_key.into_bytes(), event_bytes));
        }
        if !writes.is_empty() {
            // The authoritative history row and each edge use one ordered
            // durable batch. A successful return exposes the complete set.
            // LastStore does not provide a cross-group WAL crash transaction.
            kv.batch_put(writes).await?;
            for event_key in &event_keys {
                crate::atom::legacy_history_memo::invalidate_for_event_key(event_key);
            }
        }
        Ok(())
    }
}
