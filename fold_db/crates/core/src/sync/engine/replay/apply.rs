//! Replay apply dispatcher for convergent molecule handling.

use super::super::helpers::*;
use super::super::SyncEngine;
use crate::crypto::CryptoProvider;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::LogOp;

pub(super) fn is_raw_delete_barrier_key(namespace: &str, key: &[u8]) -> bool {
    namespace == "main"
        && std::str::from_utf8(key)
            .ok()
            .and_then(|key| position_after_prefix(key, "rdel:v2:"))
            .is_some()
}

impl SyncEngine {
    /// Replay a single put with convergent molecule handling.
    ///
    /// Several key classes get special, order-independent merge treatment so
    /// every node converges to the same state regardless of replay order:
    ///
    /// - **Per-key molecule records** (`mk:{M}:{key}`, the source of truth as of
    ///   the per-key-storage migration) — per-key last-writer-wins on
    ///   `AtomEntry.written_at`, recording a `MergeConflict` when the incoming
    ///   record displaces a different `atom_uuid`. Identical merge *semantics*
    ///   to the old blob-level merge, now applied one record at a time.
    /// - **Per-key molecule headers** (`mh:{M}`) — molecule-level metadata
    ///   merged field-wise: `version = max`, `updated_at = max`.
    /// - **HashRange order append-log** (`mord:{M}:{seq}` entries + `moc:{M}`
    ///   count) — the molecule-global `update_order` that drives `SampleN`, now
    ///   stored append-only. Each `mord:` entry is immutable once written, so it
    ///   is merged keep-if-absent (first peer to fill a seq slot wins it); the
    ///   `moc:` count keeps the larger value. Both rules are order-independent.
    /// - **Legacy `ref:` blobs** — retired by the dead-system deletion train.
    ///   Whole-molecule `ref:{M}` / `{prefix}:ref:{M}` keys are **dropped** on
    ///   replay (no migrate-on-receive, no dual-read, no opaque put). Peers must
    ///   already speak per-key `mk:`/`mh:` layout.
    ///
    /// Everything else is written unconditionally.
    pub(super) async fn replay_put(
        &self,
        namespace: &str,
        key_b64: &str,
        value_b64: &str,
        crypto: &dyn CryptoProvider,
    ) -> SyncResult<()> {
        // Invalid base64 framing after successful envelope decrypt is poison
        // (never becomes valid on retry) — map via decode_bytes_for_replay so
        // handle_replay_apply_error can skip-and-advance instead of wedging.
        let key_bytes = LogOp::decode_bytes_for_replay(namespace, "key", key_b64)?;
        if is_raw_delete_barrier_key(namespace, &key_bytes) {
            return Err(SyncError::ReplayApplyFailed {
                target: namespace.into(),
                seq: 0,
                reason: "raw Delete barrier Put needs a safe recovery path".into(),
            });
        }
        let value_bytes = LogOp::decode_bytes_for_replay(namespace, "value", value_b64)?;
        let key_str = std::str::from_utf8(&key_bytes).ok();

        // 1. Per-key molecule record: `mk:{M}:{key}` (optionally `{org}:`- or
        //    `from:{sender}:`-prefixed).
        if let Some(s) = key_str {
            let base_key =
                crate::sync::org_sync::strip_storage_prefix(s).map_or(s, |(_, base_key)| base_key);
            // A partition-prefixed atom body contains an embedded `mk:`
            // partition (`atom:mk:{M}:…\0{uuid}`). Do not let the molecule-key
            // parser mistake that embedded prefix for a top-level per-key
            // molecule record: atom bodies are opaque puts, followed by local
            // locator reconstruction below.
            let is_atom_body = crate::atom::atom_key_codec::uuid_of(base_key).is_some();
            if !is_atom_body {
                if let Some(mol_uuid) = molecule_uuid_from_record_key(s) {
                    return self
                        .replay_per_key_record(
                            namespace,
                            &key_bytes,
                            &value_bytes,
                            &mol_uuid,
                            crypto,
                        )
                        .await;
                }
                // 2. Per-key molecule header: `mh:{M}`.
                if molecule_uuid_from_header_key(s).is_some() {
                    return self
                        .replay_header_record(namespace, &key_bytes, &value_bytes, crypto)
                        .await;
                }
                // 3a. HashRange append-log order entry: `mord:{M}:{seq}`. Each entry
                //     is append-only / immutable once written (its `(hash, range)` at
                //     a fixed seq never changes), so keep-if-absent is order-
                //     independent: whichever peer fills a seq slot first wins it and
                //     re-receipt is a no-op. (`mord:` is checked before `mo:`; the two
                //     prefixes are distinct so the order is just for clarity.)
                if is_order_log_entry_key(s) {
                    return self
                        .replay_order_log_entry(namespace, &key_bytes, &value_bytes, crypto)
                        .await;
                }
                // 3b. HashRange append-log count: `moc:{M}`. Keep the LARGER count
                //     (the more complete log). Order-independent and used only as
                //     the write path's append offset — the read path scans the
                //     `mord:` entries.
                if is_order_count_key(s) {
                    return self
                        .replay_order_count_record(namespace, &key_bytes, &value_bytes, crypto)
                        .await;
                }
            }
        }

        // 3c. Legacy whole-molecule `ref:` blobs are retired — drop, do not store
        //     or migrate. Product truth is per-key only (`mk:`/`mh:`).
        let is_ref_key =
            key_bytes.starts_with(b"ref:") || key_str.is_some_and(|s| s.contains(":ref:"));
        if is_ref_key {
            tracing::warn!(
                namespace,
                key = %key_str.unwrap_or(""),
                "dropping legacy ref: whole-molecule blob during replay (product path retired)"
            );
            return Ok(());
        }

        // 4. Everything else — unconditional.
        let kv = self.store.open_namespace(namespace).await?;
        let stored = self
            .stored_replay_value(namespace, &key_bytes, crypto, &value_bytes)
            .await?;
        kv.put(&key_bytes, stored).await?;
        self.rebuild_atom_locator_if_needed(namespace, &key_bytes, crypto)
            .await?;
        // Opaque put always applies — absorb into capture watermark (C4).
        self.absorb_put_or_warn(namespace, &key_bytes, &value_bytes, true, "opaque put")
            .await;
        Ok(())
    }

    /// Rebuild the local `aloc:{uuid}` row from a partition-prefixed atom key.
    ///
    /// Capture deliberately omits locator rows: the `atom:` key carries the
    /// exact partition prefix and UUID, so shipping the locator duplicates
    /// information. Rebuilding here preserves uuid-only reads on the receiver
    /// while flat atom keys remain unchanged (they need no locator).
    async fn rebuild_atom_locator_if_needed(
        &self,
        namespace: &str,
        key_bytes: &[u8],
        crypto: &dyn CryptoProvider,
    ) -> SyncResult<()> {
        if namespace != "main" {
            return Ok(());
        }
        let Ok(key) = std::str::from_utf8(key_bytes) else {
            return Ok(());
        };
        let (storage_prefix, base_key) = crate::sync::org_sync::strip_storage_prefix(key)
            .map_or((None, key), |(prefix, base_key)| (Some(prefix), base_key));
        let Some(partition) = crate::atom::atom_key_codec::partition_of(base_key) else {
            return Ok(());
        };
        let Some(atom_uuid) = crate::atom::atom_key_codec::uuid_of(base_key) else {
            return Ok(());
        };

        let locator_key = crate::schema::types::field::build_storage_key(
            storage_prefix,
            &crate::atom::atom_locator_codec::locator_key(atom_uuid),
        );
        let locator_plaintext =
            serde_json::to_vec(&crate::atom::atom_locator_codec::encode_value(&partition))
                .map_err(|error| crate::sync::SyncError::Serialization(error.to_string()))?;
        let locator_stored = self
            .stored_replay_value(
                namespace,
                locator_key.as_bytes(),
                crypto,
                &locator_plaintext,
            )
            .await?;
        let kv = self.store.open_namespace(namespace).await?;
        kv.put(locator_key.as_bytes(), locator_stored).await?;
        self.absorb_put_or_warn(
            namespace,
            locator_key.as_bytes(),
            &locator_plaintext,
            true,
            "derived atom locator",
        )
        .await;
        Ok(())
    }
}
