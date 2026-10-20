//! Replay apply dispatcher for convergent molecule handling.

use super::super::helpers::*;
use super::super::SyncEngine;
use crate::crypto::CryptoProvider;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::LogOp;
use crate::sync::{ReplayCause, ReplayOperation};

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
    /// Current per-key records use the accepted tip identity. Headers merge
    /// their counters. Older key layouts receive an error before any write.
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
        crate::storage::current_keys::validate(namespace, &key_bytes)?;
        if is_raw_delete_barrier_key(namespace, &key_bytes) {
            return Err(SyncError::replay_refused(
                namespace,
                0,
                ReplayOperation::Put,
                ReplayCause::RawDeleteBarrierPut,
            ));
        }
        if crate::sync::policy::is_node_local_gc_scratch_key(&key_bytes) {
            // Node-local probe scratch from an older build or a peer. Applying
            // it would let a foreign checkpoint or marker set steer this
            // node's delete decisions.
            tracing::debug!(
                namespace,
                "dropping node-local gc-atoms scratch put during replay"
            );
            return Ok(());
        }
        let value_bytes = LogOp::decode_bytes_for_replay(namespace, "value", value_b64)?;
        let key_str = std::str::from_utf8(&key_bytes).ok();

        if let Some(s) = key_str {
            if let Some(result) = self
                .replay_structured_put(namespace, s, &key_bytes, &value_bytes, crypto)
                .await
            {
                return result;
            }
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

    /// Route a current molecule key (`mk:`, `mh:`) to its
    /// record-specific replay. Returns `None` when `s` is an opaque key.
    async fn replay_structured_put(
        &self,
        namespace: &str,
        s: &str,
        key_bytes: &[u8],
        value_bytes: &[u8],
        crypto: &dyn CryptoProvider,
    ) -> Option<SyncResult<()>> {
        // 1. Per-key molecule record: `mk:{M}:{key}` (optionally `{org}:`- or
        //    `from:{sender}:`-prefixed).
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
                return Some(
                    self.replay_per_key_record(
                        namespace,
                        key_bytes,
                        value_bytes,
                        &mol_uuid,
                        crypto,
                    )
                    .await,
                );
            }
            // 2. Per-key molecule header: `mh:{M}`.
            if molecule_uuid_from_header_key(s).is_some() {
                return Some(
                    self.replay_header_record(namespace, key_bytes, value_bytes, crypto)
                        .await,
                );
            }
        }
        None
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
