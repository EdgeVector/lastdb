//! Opaque metadata replay; counted records require complete mutations.

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
        let value_bytes = LogOp::decode_bytes_for_replay(namespace, "value", value_b64)?;
        if crate::storage::current_keys::counter_owned(namespace, &key_bytes)? {
            return Err(SyncError::replay_refused(
                namespace,
                0,
                ReplayOperation::Put,
                ReplayCause::CounterWriterRequired,
            ));
        }

        // 4. Everything else — unconditional.
        let kv = self.store.open_namespace(namespace).await?;
        let stored = self
            .stored_replay_value(namespace, &key_bytes, crypto, &value_bytes)
            .await?;
        kv.put(&key_bytes, stored).await?;
        // Opaque put always applies — absorb into capture watermark (C4).
        self.absorb_put_or_warn(namespace, &key_bytes, &value_bytes, true, "opaque put")
            .await;
        Ok(())
    }
}
