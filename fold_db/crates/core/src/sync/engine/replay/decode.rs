//! At-rest unwrap / decode / key rewrite for replay.

use super::super::types::*;
use super::super::SyncEngine;
use crate::crypto::{is_sealed_at_rest, seal_at_rest, CryptoProvider};
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::LogOp;
use crate::sync::org_sync::SyncTarget;
use base64::Engine as _;

impl SyncEngine {
    pub(crate) async fn should_log_unseal_failure(
        &self,
        target_label: &str,
        seq: u64,
        err: &SyncError,
    ) -> bool {
        let key = UnsealFailureLogKey {
            target_label: target_label.to_string(),
            seq,
            reason: err.to_string(),
        };
        self.unseal_failure_log_cache.lock().await.insert(key)
    }

    pub(crate) fn should_quarantine_unsealed_entry(err: &SyncError) -> bool {
        // Typed forward-compat marker: a log-envelope version this build does
        // not understand is safe to skip-and-advance. A wrong-key AEAD failure
        // (`Crypto` / `CorruptEntry`) is deliberately NOT quarantined — that
        // must block replay so a drifted key cannot silently step over data it
        // simply cannot decrypt. `unseal` now returns the typed variant, so we
        // no longer string-match decrypt error text (which was brittle and
        // could misfire on unrelated messages).
        matches!(err, SyncError::UnsupportedEnvelope { .. })
    }

    /// Strip an at-rest envelope from a sync-log record value, returning the
    /// plaintext JSON bytes.
    ///
    /// **Why this exists.** Older nodes recorded values after the at-rest seam,
    /// so sync payloads could contain a sealed value instead of
    /// plaintext JSON. New nodes record above the at-rest seam so cloud payloads
    /// are portable across devices, but replay remains dual-read so mixed
    /// fleets can still consume already-uploaded legacy entries.
    ///
    /// **Dual-read.** A value without a sealed prefix is returned verbatim —
    /// legacy plaintext from a pre-at-rest-encryption peer (or a keyless test
    /// node) still round-trips. `crypto` is the prefix's content-key provider
    /// (`target.crypto` for org/share replay, else the node's personal
    /// `self.crypto`); it matches the provider the value was sealed under,
    /// because the at-rest seam and the sync target share the same E2E key.
    pub(crate) async fn unwrap_at_rest_value(
        namespace: &str,
        crypto: &dyn CryptoProvider,
        at_rest_key: Option<&[u8; 32]>,
        value_bytes: &[u8],
    ) -> SyncResult<Vec<u8>> {
        use crate::crypto::envelope::{decrypt_envelope_with_context, peek_envelope};
        use crate::crypto::keyring::Keyring;
        use crate::crypto::{AT_REST_ENC_DEFLATE_PREFIX, AT_REST_ENC_PREFIX};
        if !is_sealed_at_rest(value_bytes) {
            // Legacy / keyless plaintext — dual-read passthrough.
            return Ok(value_bytes.to_vec());
        }
        // At-rest unwrap failures are Crypto (abort, do NOT advance the download
        // cursor), never Poison. Poison is reserved for JSON-shape rejects after
        // a successful decrypt — wrong-key / key-drift must not permanently skip
        // peer values (teardown-sync-incoming-at-rest-decrypt-poison-skip).
        let (deflated, ciphertext) = if let Some((deflated, ciphertext)) =
            crate::crypto::at_rest::decode_binary_ciphertext(value_bytes).map_err(|e| {
                SyncError::Crypto(format!(
                    "at-rest envelope decode failed in '{namespace}': {e}"
                ))
            })? {
            (deflated, ciphertext.to_vec())
        } else {
            let deflated = value_bytes.starts_with(AT_REST_ENC_DEFLATE_PREFIX.as_bytes());
            let b64 = &value_bytes[AT_REST_ENC_PREFIX.len()..];
            let ciphertext = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|e| {
                    SyncError::Crypto(format!(
                        "at-rest envelope base64 decode failed in '{namespace}': {e}"
                    ))
                })?;
            (deflated, ciphertext)
        };
        let header = peek_envelope(&ciphertext).map_err(|e| {
            SyncError::Crypto(format!(
                "at-rest envelope header decode failed in '{namespace}': {e}"
            ))
        })?;
        if header.key_id.is_none() || header.key_id == Some(Keyring::LEGACY_KEY_ID) {
            if let Some(key) = at_rest_key {
                let plaintext =
                    decrypt_envelope_with_context(key, &ciphertext, &[]).map_err(|e| {
                        SyncError::Crypto(format!(
                            "at-rest envelope decrypt failed in '{namespace}': {e}"
                        ))
                    })?;
                return if deflated {
                    crate::crypto::at_rest::inflate_at_rest(&plaintext).map_err(|e| {
                        SyncError::Crypto(format!(
                            "at-rest envelope inflate failed in '{namespace}': {e}"
                        ))
                    })
                } else {
                    Ok(plaintext)
                };
            }
        }
        let plaintext = crypto.decrypt(&ciphertext).await.map_err(|e| {
            SyncError::Crypto(format!(
                "at-rest envelope decrypt failed in '{namespace}': {e}"
            ))
        })?;
        if deflated {
            crate::crypto::at_rest::inflate_at_rest(&plaintext).map_err(|e| {
                SyncError::Crypto(format!(
                    "at-rest envelope inflate failed in '{namespace}': {e}"
                ))
            })
        } else {
            Ok(plaintext)
        }
    }

    /// Deserialize an incoming replayed record value, unwrapping the at-rest
    /// envelope first (see [`Self::unwrap_at_rest_value`]). A `serde_json`
    /// failure on the **already-unsealed, already-decrypted** bytes is
    /// deterministic JSON-shape poison, so the replay loop skips it (see
    /// [`Self::handle_replay_apply_error`]) instead of aborting the cursor
    /// forever. At-rest unwrap / decrypt failures are [`SyncError::Crypto`] and
    /// still abort-and-retry (never advance past unrecovered peer data).
    pub(super) async fn decode_incoming<T: serde::de::DeserializeOwned>(
        namespace: &str,
        crypto: &dyn CryptoProvider,
        at_rest_key: Option<&[u8; 32]>,
        value_bytes: &[u8],
    ) -> SyncResult<T> {
        let plaintext =
            Self::unwrap_at_rest_value(namespace, crypto, at_rest_key, value_bytes).await?;
        serde_json::from_slice(&plaintext).map_err(|e| SyncError::PoisonEntry {
            namespace: namespace.to_string(),
            reason: format!(
                "value did not deserialize as {}: {e}",
                std::any::type_name::<T>()
            ),
        })
    }

    /// Decode the *local* record stored under a replay key. New production
    /// wiring reads through the encrypted store view, so the value is usually
    /// already plaintext JSON; legacy/test wiring may still expose an `ENC:`
    /// envelope, so this keeps the same dual-read path as incoming entries.
    ///
    /// Returns:
    /// - `Ok(None)` when the key is genuinely absent — LWW may accept incoming.
    /// - `Ok(Some(T))` when the local value decrypts and deserializes.
    /// - `Err(_)` when a local value is **present but unreadable** (wrong/rotated
    ///   at-rest key, corrupt ciphertext, or non-JSON plaintext). Callers must
    ///   **not** treat this as "absent" and silently overwrite: a drifted key
    ///   must block replay the same way outer unseal wrong-key failures do
    ///   (see [`Self::should_quarantine_unsealed_entry`]).
    ///
    /// Unwrap failures already return [`SyncError::Crypto`] (abort). Local
    /// JSON-shape failures also block (never treated as "absent") so a drifted
    /// local key cannot be silently overwritten by an incoming peer value.
    pub(super) async fn decode_local<T: serde::de::DeserializeOwned>(
        namespace: &str,
        crypto: &dyn CryptoProvider,
        at_rest_key: Option<&[u8; 32]>,
        stored: Option<Vec<u8>>,
    ) -> SyncResult<Option<T>> {
        let Some(bytes) = stored else {
            return Ok(None);
        };
        let plaintext = Self::unwrap_at_rest_value(namespace, crypto, at_rest_key, &bytes)
            .await
            .map_err(|e| match e {
                // Defensive: unwrap is Crypto today; if a future path reintroduces
                // Poison on the local dual-read, still abort rather than overwrite.
                SyncError::PoisonEntry { namespace, reason } => SyncError::Crypto(format!(
                    "local record present but undecryptable in '{namespace}': {reason}"
                )),
                SyncError::Crypto(msg) => SyncError::Crypto(format!(
                    "local record present but undecryptable in '{namespace}': {msg}"
                )),
                other => other,
            })?;
        match serde_json::from_slice(&plaintext) {
            Ok(v) => Ok(Some(v)),
            Err(e) => Err(SyncError::Crypto(format!(
                "local record present but undecodable as {}: {e}",
                std::any::type_name::<T>()
            ))),
        }
    }

    /// Decode an incoming replay value before writing it into `self.store`.
    ///
    /// Production wires `self.store` to the encrypted store view, so callers
    /// must pass plaintext and let the local at-rest seam choose the right
    /// provider for the final replay key. Legacy incoming values that still
    /// carry `ENC:` are unwrapped here.
    pub(super) async fn stored_replay_value(
        &self,
        namespace: &str,
        _key_bytes: &[u8],
        read_crypto: &dyn CryptoProvider,
        value_bytes: &[u8],
    ) -> SyncResult<Vec<u8>> {
        let kv = self.store.open_namespace(namespace).await?;
        if kv.backend_name() == "encrypting" {
            return Self::unwrap_at_rest_value(
                namespace,
                read_crypto,
                self.enc_key.as_ref(),
                value_bytes,
            )
            .await;
        }

        // Production normally gives replay the encrypting store view, which
        // already keeps LastStore's catalog collections plaintext. Recovery
        // and legacy wiring can expose raw LastStore directly, though. In
        // that shape, preserving a historical `ENC:` envelope makes
        // LastStore's catalog guard reject the replay and pins the whole cloud
        // cursor on an entry that is recoverable with the content key.
        //
        // Keep this exception at the replay boundary: unwrap only namespaces
        // whose LastStore policy is plaintext, and only for a raw LastStore
        // backend. Direct/live KvStore::put calls still reach
        // `reject_enc_on_plaintext_catalog` unchanged and fail closed.
        if kv.backend_name() == "laststore"
            && crate::storage::LASTSTORE_PLAINTEXT_NAMESPACES.contains(&namespace)
        {
            return Self::unwrap_at_rest_value(
                namespace,
                read_crypto,
                self.enc_key.as_ref(),
                value_bytes,
            )
            .await
            .map_err(|e| match e {
                // Unwrap failure is Crypto (abort, do NOT advance) everywhere
                // else, so key drift can never permanently drop a peer value
                // that a later key rotation would recover. A plaintext catalog
                // namespace is the one place that reasoning inverts: LastStore
                // refuses `ENC:` here unconditionally, so a row we cannot open
                // is un-applicable *forever*, not merely until the key returns.
                // Pinning the cursor on it costs far more than the row —
                // download failure blocks the same cycle's upload, so one
                // unopenable historical catalog row stops every future backup
                // (Tom's primary, 2026-08-16: `Backup: no cycle has completed
                // yet` while replay retried the same seq for hours). The local
                // catalog row is already the healed authority (fold #1502), so
                // skipping this one drops nothing that was recoverable.
                SyncError::Crypto(msg) => SyncError::PoisonEntry {
                    namespace: namespace.to_string(),
                    reason: format!(
                        "unopenable at-rest envelope in plaintext catalog namespace \
                         (LastStore rejects it unconditionally, so it can never \
                         apply): {msg}"
                    ),
                },
                other => other,
            });
        }

        if is_sealed_at_rest(value_bytes) {
            return Ok(value_bytes.to_vec());
        }

        match self.enc_key.as_ref() {
            // Local seal failure must abort (Crypto), not poison-skip a good
            // remote payload — advancing past a seal failure permanently drops
            // the peer value.
            Some(key) => seal_at_rest(key, value_bytes).map_err(|e| {
                SyncError::Crypto(format!(
                    "at-rest envelope seal failed in '{namespace}': {e}"
                ))
            }),
            None => Ok(value_bytes.to_vec()),
        }
    }

    /// Rewrites log entry keys based on namespace isolation rules.
    ///
    /// Two rewrites apply:
    ///
    /// 1. **Share subscriptions** — `{share_prefix}:…` keys replayed from an
    ///    inbound share become `from:{sender_hash}:…` locally, so the
    ///    receiver reads shared data through a distinct namespace.
    /// 2. **Org schemas** — `{storage_prefix}:{schema_name}` entries replayed into
    ///    the `schemas` or `schema_states` namespaces from an org target are
    ///    stripped back to the bare `{schema_name}`. Schemas are addressed
    ///    by name locally; the org prefix exists only to drive sync routing
    ///    on the writer side. Without this rewrite, peers would store the
    ///    schema under a name like `{storage_prefix}:sync_notes` and name-based
    ///    lookups (`/api/schemas`, `get_schema`) would miss it — orphaning
    ///    every org-prefixed molecule (alpha BLOCKER af4ba).
    ///
    ///    The org-schema strip applies to **writes only** (`KeyRewriteOp::Write`).
    ///    A `Delete`/`BatchDelete` of the writer's org-routing companion
    ///    (`{storage_prefix}:{schema_name}`) must NOT be stripped: stripping it to the
    ///    bare name makes the replay delete the receiver's *canonical* schema
    ///    entry — wiping its `storage_prefix` tag and breaking descriptive-name
    ///    resolution, so org data goes invisible until a manual `set-org-hash`.
    ///    This bites when the receiver also tagged the same schema (both peers
    ///    hold a companion key). Left unstripped, the delete targets the
    ///    org-prefixed companion only (a harmless routing-duplicate cleanup if
    ///    present, a no-op otherwise) and the canonical schema survives.
    pub(crate) fn rewrite_key_if_needed(
        namespace: &str,
        key_b64: &str,
        target: Option<&SyncTarget>,
        op: KeyRewriteOp,
    ) -> SyncResult<Vec<u8>> {
        // Deterministic framing poison after decrypt → PoisonEntry (skip), not
        // Serialization (cursor pin). See LogOp::decode_bytes_for_replay.
        let key_bytes = LogOp::decode_bytes_for_replay(namespace, "key", key_b64)?;

        if let Some(t) = target {
            if t.prefix.starts_with("share:") {
                let mut parts = t.prefix.split(':');
                parts.next(); // skip "share"
                if let Some(sender_hash) = parts.next() {
                    let prefix_str = format!("{}:", t.prefix);
                    let prefix_bytes = prefix_str.as_bytes();

                    if key_bytes.starts_with(prefix_bytes) {
                        let new_prefix_str = format!("from:{sender_hash}:");
                        let new_prefix_bytes = new_prefix_str.as_bytes();

                        let mut final_key = Vec::with_capacity(
                            new_prefix_bytes.len() + key_bytes.len() - prefix_bytes.len(),
                        );
                        final_key.extend_from_slice(new_prefix_bytes);
                        final_key.extend_from_slice(&key_bytes[prefix_bytes.len()..]);

                        return Ok(final_key);
                    }
                }
            } else if op == KeyRewriteOp::Write
                && !t.prefix.is_empty()
                && (namespace == "schemas" || namespace == "schema_states")
            {
                let prefix_str = format!("{}:", t.prefix);
                let prefix_bytes = prefix_str.as_bytes();
                if key_bytes.starts_with(prefix_bytes) {
                    return Ok(key_bytes[prefix_bytes.len()..].to_vec());
                }
            }
        }

        Ok(key_bytes)
    }
}
