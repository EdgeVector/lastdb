use super::*;

impl EncryptingNamespacedStore {
    fn plaintext_sweep_marker_key(namespace: &str) -> Vec<u8> {
        format!("{PLAINTEXT_SWEEP_MARKER_PREFIX}{namespace}").into_bytes()
    }

    /// Read the completion sentinel for a plaintext-policy unwrap sweep.
    ///
    /// Once present, the boot migration can skip the O(rows) raw scan for that
    /// namespace on later boots. The marker is cleared when a namespace rejoins
    /// the encrypted set so policy flips cannot inherit stale plaintext-era
    /// completion state.
    pub async fn plaintext_sweep_completed(&self, namespace: &str) -> StorageResult<bool> {
        let markers = self.inner.open_namespace(STRICT_MARKER_NAMESPACE).await?;
        Ok(markers
            .get(&Self::plaintext_sweep_marker_key(namespace))
            .await?
            .is_some())
    }

    /// Persist the completion sentinel for a plaintext-policy unwrap sweep.
    pub async fn mark_plaintext_sweep_completed(&self, namespace: &str) -> StorageResult<()> {
        let markers = self.inner.open_namespace(STRICT_MARKER_NAMESPACE).await?;
        markers
            .put(
                &Self::plaintext_sweep_marker_key(namespace),
                PLAINTEXT_SWEEP_MARKER_VALUE.to_vec(),
            )
            .await
    }

    /// Startup migration sweep: re-encrypt legacy plaintext values under the
    /// given key prefixes of an encrypted namespace.
    ///
    /// A namespace that becomes encrypted after data already exists (the
    /// namespaces that flipped to default-encrypt when the
    /// [`PLAINTEXT_NAMESPACES`] allowlist was inverted) leaves pre-existing
    /// rows in plaintext; `migration_mode` keeps them readable, but lazy
    /// rewrite-on-write would let rows for never-rewritten records linger in
    /// plaintext indefinitely. This sweep scans the namespace through the
    /// *inner* (non-decrypting) store so pre-encryption values are visible
    /// verbatim, and rewrites every value that lacks the `ENC:` marker back
    /// through the encrypting layer. Already-encrypted values are left
    /// untouched, so the sweep is idempotent and effectively free once
    /// migration has completed.
    ///
    /// Returns the number of values re-encrypted. No-op (`Ok(0)`) when the
    /// namespace is not in the encrypted set.
    pub async fn encrypt_legacy_plaintext(
        &self,
        namespace: &str,
        prefixes: &[&str],
    ) -> StorageResult<usize> {
        if !self.should_encrypt(namespace) {
            return Ok(0);
        }
        let inner = self.inner.open_namespace(namespace).await?;
        let enc = EncryptingKvStore::new(namespace, Arc::clone(&inner), self.crypto.clone());
        let mut migrated = 0usize;
        for prefix in prefixes {
            for (key, value) in inner.scan_prefix(prefix.as_bytes()).await? {
                if EncryptingKvStore::is_encrypted_value(&value) {
                    continue;
                }
                enc.put(&key, value).await?;
                migrated += 1;
            }
        }
        Ok(migrated)
    }

    /// Startup migration sweep over an **entire** encrypted namespace.
    ///
    /// Like [`encrypt_legacy_plaintext`](Self::encrypt_legacy_plaintext) but
    /// for namespaces without a meaningful key prefix to scope on — every key
    /// (empty-prefix scan) is re-encrypted if it lacks the `ENC:` marker. This
    /// is the sweep used for [`DEFAULT_ENCRYPT_FLIPPED_NAMESPACES`], whose
    /// values became encrypted wholesale when the [`PLAINTEXT_NAMESPACES`]
    /// allowlist was inverted. Idempotent and effectively free once migration
    /// has completed (already-encrypted values are skipped). Returns the number
    /// of values re-encrypted; `Ok(0)` when the namespace is plaintext-by-policy
    /// or empty.
    /// **Run this before a namespace joins the encrypted set, never after.**
    ///
    /// Since `decision-2026-09-14-drop-dual-read-unsealed-is-gone` an
    /// un-enveloped row in an encrypted namespace reads as absent. So a
    /// namespace that flips from plaintext to encrypted loses sight of every
    /// row it still holds in cleartext the moment it flips. This sweep is the
    /// one way to carry those rows across, and it only works while the
    /// namespace is still plaintext by policy.
    ///
    /// It is no longer driven from boot. A boot-time sweep would have sealed
    /// whatever plaintext it found, which is the bulk form of the read-path
    /// healing that decision removed: it takes a row nothing vouched for and
    /// makes it durable and trusted. Sealing cleartext is now a deliberate
    /// operator act with a named reason, not something a restart does quietly.
    pub async fn encrypt_legacy_plaintext_namespace(
        &self,
        namespace: &str,
    ) -> StorageResult<usize> {
        // An empty prefix matches every key in the namespace.
        self.encrypt_legacy_plaintext(namespace, &[""]).await
    }

    /// Startup migration sweep: **decrypt** residual `ENC:` values to plaintext
    /// for a namespace that is plaintext-by-policy (or becoming so).
    ///
    /// Inverse of [`encrypt_legacy_plaintext`](Self::encrypt_legacy_plaintext).
    /// Used when a namespace moves onto a plaintext allowlist so residual
    /// `ENC:` rows can be unwrapped once. Scans through the *inner* store,
    /// decrypts sealed values with the store crypto, and writes plaintext
    /// back. Already-plain rows are skipped (idempotent).
    ///
    /// Returns the number of values decrypted. Works for both
    /// plaintext-by-policy and encrypted namespaces (policy does not gate the
    /// sweep — residual sealed rows are always rewritten when found). Not a
    /// production boot path for any product collection as of 2026-08-05
    /// (the retired `native_index` special case was removed).
    pub async fn decrypt_sealed_to_plaintext(
        &self,
        namespace: &str,
        prefixes: &[&str],
    ) -> StorageResult<usize> {
        let inner = self.inner.open_namespace(namespace).await?;
        // Unwrap through the seam's own decoder rather than a second hand-rolled
        // base64+decrypt here. The seam seals two ways — `ENC:` and, when the
        // KV compression switch is on, `ENZ:` (deflate-then-encrypt) — and a
        // local decoder that only knew `ENC:` would write a still-deflated
        // payload out as "plaintext", corrupting exactly the rows this sweep
        // exists to rescue. The `is_encrypted_value` guard below means the
        // decoder only ever sees enveloped rows, so it never discards one.
        let decryptor = EncryptingKvStore::new(namespace, Arc::clone(&inner), self.crypto.clone());
        let mut migrated = 0usize;
        for prefix in prefixes {
            for (key, value) in inner.scan_prefix(prefix.as_bytes()).await? {
                if !EncryptingKvStore::is_encrypted_value(&value) {
                    continue;
                }
                let plaintext = decryptor
                    .open_sealed_value(value, self.crypto.as_ref())
                    .await
                    .map_err(|e| {
                        crate::storage::error::StorageError::EncryptionError(format!(
                            "decrypt_sealed_to_plaintext failed for {namespace} key {}: {e}",
                            String::from_utf8_lossy(&key)
                        ))
                    })?
                    .ok_or_else(|| {
                        // Unreachable: the `is_encrypted_value` guard above only
                        // admits enveloped rows.
                        crate::storage::error::StorageError::EncryptionError(format!(
                            "decrypt_sealed_to_plaintext saw an un-enveloped row past its guard \
                             for {namespace} key {}",
                            String::from_utf8_lossy(&key)
                        ))
                    })?;
                inner.put(&key, plaintext).await?;
                migrated += 1;
            }
        }
        Ok(migrated)
    }

    /// Startup migration for namespaces that have become plaintext-by-policy.
    ///
    /// Homes written while a namespace was value-encrypted hold `ENC:` rows.
    /// Once policy moves that namespace to plaintext, normal opens return the
    /// inner store directly, so those rows must be unwrapped exactly once. This
    /// scans the raw namespace, decrypts only `ENC:` values with the current
    /// seam provider, and writes the plaintext bytes back through the inner
    /// store. Plaintext rows are left untouched, making the migration
    /// idempotent and safe to retry.
    pub async fn decrypt_legacy_encrypted_namespace(
        &self,
        namespace: &str,
    ) -> StorageResult<usize> {
        if self.should_encrypt(namespace) {
            return Ok(0);
        }
        let inner = self.inner.open_namespace(namespace).await?;
        let decryptor = EncryptingKvStore::new(namespace, Arc::clone(&inner), self.crypto.clone());
        let mut migrated = 0usize;
        for (key, stored) in inner.scan_prefix(b"").await? {
            if !EncryptingKvStore::is_encrypted_value(&stored) {
                continue;
            }
            let crypto = self.crypto.as_ref();
            // The guard above admits only enveloped rows, so `None` cannot
            // happen here; skip rather than invent a failure if it ever does.
            let Some(plaintext) = decryptor.open_sealed_value(stored, crypto).await? else {
                continue;
            };
            inner.put(&key, plaintext).await?;
            migrated += 1;
        }
        Ok(migrated)
    }

    /// Drop a durable strict-mode marker when a namespace leaves the encrypted
    /// set (e.g. a catalog moving onto the plaintext-policy list). Missing
    /// markers are fine.
    pub async fn clear_strict_marker(&self, namespace: &str) -> StorageResult<()> {
        let markers = self.inner.open_namespace(STRICT_MARKER_NAMESPACE).await?;
        markers.delete(namespace.as_bytes()).await?;
        Ok(())
    }
}
