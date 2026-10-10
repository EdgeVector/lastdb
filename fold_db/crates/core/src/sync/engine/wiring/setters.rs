//! Post-construction wiring: keys, cursors, callbacks, and cut barriers.

use super::*;

impl SyncEngine {
    /// Wire the node's 32-byte E2E content key for download cursor payloads.
    /// See the `enc_key` field doc.
    pub fn set_at_rest_key(&mut self, key: [u8; 32]) {
        self.enc_key = Some(key);
    }

    /// Override the store used for sync cursor bookkeeping.
    ///
    /// Factories call this when the main sync store is an encrypted view but
    /// cursor rows should remain in their historical raw namespace, protected
    /// by the engine's own E2E cursor envelope.
    pub fn set_cursor_store(&mut self, store: Arc<dyn NamespacedStore>) {
        self.cursor_store = store;
    }

    /// Handle to the wake notification. The background sync coordinator holds
    /// a clone and races its next sleep against `wake.notified()`, so local
    /// writes can trigger an immediate sync cycle instead of waiting out the
    /// full `sync_interval_ms`.
    pub fn wake_handle(&self) -> Arc<tokio::sync::Notify> {
        self.wake.clone()
    }

    /// Publish the node's latest foreground-pressure sample for the next upload
    /// policy refresh. This is intentionally a tiny value object so core does
    /// not depend on node QoS or request-telemetry internals.
    pub fn set_foreground_pressure_sample(
        &self,
        sample: super::super::upload_policy::ForegroundPressure,
    ) {
        if let Ok(mut guard) = self.foreground_pressure.lock() {
            *guard = Some(sample);
        }
    }

    /// Install the path the keep set is mirrored to after every committed
    /// backup publish. See the `backup_manifest_cache_path` field doc for why
    /// the operator route alone was not enough.
    pub fn set_backup_manifest_cache_path(&self, path: std::path::PathBuf) {
        if let Ok(mut guard) = self.backup_manifest_cache_path.lock() {
            *guard = Some(path);
        }
    }

    /// The configured keep-set mirror path, if the embedding node installed one.
    pub fn backup_manifest_cache_path(&self) -> Option<std::path::PathBuf> {
        self.backup_manifest_cache_path
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Seal a cursor's 8-byte big-endian seq for at-rest storage. With an
    /// E2E key configured the bytes use the shared at-rest envelope. Keyless
    /// test construction stores the raw bytes. Returns
    /// `Err` only if sealing itself fails (the caller logs and skips the
    /// persist — no plaintext fallback).
    pub(crate) fn encode_cursor(&self, seq: u64) -> Result<Vec<u8>, String> {
        let raw = seq.to_be_bytes();
        match self.enc_key.as_ref() {
            Some(key) => seal_at_rest(key, &raw).map_err(|e| e.to_string()),
            None => Ok(raw.to_vec()),
        }
    }

    /// Decode a stored cursor value back to its seq. Transparently handles
    /// legacy pre-seal plaintext (dual-read via [`crate::crypto::open_at_rest`])
    /// and sealed values. Returns `None` for an unreadable/malformed value so
    /// the caller resumes that prefix from seq 0 (a full, safe re-download)
    /// rather than trusting garbage.
    pub(crate) fn decode_cursor(&self, bytes: &[u8]) -> Option<u64> {
        let plaintext = match self.enc_key.as_ref() {
            Some(key) => open_at_rest(key, bytes).ok()?,
            None => bytes.to_vec(),
        };
        let arr: [u8; 8] = plaintext.as_slice().try_into().ok()?;
        Some(u64::from_be_bytes(arr))
    }

    /// Load persisted download cursors from storage.
    /// Called on startup to resume incremental downloads.
    pub async fn load_download_cursors(&self) {
        let kv = match self.cursor_store.open_namespace("sync_cursors").await {
            Ok(kv) => kv,
            Err(e) => {
                tracing::warn!("Failed to open sync_cursors namespace: {}", e);
                return;
            }
        };
        let entries = match kv.scan_prefix(b"cursor:").await {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("Failed to scan cursor keys: {}", e);
                return;
            }
        };
        let mut cursors = self.download_cursors.lock().await;
        for (key_bytes, val_bytes) in entries {
            if let Ok(key) = std::str::from_utf8(&key_bytes) {
                let prefix = key.strip_prefix("cursor:").unwrap_or(key);
                if let Some(seq) = self.decode_cursor(&val_bytes) {
                    cursors.insert(prefix.to_string(), seq);
                }
            }
        }
        if !cursors.is_empty() {
            tracing::info!("Loaded {} download cursors from storage", cursors.len());
        }
    }

    /// Packing lock and cloud-pause slot this engine shares with plane compaction.
    ///
    /// FoldDB init starts the local cadence before `set_sync_engine`. Pass
    /// these Arcs into [`crate::fold_db_core::fold_db::FoldDbInit`] so the
    /// coordinator's local compactor uses the same mutex a later cut takes.
    pub(crate) fn packing_slots(
        &self,
    ) -> (
        crate::sync::capture::plane_compactor::BackupPublishTargetSlot,
        Arc<Mutex<Option<u64>>>,
    ) {
        (
            Arc::clone(&self.backup_publish_target),
            Arc::clone(&self.cloud_sync_disabled_at),
        )
    }

    /// Set a callback that refreshes authentication credentials on 401.
    ///
    /// When the sync engine encounters an auth error (expired token, etc.),
    /// it calls this callback to obtain fresh credentials, updates the
    /// `AuthClient`, and retries the sync cycle once.
    pub fn set_auth_refresh(&mut self, cb: AuthRefreshCallback) {
        self.auth_refresh = Some(cb);
    }

    /// Register a callback that reloads the SchemaCore cache after sync
    /// replays schema entries into Sled. The callback returns the number
    /// of newly added schemas, or an error string.
    pub async fn set_schema_reloader(&self, reloader: SchemaReloadCallback) {
        *self.schema_reloader.lock().await = Some(reloader);
    }

    /// Register the serving mutation apply path for [`LogOp::MutationIntent`].
    pub async fn set_mutation_intent_applier(
        &self,
        applier: crate::sync::engine::MutationIntentApplier,
    ) {
        *self.mutation_intent_applier.lock().await = Some(applier);
    }

    pub async fn set_mutation_intent_materializer(
        &self,
        materializer: crate::sync::engine::types::MutationIntentMaterializer,
    ) {
        *self.mutation_intent_materializer.lock().await = Some(materializer);
    }

    /// Register the serving atom store for automatic-GC pin protection.
    pub async fn set_automatic_gc_atom_store(&self, atoms: crate::db_operations::AtomStore) {
        *self.automatic_gc_atom_store.lock().await = Some(atoms);
    }

    /// Register the local persistence barrier for a named photograph cut.
    pub async fn set_photograph_cut_barrier(&self, barrier: PhotographCutBarrier) {
        *self.photograph_cut_barrier.lock().await = Some(barrier);
    }

    pub(crate) async fn set_photograph_mutation_router(
        &self,
        router: Arc<crate::sync::capture::MutationLogCaptureRouter>,
    ) {
        *self.photograph_mutation_router.lock().await = Some(router);
    }

    /// Register the serving-store refresh that runs after photograph restore.
    pub async fn set_photograph_restore_barrier(&self, barrier: PhotographCutBarrier) {
        *self.photograph_restore_barrier.lock().await = Some(barrier);
    }

    pub(crate) async fn invoke_photograph_cut_barrier(&self) -> Result<(), String> {
        match self.photograph_cut_barrier.lock().await.clone() {
            Some(barrier) => barrier().await,
            None => Ok(()),
        }
    }

    pub(crate) async fn invoke_required_primary_resume_cut_barrier(&self) -> Result<(), String> {
        let barrier = self
            .photograph_cut_barrier
            .lock()
            .await
            .clone()
            .ok_or_else(|| "primary resume requires a local photograph cut barrier".to_string())?;
        barrier().await
    }

    pub(crate) async fn invoke_photograph_restore_barrier(&self) -> Result<(), String> {
        match self.photograph_restore_barrier.lock().await.clone() {
            Some(barrier) => barrier().await,
            None => Ok(()),
        }
    }

    pub(crate) async fn materialize_mutation_intent(
        &self,
        envelopes: Vec<crate::sync::log::MutationEnvelope>,
    ) -> Result<Vec<crate::sync::log::MutationEnvelope>, String> {
        let materializer = self.mutation_intent_materializer.lock().await.clone();
        match materializer {
            Some(materializer) => materializer(envelopes).await,
            None if envelopes
                .iter()
                .all(|envelope| !envelope.fields_and_values.is_empty()) =>
            {
                Ok(envelopes)
            }
            None => Err("no MutationIntent atom materializer registered".to_string()),
        }
    }

    /// Invoke a reload callback, logging the result. `kind` is a human label
    /// (e.g. "schema", "embedding") and `target_label` identifies the sync target.
    pub(crate) async fn invoke_reloader(
        &self,
        reloader_slot: &Mutex<Option<ReloadCallback>>,
        kind: &str,
        target_label: &str,
    ) {
        if let Some(reloader) = reloader_slot.lock().await.as_ref() {
            match reloader().await {
                Ok(count) if count > 0 => {
                    tracing::info!(
                        "{kind} reloader added {count} item(s) after sync from '{target_label}'"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("failed to reload {kind}s after sync: {e}");
                }
            }
        }
    }
}
