use super::error::{SyncError, SyncResult};
use super::org_sync::{storage_prefix_for_key, strip_storage_prefix};
use crate::atom::molecule_key_codec::{order_entry_key, MOC_PREFIX, MORD_PREFIX};
use crate::crypto::CryptoProvider;
use crate::kind_partition::{colon_prefix_matches, form_twin, rest_of, rewrite_key_like};
use crate::storage::traits::NamespacedStore;
use crate::sync::policy;
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

mod order_baseline;
mod reports;
mod restore_scan;

pub use reports::{ScrubReport, SnapshotRestoreReport, SnapshotScrubReport, UndecryptableRow};

use order_baseline::*;
use restore_scan::*;

/// How values in a [`Snapshot`] are encoded under the outer cloud seal.
///
/// Mini personal at-rest uses the same account E2E content key as cloud
/// (`design-portable-same-key-at-rest-cloud`). When that invariant holds,
/// checkpoints can carry on-disk `ENC:…` envelopes without decrypting to
/// logical plaintext (cheaper heal / backup).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotValueEncoding {
    /// Values are logical plaintext (post at-rest decrypt). Historical default.
    /// Restore through the encrypting store so puts re-seal under at-rest.
    #[default]
    Logical,
    /// Values are on-disk at-rest envelopes (`ENC:<base64(ct)>` or raw sealed
    /// bytes as stored). Create/restore use the **raw** store so we never
    /// decrypt-all or double-encrypt. Requires same-key Mini content-key at-rest.
    AtRestEnc,
}

/// A serialized snapshot of the entire database.
///
/// The snapshot format is backend-agnostic: a list of namespaces,
/// each containing a list of key-value pairs. Keys and values are
/// base64-encoded bytes.
///
/// For streaming, the snapshot is written namespace-by-namespace to
/// a temp file, then encrypted and uploaded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Format version for forward compatibility.
    pub version: u32,
    /// Timestamp when the snapshot was created (millis since epoch).
    pub created_at_ms: u64,
    /// Device ID that created this snapshot.
    pub device_id: String,
    /// The log sequence number this snapshot covers up to (inclusive).
    pub last_seq: u64,
    /// How namespace entry values are encoded. Omitted on pre-2026-07-18
    /// cloud objects → deserializes as [`SnapshotValueEncoding::Logical`].
    #[serde(default)]
    pub value_encoding: SnapshotValueEncoding,
    /// All namespaces and their key-value pairs.
    pub namespaces: Vec<NamespaceData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceData {
    pub name: String,
    /// Key-value pairs, both base64-encoded.
    pub entries: Vec<SnapshotEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub key: String,
    pub value: String,
}

const SNAPSHOT_VERSION: u32 = 1;
const HASH_SIZE: usize = 32;
// Large enough for the current photograph namespaces while still bounding the
// decoded key/value staging allocation. One namespace batch lets LastStore
// group every destination shard once instead of revisiting it per small chunk.
const RESTORE_BATCH_SIZE: usize = 262_144;
const RESTORE_NAMESPACE_CONCURRENCY: usize = 4;

enum SnapshotNamespaceMode<'a> {
    All,
    Reporting(&'a mut SnapshotScrubReport),
    Scoped(&'a [String]),
}

impl Snapshot {
    /// Create a snapshot from a NamespacedStore by iterating all namespaces and keys.
    ///
    /// This loads one namespace at a time to limit memory usage.
    pub async fn create(
        store: &dyn NamespacedStore,
        device_id: &str,
        last_seq: u64,
    ) -> SyncResult<Self> {
        let namespaces = prepare_logical_photograph(
            collect_snapshot_namespaces(store, SnapshotNamespaceMode::All).await?,
            true,
        )?;
        Ok(snapshot_from_namespaces(
            device_id,
            last_seq,
            namespaces,
            SnapshotValueEncoding::Logical,
        ))
    }

    /// Like [`create`](Self::create) but **non-aborting**: rows whose at-rest
    /// value cannot be decrypted are skipped and reported in the returned
    /// [`SnapshotScrubReport`] instead of failing the whole snapshot at the
    /// first bad row.
    ///
    /// This is what lets a backup complete on a store holding a few local
    /// at-rest "poison" rows (unrecoverable data written by a wrong-crypto-state
    /// process) rather than dying on the first one: the snapshot omits only the
    /// unreadable rows — which carry no recoverable data — and the caller logs
    /// the skipped count loudly. Every readable row is still captured exactly.
    pub async fn create_reporting(
        store: &dyn NamespacedStore,
        device_id: &str,
        last_seq: u64,
    ) -> SyncResult<(Self, SnapshotScrubReport)> {
        let mut report = SnapshotScrubReport::default();
        let namespaces = prepare_logical_photograph(
            collect_snapshot_namespaces(store, SnapshotNamespaceMode::Reporting(&mut report))
                .await?,
            true,
        )?;
        Ok((
            snapshot_from_namespaces(
                device_id,
                last_seq,
                namespaces,
                SnapshotValueEncoding::Logical,
            ),
            report,
        ))
    }

    /// Capture a same-key **ciphertext pass-through** checkpoint from a **raw**
    /// store (Sled / base layer), without decrypting values.
    ///
    /// Values are stored as base64 of the on-disk bytes (`ENC:…` under Mini
    /// content-key at-rest). Outer cloud seal still uses the account E2E key.
    ///
    /// Prefer this for `backup_snapshot` / overflow heal on Mini so we do not
    /// decrypt-all → re-JSON logical → re-encrypt-all.
    ///
    /// Restore with [`Self::restore`] against the **same raw store** (not the
    /// encrypting wrapper), or double-`ENC:` corruption results.
    pub async fn create_at_rest_passthrough(
        raw_store: &dyn NamespacedStore,
        device_id: &str,
        last_seq: u64,
    ) -> SyncResult<Self> {
        let namespaces = collect_snapshot_namespaces(raw_store, SnapshotNamespaceMode::All).await?;
        Ok(snapshot_from_namespaces(
            device_id,
            last_seq,
            namespaces,
            SnapshotValueEncoding::AtRestEnc,
        ))
    }

    /// True when values are on-disk at-rest envelopes (pass-through).
    pub fn is_at_rest_enc(&self) -> bool {
        self.value_encoding == SnapshotValueEncoding::AtRestEnc
    }

    /// Create a snapshot containing only entries for one non-personal sync target.
    ///
    /// Org/share targets are represented in Sled by key prefixes. The snapshot
    /// keeps every namespace present in the source store, even if that target
    /// currently has zero entries in it, so a scoped restore can clear stale
    /// target keys from those namespaces without touching unrelated data.
    pub async fn create_scoped(
        store: &dyn NamespacedStore,
        device_id: &str,
        last_seq: u64,
        target_prefix: &str,
    ) -> SyncResult<Self> {
        Self::create_scoped_to_prefixes(store, device_id, last_seq, &[target_prefix.to_string()])
            .await
    }

    /// Create a snapshot containing entries for multiple local prefixes that
    /// share one remote target head.
    pub async fn create_scoped_to_prefixes(
        store: &dyn NamespacedStore,
        device_id: &str,
        last_seq: u64,
        target_prefixes: &[String],
    ) -> SyncResult<Self> {
        let namespaces = prepare_logical_photograph(
            collect_snapshot_namespaces(store, SnapshotNamespaceMode::Scoped(target_prefixes))
                .await?,
            true,
        )?;
        Ok(snapshot_from_namespaces(
            device_id,
            last_seq,
            namespaces,
            SnapshotValueEncoding::Logical,
        ))
    }

    /// Serialize, hash, and encrypt the snapshot.
    ///
    /// Returns the encrypted bytes (hash + ciphertext).
    pub async fn seal(&self, crypto: &Arc<dyn CryptoProvider>) -> SyncResult<Vec<u8>> {
        let json = serde_json::to_vec(self).map_err(|e| SyncError::Serialization(e.to_string()))?;

        // Hash the plaintext for integrity verification
        let mut hasher = Sha256::new();
        hasher.update(&json);
        let hash: [u8; 32] = hasher.finalize().into();

        // Drop `json` as soon as plaintext is built so peak is not
        // json + plaintext + ciphertext all at once longer than needed.
        let mut plaintext = Vec::with_capacity(HASH_SIZE + json.len());
        plaintext.extend_from_slice(&hash);
        plaintext.extend_from_slice(&json);
        drop(json);

        let ciphertext = crypto.encrypt(&plaintext).await?;
        drop(plaintext);
        Ok(ciphertext)
    }

    /// Seal this snapshot and write the ciphertext to `path` (atomic write:
    /// temp sibling + rename). Caller dual-uploads from disk so we never hold
    /// two full ciphertext buffers for `{seq}.enc` + `latest.enc`.
    ///
    /// Returns the byte length of the sealed object.
    pub async fn seal_to_path(
        &self,
        crypto: &Arc<dyn CryptoProvider>,
        path: &std::path::Path,
    ) -> SyncResult<u64> {
        let sealed = self.seal(crypto).await?;
        let len = sealed.len() as u64;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = path.with_extension("enc.tmp");
        tokio::fs::write(&tmp, &sealed).await?;
        drop(sealed);
        tokio::fs::rename(&tmp, path).await?;
        Ok(len)
    }

    /// Decrypt and deserialize a snapshot.
    pub async fn unseal(data: &[u8], crypto: &Arc<dyn CryptoProvider>) -> SyncResult<Self> {
        let plaintext = crypto
            .decrypt(data)
            .await
            .map_err(|_| SyncError::WrongKey)?;

        if plaintext.len() < HASH_SIZE {
            return Err(SyncError::Crypto("snapshot too short for hash".to_string()));
        }

        let (stored_hash, json_bytes) = plaintext.split_at(HASH_SIZE);

        let mut hasher = Sha256::new();
        hasher.update(json_bytes);
        let computed_hash: [u8; 32] = hasher.finalize().into();

        if stored_hash != computed_hash.as_slice() {
            return Err(SyncError::Crypto(
                "snapshot hash mismatch — data corrupted".to_string(),
            ));
        }

        let snapshot: Self = serde_json::from_slice(json_bytes)?;

        if snapshot.version != SNAPSHOT_VERSION {
            return Err(SyncError::Crypto(format!(
                "unsupported snapshot version: {} (expected {})",
                snapshot.version, SNAPSHOT_VERSION
            )));
        }

        Ok(snapshot)
    }

    /// Restore a snapshot into a NamespacedStore.
    ///
    /// Personal bootstrap restore replaces **non-org** keys from the snapshot
    /// and leaves local org-scoped rows alone:
    /// - Clear phase deletes only non-org keys (never mass-deletes org rows).
    /// - Write phase still **skips** org-scoped keys from the snapshot payload
    ///   with a loud count — personal snapshots must not rehydrate org-E2E
    ///   ciphertext under the personal single-provider seam (incident
    ///   2026-07-13).
    ///
    /// Org-scoped storage keys: `{storage_prefix}:…`, `emb:{storage_prefix}:…`,
    /// `graveyard:emb:{storage_prefix}:…` (see [`is_org_scoped_key_bytes`]).
    ///
    /// **Encoding:**
    /// - [`SnapshotValueEncoding::Logical`] — pass the **encrypting** store so
    ///   values are re-sealed under local at-rest crypto.
    /// - [`SnapshotValueEncoding::AtRestEnc`] — pass the **raw** store so
    ///   `ENC:…` envelopes are written as-is (no double-encrypt).
    pub async fn restore(&self, store: &dyn NamespacedStore) -> SyncResult<SnapshotRestoreReport> {
        let normalized;
        let namespaces = if self.value_encoding == SnapshotValueEncoding::Logical {
            normalized = prepare_logical_photograph(self.namespaces.clone(), false)?;
            normalized.as_slice()
        } else {
            self.validate_unique_restore_keys()?;
            self.namespaces.as_slice()
        };
        let materialize_started = Instant::now();
        let mut org_skipped = 0usize;
        let mut entries = 0usize;
        let mut batches = 0usize;
        let results = stream::iter(
            namespaces
                .iter()
                .map(|namespace| restore_namespace(store, namespace, RestoreMode::Personal)),
        )
        .buffer_unordered(RESTORE_NAMESPACE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        for result in results {
            let result = result?;
            entries = entries.saturating_add(result.entries);
            batches = batches.saturating_add(result.batches);
            org_skipped = org_skipped.saturating_add(result.org_skipped);
        }

        let materialize_ms = elapsed_ms(materialize_started);
        let flush_started = Instant::now();
        store.restore_durability_barrier().await?;
        let final_flush_ms = elapsed_ms(flush_started);

        if org_skipped > 0 {
            tracing::warn!(
                target: "fold_db::sync::snapshot",
                skipped = org_skipped,
                "snapshot restore SKIPPED {org_skipped} org-scoped storage key(s)                  (consented drop after org-crypto strip)"
            );
        }

        Ok(SnapshotRestoreReport {
            namespaces: namespaces.len(),
            entries,
            batches,
            materialize_ms,
            final_flush_ms,
        })
    }

    /// Restore a scoped target snapshot without clearing unrelated data.
    pub async fn restore_scoped(
        &self,
        store: &dyn NamespacedStore,
        target_prefix: &str,
    ) -> SyncResult<SnapshotRestoreReport> {
        self.restore_scoped_to_prefixes(store, &[target_prefix.to_string()])
            .await
    }

    /// Restore a scoped target snapshot for multiple local storage prefixes.
    pub async fn restore_scoped_to_prefixes(
        &self,
        store: &dyn NamespacedStore,
        target_prefixes: &[String],
    ) -> SyncResult<SnapshotRestoreReport> {
        let normalized;
        let namespaces = if self.value_encoding == SnapshotValueEncoding::Logical {
            normalized = prepare_logical_photograph(self.namespaces.clone(), false)?;
            normalized.as_slice()
        } else {
            self.validate_unique_restore_keys()?;
            self.namespaces.as_slice()
        };
        let materialize_started = Instant::now();
        let mut entries = 0usize;
        let mut batches = 0usize;
        let results = stream::iter(namespaces.iter().map(|namespace| {
            restore_namespace(store, namespace, RestoreMode::Scoped(target_prefixes))
        }))
        .buffer_unordered(RESTORE_NAMESPACE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        for result in results {
            let result = result?;
            entries = entries.saturating_add(result.entries);
            batches = batches.saturating_add(result.batches);
        }

        let materialize_ms = elapsed_ms(materialize_started);
        let flush_started = Instant::now();
        store.restore_durability_barrier().await?;
        let final_flush_ms = elapsed_ms(flush_started);

        Ok(SnapshotRestoreReport {
            namespaces: namespaces.len(),
            entries,
            batches,
            materialize_ms,
            final_flush_ms,
        })
    }

    fn validate_unique_restore_keys(&self) -> SyncResult<()> {
        let mut namespaces = HashSet::with_capacity(self.namespaces.len());
        for namespace in &self.namespaces {
            if !namespaces.insert(namespace.name.as_str()) {
                return Err(SyncError::Serialization(format!(
                    "snapshot contains duplicate namespace '{}'",
                    namespace.name
                )));
            }
            let mut keys = HashSet::with_capacity(namespace.entries.len());
            for entry in &namespace.entries {
                if !keys.insert(entry.key.as_str()) {
                    return Err(SyncError::Serialization(format!(
                        "snapshot namespace '{}' contains a duplicate key",
                        namespace.name
                    )));
                }
            }
        }
        Ok(())
    }
}

fn snapshot_from_namespaces(
    device_id: &str,
    last_seq: u64,
    namespaces: Vec<NamespaceData>,
    value_encoding: SnapshotValueEncoding,
) -> Snapshot {
    let created_at_ms = crate::clock::unix_millis();

    Snapshot {
        version: SNAPSHOT_VERSION,
        created_at_ms,
        device_id: device_id.to_string(),
        last_seq,
        value_encoding,
        namespaces,
    }
}

pub(crate) fn snapshot_should_skip_namespace(name: &str) -> bool {
    policy::snapshot_should_skip_namespace(name)
}
