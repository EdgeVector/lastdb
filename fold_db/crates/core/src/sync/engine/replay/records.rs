//! Per-key molecule record and header replay paths.

use super::super::helpers::*;
use super::super::SyncEngine;
use crate::atom::MergeConflict;
use crate::crypto::CryptoProvider;
use crate::sync::error::{SyncError, SyncResult};
use std::sync::Arc;

/// The replayed `mk:` key, its at-rest value and the molecule it belongs to.
#[derive(Clone, Copy)]
struct ReplayedKey<'a> {
    key_bytes: &'a [u8],
    value_bytes: &'a [u8],
    mol_uuid: &'a str,
}

impl SyncEngine {
    /// Merge one incoming per-key molecule record (`mk:{M}:{key}`) into the
    /// local store with last-writer-wins on `AtomEntry.written_at`. Records a
    /// `MergeConflict` when the incoming record both wins and displaces a
    /// different `atom_uuid` — the same condition (and the same
    /// winner-is-peer / loser-is-local shape) the in-memory molecule merge
    /// uses, so the conflict audit is byte-for-byte identical to the old
    /// blob-level path.
    pub(super) async fn replay_per_key_record(
        &self,
        namespace: &str,
        key_bytes: &[u8],
        value_bytes: &[u8],
        mol_uuid: &str,
        crypto: &dyn CryptoProvider,
    ) -> SyncResult<()> {
        use crate::db_operations::atom_store::PerKeyRecord;

        let kv = self.store.open_namespace(namespace).await?;
        // `value_bytes` is the at-rest `ENC:` envelope as recorded by the sync
        // log (Syncing sits below the encrypting seam). Decode the record by
        // decrypting first; accepted values are re-sealed for the final local
        // storage key before they touch the raw store.
        let incoming: PerKeyRecord =
            Self::decode_incoming(namespace, crypto, self.enc_key.as_ref(), value_bytes).await?;

        let _tip_guards = self.lock_replay_tip_guards(key_bytes).await;

        // A deleted tip has no local `mk:` row to compare against. Keep the
        // delete's origin order in a separate durable row so a delayed older
        // Put cannot revive it after a restart or an out-of-order cloud list.
        let marker = self
            .read_replay_delete_barrier(&kv, namespace, key_bytes, mol_uuid, crypto)
            .await?;
        if marker
            .as_ref()
            .is_some_and(|marker| marker.blocks_tip(&incoming.entry))
        {
            // A physical tip row does not carry the source atom or enough
            // mutation data to preserve a losing Put as durable history.
            // Keep the cloud cursor before this entry until a recovery path
            // can retain that history.
            return Err(SyncError::Storage(
                "legacy physical mk Put blocked by Delete; durable history is unavailable".into(),
            ));
        }

        // The local value is likewise the at-rest envelope (read straight off the
        // raw store). Decrypt before decoding so the LWW comparison sees the real
        // `written_at` / `atom_uuid` instead of silently treating it as absent.
        let local: Option<PerKeyRecord> = Self::decode_local(
            namespace,
            crypto,
            self.enc_key.as_ref(),
            kv.get(key_bytes).await?,
        )
        .await?;

        let accepted_incoming = self
            .merge_per_key_lww(
                &kv,
                namespace,
                ReplayedKey {
                    key_bytes,
                    value_bytes,
                    mol_uuid,
                },
                crypto,
                &incoming,
                local,
            )
            .await?;
        if accepted_incoming {
            Self::invalidate_hash_range_page_index_if_needed(&kv, key_bytes, mol_uuid).await?;
        }
        // C4: absorb only when remote actually applied (local-win stays dirty).
        self.absorb_put_or_warn(
            namespace,
            key_bytes,
            value_bytes,
            accepted_incoming,
            "per-key record",
        )
        .await;
        Ok(())
    }

    /// Hold the tip commit and publication locks for one replayed key, when
    /// the automatic-GC atom store is installed and the key is valid UTF-8.
    async fn lock_replay_tip_guards(&self, key_bytes: &[u8]) -> Option<(impl Sized, impl Sized)> {
        let atoms = self.atom_store.lock().await.clone()?;
        let key = std::str::from_utf8(key_bytes).ok()?;
        let keys = [key.to_string()];
        let durable = atoms.lock_tip_commits(&keys).await;
        let publication = atoms.lock_tip_publications(&keys).await;
        Some((durable, publication))
    }

    /// Apply the LWW decision for one per-key record. Returns whether the
    /// incoming value was written.
    async fn merge_per_key_lww(
        &self,
        kv: &Arc<dyn crate::storage::traits::KvStore>,
        namespace: &str,
        entry: ReplayedKey<'_>,
        crypto: &dyn CryptoProvider,
        incoming: &crate::db_operations::atom_store::PerKeyRecord,
        local: Option<crate::db_operations::atom_store::PerKeyRecord>,
    ) -> SyncResult<bool> {
        let ReplayedKey {
            key_bytes,
            value_bytes,
            mol_uuid,
        } = entry;
        let mut accepted_incoming = false;
        match local {
            None => {
                // No local record — accept the incoming one.
                let stored = self
                    .stored_replay_value(namespace, key_bytes, crypto, value_bytes)
                    .await?;
                kv.put(key_bytes, stored).await?;
                accepted_incoming = true;
            }
            Some(local) => {
                // LWW total order: (written_at, logical_counter,
                // device_id|writer_pubkey, mutation_uuid, atom_uuid). The
                // original device write time leads; the author counter breaks ties.
                // Dual-read accepts fat or thin tip values from peers.
                if local.entry.atom_uuid == incoming.entry.atom_uuid {
                    if crate::atom::incoming_wins_lww(
                        incoming.entry.lww_key(),
                        local.entry.lww_key(),
                    ) {
                        let stored = self
                            .stored_replay_value(namespace, key_bytes, crypto, value_bytes)
                            .await?;
                        kv.put(key_bytes, stored).await?;
                        accepted_incoming = true;
                    }
                } else if crate::atom::incoming_wins_lww(
                    incoming.entry.lww_key(),
                    local.entry.lww_key(),
                ) {
                    // The conflict rows reach disk before the tip. A crash
                    // between the two leaves the old tip, and a retry
                    // records the same keys again.
                    self.record_replaced_tip(kv, key_bytes, mol_uuid, incoming, &local)
                        .await?;
                    let stored = self
                        .stored_replay_value(namespace, key_bytes, crypto, value_bytes)
                        .await?;
                    kv.put(key_bytes, stored).await?;
                    accepted_incoming = true;
                }
            }
        }
        Ok(accepted_incoming)
    }

    /// Record the conflict of a legacy `mk:` tip that replaces a different
    /// atom, before the caller writes the tip. The incoming row is from a
    /// peer by construction, so no peer test applies. The recorder skips the
    /// pair only when both tips name one device.
    async fn record_replaced_tip(
        &self,
        kv: &Arc<dyn crate::storage::traits::KvStore>,
        key_bytes: &[u8],
        mol_uuid: &str,
        incoming: &crate::db_operations::atom_store::PerKeyRecord,
        local: &crate::db_operations::atom_store::PerKeyRecord,
    ) -> SyncResult<()> {
        let field_key = Self::field_key_for_record_key(key_bytes, mol_uuid);
        let storage_prefix = std::str::from_utf8(key_bytes)
            .ok()
            .and_then(|key| storage_scope_for_key_marker(key, "mk:"));
        let atoms = self.atom_store.lock().await.clone();
        let Some(atoms) = atoms else {
            // No atom store is attached: keep the raw v1 rows, written first.
            let conflict = MergeConflict {
                key: MergeConflict::display_key(&field_key),
                field_key,
                winner_atom: incoming.entry.atom_uuid.clone(),
                loser_atom: local.entry.atom_uuid.clone(),
                winner_written_at: incoming.entry.written_at,
                loser_written_at: local.entry.written_at,
            };
            return Self::store_merge_conflicts(kv, mol_uuid, storage_prefix, &[conflict]).await;
        };
        let conflict = crate::db_operations::atom_store::TipConflict {
            molecule_uuid: mol_uuid.to_string(),
            storage_hash: field_key.hash.clone().unwrap_or_default(),
            storage_range: field_key.range.clone().unwrap_or_default(),
            field_key,
            kind: crate::atom::TipConflictKind::Replaced,
            winner: incoming.entry.clone(),
            loser: local.entry.clone(),
        };
        // Boxed: the replay future is already deep in the node's async graph.
        Box::pin(atoms.record_tip_conflicts(vec![conflict], storage_prefix))
            .await
            .map(|_| ())
            .map_err(|error| SyncError::Storage(error.to_string()))
    }

    /// Merge an incoming molecule header (`mh:{M}`) field-wise: `version = max`,
    /// `updated_at = max`. The header's mere presence is the "migrated to
    /// per-key layout" marker, so a missing local header is replaced outright.
    pub(super) async fn replay_header_record(
        &self,
        namespace: &str,
        key_bytes: &[u8],
        value_bytes: &[u8],
        crypto: &dyn CryptoProvider,
    ) -> SyncResult<()> {
        use crate::db_operations::atom_store::MoleculeHeader;

        let kv = self.store.open_namespace(namespace).await?;
        let incoming: MoleculeHeader =
            Self::decode_incoming(namespace, crypto, self.enc_key.as_ref(), value_bytes).await?;

        let local: Option<MoleculeHeader> = Self::decode_local(
            namespace,
            crypto,
            self.enc_key.as_ref(),
            kv.get(key_bytes).await?,
        )
        .await?;

        let merged = match local {
            None => incoming,
            Some(local) => MoleculeHeader {
                version: local.version.max(incoming.version),
                updated_at: local.updated_at.max(incoming.updated_at),
            },
        };
        let bytes = serde_json::to_vec(&merged)?;
        let stored = self
            .stored_replay_value(namespace, key_bytes, crypto, &bytes)
            .await?;
        kv.put(key_bytes, stored).await?;
        self.absorb_put_or_warn(namespace, key_bytes, &bytes, true, "molecule header")
            .await;
        Ok(())
    }

    /// Build the typed `FieldKey` for a per-key conflict by decoding the
    /// unified `mk:{M}:{esc(hash)}\0{range}` record key. Empty components map
    /// to `Option::None` for hash-only / range-only / Single slots.
    pub(super) fn field_key_for_record_key(
        key_bytes: &[u8],
        mol_uuid: &str,
    ) -> crate::atom::FieldKey {
        use crate::atom::molecule_key_codec;
        use crate::atom::FieldKey;

        let key_str = std::str::from_utf8(key_bytes).unwrap_or("");
        // The record prefix may be org-prefixed; locate `mk:{M}:` within it.
        let record_prefix = molecule_key_codec::molecule_record_prefix(mol_uuid);
        let suffix = match key_str.find(&record_prefix) {
            Some(i) => &key_str[i + record_prefix.len()..],
            None => key_str,
        };

        if let Some((hash, range)) = molecule_key_codec::decode_hash_range_suffix(suffix) {
            return FieldKey {
                hash: if hash.is_empty() { None } else { Some(hash) },
                range: if range.is_empty() { None } else { Some(range) },
            };
        }

        // Unrecognized key shape — best-effort display key.
        FieldKey::hash(suffix.to_string())
    }

    pub(super) async fn invalidate_hash_range_page_index_if_needed(
        kv: &Arc<dyn crate::storage::traits::KvStore>,
        key_bytes: &[u8],
        mol_uuid: &str,
    ) -> SyncResult<()> {
        let Ok(key_str) = std::str::from_utf8(key_bytes) else {
            return Ok(());
        };
        let Some(index_key) = hash_range_page_index_complete_key_for_record_key(key_str, mol_uuid)
        else {
            return Ok(());
        };
        kv.delete(index_key.as_bytes()).await?;
        Ok(())
    }
}
