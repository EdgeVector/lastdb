//! HashRange order record replay paths.

use super::super::SyncEngine;
use crate::atom::molecule_key_codec::order_count_key;
use crate::crypto::CryptoProvider;
use crate::schema::types::key_value::KeyValue;
use crate::sync::error::SyncResult;

/// Split `…mord:{M}:{seq}` (or `…mord\0{M}:{seq}`) into its `…moc:{M}` sibling
/// and the entry's `seq`.
///
/// The scope (`{org}:`, or empty for personal) is carried across verbatim so the
/// count lands at the same storage prefix as the entry it covers.
fn count_key_and_seq_for_entry_key(key: &str) -> Option<(String, usize)> {
    let rest = crate::kind_partition::rest_of(key, "mord")?;
    if rest.contains('\0') {
        return None;
    }
    let (molecule, seq) = rest.rsplit_once(':')?;
    if molecule.is_empty() {
        return None;
    }
    let kind_at = key.find("mord\0").or_else(|| key.find("mord:"))?;
    let scope = &key[..kind_at];
    Some((
        format!("{scope}{}", order_count_key(molecule)),
        seq.parse().ok()?,
    ))
}

impl SyncEngine {
    /// Merge one incoming HashRange append-log order entry (`mord:{M}:{seq}`).
    ///
    /// Each entry is **append-only and immutable**: a given `(hash, range)`
    /// `KeyValue` is written once at its seq and never reordered or rewritten
    /// (see `MoleculeHashRange::set_atom_uuid*`). So the merge is keep-if-absent
    /// for a **valid** local value — the first peer to fill a seq slot with a
    /// decodeable `KeyValue` wins it, and any re-receipt (or a divergent peer's
    /// entry for the same slot) is dropped. That rule is order-independent (the
    /// slot's contents don't depend on arrival order once filled), matching the
    /// "deterministic given the same inputs" guarantee the append-log contract
    /// has. `SampleN` is a sampling query whose cross-peer order need not be
    /// byte-for-byte convergent.
    ///
    /// **Presence alone is not enough to keep the local slot.** A local value
    /// that is present but undecryptable / unparseable as `KeyValue` is *not*
    /// a filled slot — it is poison that would hard-error every later
    /// `load_update_order` read. In that case the peer value is written
    /// (repair), same as when the key is genuinely absent. Only a successfully
    /// decoded local `KeyValue` skips the put (true keep-if-absent).
    ///
    /// **The entry's `moc:{M}` count is raised to cover it.** `moc:` is not a
    /// hint the reader can second-guess: it *describes the key set*, because
    /// `load_update_order` derives the keys it fetches from the count rather than
    /// discovering them with a prefix scan (`mord:{M}:` has no partition
    /// separator, so a scan sweeps every hash group in the collection). Replay
    /// writes entries and counts as INDEPENDENT records with no ordering
    /// guarantee between them, so an entry that arrived before — or without — its
    /// peer's count record would otherwise sit at a seq the count does not cover
    /// and be invisible to every subsequent read. Raising the count here keeps
    /// "the count covers every persisted entry" true for every writer.
    ///
    /// This also repairs a pre-existing loss: `load_update_order` has always
    /// truncated to `moc:`, so entries replayed past a stale local count were
    /// already being dropped until the next local write happened to restamp it.
    ///
    /// **The payload is shape-checked before it is persisted.** `mord:` used to
    /// be the one merge path that wrote incoming bytes opaquely, from back when
    /// a bad entry was merely invisible: the read path discovered entries with a
    /// `mord:{M}:` prefix scan and simply skipped anything that would not parse.
    /// That scan is gone (it swept every hash group in the collection), so
    /// `load_update_order` now addresses `mord:{M}:{seq}` directly and
    /// `get_item::<KeyValue>` answers un-parseable bytes with
    /// `SerializationError`, not `Ok(None)` — one corrupt entry hard-errors
    /// every future read of that molecule's update order. So a payload that
    /// does not decode as [`KeyValue`] is classified as poison and skipped like
    /// every other malformed replay value, rather than written and left to
    /// break reads. The count is not raised either: covering a seq we refused
    /// to persist is exactly the "count claims more entries than the log holds"
    /// state the read path warns about.
    pub(super) async fn replay_order_log_entry(
        &self,
        namespace: &str,
        key_bytes: &[u8],
        value_bytes: &[u8],
        crypto: &dyn CryptoProvider,
    ) -> SyncResult<()> {
        Self::decode_incoming::<KeyValue>(namespace, crypto, self.enc_key.as_ref(), value_bytes)
            .await?;

        let kv = self.store.open_namespace(namespace).await?;
        // Keep-if-absent only when the local slot already holds a *valid*
        // KeyValue. A present-but-unreadable local (wrong at-rest key, corrupt
        // ciphertext, non-JSON / non-KeyValue plaintext) must not block peer
        // repair: presence-only keep left poison bytes in place, advanced the
        // cursor past a good peer entry, and permanently broke SampleN /
        // update-order for that molecule.
        let local_stored = kv.get(key_bytes).await?;
        // Absent (Ok(None)) and present-but-unreadable (Err) both mean the
        // slot is not a filled valid KeyValue — peer may repair.
        let local_is_valid_keyvalue = matches!(
            Self::decode_local::<KeyValue>(namespace, crypto, self.enc_key.as_ref(), local_stored,)
                .await,
            Ok(Some(_))
        );
        if !local_is_valid_keyvalue {
            let stored = self
                .stored_replay_value(namespace, key_bytes, crypto, value_bytes)
                .await?;
            kv.put(key_bytes, stored).await?;
            self.absorb_put_or_warn(namespace, key_bytes, value_bytes, true, "order entry")
                .await;
        }

        // Cover the entry with the count, whether or not this replay is the one
        // that persisted it: a re-receipt of an entry whose count was lost still
        // has to restore the invariant.
        let Some((count_key, seq)) = std::str::from_utf8(key_bytes)
            .ok()
            .and_then(count_key_and_seq_for_entry_key)
        else {
            return Ok(());
        };
        let needed = seq + 1;
        let local: usize = Self::decode_local(
            namespace,
            crypto,
            self.enc_key.as_ref(),
            kv.get(count_key.as_bytes()).await?,
        )
        .await?
        .unwrap_or(0);
        if local < needed {
            // A bare integer is its own JSON encoding, so this is exactly what
            // `serde_json::to_vec(&needed)` produces — without an infallible
            // error path to explain.
            let plaintext = needed.to_string().into_bytes();
            let stored = self
                .stored_replay_value(namespace, count_key.as_bytes(), crypto, &plaintext)
                .await?;
            kv.put(count_key.as_bytes(), stored).await?;
            self.absorb_put_or_warn(
                namespace,
                count_key.as_bytes(),
                &plaintext,
                true,
                "derived order count",
            )
            .await;
        }
        Ok(())
    }

    /// Merge one incoming HashRange append-log count record (`moc:{M}`). Keep the
    /// LARGER count (the more complete log) — a count that went backwards would
    /// hide entries, since `load_update_order` reads exactly `seq in 0..count`.
    pub(super) async fn replay_order_count_record(
        &self,
        namespace: &str,
        key_bytes: &[u8],
        value_bytes: &[u8],
        crypto: &dyn CryptoProvider,
    ) -> SyncResult<()> {
        let kv = self.store.open_namespace(namespace).await?;
        let incoming: usize =
            Self::decode_incoming(namespace, crypto, self.enc_key.as_ref(), value_bytes).await?;
        let local: usize = Self::decode_local(
            namespace,
            crypto,
            self.enc_key.as_ref(),
            kv.get(key_bytes).await?,
        )
        .await?
        .unwrap_or(0);
        if incoming >= local {
            let stored = self
                .stored_replay_value(namespace, key_bytes, crypto, value_bytes)
                .await?;
            kv.put(key_bytes, stored).await?;
            self.absorb_put_or_warn(namespace, key_bytes, value_bytes, true, "order count")
                .await;
        }
        Ok(())
    }
}
