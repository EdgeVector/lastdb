use super::*;

impl AtomStore {
    /// Load all mutation events for a molecule, sorted chronologically.
    ///
    /// When `storage_prefix` is `Some`, the scan prefix is `{storage_prefix}:history:{mol}:`.
    /// Exact prefix only (no bare dual-read).
    ///
    /// Prefer the tip-version chain (`prev_tip_id` / `tv:`) for new `as_of`
    /// work; this remains for legacy `history:` rows.
    pub async fn get_mutation_events(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<MutationEvent>, SchemaError> {
        Ok(self
            .get_mutation_event_rows(molecule_uuid, storage_prefix)
            .await?
            .into_iter()
            .map(|(_, event)| event)
            .collect())
    }

    /// Load mutation history with each authoritative storage key intact.
    pub async fn get_mutation_event_rows(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, MutationEvent)>, SchemaError> {
        let base_prefix = molecule_key_codec::history_molecule_prefix(molecule_uuid);

        // This scan cannot be pruned to a partition — the key is `:`-separated
        // and carries no `PARTITION_SEP` — so LastStore enumerates every group
        // in the collection, and proving the prefix EMPTY costs exactly as much
        // as finding rows. The purge planner runs it once per field inside the
        // schema's exclusive write barrier, which is how a purge that deletes
        // nothing still held that barrier for a minute. Emptiness is monotone
        // within a process and both writers invalidate, so answer from the memo
        // when we have already proven this prefix empty.
        // See `crate::atom::legacy_history_memo`.
        let scan_key = build_storage_key(storage_prefix, &base_prefix);
        if crate::atom::legacy_history_memo::is_known_empty(self.store_id(), &scan_key) {
            return Ok(Vec::new());
        }

        crate::atom::legacy_history_memo::record_scan();
        let items: Vec<(String, MutationEvent)> = self
            .scan_exact_storage_prefix(
                &base_prefix,
                storage_prefix,
                "Failed to load mutation events",
            )
            .await?;

        // Only emptiness is cached. A prefix that HAS rows takes the scan every
        // time, so a legacy store behaves exactly as it does today and rows
        // written later are always seen.
        if items.is_empty() {
            crate::atom::legacy_history_memo::mark_empty(self.store_id(), &scan_key);
        }

        // Rows are already in lexicographic order, which is chronological for
        // the zero-padded history key.
        Ok(items)
    }

    /// Load one archived tip version (`tv:{version_id}`).
    pub async fn get_tip_version(
        &self,
        version_id: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<crate::atom::AtomEntry>, SchemaError> {
        use crate::atom::molecule_key_codec;
        use crate::schema::types::field::build_storage_key;

        if version_id.is_empty() {
            return Ok(None);
        }
        let key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::tip_version_key(version_id),
        );
        let raw = self
            .main_store
            .get_item::<crate::atom::AtomEntry>(&key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("load tip version {version_id}: {e}")))?;
        Ok(raw)
    }

    /// Walk a tip-version chain from `head` until `written_at <= as_of_nanos`.
    /// Returns `None` if the slot did not exist yet at that time (chain ends
    /// while still after `as_of`). Caps walk length to avoid cycles.
    pub async fn tip_entry_at_as_of(
        &self,
        head: &crate::atom::AtomEntry,
        as_of_nanos: u64,
        storage_prefix: Option<&str>,
    ) -> Result<Option<crate::atom::AtomEntry>, SchemaError> {
        const MAX_WALK: usize = 1_000_000;
        let mut cur = head.clone();
        for _ in 0..MAX_WALK {
            if cur.written_at <= as_of_nanos {
                return Ok(Some(cur));
            }
            if cur.prev_tip_id.is_empty() {
                return Ok(None);
            }
            match self
                .get_tip_version(&cur.prev_tip_id, storage_prefix)
                .await?
            {
                Some(prev) => cur = prev,
                // Missing node (legacy depth-1 atom-id prev, or GC): stop.
                None => return Ok(None),
            }
        }
        Err(SchemaError::InvalidData(
            "tip version chain exceeded max walk length".into(),
        ))
    }
}
